from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from plan import PlanDiff, Plan, TaskInstance
from task_registry import TaskRegistry, _REGISTRY
from promise import SimpleViolation, MaxParallelism, scope_applies
from event_system import EventWriter
from execution_context import ExecutionContext

class Orchestrator:
    def __init__(self, registry: TaskRegistry, events_path: str = "outputs/events.jsonl"):
        self.registry = registry
        self.plan = Plan()
        self.events = EventWriter(events_path)
        self._pool: Dict[str, mp.Process] = {}
        self._ctrl_q: mp.Queue = mp.Queue()
    
    def start(self, entry: str | Callable[..., Any], inputs: Dict[str, Any]) -> str:
        spec_name = entry if isinstance(entry, str) else entry.__name__
        if not self.registry.exists(spec_name):
            raise ValueError(f"Entry task '{spec_name}' is not registered")
        root_id = str(uuid.uuid4())
        node = TaskInstance(id=root_id, spec_name=spec_name, inputs=inputs)
        self.plan.nodes[root_id] = node
        self.events.write({"type": "NodeCreated", "id": root_id, "spec": spec_name})
        self.events.write({"type": "WorkflowStarted", "wf_root": root_id, "entry_spec": spec_name})
        return root_id
    
    def run_to_completion(self, root_id: str) -> None:
        while True:
            # schedule ready tasks
            for tid, node in list(self.plan.nodes.items()):
                if node.status == "pending" and self._deps_done(tid):
                    if self._can_start_due_to_parallelism(node):
                        self._start_task(node)
                    # else: leave pending; will retry next tick

            self._drain_ctrl_queue()

            # exit when all tasks are terminal and no processes alive
            if self._all_terminal() and not any(p.is_alive() for p in self._pool.values()):
                self.events.write({"type": "WorkflowCompleted", "wf_root": root_id})
                break
            time.sleep(0.01)

    def _deps_done(self, tid: str) -> bool:
        preds = self.plan.predecessors(tid)
        return all(self.plan.nodes[p].status == "success" for p in preds)

    def _can_start_due_to_parallelism(self, node: TaskInstance) -> bool:
        #to do, check that number running < active_cap
        # for each MaxParallelism(label=k), ensure running nodes with that label < k
        active_caps: List[Tuple[str, int]] = []
        for sc in self.plan.promises:
            if isinstance(sc.constraint, MaxParallelism):
                if sc.constraint.label in node.labels:
                    active_caps.append((sc.constraint.label, sc.constraint.k))
        if not active_caps:
            return True
        min_k = min(k for _, k in active_caps)
        # assume one label per group in this POC
        label = active_caps[0][0]
        running = sum(1 for n in self.plan.nodes.values() if (n.status == "running" and (label in n.labels)))
        return running < min_k

    def _start_task(self, node: TaskInstance) -> None:
        # If reduce node with gather marker, materialize its inputs now (all preds have completed)
        if "__gather_from__" in node.inputs:
            sources: List[str] = node.inputs.pop("__gather_from__")
            node.inputs["parts"] = [self.plan.nodes[s].outputs for s in sources]

        if "__gather_from_named__" in node.inputs:
            sources_map: Dict[str, str] = node.inputs.pop("__gather_from_named__")
            for param_name, src_id in sources_map.items():
                node.inputs[param_name] = self.plan.nodes[src_id].outputs

        node.status = "running"
        node.started_at = time.time()
        self.events.write({"type": "TaskStarted", "id": node.id, "spec": node.spec_name})
        p = mp.Process(target=_worker_entry, args=(node.id, node.spec_name, node.inputs, self._ctrl_q))
        p.daemon = True
        p.start()
        self._pool[node.id] = p

    def _drain_ctrl_queue(self) -> None:
        while True:
            try:
                kind, payload = self._ctrl_q.get_nowait()
            except queue.Empty:
                return

            if kind == "plandiff":
                self._handle_plandiff(payload)
            elif kind == "log":
                self.events.write({"type": "TaskLog", **payload})
            elif kind == "emit":
                tid = payload["task_id"]
                node = self.plan.nodes[tid]
                node.outputs = payload.get("output")
                node.status = "success"
                node.ended_at = time.time()
                self.events.write({"type": "TaskCompleted", "id": tid, "spec": node.spec_name})

    def _handle_plandiff(self, diff: PlanDiff) -> None:
        violations: List[SimpleViolation] = []
        for sc in self.plan.promises:
            if scope_applies(sc.scope, diff):
                violations.extend(sc.constraint.validate(self.plan, diff))
        if violations:
            self.events.write({
                "type": "ConstraintViolation",
                "diff_emitter": diff.emitter_id,
                "violations": violations #[_params(v) for v in violations],
            })
            for node in self.plan.nodes.values():
                if node.status in ("pending", "running"):
                    node.status = "terminated"
            return

        if diff.new_promises:
            for sc in diff.new_promises:
                self.plan.promises.append(sc)
                self.events.write({
                    "type": "PromiseDeclared",
                    "owner": getattr(sc.scope, "owner_id", None),
                    "scope": sc.scope.__class__.__name__,
                    "constraint": sc.constraint.__class__.__name__,
                    "params": sc.constraint, #should i use _params here?
                })

        for n in diff.new_nodes:
            self.plan.nodes[n.id] = TaskInstance(
                id=n.id,
                spec_name=n.spec_name,
                inputs=n.inputs,
                parent_id=n.parent_id,
                labels=set(n.labels),
            )
            self.events.write({"type": "NodeCreated", "id": n.id, "spec": n.spec_name, "parent_id": n.parent_id, "labels": list(n.labels)})
        for a, b in diff.new_edges:
            self.plan.edges.add((a, b))
            self.events.write({"type": "EdgeCreated", "from": a, "to": b, "from_spec": self.plan.spec_of(a), "to_spec": self.plan.spec_of(b)})

    def export_dot(self, base_path: str, write_png: bool = True) -> tuple[str, Optional[str]]:
        os.makedirs(os.path.dirname(base_path) or ".", exist_ok=True)

        # normalize base path to a .dot path
        dot_path = base_path if base_path.endswith(".dot") else base_path + ".dot"
        png_path = os.path.splitext(dot_path)[0] + ".png" if write_png else None

        lines = ["digraph G {", "rankdir=LR;"]
        for node in self.plan.nodes.values():
            label = f"{node.spec_name}\\n{node.id[:6]}"
            style = ""
            if node.status == "running":
                style = ", shape=doublecircle"
            elif node.status == "success":
                style = ", style=filled"
            elif node.status in ("failed", "terminated"):
                style = ", color=red"
            lines.append(f'  "{node.id}" [label="{label}"{style}];')
        for a, b in self.plan.edges:
            lines.append(f'  "{a}" -> "{b}";')
        lines.append("}")
        dot_str = "\n".join(lines)

        # write .dot
        with open(dot_path, "w", encoding="utf-8") as f:
            f.write(dot_str)

        if write_png:
            rendered = False
            # try python-graphviz
            try:
                from graphviz import Source
                src = Source(dot_str, filename=os.path.splitext(os.path.basename(dot_path))[0],
                            format="png", directory=os.path.dirname(dot_path) or ".")
                src.render(cleanup=True)
                rendered = True
            except Exception as e:
                # call dot directly if available
                try:
                    import shutil, subprocess
                    dot_bin = shutil.which("dot")
                    if dot_bin:
                        subprocess.run([dot_bin, "-Tpng", dot_path, "-o", png_path], check=True)
                        rendered = True
                except Exception as e2:
                    # fall through to warning
                    pass

            if not rendered:
                self.events.write({
                    "type": "ExportWarning",
                    "warning": "Graphviz not available; PNG not written",
                    "dot_path": dot_path,
                })
                png_path = None

        return dot_path, png_path


    def _all_terminal(self) -> bool:
        return all(n.status in ("success", "failed", "terminated") for n in self.plan.nodes.values())

def _params(obj: Any) -> Dict[str, Any] | str:
        try:
            if is_dataclass(obj):
                return asdict(obj)
        except Exception:
            pass
        d = getattr(obj, "__dict__", None)
        return d if isinstance(d, dict) else {"repr": repr(obj)}

def _worker_entry(node_id: str, spec_name: str, inputs: Dict[str, Any], ctrl_q: mp.Queue) -> None:
    ctx = ExecutionContext(ctrl_q, me_id=node_id)
    try:
        fn = _REGISTRY.get(spec_name).fn
        result = fn(ctx, **inputs)
        if result is not None:
            ctx.emit(result)
    except Exception as e:
        ctx.log("ERROR", error=str(e))
        ctx.emit(None)