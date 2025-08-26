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
from promise import SimpleViolation, scope_applies
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
            # 1) schedule ready tasks
            for tid, node in list(self.plan.nodes.items()):
                if node.status == "pending" and self._deps_done(tid):
                    if self._can_start_due_to_parallelism(node):
                        self._start_task(node)
                    # else: leave pending; will retry next tick

            # 2) handle control queue
            self._drain_ctrl_queue()

            # 3) exit when all tasks are terminal and no processes alive
            if self._all_terminal() and not any(p.is_alive() for p in self._pool.values()):
                self.events.write({"type": "WorkflowCompleted", "wf_root": root_id})
                break
            time.sleep(0.01)

    def _deps_done(self, tid: str) -> bool:
        preds = self.plan.predecessors(tid)
        return all(self.plan.nodes[p].status == "success" for p in preds)

    def _can_start_due_to_parallelism(self, node: TaskInstance) -> bool:
        #to do, check that number running < active_cap
        # should TaskInstance have a set of labels or just one? When would multiple be necessary?
        return True

    def _start_task(self, node: TaskInstance) -> None:
        # If reduce node with gather marker, materialize its inputs now (all preds have completed)
        if "__gather_from__" in node.inputs:
            sources: List[str] = node.inputs.pop("__gather_from__")
            node.inputs["parts"] = [self.plan.nodes[s].outputs for s in sources]
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
                "violations": [_params(v) for v in violations],
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
                    "params": _params(sc.constraint),
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

    def export_dot(self, path: str) -> None:
        #Iterates through plan.nodes and plan.edges to create graphviz visual. Change line styling based on each node’s status
        pass

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