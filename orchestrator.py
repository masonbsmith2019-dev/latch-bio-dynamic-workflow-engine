from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from pathlib import Path
from uuid import UUID
from plan import PlanDiff, Plan, TaskInstance, Status, From
from task_registry import TaskRegistry, _REGISTRY, spec_name_from_callable
from promise import SimpleViolation, MaxParallelism, scope_applies
from event_system import EventWriter
from execution_context import ExecutionContext
from graphviz_export import build_dot, write_dot_file, render_png_file

class Orchestrator:
    def __init__(self, registry: TaskRegistry, events_path: Path = Path("outputs/events.jsonl")):
        self.registry = registry
        self.plan = Plan()
        self.events = EventWriter(events_path)
        self._pool: dict[str, mp.Process] = {}
        self._ctrl_q: mp.Queue = mp.Queue()
        self._mgr = mp.Manager()
        self._status = self._mgr.dict()   # {task_id: "pending"/"running"/"success"/"failed"/"terminated"}
        self._results = self._mgr.dict()
    
    def start(self, entry: Callable, inputs: dict[str, Any], output_path: Path) -> str:
        if not callable(entry):
            raise TypeError(f"start(entry=...) must be a function, got {type(entry).__name__}")
        
        spec_name = spec_name_from_callable(entry)
        if not self.registry.exists(spec_name):
            raise ValueError(f"Entry task '{spec_name}' is not registered")
        
        self._output_path = output_path
        self._dots_created = 0

        root_id = uuid.uuid4()
        self.plan.taskInstances[root_id] = TaskInstance(id=root_id, spec_name=spec_name, inputs=inputs)
        self.events.write({"type": "NodeCreated", "id": root_id, "spec": spec_name})
        self.events.write({"type": "WorkflowStarted", "wf_root": root_id, "entry_spec": spec_name})
        return root_id
    
    def run_to_completion(self, root_id: str) -> None:
        while True:
            # schedule ready tasks
            for tid, node in list(self.plan.taskInstances.items()):
                if node.status == Status.PENDING and self._deps_done(tid):
                    if self._can_start_due_to_parallelism(node):
                        self._start_task(node)
                    # else: leave pending; will retry next tick

            self._drain_ctrl_queue()

            # exit when all tasks are terminal and no processes alive
            if self._all_terminal() and not any(p.is_alive() for p in self._pool.values()):
                self.events.write({"type": "WorkflowCompleted", "wf_root": root_id})
                break
            time.sleep(0.01)

    def _deps_done(self, tid: UUID) -> bool:
        preds = self.plan.predecessors(tid)
        return all(self.plan.taskInstances[p].status == Status.SUCCESS for p in preds)

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
        running = sum(1 for n in self.plan.taskInstances.values() if (n.status == Status.RUNNING and (label in n.labels)))
        return running < min_k
    
    def _resolve_inputs(self, obj):
        # Replace From(...) with producer outputs; recurse into dicts/lists/tuples
        if isinstance(obj, From):
            out = self.plan.taskInstances[obj.task_id].outputs
            if obj.path:
                for part in obj.path.split("."):
                    out = out[part]
            return out
        if isinstance(obj, dict):
            return {k: self._resolve_inputs(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._resolve_inputs(v) for v in obj]
        if isinstance(obj, tuple):
            return tuple(self._resolve_inputs(v) for v in obj)
        return obj

    def _start_task(self, node: TaskInstance) -> None:
        # If reduce node with gather marker, materialize its inputs now (all preds have completed)
        if "__gather_from__" in node.inputs:
            sources = node.inputs.pop("__gather_from__")
            reduce_param = node.inputs.pop("__reduce_param__", "parts")
            gathered = [self.plan.taskInstances[s].outputs for s in sources]
            node.inputs[reduce_param] = gathered

        if "__gather_from_named__" in node.inputs:
            sources_map: Dict[str, str] = node.inputs.pop("__gather_from_named__")
            for param_name, src_id in sources_map.items():
                node.inputs[param_name] = self.plan.taskInstances[src_id].outputs
            
        resolved_inputs = self._resolve_inputs(node.inputs)
        node.inputs = resolved_inputs

        node.status = Status.RUNNING
        self._status[node.id] = Status.RUNNING
        node.started_at = time.time()
        self.events.write({"type": "TaskStarted", "id": node.id, "spec": node.spec_name})
        p = mp.Process(
            target=_worker_entry, 
            args=(node.id, node.spec_name, resolved_inputs, self._ctrl_q, self._status, self._results)
        )
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
                node = self.plan.taskInstances[tid]
                node.outputs = payload.get("output")
                node.status = Status.SUCCESS
                self._status[tid] = Status.SUCCESS
                self._results[tid] = node.outputs
                node.ended_at = time.time()
                self.events.write({
                    "type": "TaskCompleted",
                    "id": tid,
                    "spec": node.spec_name,
                    "output": node.outputs,
                })
                self.export_dot()
    
    def export_dot(self, write_png: bool = True) -> tuple[str, Optional[str]]:
        base, _ = os.path.splitext(self._output_path)  # self._output_path is your base path
        dot_path = f"{base}{self._dots_created}.dot"
        png_path = os.path.splitext(dot_path)[0] + ".png" if write_png else None
        self._dots_created += 1

        dot_str = build_dot(self.plan)
        write_dot_file(dot_str, Path(dot_path))

        if write_png:
            ok = render_png_file(dot_str, Path(png_path))
            if not ok:
                self.events.write({
                    "type": "ExportWarning",
                    "warning": "Graphviz not available; PNG not written",
                    "dot_path": dot_path,
                })
                png_path = None

        return dot_path, png_path

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
            for node in self.plan.taskInstances.values():
                if node.status in (Status.PENDING, Status.RUNNING):
                    node.status = Status.TERMINATED
            #could do one loop instead, using _pool.items
            #this gives taskID, which we can use to index self.plan.taskInstance[taskID].status = terminated
            for p in self._pool.values():
                if p.is_alive():
                    p.terminate()
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
            self.plan.taskInstances[n.id] = TaskInstance(
                id=n.id,
                spec_name=n.spec_name,
                inputs=n.inputs,
                parent_id=n.parent_id,
                labels=set(n.labels),
            )
            self.events.write({"type": "NodeCreated", "id": n.id, "spec": n.spec_name, "parent_id": n.parent_id, "labels": list(n.labels)})
        for new_edge in diff.new_edges:
            self.plan.edges.add(new_edge)
            self.events.write({
                "type": "EdgeCreated",
                "from": str(new_edge.from_id),
                "to": str(new_edge.to_id),
                "from_spec": self.plan.spec_of(new_edge.from_id),
                "to_spec": self.plan.spec_of(new_edge.to_id),
            })
        self.export_dot()

    def _all_terminal(self) -> bool:
        terminal_status = lambda status : status in (Status.SUCCESS, Status.FAILED, Status.TERMINATED)
        return all(terminal_status(n.status) for n in self.plan.taskInstances.values())

def _worker_entry(
        node_id: str, 
        spec_name: str, 
        inputs: dict[str, Any], 
        ctrl_q: mp.Queue, 
        status_proxy, 
        results_proxy
    ) -> None:
    ctx = ExecutionContext(
        ctrl_q, 
        me_id=node_id, 
        status_proxy=status_proxy, 
        results_proxy=results_proxy
    )
    try:
        fn = _REGISTRY.get(spec_name).fn
        result = fn(ctx, **inputs)
        if result is not None:
            ctx.emit(result)
    except Exception as e:
        ctx.log("ERROR", error=str(e))
        ctx.emit(None)