from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
import shutil
from typing import Any, Callable, Literal, Optional, Protocol
from pathlib import Path
from uuid import UUID
from plan import PlanDiff, Plan, TaskInstance, Status, From
from task_registry import TaskRegistry, _REGISTRY, spec_name_from_callable
from promise import (
    SimpleViolation, MaxParallelism, scope_applies,
    ScopedConstraint, OwnerDescendantsScope,
    OnlySpecificNodesAllowed, OnlySpecificEdgesAllowed,
    NoNewNodes, NoNewEdges,
)
from event_system import EventWriter
from execution_context import ExecutionContext
from graphviz_export import build_dot, write_dot_file, render_png_file

class Orchestrator:
    def __init__(self, registry: TaskRegistry, output_dir: Path):
        self.registry = registry
        self.plan = Plan()
        # Base output directory and subpaths
        self._output_dir = Path(output_dir)
        # Clean any prior contents to ensure a fresh run
        if self._output_dir.exists():
            for child in self._output_dir.iterdir():
                try:
                    if child.is_file() or child.is_symlink():
                        child.unlink(missing_ok=True)
                    else:
                        shutil.rmtree(child, ignore_errors=True)
                except Exception:
                    # Best-effort cleanup
                    pass
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._dots_dir = self._output_dir / "dots"
        self._pngs_dir = self._output_dir / "pngs"
        self._base_prefix = self._output_dir.name
        self._dots_created = 0
        # Events writer
        self.events = EventWriter(self._output_dir / "events.jsonl")
        # Runtime state
        self._pool: dict[str, mp.Process] = {}
        self._ctrl_q: mp.Queue = mp.Queue()
        self._mgr = mp.Manager()
        self._status = self._mgr.dict()   # {task_id: "pending"/"running"/"success"/"failed"/"terminated"}
        self._results = self._mgr.dict()
        self._spec_nodes: dict[str, dict] = {}           # spec_id -> {"spec": str, "parent": UUID|None, "source": "registration"|"promise"}
        self._spec_edges: set[tuple[str, str]] = set()   # (from_uuid_or_str, to_spec_id)
    
    def start(self, entry: Callable, inputs: dict[str, Any]) -> UUID:
        if not callable(entry):
            raise TypeError(f"start(entry=...) must be a function, got {type(entry).__name__}")
        
        spec_name = spec_name_from_callable(entry)
        if not self.registry.exists(spec_name):
            raise ValueError(f"Entry task '{spec_name}' is not registered")
        

        root_id = uuid.uuid4()
        self.plan.taskInstances[root_id] = TaskInstance(id=root_id, spec_name=spec_name, inputs=inputs)
        self.events.write({"type": "NodeCreated", "id": root_id, "spec": spec_name})
        self.events.write({"type": "WorkflowStarted", "wf_root": root_id, "entry_spec": spec_name})
        
        self._activate_default_promises_for(self.plan.taskInstances[root_id])
        self._seed_speculative_from_static(root_id, spec_name, depth=1)
        self.export_dot()
        return root_id
    
    def run_to_completion(self, root_id: UUID) -> None:
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
        active_caps: list[tuple[str, int]] = []
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
    
    def _mk_spec_id(self, parent: UUID | str, spec_name: str) -> str:
        # deterministic ghost id so multiple promises don't duplicate nodes."""
        return f"ghost:{parent}:{spec_name}"

    def _has_real_child(self, parent: UUID, spec: str) -> bool:
        return any(
            t.parent_id == parent and t.spec_name == spec
            for t in self.plan.taskInstances.values()
        )

    def _seed_speculative_from_static(self, parent_real_id, parent_spec: str, *, depth: int = 1) -> None:
        # registration-time speculative skeleton from @task(..., calls=[...])."""
        if depth <= 0:
            return
        if not self.registry.exists(parent_spec):
            return
        
        parent_spec_obj = self.registry.get(parent_spec)
        for child_spec in getattr(parent_spec_obj, "preview_calls", ()):
            sid = self._mk_spec_id(parent_real_id, child_spec)
            # parent is a real UUID here
            self._spec_nodes[sid] = {"spec": child_spec, "parent": parent_real_id, "source": "registration"}
            self._spec_edges.add((str(parent_real_id), sid))
    
    def _activate_default_promises_for(self, ti: TaskInstance) -> None:
        """Attach NoNewNodes/NoNewEdges automatically for static tasks."""
        try:
            spec = self.registry.get(ti.spec_name)
        except KeyError:
            return

        # Use `static` (not `is_static`) per your TaskSpec
        if not getattr(spec, "static", False):
            return

        auto = [
            ScopedConstraint(NoNewNodes(), OwnerDescendantsScope(owner_id=ti.id)),
            ScopedConstraint(NoNewEdges(), OwnerDescendantsScope(owner_id=ti.id)),
        ]

        # De-dupe: don't add identical (constraint type + owner) twice
        existing = {(type(s.constraint), getattr(s.scope, "owner_id", None))
                    for s in self.plan.promises}

        for sc in auto:
            key = (type(sc.constraint), getattr(sc.scope, "owner_id", None))
            if key in existing:
                continue
            self.plan.promises.append(sc)
            self.events.write({
                "type": "PromiseDeclared",
                "owner": getattr(sc.scope, "owner_id", None),
                "scope": sc.scope.__class__.__name__,
                "constraint": sc.constraint.__class__.__name__,
                "params": sc.constraint,
            })

    def _start_task(self, node: TaskInstance) -> None:
        # If reduce node with gather marker, materialize its inputs now (all preds have completed)
        if "__gather_from__" in node.inputs:
            sources = node.inputs.pop("__gather_from__")
            reduce_param = node.inputs.pop("__reduce_param__", "parts")
            gathered = [self.plan.taskInstances[s].outputs for s in sources]
            node.inputs[reduce_param] = gathered

        if "__gather_from_named__" in node.inputs:
            sources_map: dict[str, str] = node.inputs.pop("__gather_from_named__")
            for param_name, src_id in sources_map.items():
                node.inputs[param_name] = self.plan.taskInstances[src_id].outputs
            
        resolved_inputs = self._resolve_inputs(node.inputs)
        node.inputs = resolved_inputs

        node.status = Status.RUNNING
        self._status[node.id] = Status.RUNNING
        node.started_at = time.time()
        self.events.write({"type": "TaskStarted", "id": node.id, "spec": node.spec_name})
        self.export_dot()
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

                # Ignore duplicates if already terminal
                if node.status in (Status.SUCCESS, Status.FAILED, Status.TERMINATED):
                    continue
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
                if self._is_branching_parent(tid):
                    self._prune_all_ghost_children(tid)
                self.export_dot()
    
    def export_dot(self, write_png: bool = True) -> tuple[Path, Optional[Path]]:
        # Derive paths under output_dir/dots and output_dir/pngs with prefix based on directory name
        stem = f"{self._base_prefix}{self._dots_created}"
        dot_path = (self._dots_dir / f"{stem}.dot")
        png_path = (self._pngs_dir / f"{stem}.png") if write_png else None
        self._dots_created += 1

        dot_str = build_dot(
            self.plan,
            spec_nodes=self._spec_nodes,   # ghosts
            spec_edges=self._spec_edges,   # ghost edges
        )
        write_dot_file(dot_str, Path(dot_path))

        if write_png:
            ok = render_png_file(dot_str, Path(png_path))
            if not ok:
                self.events.write({
                    "type": "ExportWarning",
                    "warning": "Graphviz not available; PNG not written",
                    "dot_path": str(dot_path),
                })
                png_path = None

        return dot_path, png_path

    def _handle_plandiff(self, diff: PlanDiff) -> None:
        violations: list[SimpleViolation] = []
        for sc in self.plan.promises:
            if scope_applies(sc.scope, diff):
                violations.extend(sc.constraint.validate(self.plan, diff))
        if violations:
            self.events.write({
                "type": "ConstraintViolation",
                "diff_emitter": diff.emitter_id,
                "violations": violations #[_params(v) for v in violations],
            })
            # mark & terminate pending/running nodes
            for tid, node in self.plan.taskInstances.items():
                if node.status in (Status.PENDING, Status.RUNNING):
                    node.status = Status.TERMINATED
                    self._status[tid] = Status.TERMINATED
                    p = self._pool.get(tid)
                    if p and p.is_alive():
                        p.terminate()
            self.export_dot()
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

                if isinstance(sc.scope, OwnerDescendantsScope):
                    owner = sc.scope.owner_id
                    owner_spec = self.plan.spec_of(owner) if owner in self.plan.taskInstances else None

                    if isinstance(sc.constraint, OnlySpecificNodesAllowed):
                        for spec in sc.constraint.allowed_specs:
                            # do not re-add a ghost if a real child already exists
                            if owner in self.plan.taskInstances and self._has_real_child(owner, spec):
                                continue
                            gid = self._mk_spec_id(owner, spec)
                            # setdefault prevents duplicates if the other promise already added it
                            self._spec_nodes.setdefault(gid, {"spec": spec, "parent": owner, "source": "promise"})
                            self._spec_edges.add((str(owner), gid))

                    elif isinstance(sc.constraint, OnlySpecificEdgesAllowed) and owner_spec:
                        for a, b in sc.constraint.allowed_spec_pairs:
                            if a == owner_spec:
                                if owner in self.plan.taskInstances and self._has_real_child(owner, b):
                                    continue
                                gid = self._mk_spec_id(owner, b)
                                self._spec_nodes.setdefault(gid, {"spec": b, "parent": owner, "source": "promise"})
                                self._spec_edges.add((str(owner), gid))

        for n in diff.new_nodes:
            ti = TaskInstance(
                id=n.id,
                spec_name=n.spec_name,
                inputs=n.inputs,
                parent_id=n.parent_id,
                labels=set(n.labels),
            )
            self.plan.taskInstances[n.id] = ti
            self.events.write({"type": "NodeCreated", "id": n.id, "spec": n.spec_name, "parent_id": n.parent_id, "labels": list(n.labels)})
            # attach default promises for static children
            self._activate_default_promises_for(ti)
            
            # clear matching ghost, if any
            to_delete = None
            for sid, meta in self._spec_nodes.items():
                if meta.get("parent") == n.parent_id and meta.get("spec") == n.spec_name:
                    to_delete = sid
                    break
            if to_delete:
                del self._spec_nodes[to_delete]
                self._spec_edges = {(a, b) for (a, b) in self._spec_edges if b != to_delete}
        
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
    
    def _is_branching_parent(self, parent_id: UUID) -> bool:
        from promise import OnlySpecificNodesAllowed, OwnerDescendantsScope
        for sc in self.plan.promises:
            if isinstance(sc.scope, OwnerDescendantsScope) and sc.scope.owner_id == parent_id:
                if isinstance(sc.constraint, OnlySpecificNodesAllowed):
                    return True
        return False

    def _prune_ghost_siblings(self, parent_id: UUID, taken_spec: str) -> None:
        # Remove ghost children under this parent that don't match the chosen spec
        to_delete = [sid for sid, meta in self._spec_nodes.items()
                    if meta.get("parent") == parent_id and meta.get("spec") != taken_spec]
        if not to_delete:
            return
        for sid in to_delete:
            del self._spec_nodes[sid]
        self._spec_edges = {(a, b) for (a, b) in self._spec_edges
                            if not (a == str(parent_id) and b in to_delete)}

    def _prune_all_ghost_children(self, parent_id: UUID) -> None:
        # Remove any remaining ghost kids once the parent is done (safe fallback)
        to_delete = [sid for sid, meta in self._spec_nodes.items()
                    if meta.get("parent") == parent_id]
        if not to_delete:
            return
        for sid in to_delete:
            del self._spec_nodes[sid]
        self._spec_edges = {(a, b) for (a, b) in self._spec_edges if a != str(parent_id)}

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
        ctx.emit(result)
    except Exception as e:
        ctx.log("ERROR", error=str(e))
        ctx.emit(None)
