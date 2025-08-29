from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Optional
from plan import PlanDiff, NewNode, Edge, From, Status
from promise import GlobalScope, ScopedConstraint, MaxParallelism, Constraint, OwnerDescendantsScope, Scope
from task_registry import _REGISTRY, spec_name_from_callable
from uuid import UUID
from inspect import signature, Parameter

@dataclass(frozen=True, slots=True)
class MapHandles:
    map_ids: list[str]
    reduce_id: Optional[str]
    group_label: str

class ExecutionContext:
    def __init__(self, control_q: mp.Queue, me_id: UUID, status_proxy=None, results_proxy=None):
        self._q = control_q
        self.me_id = me_id
        self._status = status_proxy      # mp.Manager().dict() proxy from orchestrator
        self._results = results_proxy    # mp.Manager().dict() proxy from orchestrator

    #creates a PlanDiff and puts it on the control queue. returns the UUID of the newly spawned task instance
    def spawn(
            self, 
            fn: Callable, 
            *, 
            inputs: dict[str, Any], 
            labels: set[str] | None = None
        ) -> str:
        spec_name = spec_name_from_callable(fn)
        if not _REGISTRY.exists(spec_name):
            raise ValueError(f"spawn target '{spec_name}' is not registered")

        child_id = uuid.uuid4()

        # Collect producer task_ids by scanning inputs for From(...)
        producers: set[uuid.UUID] = set()
        def collect(x: Any) -> None:
            if isinstance(x, From):
                producers.add(x.task_id)
            elif isinstance(x, dict):
                for v in x.values():
                    collect(v)
            elif isinstance(x, (list, tuple)):
                for v in x:
                    collect(v)

        collect(inputs)

        nn = NewNode(
            id=child_id,
            spec_name=spec_name,
            inputs=inputs,
            parent_id=self.me_id,
            labels=set(labels or []),
        )

        # Edges: (parent -> child) + (each producer -> child)
        edges = [Edge(pid, child_id) for pid in producers]

        self._q.put(("plandiff", PlanDiff(
            emitter_id=self.me_id,
            new_nodes=[nn],
            new_edges=edges,
            new_promises=[],
            annotations=None,
        )))
        return child_id
    
    def wait_for_all(self, ids: list[uuid.UUID], *, timeout: float | None = None, poll_ms: int = 10) -> list[Any]:
        if self._status is None or self._results is None:
            raise RuntimeError("wait_for_all requires status/results proxies from the orchestrator")

        terminals = {Status.SUCCESS, Status.FAILED, Status.TERMINATED}
        remaining = set(ids)
        deadline = (time.monotonic() + timeout) if timeout is not None else None

        while remaining:
            done = [i for i in list(remaining) if self._status.get(i) in terminals]
            for i in done:
                remaining.remove(i)
            if remaining:
                if deadline is not None and time.monotonic() > deadline:
                    raise TimeoutError(f"wait_for_all timed out; remaining={[str(r) for r in remaining]}")
                time.sleep(poll_ms / 1000.0)

        # Return outputs in the same order as ids (may be None if task emitted None)
        return [self._results.get(i) for i in ids]

    #confused on how scoping works
    def promise(self, *constraints: Constraint, scope: Scope | None = None) -> None:
        sc = [ScopedConstraint(c, scope or OwnerDescendantsScope(owner_id=self.me_id)) for c in constraints]
        self._q.put(("plandiff", PlanDiff(emitter_id=self.me_id, new_promises=sc)))

    def log(self, msg: str, **fields: Any) -> None:
        payload = {"task_id": self.me_id, "msg": msg, **fields}
        self._q.put(("log", payload))

    def emit(self, output: Any) -> None:
        self._q.put(("emit", {"task_id": self.me_id, "output": output}))