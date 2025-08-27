from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from plan import PlanDiff, NewNode, Edge
from promise import GlobalScope, ScopedConstraint, MapOnly, MaxParallelism, Constraint, OwnerDescendantsScope, Scope

@dataclass(frozen=True, slots=True)
class MapHandles:
    map_ids: List[str]
    reduce_id: Optional[str]
    group_label: str

class ExecutionContext:
    def __init__(self, control_q: mp.Queue, me_id: str):
        self._q = control_q
        self.me_id = me_id
    
    def join_named(
        self,
        name_to_parent: Dict[str, str], # would look like {"x": task1_id, "y": task2_id}
        *,
        spec: str | Callable[..., Any],
        extra_inputs: Optional[Dict[str, Any]] = None,
        labels: Optional[Set[str]] = None,
    ) -> str:
        """Create a node that depends on the parents and receives their outputs as named args."""
        spec_name = spec if isinstance(spec, str) else spec.__name__
        join_id = str(uuid.uuid4())
        nn = NewNode(
            id=join_id,
            spec_name=spec_name,
            inputs={"__gather_from_named__": dict(name_to_parent), **(extra_inputs or {})},
            parent_id=self.me_id,
            labels=labels or set(),
        )
        diff = PlanDiff(
            emitter_id=self.me_id,
            new_nodes=[nn],
            new_edges=[(pid, join_id) for pid in name_to_parent.values()],
        )
        self._q.put(("plandiff", diff))
        return join_id

    #creates a PlanDiff and puts it on the control queue. returns the UUID of the newly spawned task instance
    def spawn(
            self, 
            task: str | Callable[..., Any], 
            *, 
            inputs: Dict[str, Any], 
            labels: Set[str] | None = None
        ) -> str:
        spec_name = task if isinstance(task, str) else task.__name__
        #this is where new UUID's are generated
        child_id = str(uuid.uuid4())
        diff = PlanDiff(
            emitter_id=self.me_id,
            new_nodes=[NewNode(id=child_id, spec_name=spec_name, inputs=inputs, parent_id=self.me_id, labels=labels or set())],
            new_edges=[(self.me_id, child_id)],
        )
        self._q.put(("plandiff", diff))
        return child_id
    
    def map(
        self,
        map_fn: str | Callable[..., Any],
        items: List[Any],
        *,
        reduce: str | Callable[..., Any] | None = None,
        max_parallel: Optional[int] = None,
        group_label: Optional[str] = None,
    ) -> MapHandles:
        map_spec = map_fn if isinstance(map_fn, str) else map_fn.__name__
        reduce_spec = reduce if (isinstance(reduce, str) or reduce is None) else reduce.__name__
        #use group label if passed in, otherwise combine emitter task instance UUID and newly generated UUID 
        gid = group_label or f"map:{self.me_id}:{uuid.uuid4().hex[:6]}"
        new_nodes: List[NewNode] = []
        new_edges: List[Edge] = []
        map_ids: List[str] = []

        # create map clones
        for it in items:
            inputs = it if isinstance(it, dict) else {"item": it}
            cid = str(uuid.uuid4())
            map_ids.append(cid)
            new_nodes.append(NewNode(id=cid, spec_name=map_spec, inputs=inputs, parent_id=self.me_id, labels={gid}))
            new_edges.append((self.me_id, cid))

        reduce_id: Optional[str] = None
        if reduce_spec is not None:
            reduce_id = str(uuid.uuid4())
            # gather from makes it so orchestrator will gather outputs into parts when scheduling
            new_nodes.append(NewNode(id=reduce_id, spec_name=reduce_spec, inputs={"__gather_from__": map_ids}, parent_id=self.me_id))
            for mid in map_ids:
                new_edges.append((mid, reduce_id))

        # group-scoped promises (global scope, but they only act on nodes with this label)
        new_promises: List[ScopedConstraint] = [
            ScopedConstraint(MapOnly(label=gid, fn=map_spec), GlobalScope()),
        ]
        if max_parallel is not None:
            new_promises.append(ScopedConstraint(MaxParallelism(label=gid, k=max_parallel), GlobalScope()))

        diff = PlanDiff(
            emitter_id=self.me_id,
            new_nodes=new_nodes,
            new_edges=new_edges,
            new_promises=new_promises,
            annotations={"group_label": gid},
        )
        self._q.put(("plandiff", diff))
        return MapHandles(map_ids=map_ids, reduce_id=reduce_id, group_label=gid)

    #confused on how scoping works
    def promise(self, *constraints: Constraint, scope: Scope | None = None) -> None:
        sc = [ScopedConstraint(c, scope or OwnerDescendantsScope(owner_id=self.me_id)) for c in constraints]
        self._q.put(("plandiff", PlanDiff(emitter_id=self.me_id, new_promises=sc)))

    def log(self, msg: str, **fields: Any) -> None:
        payload = {"task_id": self.me_id, "msg": msg, **fields}
        self._q.put(("log", payload))

    def emit(self, output: Any) -> None:
        self._q.put(("emit", {"task_id": self.me_id, "output": output}))