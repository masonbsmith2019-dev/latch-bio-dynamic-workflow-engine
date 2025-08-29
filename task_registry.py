from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Iterable

def spec_name_from_callable(fn: Callable) -> str:
    # @task decorator should have set __task_spec__; fall back to __name__
    return getattr(fn, "__task_spec__", fn.__name__)

@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    fn: Callable
    input_schema: Any | None = None
    output_schema: Any | None = None
    static: bool = False
    preview_calls: tuple[str, ...] = ()
    preview_edges: tuple[tuple[str, str], ...] = () 

class TaskRegistry:
    def __init__(self):
        self._specs: dict[str, TaskSpec] = {}

    def add(self, spec: TaskSpec) -> None:
        if spec.name in self._specs:    
            raise ValueError(f"Task {spec.name} already registered")
        self._specs[spec.name] = spec

    def get(self, name: str) -> TaskSpec:
        return self._specs[name]

    def exists(self, name: str) -> bool:
        return name in self._specs

    def names(self) -> list[str]:
        return list(self._specs.keys())
    
_REGISTRY = TaskRegistry()
    
def task(
        fn: Callable | None = None, 
        *,
        static: bool = False, 
        calls: Iterable[Callable] = (),
        edges: Iterable[tuple[Callable, Callable]] = ()
    ):
    # infer spec name from fn.__name__, require callables
    # static=True marks a task as not allowed to mutate the DAG (NoNewNodes/NoNewEdges).
    # calls=[...] lists child task callables for registration-time preview.
    # edges=[(A,B), ...] lists preview edges between those children (A->B) for preview layout.
    def register(f: Callable):
        spec_name = f.__name__
        setattr(f, "__task_spec__", spec_name)   # used by ctx.map/spawn
        preview = tuple(spec_name_from_callable(c) for c in calls)

        # convert preview children and edges to spec-name strings
        preview = tuple(spec_name_from_callable(c) for c in calls)
        preview_edges = tuple(
            (spec_name_from_callable(a), spec_name_from_callable(b))
            for (a, b) in edges
        )

        # light validation: edge endpoints should be in preview children
        if preview_edges:
            known = set(preview)
            for a, b in preview_edges:
                if a not in known or b not in known:
                    raise ValueError(
                        f"Preview edge ({a} -> {b}) must reference tasks listed in calls=[...]; "
                        f"got calls={list(preview)}"
                    )
        
        _REGISTRY.add(TaskSpec(
            name=spec_name,
            fn=f,
            static=static,
            preview_calls=preview,
            preview_edges=preview_edges,
        ))
        return f
    return register if fn is None else register(fn)
