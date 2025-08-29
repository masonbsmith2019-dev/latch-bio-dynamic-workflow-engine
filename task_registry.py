from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable
# from promise import ScopedConstraint

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
    
def task(fn: Callable | None = None, static: bool = False, calls: Iterable[Callable] = ()):
    #infer spec name from fn.__name__, require callables
    # static=True: runtime enforcement (inject NoNewNodes/NoNewEdges when it starts)
    # calls=[...] : registration time preview (speculative children to draw)
    def register(f: Callable):
        spec_name = f.__name__
        setattr(f, "__task_spec__", spec_name)   # used by ctx.map/spawn
        preview = tuple(spec_name_from_callable(c) for c in calls)
        _REGISTRY.add(TaskSpec(
            name=spec_name,
            fn=f,
            static=static,
            preview_calls=preview,
        ))
        return f
    return register if fn is None else register(fn)
