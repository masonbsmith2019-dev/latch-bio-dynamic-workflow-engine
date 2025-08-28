from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional
from promise import ScopedConstraint

@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    fn: Callable
    input_schema: Any | None = None
    output_schema: Any | None = None
    default_promises: list["ScopedConstraint"] = field(default_factory=list)

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

    def names(self) -> List[str]:
        return list(self._specs.keys())
    
_REGISTRY = TaskRegistry()
    
def task(name: Optional[str] = None, **kwargs):
    def deco(fn: Callable[..., Any]):
        spec_name = name if name is not None else fn.__name__
        _REGISTRY.add(TaskSpec(name=spec_name, fn=fn, **kwargs))
        return fn
    return deco
