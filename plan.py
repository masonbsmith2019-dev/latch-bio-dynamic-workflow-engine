from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple, TYPE_CHECKING
from enum import Enum
from uuid import UUID

if TYPE_CHECKING:
    from promise import ScopedConstraint

#Status = Literal["pending", "running", "success", "failed", "terminated"]

class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TERMINATED = "terminated"

@dataclass(slots=True)
class TaskInstance:
    id: UUID
    spec_name: str
    inputs: dict[str, Any]
    status: Status = Status.PENDING
    parent_id: Optional[str] = None
    labels: set[str] = field(default_factory=set)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    outputs: Any = None

@dataclass(frozen=True, slots=True)
class Edge:
    from_id: UUID
    to_id: UUID

@dataclass(slots=True)
class Plan:
    taskInstances: dict[UUID, TaskInstance] = field(default_factory=dict) #maps node_id to TaskIntance's
    edges: set[Edge] = field(default_factory=set)
    promises: list["ScopedConstraint"] = field(default_factory=list)

    #returns a list of node_id
    #should pre-compute this instead
    def predecessors(self, node_id: UUID) -> list[UUID]:
        return [e.from_id for e in self.edges if e.to_id == node_id]

    def spec_of(self, node_id: UUID) -> str:
        return self.taskInstances[node_id].spec_name

@dataclass(slots=True)
class NewNode:
    id: UUID
    spec_name: str
    inputs: dict[str, Any]
    parent_id: Optional[str] = None
    labels: set[str] = field(default_factory=set)
    status: Status = Status.PENDING

@dataclass(slots=True)
class PlanDiff:
    emitter_id: str | None = None  # which task proposed this diff
    new_nodes: list[NewNode] = field(default_factory=list)
    new_edges: list[Edge] = field(default_factory=list)
    new_promises: list["ScopedConstraint"] = field(default_factory=list)
    annotations: dict[str, Any] = field(default_factory=dict)
