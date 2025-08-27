from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from promise import ScopedConstraint

Status = Literal["pending", "running", "success", "failed", "terminated"]

@dataclass(slots=True)
class TaskInstance:
    id: str #UUID
    spec_name: str
    inputs: Dict[str, Any]
    status: Status = "pending"
    parent_id: Optional[str] = None
    labels: Set[str] = field(default_factory=set)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    outputs: Any = None

Edge = Tuple[str, str]  # (from_id, to_id)

@dataclass(slots=True)
class Plan:
    nodes: Dict[str, TaskInstance] = field(default_factory=dict) #maps node_id to TaskIntance's
    edges: Set[Edge] = field(default_factory=set)
    promises: List["ScopedConstraint"] = field(default_factory=list)

    #returns a list of node_id
    def predecessors(self, node_id: str) -> List[str]:
        return [a for (a, b) in self.edges if b == node_id]

    def spec_of(self, node_id: str) -> str:
        return self.nodes[node_id].spec_name

@dataclass(slots=True)
class NewNode:
    id: str
    spec_name: str
    inputs: Dict[str, Any]
    parent_id: Optional[str] = None
    labels: Set[str] = field(default_factory=set)
    status: Status = "pending"

@dataclass(slots=True)
class PlanDiff:
    emitter_id: str = ""  # which task proposed this diff
    new_nodes: List[NewNode] = field(default_factory=list)
    new_edges: List[Edge] = field(default_factory=list)
    new_promises: List["ScopedConstraint"] = field(default_factory=list)
    annotations: Dict[str, Any] = field(default_factory=dict)
