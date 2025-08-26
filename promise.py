from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from plan import Plan, PlanDiff

class Violation(Protocol):
    type: str
    details: Dict[str, Any]

@dataclass(frozen=True, slots=True)
class SimpleViolation:
    type: str
    details: Dict[str, Any]

class Constraint(Protocol):
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]: ...

# Scopes: which diffs a constraint applies to
@dataclass(frozen=True, slots=True)
class GlobalScope:
    kind: Literal["global"] = "global"

@dataclass(frozen=True, slots=True)
class OwnerDescendantsScope:
    owner_id: str
    kind: Literal["owner_descendants"] = "owner_descendants"

Scope = GlobalScope | OwnerDescendantsScope

@dataclass(frozen=True, slots=True)
class ScopedConstraint:
    constraint: Constraint
    scope: Scope

def scope_applies(scope: Scope, diff: PlanDiff) -> bool:
    if isinstance(scope, GlobalScope):
        return True
    if isinstance(scope, OwnerDescendantsScope):
        return scope.owner_id == diff.emitter_id
    return False

# Concrete constraints

@dataclass(frozen=True, slots=True)
class OnlySpecificNodesAllowed:
    allowed: Set[str] #the new nodes specified by the 
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        bad = [n.spec_name for n in diff.new_nodes if n.spec_name not in self.allowed]
        return [SimpleViolation("OnlySpecificNodesAllowed", {"bad_nodes": bad})] if bad else []

@dataclass(frozen=True, slots=True)
class OnlySpecificEdgesAllowed:
    allowed_spec_pairs: Set[Tuple[str, str]]  # (from_spec, to_spec)
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        bad: List[Dict[str, str]] = []
        for a, b in diff.new_edges:
            pair = (plan.spec_of(a), plan.spec_of(b))
            if pair not in self.allowed_spec_pairs:
                bad.append({"from": pair[0], "to": pair[1]})
        return [SimpleViolation("OnlySpecificEdgesAllowed", {"bad_edges": bad})] if bad else []
    
@dataclass(frozen=True, slots=True)
class MapOnly:
    label: str
    fn: str  # allowed map function spec name
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        #what is self.label in n.labels doing here? Second check makes sense
        bad = [n.spec_name for n in diff.new_nodes if (self.label in n.labels) and (n.spec_name != self.fn)] 
        return [SimpleViolation("MapOnly", {"label": self.label, "bad_nodes": bad})] if bad else []

@dataclass(frozen=True, slots=True)
class MaxParallelism:
    label: str  # enforce cap among nodes carrying this label
    k: int
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        # Enforced by the scheduler at start time; nothing to validate at diff-apply time.
        return []
