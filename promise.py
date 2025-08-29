from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Protocol, Iterable
from plan import Plan, PlanDiff, Edge
from task_registry import spec_name_from_callable
from uuid import UUID

def _overlay_specs(plan: Plan, diff: PlanDiff) -> dict[UUID, str]:
    # plan nodes + nodes introduced in this diff
    ov: dict[UUID, str] = {nid: ti.spec_name for nid, ti in plan.taskInstances.items()}
    ov.update({n.id: n.spec_name for n in diff.new_nodes})
    return ov

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

# concrete constraints

@dataclass(frozen=True, slots=True)
class NoNewNodes(Constraint):
    def validate(self, plan, diff: PlanDiff) -> list[SimpleViolation]:
        return [SimpleViolation("NoNewNodes", diff.emitter_id)] if diff.new_nodes else []

@dataclass(frozen=True, slots=True)
class NoNewEdges(Constraint):
    def validate(self, plan, diff: PlanDiff) -> list[SimpleViolation]:
        return [SimpleViolation("NoNewEdges", diff.emitter_id)] if diff.new_edges else []

@dataclass(frozen=True, slots=True)
class OnlySpecificNodesAllowed:
    allowed_specs: tuple[str, ...] #the new nodes specified by the 
    
    def __init__(self, allowed: Iterable[Callable]):
        names: list[str] = []
        for x in allowed:
            if not callable(x):
                raise TypeError(f"OnlySpecificNodesAllowed expects callables; got {type(x).__name__}")
            names.append(spec_name_from_callable(x))
        object.__setattr__(self, "allowed_specs", tuple(names))

    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        allowed = set(self.allowed_specs)
        bad = [n.spec_name for n in diff.new_nodes if n.spec_name not in allowed]
        return [] if not bad else [SimpleViolation("OnlySpecificNodesAllowed", {"bad_nodes": bad})]

@dataclass(frozen=True, slots=True)
class OnlySpecificEdgesAllowed:
    allowed_spec_pairs: tuple[tuple[str, str], ...]  # (from_spec, to_spec)

    def __init__(self, pairs: Iterable[tuple[Callable, Callable]]):
        norm: list[tuple[str, str]] = []
        for pair in pairs:
            if not (isinstance(pair, tuple) and len(pair) == 2):
                raise TypeError("OnlySpecificEdgesAllowed expects an iterable of 2-tuples of callables")
            a, b = pair
            if not callable(a) or not callable(b):
                raise TypeError(f"OnlySpecificEdgesAllowed expects callables; got {type(a).__name__}, {type(b).__name__}")
            norm.append((spec_name_from_callable(a), spec_name_from_callable(b)))
        object.__setattr__(self, "allowed_spec_pairs", tuple(norm))

    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]: #should change to true/false and emit violations?
        # Build an overlay: known nodes in the plan + nodes introduced by this diff
        overlay = _overlay_specs(plan, diff)
        allowed = set(self.allowed_spec_pairs)

        bad: list[dict[str, str]] = []
        for new_edge in diff.new_edges:
            from_spec = overlay.get(new_edge.from_id)
            to_spec = overlay.get(new_edge.to_id)
            #invalid edge
            if from_spec is None or to_spec is None:
                #maybe make an EdgeViolationEvent?
                bad.append({"from": str(new_edge.from_id), "to": str(new_edge.to_id), "reason": "unknown_node_in_diff"})
                continue
            #unallowed edge
            if (from_spec, to_spec) not in self.allowed_spec_pairs:
                bad.append({"from": from_spec, "to": to_spec})
        return [] if not bad else [SimpleViolation("OnlySpecificEdgesAllowed", {"bad_edges": bad})]
    
@dataclass(frozen=True, slots=True)
class MaxParallelism:
    label: str  # enforce cap among nodes carrying this label
    k: int
    def validate(self, plan: Plan, diff: PlanDiff) -> List[SimpleViolation]:
        # this can be handled by orchestrator, nothing to validate at diff-apply time
        return []
