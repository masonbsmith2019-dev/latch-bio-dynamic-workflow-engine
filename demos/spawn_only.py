from __future__ import annotations
import os, sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from plan import From
from orchestrator import Orchestrator

@task
def square(ctx: ExecutionContext, n: int) -> int:
    return n * n

@task
def cube(ctx: ExecutionContext, n: int) -> int:
    return n * n * n

@task
def batch(ctx: ExecutionContext, numbers: list[int]) -> int:
    kids = []
    for n in numbers:
        fn = square if n > 0 else cube
        kids.append(ctx.spawn(fn, inputs={"n": n}))
    vals = ctx.wait_for_all(kids)
    total = sum(vals)
    ctx.log("sum", total=total, count=len(vals))
    return total

# Optionally show A→B chaining via From(...)
@task
def chain(ctx: ExecutionContext, n: int) -> int:
    a = ctx.spawn(square, inputs={"n": n})
    b = ctx.spawn(cube,   inputs={"n": From(a)})  # cube(square(n))
    [out] = ctx.wait_for_all([b])
    return out

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY, events_path=Path("outputs/spawn_only_batch/events.jsonl"))
    numbers = list(range(-3, 4))  # -3..0..3
    root = orc.start(entry=batch, inputs={"numbers": numbers}, output_path=Path("outputs/spawn_only_batch"))
    orc.run_to_completion(root)
    print("Batch demo complete → outputs/spawn_only_batch (events.jsonl, DOT/PNG)")