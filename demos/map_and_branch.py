# demos/map_and_branch.py
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

# Add parent directory to path to import project modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificEdgesAllowed, OnlySpecificNodesAllowed, MaxParallelism, LimitedSpawns

@task(static=True)
def square(ctx: ExecutionContext, n: int) -> int:
    ctx.log("square", n=n)
    time.sleep(0.05)
    return n * n

@task(static=True)
def cube(ctx: ExecutionContext, n: int) -> int:
    ctx.log("cube", n=n)
    time.sleep(0.05)
    return n * n * n

@task
def check_number(ctx: ExecutionContext, n: int) -> int:
    # promise that only {square, cube} can be created under this node and only
    ctx.promise(
        OnlySpecificNodesAllowed({square, cube}),
        OnlySpecificEdgesAllowed({(check_number, square), (check_number, cube)}),
    )

    ctx.log("checking", n=n)
    if n > 0:
        child = ctx.spawn(square, inputs={"n": n}, labels={"numbers"})
    else:
        child = ctx.spawn(cube, inputs={"n": n}, labels={"numbers"})

    # Wait for the one child and return its value
    [val] = ctx.wait_for_all([child])
    return val


@task
def square_batch(ctx: ExecutionContext, numbers: list[int]) -> int:
    # Cap concurrency across children labeled "numbers"
    ctx.promise(
        LimitedSpawns(check_number, max_count=len(numbers)),
        MaxParallelism(label="squares", k=3),
    )

    kids: list[Any] = []
    for n in numbers:
        kids.append(ctx.spawn(check_number, inputs={"n": n}))

    vals = ctx.wait_for_all(kids)
    total = sum(vals)
    ctx.log("sum", total=total, count=len(vals))
    return total

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY, output_dir=Path("outputs/map_and_branch"))
    numbers = list(range(-2, 3))  # -3, -2, -1, 0, 1, 2, 3
    #-8 + -1 + 0 + 1 + 4 = -4
    root = orc.start(entry=square_batch, inputs={"numbers": numbers})
    orc.run_to_completion(root)
    print("Run complete → outputs/map_and_branch (events.jsonl, dots/, pngs/).")
