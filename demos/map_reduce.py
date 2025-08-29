# demos/map_reduce.py
from __future__ import annotations
import os, sys, time
from pathlib import Path
from typing import List

# Local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import LimitedSpawns, MaxParallelism
from plan import From

# map: square a number
@task
def square(ctx: ExecutionContext, item: int) -> int:
    ctx.log("square", n=item)
    # Tiny stagger so parallel runs are visible in the timeline
    time.sleep(0.03 + (hash(item) % 10) * 0.002)
    return item * item

# reduce: sum the squares
@task
def sum_reduce(ctx: ExecutionContext, parts: List[int]) -> int:
    total = sum(parts)
    ctx.log("sum", total=total, count=len(parts))
    return total

# parent: fan out via spawn, then reduce
@task
def square_batch(ctx: ExecutionContext, numbers: List[int]) -> int:
    # Promise: this task may spawn at most len(numbers) `square`s.
    # Also (optional) cap their parallelism via a label.
    ctx.promise(
        LimitedSpawns(square, max_count=len(numbers)),
        MaxParallelism(label="squares", k=3),
    )

    # Fan out
    kids = []
    for n in numbers:
        kids.append(ctx.spawn(square, inputs={"item": n}, labels={"squares"}))

    # Reduce as a separate node with real data edges from each map output
    reduce_id = ctx.spawn(sum_reduce, inputs={"parts": [From(k) for k in kids]})

    # Wait for the reduce output and return it
    [total] = ctx.wait_for_all([reduce_id])
    return total

if __name__ == "__main__":
    outdir = Path("outputs/map_reduce")
    orc = Orchestrator(_REGISTRY, output_dir=outdir)
    numbers = list(range(1, 5))  # 1..5
    root = orc.start(entry=square_batch, inputs={"numbers": numbers})
    orc.run_to_completion(root)
    print("Run complete → outputs/map_reduce (events.jsonl, dots/, pngs/)")
