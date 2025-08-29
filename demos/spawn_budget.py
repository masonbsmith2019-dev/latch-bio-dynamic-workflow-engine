# demos/spawn_budget_demo.py
from __future__ import annotations
import os, sys, time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import LimitedSpawns

@task
def worker(ctx: ExecutionContext, i: int) -> int:
    time.sleep(0.05)
    return i * i

@task
def fan_out_with_budget(ctx: ExecutionContext, k: int) -> int:
    # This task may spawn at most 5 `worker`s (direct children)
    ctx.promise(LimitedSpawns(worker, max_count=5))

    kids = []
    for i in range(k):
        kids.append(ctx.spawn(worker, inputs={"i": i}))

    vals = ctx.wait_for_all(kids)
    total = sum(vals)
    ctx.log("total", total=total, spawned=len(kids))
    return total

if __name__ == "__main__":
    outdir = Path("outputs/spawn_budget")
    orc = Orchestrator(_REGISTRY, output_dir=outdir)

    # Try k=3 (leaves "2 left"), k=5 (exact), k=6 (should violate and terminate)
    root = orc.start(entry=fan_out_with_budget, inputs={"k": 4})
    orc.run_to_completion(root)
    print(f"Done → {outdir} (events.jsonl, dots/, pngs/)")
