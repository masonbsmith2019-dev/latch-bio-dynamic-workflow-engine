from __future__ import annotations
import os, sys, time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import LimitedSpawns

@task
def worker_a(ctx: ExecutionContext, i: int) -> int:
    return i  # trivial

@task
def worker_b(ctx: ExecutionContext, x: int) -> int:
    return x * 10

@task
def fan_out_two_budgets(ctx: ExecutionContext, ka: int, kb: int) -> dict:
    # declare two separate spawn budgets: up to 3 worker_a, up to 2 worker_b
    ctx.promise(
        LimitedSpawns(worker_a, max_count=3),
        LimitedSpawns(worker_b, max_count=2),
    )

    kids = []
    # Spawn A's
    for i in range(ka):
        kids.append(ctx.spawn(worker_a, inputs={"i": i}))
    # Spawn B's
    for j in range(kb):
        kids.append(ctx.spawn(worker_b, inputs={"x": j}))

    vals = ctx.wait_for_all(kids)
    total = sum(vals)
    ctx.log("total", total=total, count=len(vals), ka=ka, kb=kb)
    return {"total": total, "ka": ka, "kb": kb}


if __name__ == "__main__":
    outdir = Path("outputs/spawn_budget_two")
    orc = Orchestrator(_REGISTRY, output_dir=outdir)

    # Try (ka, kb) = (2, 2) -> OK (leaves 1 A left, 0 B left)
    # Try (ka, kb) = (3, 2) -> OK (exact)
    # Try (ka, kb) = (4, 1) -> should VIOLATE (A over budget)
    root = orc.start(entry=fan_out_two_budgets, inputs={"ka": 2, "kb": 2})
    orc.run_to_completion(root)
    print(f"Done → {outdir} (events.jsonl, dots/, pngs/)")
