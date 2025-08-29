from __future__ import annotations
import os, sys, time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from plan import From
from promise import LimitedSpawns  # <-- NEW

@task(static=True)
def b(ctx: ExecutionContext, x: int) -> int:
    ctx.log("b:start", x=x)
    out = x + 1
    ctx.log("b:done", out=out)
    return out

@task(static=True)
def c(ctx: ExecutionContext, y: int) -> int:
    ctx.log("c:start", y=y)
    out = y * 2
    ctx.log("c:done", out=out)
    return out

@task(static=True)
def d(ctx: ExecutionContext, b_out: int, c_out: int) -> int:
    ctx.log("d:start", b=b_out, c=c_out)
    out = b_out + c_out
    ctx.log("d:done", out=out)
    return out

@task(
    calls=[b, c, d],
    edges=[(b, d), (c, d)]
)
def A(ctx: ExecutionContext, x: int, y: int) -> int:
    # Branch 1: A's own b,c,d chain
    b_id = ctx.spawn(b, inputs={"x": x})
    c_id = ctx.spawn(c, inputs={"y": y})
    d_id = ctx.spawn(d, inputs={"b_out": From(b_id), "c_out": From(c_id)})

    [a_out] = ctx.wait_for_all([d_id])

    ctx.log("A:done", a_out=a_out)
    return a_out


if __name__ == "__main__":
    outdir = Path("outputs/static_branch_demo")
    orc = Orchestrator(_REGISTRY, output_dir=outdir)

    # Start with entry A; you should see:
    # - Static preview under A (b,c,d with b→d and c→d)
    # - A budget ghost for E: "(1 left)" that disappears after the single spawn
    # - When E is spawned (dynamic→real), a static preview appears under E as well
    root = orc.start(entry=A, inputs={"x": 2, "y": 3})
    orc.run_to_completion(root)

    print(f"Static-branch demo complete → {outdir} (events.jsonl, dots/, pngs/)")
