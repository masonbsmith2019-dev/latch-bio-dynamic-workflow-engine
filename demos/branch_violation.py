from __future__ import annotations
import os, sys
from pathlib import Path
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificNodesAllowed, OnlySpecificEdgesAllowed

@task
def square(ctx: ExecutionContext, n: int) -> int:
    return n * n

@task
def cube(ctx: ExecutionContext, n: int) -> int:
    return n * n * n

@task
def rogue(ctx: ExecutionContext, n: int) -> int:
    # Something obviously not allowed by the parent’s promises
    return -999

@task
def branch_or_break(ctx: ExecutionContext, n: int) -> None:
    # Promise: only square or cube may be spawned by this node
    ctx.promise(
        OnlySpecificNodesAllowed({square, cube}),
        OnlySpecificEdgesAllowed({(branch_or_break, square), (branch_or_break, cube)}),
    )

    ctx.log("deciding", n=n)

    # violate the promises on purpose:
    ctx.spawn(rogue, inputs={"n": n})

    # if constraints work, the orchestrator will flag a violation and terminate.
    ctx.emit({"note": "you should not see this"})

if __name__ == "__main__":
    out = Path("outputs/branch_violation")
    orc = Orchestrator(_REGISTRY, output_dir=out)
    root = orc.start(entry=branch_or_break, inputs={"n": 5})
    orc.run_to_completion(root)
    print("Done. Check:", out, "(events.jsonl, dots/, pngs/)")
