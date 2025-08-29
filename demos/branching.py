from __future__ import annotations
import os, sys, time
from pathlib import Path

# Add parent dir for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificNodesAllowed, OnlySpecificEdgesAllowed


@task
def check_number(ctx: ExecutionContext, n: int) -> None:
    # Promise: this node may only spawn {positive, nonpositive}
    # and only edges (check_number -> positive|nonpositive) are allowed.
    ctx.promise(
        OnlySpecificNodesAllowed({positive, nonpositive}),
        OnlySpecificEdgesAllowed({(check_number, positive), (check_number, nonpositive)}),
    )
    ctx.log("checking", n=n)
    if n > 0:
        ctx.spawn(positive, inputs={"n": n})
        ctx.emit({"decision": "positive"})
    else:
        ctx.spawn(nonpositive, inputs={"n": n})
        ctx.emit({"decision": "nonpositive"})


@task
def positive(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("positive", n=n)
    time.sleep(0.05)
    return {"n": n, "sign": "+"}


@task
def nonpositive(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("nonpositive", n=n)
    time.sleep(0.05)
    return {"n": n, "sign": "≤0"}


if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY, output_dir=Path("outputs/branching"))
    # Change n to >0 to exercise the other branch
    root = orc.start(entry=check_number, inputs={"n": -1})
    orc.run_to_completion(root)
    print("Branching demo complete. See outputs/branching (events.jsonl, dots/, pngs/)")
