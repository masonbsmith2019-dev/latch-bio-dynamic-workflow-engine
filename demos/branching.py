from __future__ import annotations
import os, sys, time

# Add parent dir for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
# If your constraints live elsewhere, adjust this import:
from promise import OnlySpecificNodesAllowed, OnlySpecificEdgesAllowed
from plan import Edge
from pathlib import Path


@task
def check_number(ctx: ExecutionContext, n: int) -> None:
    # Promise: this node may only spawn Positive or NonPositive
    ctx.promise(
        OnlySpecificNodesAllowed({"Positive", "NonPositive"}),
        OnlySpecificEdgesAllowed({("CheckNumber", "Positive"), ("CheckNumber", "NonPositive")}),
    )
    ctx.log("checking", n=n)
    if n > 0:
        ctx.spawn("Positive", inputs={"n": n})
        ctx.emit({"decision": "positive"})
    else:
        ctx.spawn("NonPositive", inputs={"n": n})
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
    orc = Orchestrator(_REGISTRY)
    # change to negative num to exercise the other branch
    root = orc.start(entry="CheckNumber", inputs={"n": -1}, output_path=Path("outputs/branching.dot"))
    orc.run_to_completion(root)
    #orc.export_dot()
    print("Branching demo complete. See outputs/branching.dot and outputs/events.jsonl")
