# demos/multi_branching.py
from __future__ import annotations
import os
import sys
from pathlib import Path

# Add parent dir for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificNodesAllowed, OnlySpecificEdgesAllowed

@task
def even(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("even", n=n)
    return {"branch": "even", "n": n}

@task
def mult3(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("mult3", n=n)
    return {"branch": "mult3", "n": n}

@task
def positive(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("positive", n=n)
    return {"branch": "positive", "n": n}

# branching task: up to three branches can be taken
@task
def classify(ctx: ExecutionContext, n: int) -> None:
    # promise: this node may only spawn {even, mult3, positive}, via (classify -> child)
    ctx.promise(
        OnlySpecificNodesAllowed({even, mult3, positive}),
        OnlySpecificEdgesAllowed({(classify, even), (classify, mult3), (classify, positive)}),
    )
    ctx.log("classify", n=n)

    taken: list[str] = []

    if n % 2 == 0:
        ctx.spawn(even, inputs={"n": n})
        taken.append("even")

    if n % 3 == 0:
        ctx.spawn(mult3, inputs={"n": n})
        taken.append("mult3")

    if n > 0:
        ctx.spawn(positive, inputs={"n": n})
        taken.append("positive")

    ctx.log("decision", taken=",".join(taken) if taken else "none")

if __name__ == "__main__":
    n = 3
    orc = Orchestrator(_REGISTRY, output_dir=Path("outputs/multi_branching"))
    root = orc.start(entry=classify, inputs={"n":n})
    orc.run_to_completion(root)
    print("Run complete → outputs/multi_branching (events.jsonl, dots/, pngs/).")
