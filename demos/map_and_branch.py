from __future__ import annotations
import os
import sys
import time
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from pathlib import Path

# Add parent directory to path to import project modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificEdgesAllowed, OnlySpecificNodesAllowed
from plan import Edge

# reduce function: sum the squares
@task
def sum_reduce(ctx: ExecutionContext, parts: List[int]) -> int:
    total = sum(parts)
    ctx.log("sum", total=total, count=len(parts))
    return total

# parent function: create the map group
@task
def square_batch(ctx: ExecutionContext, numbers: List[int]) -> None:
    handles = ctx.map(check_number, numbers, reduce=sum_reduce, max_parallel=3, group_label="squares")
    ctx.emit({"group": handles.group_label, "count": len(handles.map_ids)})

@task
def check_number(ctx: ExecutionContext, n: int) -> None:
    ctx.promise(
        OnlySpecificNodesAllowed({square, cube}),
        OnlySpecificEdgesAllowed({(check_number, square), (check_number, cube)}),
    )
    ctx.log("checking", n=n)
    if n > 0:
        ctx.spawn(square, inputs={"n": n})
        ctx.log("decision", take="square")
    else:
        ctx.spawn(cube, inputs={"n": n})
        ctx.emit({"decision": "cube non-positive"})

@task
def square(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("square positive", n=n)
    time.sleep(0.05)
    return n ** 2

@task
def cube(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("cube nonpositive", n=n)
    time.sleep(0.05)
    return n ** 3

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY)
    numbers = list(range(-3, 4))  # -3,...,0,...,3
    root = orc.start(entry=square_batch, inputs={"numbers": numbers}, output_path=Path("outputs/map_and_branch"))
    orc.run_to_completion(root)
    print("Run complete.")
