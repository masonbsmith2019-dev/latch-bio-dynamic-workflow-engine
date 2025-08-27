from __future__ import annotations
import os
import sys
import time
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple

# Add parent directory to path to import project modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import OnlySpecificEdgesAllowed, OnlySpecificNodesAllowed

# reduce function: sum the squares
@task(name="SumReduce")
def sum_reduce(ctx: ExecutionContext, parts: List[int]) -> int:
    total = sum(parts)
    ctx.log("sum", total=total, count=len(parts))
    return total

# parent function: create the map group
@task(name="SquareOrCubeBatch")
def square_batch(ctx: ExecutionContext, numbers: List[int]) -> None:
    handles = ctx.map("CheckNumber", numbers, reduce="SumReduce", max_parallel=3, group_label="squares")
    ctx.emit({"group": handles.group_label, "count": len(handles.map_ids)})

@task(name="CheckNumber")
def check_number(ctx: ExecutionContext, item: int) -> None:
    print(f"item:{item}")
    ctx.promise(
        OnlySpecificNodesAllowed({"Square", "Cube"}),
        OnlySpecificEdgesAllowed({("CheckNumber", "Square"), ("CheckNumber", "Cube")}),
    )
    ctx.log("checking", item=item)
    if item > 0:
        ctx.spawn("Square", inputs={"n": item})
        ctx.emit({"decision": "square positive"})
    else:
        ctx.spawn("Cube", inputs={"n": item})
        ctx.emit({"decision": "cube non-positive"})

@task(name="Square")
def positive(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("square positive", n=n)
    time.sleep(0.05)
    return n ** 2

@task(name="Cube")
def nonpositive(ctx: ExecutionContext, n: int) -> dict:
    ctx.log("cube nonpositive", n=n)
    time.sleep(0.05)
    return n ** 3

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY)
    numbers = list(range(-3, 4))  # -3,...,0,...,3
    root = orc.start(entry="SquareOrCubeBatch", inputs={"numbers": numbers}, output_path="outputs/map_and_branch")
    orc.run_to_completion(root)
    print("Run complete.")
