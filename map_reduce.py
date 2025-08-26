from __future__ import annotations
import json
import os
import queue
import time
import uuid
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator

# map function: square a number
@task(name="Square")
def square(ctx: ExecutionContext, item: int) -> int:
    ctx.log("square", n=item)
    time.sleep(0.03 + (hash(item) % 10) * 0.002)  # tiny stagger
    return item * item

# reduce function: sum the squares
@task(name="SumReduce")
def sum_reduce(ctx: ExecutionContext, parts: List[int]) -> int:
    total = sum(parts)
    ctx.log("sum", total=total, count=len(parts))
    return total

# parent function: create the map group
@task(name="SquareBatch")
def square_batch(ctx: ExecutionContext, numbers: List[int]) -> None:
    handles = ctx.map("Square", numbers, reduce="SumReduce", max_parallel=3, group_label="squares")
    ctx.emit({"group": handles.group_label, "count": len(handles.map_ids)})

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY)
    numbers = list(range(1, 11))  # 1..10
    root = orc.start(entry="SquareBatch", inputs={"numbers": numbers})
    orc.run_to_completion(root)
    orc.export_dot("outputs/plan.dot")
    print("Run complete. See outputs/plan.dot and outputs/events.jsonl")
