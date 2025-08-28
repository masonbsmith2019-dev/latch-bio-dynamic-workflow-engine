from __future__ import annotations
import os, sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from plan import From
from orchestrator import Orchestrator

@task
def add(ctx: ExecutionContext, x: int, y: int) -> int:
    return x + y

@task
def mul(ctx: ExecutionContext, x: int, y: int) -> int:
    return x * y

@task
def combine(ctx: ExecutionContext, a: int, b: int) -> int:
    # combine results from add/mul
    return a + b

@task
def fork_join(ctx: ExecutionContext, x: int, y: int) -> int:
    add_id = ctx.spawn(add, inputs={"x": x, "y": y})
    mul_id = ctx.spawn(mul, inputs={"x": x, "y": y})
    # third task depends on outputs of the first two
    comb_id = ctx.spawn(combine, inputs={"a": From(add_id), "b": From(mul_id)})
    # wait so this parent returns the final scalar
    [result] = ctx.wait_for_all([comb_id])
    return result

if __name__ == "__main__":
    outdir = Path("outputs/from_fork_join")
    orc = Orchestrator(_REGISTRY, events_path=outdir / "events.jsonl")
    root = orc.start(entry=fork_join, inputs={"x": 2, "y": 3}, output_path=outdir / "plan")
    orc.run_to_completion(root)
    print("Done → see outputs/from_fork_join (events.jsonl and DOT/PNGs)")
