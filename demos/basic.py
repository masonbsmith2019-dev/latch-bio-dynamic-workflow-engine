from __future__ import annotations
import os, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator

@task(name="task_1")
def task_1(ctx: ExecutionContext, a: int, b: int, c: int) -> int:
    time.sleep(0.02)
    return a + b + c

@task(name="task_2")
def task_2(ctx: ExecutionContext, m: int, n: int) -> int:
    time.sleep(0.02)
    return m * n

@task(name="task_3")
def task_3(ctx: ExecutionContext, x: int, y: int) -> int:
    time.sleep(0.02)
    return x - y

# This is your “workflow” task: it wires a static DAG (A,B)->C.
@task(name="my_workflow")
def my_workflow(ctx: ExecutionContext, a: int, b: int, c: int, m: int, n: int) -> None:
    # Spawn the two independent producers
    t1 = ctx.spawn("task_1", inputs={"a": a, "b": b, "c": c})
    t2 = ctx.spawn("task_2", inputs={"m": m, "n": n})

    # Wire the consumer (depends on both), with named inputs x, y
    t3 = ctx.join_named({"x": t1, "y": t2}, spec="task_3")

    # The root workflow task can finish now; task_3 will complete later.
    ctx.emit({"wired": True, "final_node": t3})

if __name__ == "__main__":
    orc = Orchestrator(_REGISTRY)
    root = orc.start(entry="my_workflow", inputs={"a": 1, "b": 2, "c": 3, "m": 4, "n": 5})
    orc.run_to_completion(root)
    dot_path, png_path = orc.export_dot("outputs/static_dag", write_png=True)
    print("Static DAG complete.")
