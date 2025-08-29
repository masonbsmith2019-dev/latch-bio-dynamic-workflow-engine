# demos/ghost_demo.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, List

# Allow "import ..." from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from plan import From


# --- Real tasks --------------------------------------------------------------

@task
def sum_all(ctx: ExecutionContext, nums: list[int]) -> int:
    total = sum(nums)
    ctx.log("sum_all", total=total, count=len(nums))
    return total


@task
def max_all(ctx: ExecutionContext, nums: list[int]) -> int:
    m = max(nums) if nums else 0
    ctx.log("max_all", max=m)
    return m


@task
def final_report(ctx: ExecutionContext, sum_value: int, max_value: int | None = None) -> str:
    msg = f"Report: sum={sum_value}, max={max_value if max_value is not None else 'N/A'}"
    ctx.log("final_report", report=msg)
    return msg


# --- Workflow (entry). We *preview* expected children via calls=[...].
# We intentionally skip spawning `max_all` at runtime to leave its ghost.
@task(calls=[sum_all, max_all, final_report])
def analyze_numbers(ctx: ExecutionContext, numbers: list[int], want_max: bool = False) -> str:
    # Spawn sum
    sum_id = ctx.spawn(sum_all, inputs={"nums": numbers})

    # Maybe spawn max (we'll set want_max=False in main to leave a ghost)
    max_id = None
    if want_max:
        max_id = ctx.spawn(max_all, inputs={"nums": numbers})

    # Build inputs for report dynamically using From(...)
    report_inputs: dict[str, Any] = {"sum_value": From(sum_id)}
    if max_id is not None:
        report_inputs["max_value"] = From(max_id)

    rep_id = ctx.spawn(final_report, inputs=report_inputs)

    # Wait for the report result and return it from the workflow
    [msg] = ctx.wait_for_all([rep_id])
    return msg


# --- Main --------------------------------------------------------------------

if __name__ == "__main__":
    out_dir = Path("outputs/ghost_demo")
    out_dir.mkdir(parents=True, exist_ok=True)

    orc = Orchestrator(_REGISTRY, output_dir=out_dir)

    # We pass want_max=False so the 'max_all' ghost remains dashed in the graph.
    numbers = [3, 1, 4, 1, 5]

    # Start the workflow. This seeds ghost/speculative nodes from @task(calls=[...]).
    root = orc.start(entry=analyze_numbers, inputs={"numbers": numbers, "want_max": False})

    # Snapshot the registration-time ghost view before anything runs:
    orc.export_dot(write_png=True)

    # Now run; spawned children will replace matching ghosts with real nodes.
    orc.run_to_completion(root)

    print(f"Done. See {out_dir}/ for events.jsonl and the DOT/PNG sequence.")
