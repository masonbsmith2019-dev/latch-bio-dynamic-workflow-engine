# demos/dynamic_static_preview.py
from __future__ import annotations
import os, sys, time
from pathlib import Path

# Enable local imports when running the demo directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from task_registry import task, _REGISTRY
from execution_context import ExecutionContext
from orchestrator import Orchestrator
from promise import LimitedSpawns
from plan import From


# -------------------- Static tasks used by the entry --------------------

@task(static=True)
def static_prep(ctx: ExecutionContext, seed: int) -> int:
    ctx.log("static_prep:start", seed=seed)
    time.sleep(0.05)  # small stagger for nicer visuals
    out = seed + 10
    ctx.log("static_prep:done", out=out)
    return out


@task(static=True)
def static_validate(ctx: ExecutionContext, value: int) -> bool:
    ctx.log("static_validate:start", value=value)
    time.sleep(0.05)
    ok = (value % 2 == 0)
    ctx.log("static_validate:done", ok=ok)
    return ok


# -------------------- Static tasks used by each dynamic work unit --------------------

@task(static=True)
def post_a(ctx: ExecutionContext, v: int) -> int:
    ctx.log("post_a:start", v=v)
    out = v + 1
    ctx.log("post_a:done", out=out)
    return out


@task(static=True)
def post_b(ctx: ExecutionContext, v: int) -> int:
    ctx.log("post_b:start", v=v)
    out = v * 2
    ctx.log("post_b:done", out=out)
    return out

@task(static=True)
def post_c(ctx: ExecutionContext, a: int, b: int) -> int:
    ctx.log("post_c:start", a=a, b=b)
    out = a + b
    ctx.log("post_c:done", out=out)
    return out

# As soon as a work_unit becomes a REAL node, the orchestrator seeds a STATIC preview under it:
#   work_unit → post_a
#   work_unit → post_b
#   work_unit → post_c
# with dependencies:
#   post_a → post_c
#   post_b → post_c
@task(calls=[post_a, post_b, post_c], edges=[(post_a, post_c), (post_b, post_c)])
def work_unit(ctx: ExecutionContext, i: int) -> int:
    ctx.log("work_unit:start", i=i)
    base = i * i

    # actually spawn the static children that were previewed
    a_id = ctx.spawn(post_a, inputs={"v": base})
    b_id = ctx.spawn(post_b, inputs={"v": base})
    c_id = ctx.spawn(post_c, inputs={"a": From(a_id), "b": From(b_id)})

    [total] = ctx.wait_for_all([c_id])
    ctx.log("work_unit:done", base=base, total=total)
    return total

@task(calls=[static_prep, static_validate])
def main_pipeline(ctx: ExecutionContext, seed: int, n: int) -> int:
    # LimitedSpawns for dynamic work_unit tasks (shows a budget ghost that counts down)
    ctx.promise(LimitedSpawns(work_unit, max_count=n))
    
    # Materialize the entry's static preview
    prep_id = ctx.spawn(static_prep, inputs={"seed": seed})
    val_id  = ctx.spawn(static_validate, inputs={"value": seed})

    # Spawn n dynamic work units. As each work_unit becomes REAL, a static preview
    # for (post_a, post_b, post_c with a→c and b→c) is automatically created beneath it.
    work_ids = []
    for i in range(n):
        wid = ctx.spawn(work_unit, inputs={"i": i})
        work_ids.append(wid)

    vals = ctx.wait_for_all(work_ids)
    total = sum(vals)

    # also wait on the static entry children (not strictly required for the demo)
    _ = ctx.wait_for_all([prep_id, val_id])

    ctx.log("main_pipeline:done", total=total, spawned=len(work_ids))
    return total


if __name__ == "__main__":
    outdir = Path("outputs/dynamic_static_preview")
    orc = Orchestrator(_REGISTRY, output_dir=outdir)
    root = orc.start(entry=main_pipeline, inputs={"seed": 5, "n": 2})
    orc.run_to_completion(root)
    print(f"Done → {outdir} (events.jsonl, dots/, pngs/)")
