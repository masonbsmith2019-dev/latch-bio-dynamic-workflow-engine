# Workflow Orchestration Engine

A lightweight Python workflow engine that executes user-defined tasks as a DAG. It supports **static previews at registration** (see an expected graph before anything runs) and **dynamic graph growth at runtime** (tasks can `spawn(...)` more tasks). A simple **promise/constraint layer** adds observability and guardrails—you can preview potential branches, cap parallelism, restrict spawns, and validate DAG mutations as they happen.

The engine renders the evolving plan to **Graphviz** (DOT → PNG) and logs every event to **JSONL** for replay and debugging.

---

## Highlights

* **Task DAG.** Each task is a node; edges represent **data dependencies** (solid) and **spawn/lineage** (dashed). Rendering handled by the Graphviz exporter. 
* **Static Preview.** At registration, declare `calls=[...]` and optional `edges=[(A, B), ...]` on a task to preview a deterministic sub-DAG under that task before anything runs. 
* **Dynamic Growth.** At runtime, tasks can `spawn(...)` additional tasks; inputs can reference upstream outputs using `From(task_id)`.  
* **Promises / Constraints (observability & guardrails).**

  * `LimitedSpawns(fn, max_count=k)` — cap how many children of a given type can be spawned from a parent. 
  * `MaxParallelism(label="x", k=n)` — bound concurrent tasks that share a label. 
  * `NoNewNodes` / `NoNewEdges` — enforce that a static task does not mutate the DAG shape. 
  * `OnlySpecificNodesAllowed`, `OnlySpecificEdgesAllowed` — preview/whitelist shapes for branching. 
* **Dataflow wiring.** Pipe outputs with `From(task_id)`; gather results with `wait_for_all([...])`. 
* **Multiprocessing runtime.** Isolated worker subprocesses, IPC queues, and a simple scheduler coordinate runs. 
* **Event-sourced logs.** Every state change is appended to `events.jsonl`. 
* **Graphviz renderer (visual semantics).**

  * **Solid black** = data dependency
  * **Dashed gray** = spawn/lineage (parent → child)
  * **Dotted gray** = speculative “ghosts” (e.g., budget previews) 

---

## System Design (High-Level)

* **Tasks** are plain Python callables decorated with `@task(...)`.

  * `static=True` marks a task as not allowed to change the DAG (auto-attaches `NoNewNodes`/`NoNewEdges`).  
  * `calls=[...]` and `edges=[(..., ...)]` describe a preview subgraph that’s shown immediately at registration and when real children materialize.  
* **Orchestrator** schedules ready tasks, enforces promises, materializes static previews, manages processes, and exports DOT/PNG on each state change. 
* **ExecutionContext** (injected into each task) provides:
  `spawn`, `wait_for_all`, `promise`, `log`, `emit`. 
* **Plan** tracks `TaskInstance`s, edges, and promises; diff application (`PlanDiff`) drives mutations. 
* **Promise Layer** validates each `PlanDiff`; violations are logged and the run halts safely.  
* **Renderer** converts the current plan (plus static previews and ghosts) into DOT/PNG for a step-by-step visual trace. 

---

## Repository Layout

* `orchestrator.py` — runtime loop; scheduling; process spawning; promise enforcement; static/dynamic preview seeding; DOT/PNG export. 
* `execution_context.py` — API used inside tasks (`spawn`, `wait_for_all`, `promise`, `log`, `emit`). 
* `plan.py` — DAG model: `TaskInstance`, `Edge`, `PlanDiff`, `From`, statuses, and dependency helpers. 
* `promise.py` — constraint primitives (`LimitedSpawns`, `MaxParallelism`, `NoNewNodes`, `NoNewEdges`, `OnlySpecific*`) and validation. 
* `task_registry.py` — `@task` decorator & registry; handles `static`, `calls`, `edges` metadata for previews. 
* `graphviz_export.py` — DOT builder with placement rules and line styles for real vs. static vs. ghost nodes. 
* `event_system.py` — append-only JSONL writer (auto-timestamps, JSON-safe encoding). 

---

## Demos (Examples)

**Confirmed working demos:**

* `demos/dynamic_static_preview.py` — entry task declares static children; dynamic children preview their own static subgraphs; shows budget “ghosts” via `LimitedSpawns`. 
* `demos/map_reduce.py` — fan-out squares via `spawn(...)`, then a reduce node with real data edges; demonstrates `LimitedSpawns` + `MaxParallelism`. 
* `demos/multi_branching.py` — branching with whitelisted shapes using `OnlySpecificNodesAllowed` and `OnlySpecificEdgesAllowed`. 

Other demos (may be in-progress or intentionally broken to illustrate failure modes):
`map_reduce_spawn_violation.py`, `static_preview.py`, `branch_violation.py`.

---

## Installation

1. **Graphviz (system package)**

   * macOS: `brew install graphviz`
   * Debian/Ubuntu: `sudo apt-get install graphviz`

2. **Python deps**

   ```bash
   pip install graphviz
   ```

---

## Running the Demos

```bash
# General
python demos/<demo_file>.py

# Examples
python demos/dynamic_static_preview.py
python demos/map_reduce.py
python demos/multi_branching.py
```

### Outputs

Each demo writes to `outputs/<demo_name>/`:

* `events.jsonl` — append-only event log (for replay/debug). 
* `dots/` and `pngs/` — step-by-step DOT and PNG snapshots of the evolving plan.  

---

## Presentation & Walkthrough

Videos and live demo clips:
[https://drive.google.com/file/d/1hcMQ0UdXL62wnORuYhIbDYu83dJ8rfno/view?usp=sharing](https://drive.google.com/file/d/1hcMQ0UdXL62wnORuYhIbDYu83dJ8rfno/view?usp=sharing)

---

## Notes

* Static tasks (`static=True`) automatically receive `NoNewNodes`/`NoNewEdges` when they run. If they propose DAG mutations, the engine records a violation and terminates remaining work safely.  
* Labels plus `MaxParallelism` give a simple global throttle for groups of tasks (e.g., “squares” in the map-reduce demo).  

---

### Quick Glance: Core API (inside a task)

```python
# spawn a child
child_id = ctx.spawn(child_fn, inputs={"x": 1}, labels={"batch"})

# depend on another task's output
from plan import From
other_id = ctx.spawn(other_fn, inputs={"x": 2})
use_id   = ctx.spawn(use_fn, inputs={"y": From(other_id)})

# wait for completion / gather outputs
result = ctx.wait_for_all([use_id])[0]

# promise constraints
from promise import LimitedSpawns, MaxParallelism
ctx.promise(LimitedSpawns(child_fn, max_count=3), MaxParallelism(label="batch", k=2))

# logging / emitting
ctx.log("doing work", step="phase1")
ctx.emit({"ok": True})
```

*(See `execution_context.py` for full details.)* 

---
