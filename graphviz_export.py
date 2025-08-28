# graphviz_export.py
from __future__ import annotations

import os
from pathlib import Path
from collections import defaultdict, deque
from typing import Optional

# These imports expect your existing project structure
from plan import Plan, Status


def build_dot(plan: Plan) -> str:
    # builds a DOT graph string from the current Plan.
    # solid edges = real data/scheduling deps (producer -> consumer)
    # dashed edges = lineage/parentage (parent_id -> child)

    # real edges (producers -> consumer)
    real_edges = {(e.from_id, e.to_id) for e in plan.edges}

    # build predecessor/successor maps for level calculation
    from collections import defaultdict, deque
    nodes = list(plan.taskInstances.keys())
    preds = defaultdict(set)   # to_id -> set(from_id)
    succs = defaultdict(set)   # from_id -> set(to_id)
    for a, b in real_edges:
        preds[b].add(a)
        succs[a].add(b)

    # longest-path levels via topological order
    indeg = {u: len(preds[u]) for u in nodes}
    level = {u: 0 for u in nodes}
    q = deque([u for u in nodes if indeg[u] == 0])
    while q:
        u = q.popleft()
        for v in succs[u]:
            level[v] = max(level.get(v, 0), level.get(u, 0) + 1)
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    # group children by parent for lineage (spawn edges)
    children_by_parent = defaultdict(list)
    for child in plan.taskInstances.values():
        pid = child.parent_id
        if pid is not None and pid in plan.taskInstances:
            children_by_parent[pid].append(child.id)

    # nudge parents one column left of their children
    vis_level = dict(level)
    for pid, kids in children_by_parent.items():
        if kids:
            min_kid_lvl = min(level.get(k, 0) for k in kids)
            vis_level[pid] = min(vis_level.get(pid, 0), min_kid_lvl - 1)

    # cluster nodes by visual level (rank=same => vertical column with rankdir=LR)
    groups = defaultdict(list)
    for nid, lvl in vis_level.items():
        groups[lvl].append(nid)

    # ---- DOT ----
    lines: list[str] = [
        "digraph G {",
        '  graph [rankdir=LR, splines=curved, nodesep=0.35, ranksep=0.6];',
        '  node  [shape=ellipse, fontsize=10];',
        '  edge  [arrowsize=0.7];',
    ]

    COLOR = {
        Status.PENDING:    ("gray80",      "black"),  # pending -> gray
        Status.RUNNING:    ("gold",        "black"),  # running -> yellow
        Status.SUCCESS:    ("palegreen3",  "black"),  # success -> green
        Status.FAILED:     ("tomato",      "white"),  # failed  -> red
        Status.TERMINATED: ("deepskyblue3","white"),  # terminated -> blue
    }
    for node in plan.taskInstances.values():
        uuid_slice = str(node.id)[:6]
        label = f"{node.spec_name}\\n{uuid_slice}"
        fill, font = COLOR.get(node.status, ("gray80", "black"))  # default to pending style
        lines.append(
            f'  "{node.id}" [label="{label}", style=filled, fillcolor="{fill}", fontcolor="{font}"];'
        )

    # real data/scheduling edges (producer -> consumer)
    for a, b in real_edges:
        lines.append(f'  "{a}" -> "{b}";')

    # rank groups (columns)
    for lvl in sorted(groups.keys()):
        ids = " ".join(f'"{nid}"' for nid in sorted(groups[lvl], key=str))
        lines.append(f"  {{ rank=same; {ids} }};")

    # parent left of its children: invisible, constraining edge to a child in the nearest column
    for pid, kids in children_by_parent.items():
        if not kids:
            continue
        anchor = min(kids, key=lambda k: vis_level.get(k, 0))
        lines.append(f'  "{pid}" -> "{anchor}" [style=invis, weight=40];')

    # left->right ordering of columns
    ordered_lvls = sorted(groups.keys())
    if len(ordered_lvls) >= 2:
        reps = [sorted(groups[l], key=str)[0] for l in ordered_lvls]
        for a, b in zip(reps, reps[1:]):
            lines.append(f'  "{a}" -> "{b}" [style=invis, weight=1];')

    # spawn (lineage) edges: dashed, gray, with arrowhead
    for pid, kids in children_by_parent.items():
        for kid in kids:
            if (pid, kid) not in real_edges:
                lines.append(
                    f'  "{pid}" -> "{kid}" '
                    f'[style=dashed, color="gray60", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.1];'
                )

    lines.append("}")
    return "\n".join(lines)


def write_dot_file(dot_str: str, dot_path: Path) -> None:
    dot_path = Path(dot_path)
    dot_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write(dot_str)


def render_png_file(dot_str: str, png_path: Path) -> bool:
    # render a PNG for the DOT content using python-graphviz package
    png_path = Path(png_path)
    png_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from graphviz import Source
        stem = png_path.stem
        directory = str(png_path.parent)
        src = Source(dot_str, filename=stem, directory=directory, format="png")
        src.render(cleanup=True)
        print("graphviz package working")
        return True
    except Exception:
        pass
    return False
