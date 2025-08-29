# graphviz_export.py
from __future__ import annotations

import os
from pathlib import Path
from collections import defaultdict, deque
from typing import Optional

# These imports expect your existing project structure
from plan import Plan, Status


def build_dot(
        plan: Plan, 
        spec_nodes: dict[str, dict] | None = None, 
        spec_edges: set[tuple[str, str]] | None = None
    ) -> str:
    # builds a DOT graph string from the current Plan.
    # solid edges = real data/scheduling deps (producer -> consumer)
    # dashed edges = lineage/parentage (parent_id -> child)

    spec_nodes = spec_nodes or {}
    spec_edges = spec_edges or set()

    # ---------------- Real graph ----------------
    real_edges = {(e.from_id, e.to_id) for e in plan.edges}
    nodes = list(plan.taskInstances.keys())

    from collections import defaultdict, deque
    preds = defaultdict(set)   # to_id -> {from_id}
    succs = defaultdict(set)   # from_id -> {to_id}
    for a, b in real_edges:
        preds[b].add(a)
        succs[a].add(b)

    # (1) dataflow longest-path level on REAL edges only
    indeg = {u: len(preds[u]) for u in nodes}
    data_level = {u: 0 for u in nodes}
    q = deque([u for u in nodes if indeg[u] == 0])
    while q:
        u = q.popleft()
        for v in succs[u]:
            data_level[v] = max(data_level[v], data_level[u] + 1)
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    # (2) lineage depth from parent_id chain (root=0, child=1, ...)
    parent_of = {nid: ti.parent_id for nid, ti in plan.taskInstances.items()}
    depth_cache: dict[object, int] = {}
    def lineage_depth(nid):
        if nid in depth_cache:
            return depth_cache[nid]
        p = parent_of.get(nid)
        d = 0 if (p is None or p not in parent_of) else lineage_depth(p) + 1
        depth_cache[nid] = d
        return d

    lin_depth = {nid: lineage_depth(nid) for nid in nodes}

    # (3) final column for REAL nodes
    final_level = {nid: max(data_level.get(nid, 0), lin_depth.get(nid, 0)) for nid in nodes}

    # real lineage (spawn) map for dashed edges
    children_by_parent = defaultdict(list)
    for child in plan.taskInstances.values():
        pid = child.parent_id
        if pid is not None and pid in plan.taskInstances:
            children_by_parent[pid].append(child.id)

    # ---------- GHOST placement ----------
    # spec_edges are (from_uuid_str, ghost_spec_id); place ghost to the right of its real parent
    ghost_level: dict[str, int] = {}
    for gid, meta in spec_nodes.items():
        # find a parent column via spec_edges if available
        parent_lvl = 0
        # there may be multiple incoming edges to same ghost; pick max parent lvl
        for (a, b) in spec_edges:
            if b == gid:
                # 'a' is a stringified UUID of a real node
                try:
                    parent_uuid = next(
                        nid for nid in final_level.keys() if str(nid) == a
                    )
                    parent_lvl = max(parent_lvl, final_level.get(parent_uuid, 0))
                except StopIteration:
                    pass
        ghost_level[gid] = parent_lvl + 1

    # group real + ghost by visual level
    groups = defaultdict(list)
    for nid, lvl in final_level.items():
        groups[lvl].append(nid)
    for gid, lvl in ghost_level.items():
        groups[lvl].append(gid)

    # ---------- DOT ----------
    lines: list[str] = [
        "digraph G {",
        '  graph [rankdir=LR, splines=curved, nodesep=0.35, ranksep=0.6];',
        '  node  [shape=ellipse, fontsize=10];',
        '  edge  [arrowsize=0.7];',
    ]

    # status colors
    COLOR = {
        Status.PENDING: ("gray80","black"),
        Status.RUNNING: ("gold","black"),
        Status.SUCCESS: ("palegreen3","black"),
        Status.FAILED: ("tomato","black"),
        Status.TERMINATED: ("orange","black"),
    }

    # real nodes
    for node in plan.taskInstances.values():
        uuid_slice = str(node.id)[:6]
        label = f"{node.spec_name}\\n{uuid_slice}"
        fill, font = COLOR.get(node.status, ("gray80", "black"))
        lines.append(f'  "{node.id}" [label="{label}", style=filled, fillcolor="{fill}", fontcolor="{font}"];')

    # ghost nodes
    for gid, meta in spec_nodes.items():
        spec_name = meta.get("spec", "unknown")
        # Budget placeholder from LimitedSpawns: show remaining count
        if meta.get("source") == "budget" and "remaining" in meta:
            rem = int(meta["remaining"])
            label = f"{spec_name}\\n({rem} left)"
        else:
            # Plain speculative/branching/preview ghost
            label = f"{spec_name}\\n(spec)"
        lines.append(
            f'  "{gid}" [label="{label}", style="filled,dashed", '
            f'fillcolor="white", color="black", fontcolor="gray30"];'
        )

    # real data edges
    for a, b in real_edges:
        lines.append(f'  "{a}" -> "{b}";')

    # dashed spawn edges (real parent -> real child) if not already real edge
    for pid, kids in children_by_parent.items():
        for kid in kids:
            if (pid, kid) not in real_edges:
                lines.append(
                    f'  "{pid}" -> "{kid}" '
                    f'[style=dashed, color="gray60", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.1];'
                )

    # dotted ghost edges (real parent -> ghost)
    for a, b in spec_edges:
        lines.append(
            f'  "{a}" -> "{b}" '
            f'[style=dotted, color="gray50", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.0];'
        )

    # rank columns (real + ghost)
    for lvl in sorted(groups.keys()):
        ids = " ".join(f'"{nid}"' for nid in sorted(groups[lvl], key=str))
        lines.append(f"  {{ rank=same; {ids} }};")

    # keep columns left->right with invisible rep chain
    ordered_lvls = sorted(groups.keys())
    if len(ordered_lvls) >= 2:
        reps = [sorted(groups[l], key=str)[0] for l in ordered_lvls]
        for a, b in zip(reps, reps[1:]):
            lines.append(f'  "{a}" -> "{b}" [style=invis, weight=1];')

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
        return True
    except Exception:
        pass
    return False
