# graphviz_export.py
from __future__ import annotations
from pathlib import Path
from collections import defaultdict, deque
from typing import Any

from plan import Plan, Status

def build_dot(
    plan: Plan,
    spec_nodes: dict[str, dict] | None = None,   # ghost/speculative nodes
    spec_edges: set[tuple[str, str]] | None = None,  # (from_real_uuid_str -> ghost_id)
    static_nodes: dict[str, dict] | None = None,
    static_edges: set[tuple[str, str]] | None = None 
) -> str:
    # build a DOT graph string from the current Plan plus speculative (ghost) nodes, and static preview nodes
    spec_nodes = spec_nodes or {}
    spec_edges = spec_edges or set()
    static_nodes = static_nodes or {}
    static_edges = static_edges or set()

    # real graph
    real_edges = _compute_real_edges(plan)
    data_level = _compute_dataflow_levels(plan, real_edges)
    children_by_parent = _compute_children_by_parent(plan)

    # Visual placement
    vis_level = _compute_visual_levels(data_level, children_by_parent)   # parents nudged left
    static_level = _compute_static_levels(static_nodes, static_edges, vis_level)
    ghost_level = _compute_ghost_levels(spec_nodes, vis_level)           # ghosts: parent + 1
    groups = _group_by_level(vis_level, static_level, ghost_level)

    # Emit DOT
    lines: list[str] = []
    lines += _dot_header()

    # Nodes
    lines += _dot_real_nodes(plan, static_nodes)
    lines += _dot_ghost_nodes(spec_nodes)

    # Edges
    lines += _dot_real_edges(real_edges)
    lines += _dot_spawn_edges(children_by_parent, real_edges)
    lines += _dot_static_spawn_edges(static_nodes)   
    lines += _dot_static_edges(static_edges, static_nodes)
    lines += _dot_ghost_edges(spec_edges)

    # Ranks & ordering
    lines += _dot_rank_columns(groups)
    lines += _dot_invisible_chain(groups)

    lines.append("}")
    return "\n".join(lines)


def write_dot_file(dot_str: str, dot_path: Path) -> None:
    dot_path = Path(dot_path)
    dot_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write(dot_str)

def render_png_file(dot_str: str, png_path: Path) -> bool:
    # render DOT → PNG, return True if successful.
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
        return False

def _compute_real_edges(plan: Plan) -> set[tuple[Any, Any]]:
    return {(e.from_id, e.to_id) for e in plan.edges}

def _compute_dataflow_levels(plan: Plan, real_edges: set[tuple[Any, Any]]) -> dict[Any, int]:
    # Longest-path levels on REAL edges only (drives left→right order)
    nodes = list(plan.taskInstances.keys())
    preds: dict[Any, set[Any]] = defaultdict(set)
    succs: dict[Any, set[Any]] = defaultdict(set)
    for a, b in real_edges:
        preds[b].add(a)
        succs[a].add(b)

    indeg = {u: len(preds[u]) for u in nodes}
    level = {u: 0 for u in nodes}
    q = deque([u for u in nodes if indeg[u] == 0])

    while q:
        u = q.popleft()
        for v in succs[u]:
            level[v] = max(level[v], level[u] + 1)
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    return level

def _compute_children_by_parent(plan: Plan) -> dict[Any, list[Any]]:
    # Map: parent_id -> [real child ids] (spawn lineage)
    children_by_parent: dict[Any, list[Any]] = defaultdict(list)
    for ti in plan.taskInstances.values():
        pid = ti.parent_id
        if pid is not None and pid in plan.taskInstances:
            children_by_parent[pid].append(ti.id)
    return children_by_parent

def _compute_static_levels(
    static_nodes: dict[str, dict],
    static_edges: set[tuple[str, str]],
    vis_level: dict[Any, int],
) -> dict[str, int]:
    # Column placement for static preview nodes:
    # base: one column to the right of their (real) parent in vis_level
    # then push static dependents to the right using static->static edges
    if not static_nodes:
        return {}
    
    # base by parent (parent must be a REAL node id in vis_level)
    static_level: dict[str, int] = {}
    for sid, meta in static_nodes.items():
        parent = meta.get("parent")
        base = vis_level.get(parent, 0)  # defaults to 0 if parent unknown
        static_level[sid] = base + 1

    # respect static->static deps: only if both endpoints are static ids
    s_preds: dict[str, set[str]] = defaultdict(set)
    s_succs: dict[str, set[str]] = defaultdict(set)
    for a, b in static_edges:
        if a in static_nodes and b in static_nodes:
            s_preds[b].add(a)
            s_succs[a].add(b)

    s_indeg = {n: len(s_preds[n]) for n in static_nodes.keys()}
    s_q = deque([n for n in static_nodes.keys() if s_indeg.get(n, 0) == 0])
    while s_q:
        u = s_q.popleft()
        for v in s_succs[u]:
            static_level[v] = max(static_level.get(v, static_level[u] + 1), static_level[u] + 1)
            s_indeg[v] -= 1
            if s_indeg[v] == 0:
                s_q.append(v)

    return static_level

def _compute_visual_levels(
    data_level: dict[Any, int],
    children_by_parent: dict[Any, list[Any]],
) -> dict[Any, int]:
    # Visual level for REAL nodes: Start from data_level (data-flow).
    # Nudge each parent one column left of its leftmost child.
    vis = dict(data_level)
    for pid, kids in children_by_parent.items():
        if not kids:
            continue
        min_kid_lvl = min(data_level.get(k, 0) for k in kids)
        vis[pid] = min(vis.get(pid, min_kid_lvl - 1), min_kid_lvl - 1)
    return vis

def _compute_ghost_levels(
    spec_nodes: dict[str, dict],
    vis_level: dict[Any, int],
) -> dict[str, int]:
    # ghosts sit exactly one column to the right of their (real) parent.
    ghost_level: dict[str, int] = {}
    for gid, meta in spec_nodes.items():
        parent = meta.get("parent")
        base = vis_level.get(parent, 0) if parent in vis_level else 0
        ghost_level[gid] = base + 1
    return ghost_level

def _group_by_level(
    vis_level: dict[Any, int],
    static_level: dict[str, int],
    ghost_level: dict[str, int],
) -> dict[int, list[Any]]:
    groups: dict[int, list[Any]] = defaultdict(list)
    for nid, lvl in vis_level.items():
        groups[lvl].append(nid)
    for sid, lvl in static_level.items():
        groups[lvl].append(sid)
    for gid, lvl in ghost_level.items():
        groups[lvl].append(gid)
    return groups

def _dot_header() -> list[str]:
    return [
        "digraph G {",
        '  graph [rankdir=LR, splines=curved, nodesep=0.35, ranksep=0.6];',
        '  node  [shape=ellipse, fontsize=10];',
        '  edge  [arrowsize=0.7];',
    ]

def _dot_real_nodes(plan: Plan, static_nodes: dict[str, dict]) -> list[str]:
    COLOR = {
        Status.PENDING:    ("gray80",    "black"),
        Status.RUNNING:    ("gold",      "black"),
        Status.SUCCESS:    ("palegreen3","black"),
        Status.FAILED:     ("tomato",    "black"),
        Status.TERMINATED: ("orange",    "black"),
    }
    lines: list[str] = []
    for node in plan.taskInstances.values():
        uuid_slice = str(node.id)[:6]
        label = f"{node.spec_name}\\n{uuid_slice}"
        fill, font = COLOR.get(node.status, ("gray80", "black"))
        lines.append(
            f'  "{node.id}" [label="{label}", style=filled, fillcolor="{fill}", fontcolor="{font}"];'
        )
    for sid, meta in static_nodes.items():
        spec_name = meta.get("spec", "unknown")
        label = f"{spec_name}" 
        fill, font = COLOR[Status.PENDING]
        lines.append(
            f'  "{sid}" [label="{label}", style=filled, fillcolor="{fill}", fontcolor="{font}"];'
        )
    return lines

def _dot_ghost_nodes(spec_nodes: dict[str, dict]) -> list[str]:
    lines: list[str] = []
    for gid, meta in spec_nodes.items():
        spec_name = meta.get("spec", "unknown")
        if meta.get("source") == "budget" and "remaining" in meta:
            rem = int(meta["remaining"])
            label = f"{spec_name}\\n({rem} left)"
        else:
            label = f"{spec_name}\\n(spec)"
        lines.append(
            f'  "{gid}" [label="{label}", style="filled,dashed", fillcolor="white", color="black", fontcolor="gray30"];'
        )
    return lines

def _dot_real_edges(real_edges: set[tuple[Any, Any]]) -> list[str]:
    return [f'  "{a}" -> "{b}";' for a, b in real_edges]

def _dot_spawn_edges(
    children_by_parent: dict[Any, list[Any]],
    real_edges: set[tuple[Any, Any]],
) -> list[str]:
    # dashed lineage parent to child, only when no real data edge already exists
    lines: list[str] = []
    for pid, kids in children_by_parent.items():
        for kid in kids:
            if (pid, kid) in real_edges:
                continue
            lines.append(
                f'  "{pid}" -> "{kid}" '
                f'[style=dashed, color="gray60", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.1];'
            )
    return lines

def _dot_static_edges(
    static_edges: set[tuple[str, str]],
    static_nodes: dict[str, dict],
) -> list[str]:
    # Draw static preview edges:
    lines: list[str] = []
    for a, b in static_edges:
        a_is_static = a in static_nodes
        b_is_static = b in static_nodes

        if not a_is_static and b_is_static:
            # Real parent -> static child (spawn preview)
            lines.append(
                f'  "{a}" -> "{b}" '
                f'[style=dashed, color="gray60", constraint=false, '
                f'arrowhead=vee, arrowsize=0.7, penwidth=1.1];'
            )
        elif a_is_static and b_is_static:
            # Static dependency (e.g., B -> D)
            lines.append(f'  "{a}" -> "{b}" [constraint=true];')
        else:
            # Fallback: treat as a dependency edge
            lines.append(f'  "{a}" -> "{b}" [constraint=true];')

    return lines

def _dot_static_spawn_edges(static_nodes: dict[str, dict]) -> list[str]:
    # dashed gray edges: real parent UUID -> static placeholder id
    lines: list[str] = []
    for sid, meta in static_nodes.items():
        parent = meta.get("parent")
        if parent is None:
            continue
        lines.append(
            f'  "{parent}" -> "{sid}" '
            f'[style=dashed, color="gray60", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.1];'
        )
    return lines

def _dot_ghost_edges(spec_edges: set[tuple[str, str]]) -> list[str]:
    # Dotted edges from real parent (uuid string) to ghost id.
    return [
        f'  "{a}" -> "{b}" [style=dotted, color="gray50", constraint=false, arrowhead=vee, arrowsize=0.7, penwidth=1.0];'
        for a, b in spec_edges
    ]

def _dot_rank_columns(groups: dict[int, list[Any]]) -> list[str]:
    lines: list[str] = []
    for lvl in sorted(groups.keys()):
        ids = " ".join(f'"{nid}"' for nid in sorted(groups[lvl], key=str))
        lines.append(f"  {{ rank=same; {ids} }};")
    return lines

def _dot_invisible_chain(groups: dict[int, list[Any]]) -> list[str]:
    # invisible chain to encourage strict left→right column order
    lines: list[str] = []
    ordered_lvls = sorted(groups.keys())
    if len(ordered_lvls) >= 2:
        reps = [sorted(groups[l], key=str)[0] for l in ordered_lvls]
        for a, b in zip(reps, reps[1:]):
            lines.append(f'  "{a}" -> "{b}" [style=invis, weight=1];')
    return lines
