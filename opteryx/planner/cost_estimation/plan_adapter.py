# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
LogicalPlan ↔ JoinGraph adapter.

Translates a chain of cross-joined leaves plus the predicates that constrain
them into the framework-free ``JoinGraph`` that DPccp consumes. Lives in
``cost_estimation`` rather than ``optimizer/strategies`` so it can be reused
and unit-tested without dragging in the full optimizer pipeline.

The chain shape and leaf descriptors come from
``cross_join_chain_reorder``; this module only consumes them.
"""

from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation.join_cardinality import KeyStats
from opteryx.planner.cost_estimation.join_graph import JoinEdge
from opteryx.planner.cost_estimation.join_graph import JoinGraph
from opteryx.planner.cost_estimation.join_graph import JoinVertex
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType


def _identifier_source(expr: Optional[Node]) -> Optional[str]:
    if expr is None or expr.node_type != NodeType.IDENTIFIER:
        return None
    return expr.source


def _identifier_column(expr: Optional[Node]) -> Optional[str]:
    if expr is None or expr.node_type != NodeType.IDENTIFIER:
        return None
    return getattr(expr, "source_column", None) or getattr(expr, "value", None)


def _walk_subplan(
    plan: LogicalPlan, root_id: str, visited: Optional[Set[str]] = None
):
    """Yield every (nid, node) under ``root_id``. Subqueries are opaque."""
    if visited is None:
        visited = set()
    if root_id in visited:
        return
    visited.add(root_id)
    node = plan[root_id]
    yield root_id, node
    if node.node_type == LogicalPlanStepType.Subquery:
        return
    for child_id, _, _ in plan.ingoing_edges(root_id):
        yield from _walk_subplan(plan, child_id, visited)


def _find_scan_for_relation(
    plan: LogicalPlan, subplan_id: str, relation_name: str
):
    """Find the Scan node inside ``subplan_id`` whose relation matches.

    Returns the LogicalPlanNode or None if no Scan with a manifest is present.
    """
    for _, node in _walk_subplan(plan, subplan_id):
        if node.node_type != LogicalPlanStepType.Scan:
            continue
        rel = getattr(node, "relation", None)
        alias = getattr(node, "alias", None)
        if rel == relation_name or alias == relation_name:
            return node
    return None


def _key_stats(scan_node, column_name: Optional[str]) -> KeyStats:
    """Resolve KeyStats for a column by reading the refreshed Scan stats."""
    if scan_node is None or column_name is None:
        return KeyStats(ndv=None, null_fraction=None)
    stats = getattr(scan_node, "statistics", None)
    if stats is None:
        return KeyStats(ndv=None, null_fraction=None)
    col = stats.columns.get(column_name)
    if col is None:
        return KeyStats(ndv=None, null_fraction=None)
    return KeyStats(ndv=col.distinct_count, null_fraction=col.null_fraction)


def _leaf_relation_to_scan(
    plan: LogicalPlan, leaf_subplan_id: str, leaf_rel_names: List[str]
) -> Dict[str, Any]:
    """Map relation_name → scan node for a leaf's subplan."""
    out: Dict[str, Any] = {}
    for rel in leaf_rel_names:
        scan = _find_scan_for_relation(plan, leaf_subplan_id, rel)
        if scan is not None:
            out[rel] = scan
    return out


def _leaf_row_count(
    plan: LogicalPlan,
    leaf_subplan_id: str,
    leaf_rel_to_scan: Dict[str, Any],
    leaf_local_above: List[Node],  # kept for signature stability; unused
) -> Optional[int]:
    """Estimate row count for a leaf by reading the refreshed Scan stats.

    Returns None if any scan in the leaf is missing statistics (refresh
    couldn't build them — typically because the scan has no manifest).

    The ``plan``, ``leaf_subplan_id``, and ``leaf_local_above`` parameters
    are preserved for signature stability with existing callers but are no
    longer used: filter selectivity is folded into Scan.statistics by
    statistics_refresh before any cost-based strategy runs.
    """
    if not leaf_rel_to_scan:
        return None
    total = 0
    for _rel, scan in leaf_rel_to_scan.items():
        stats = getattr(scan, "statistics", None)
        if stats is None:
            return None
        total += stats.row_count
    return max(1, total)


def _classify_predicate(
    pred: Node, rel_to_leaf: Dict[str, int]
) -> Tuple[Optional[int], Optional[int], bool]:
    """Return ``(left_leaf, right_leaf, is_equality)`` for a predicate.

    ``left_leaf`` / ``right_leaf`` are leaf indices, or None if either side
    isn't a simple identifier bound to a tracked relation. Single-relation
    predicates (column op literal) return (None, None, False) — their
    selectivity is already folded into Scan.statistics by the refresh pass.
    """
    if pred.node_type != NodeType.COMPARISON_OPERATOR:
        return None, None, False
    left_src = _identifier_source(pred.left)
    right_src = _identifier_source(pred.right)
    if left_src is None or right_src is None:
        return None, None, False
    left_leaf = rel_to_leaf.get(left_src)
    right_leaf = rel_to_leaf.get(right_src)
    return left_leaf, right_leaf, pred.value == "Eq"


def _build_equiv_tdoms(
    cross_equi: List[Tuple[int, int, Node]],
    per_leaf_scans: List[Dict[str, Any]],
    vertices: List[JoinVertex],
) -> Dict[Tuple[int, str], int]:
    """Compute tdom for each join column using equivalence sets (Ebergen 2022 §3.2).

    Columns that are transitively joined by equality form one equivalence set.
    tdom for the set = max(known NDVs in set) if any NDV is available,
                       min(row_count of leaves in set) otherwise.

    The fallback (min row_count) assumes the smallest table is the PK/dimension
    side, so its cardinality upper-bounds the number of distinct join-key values.
    This is strictly better than a magic constant because it uses actual table
    sizes from the manifest.

    Returns a dict mapping (leaf_idx, col_name) → tdom for every column seen in
    a join predicate. Columns not present in the returned dict had no join
    predicate and are unaffected.
    """
    parent: Dict[Tuple[int, str], Tuple[int, str]] = {}

    def find(key: Tuple[int, str]) -> Tuple[int, str]:
        if key not in parent:
            parent[key] = key
        root = key
        while parent[root] != root:
            root = parent[root]
        # Path compression.
        cur = key
        while cur != root:
            parent[cur], cur = root, parent[cur]
        return root

    def union(a: Tuple[int, str], b: Tuple[int, str]) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for left_leaf, right_leaf, pred in cross_equi:
        left_col = _identifier_column(pred.left)
        right_col = _identifier_column(pred.right)
        if left_col is not None and right_col is not None:
            union((left_leaf, left_col), (right_leaf, right_col))

    # Group members by representative.
    sets: Dict[Tuple[int, str], List[Tuple[int, str]]] = defaultdict(list)
    for key in parent:
        sets[find(key)].append(key)

    result: Dict[Tuple[int, str], int] = {}
    for members in sets.values():
        known_ndvs: List[int] = []
        leaf_set: set = set()
        for leaf_idx, col_name in members:
            leaf_set.add(leaf_idx)
            for scan in per_leaf_scans[leaf_idx].values():
                stats = getattr(scan, "statistics", None)
                if stats is None:
                    continue
                col_stat = stats.columns.get(col_name)
                if col_stat is not None and col_stat.distinct_count is not None:
                    known_ndvs.append(col_stat.distinct_count)

        tdom = max(known_ndvs) if known_ndvs else min(vertices[li].row_count for li in leaf_set)
        tdom = max(1, tdom)
        for member in members:
            result[member] = tdom

    return result


def build_join_graph(
    plan: LogicalPlan,
    leaves: List[Any],
    predicates: List[Node],
) -> Optional[JoinGraph]:
    """Build a JoinGraph from a leaf list and the predicates above the chain.

    ``leaves`` are the ``_Leaf`` objects from
    ``cross_join_chain_reorder._gather_leaves`` — each carries
    ``subplan_id`` and ``rel_names``.

    Returns None when the graph cannot be built (missing manifests, no
    edges, or the graph would be disconnected — DPccp requires connectivity).

    Non-equi cross-leaf predicates and any predicate the adapter doesn't
    understand are left in place; the caller's existing Filter nodes
    continue to enforce them after the rewrite.
    """
    if not leaves:
        return None

    rel_to_leaf: Dict[str, int] = {}
    for i, leaf in enumerate(leaves):
        for rel in leaf.rel_names:
            rel_to_leaf[rel] = i

    # Partition predicates: cross-leaf-equi only. Single-relation predicates
    # are no longer routed here — refresh has already folded their
    # selectivity into Scan.statistics.row_count.
    cross_equi: List[Tuple[int, int, Node]] = []
    for pred in predicates:
        l, r, is_eq = _classify_predicate(pred, rel_to_leaf)
        if l is None or r is None:
            continue
        if l != r and is_eq:
            cross_equi.append((l, r, pred))
        # Same-leaf predicates and cross-leaf non-equi predicates are
        # ignored for graph building (the original Filter still enforces
        # them after the rewrite).

    # Resolve scan nodes per leaf (relation -> scan).
    per_leaf_scans: List[Dict[str, Any]] = [
        _leaf_relation_to_scan(plan, leaf.subplan_id, leaf.rel_names) for leaf in leaves
    ]

    # Build vertices.
    vertices: List[JoinVertex] = []
    for i, leaf in enumerate(leaves):
        rows = _leaf_row_count(plan, leaf.subplan_id, per_leaf_scans[i], [])
        if rows is None:
            return None
        name = leaf.rel_names[0] if leaf.rel_names else f"leaf_{i}"
        vertices.append(JoinVertex(id=i, name=name, row_count=rows, payload=leaf))

    # Compute equivalence-set tdoms from all cross-equi predicates. When NDV is
    # absent from scan statistics (common with Parquet files) the tdom falls
    # back to min(row_count of leaves in the set), which is far better than the
    # flat 0.1 constant used by _key_selectivity. See Ebergen (2022) §3.2.
    equiv_tdoms = _build_equiv_tdoms(cross_equi, per_leaf_scans, vertices)

    def _key_stats_with_tdom(scan_node, col_name: Optional[str], leaf_idx: int) -> KeyStats:
        ks = _key_stats(scan_node, col_name)
        if ks.ndv is None and col_name is not None:
            tdom = equiv_tdoms.get((leaf_idx, col_name))
            if tdom is not None:
                ks = KeyStats(ndv=tdom, null_fraction=ks.null_fraction)
        return ks

    # Build edges, one JoinEdge per equality predicate (DPccp combines them).
    edges: List[JoinEdge] = []
    for left_leaf, right_leaf, pred in cross_equi:
        left_src = _identifier_source(pred.left)
        right_src = _identifier_source(pred.right)
        left_col = _identifier_column(pred.left)
        right_col = _identifier_column(pred.right)
        # Order endpoints so the edge's ``left`` matches the predicate's left.
        # If the predicate's left identifier belongs to the right leaf, swap.
        if rel_to_leaf.get(left_src) == right_leaf:
            left_leaf, right_leaf = right_leaf, left_leaf
            left_src, right_src = right_src, left_src
            left_col, right_col = right_col, left_col

        left_scan = per_leaf_scans[left_leaf].get(left_src) if left_src else None
        right_scan = per_leaf_scans[right_leaf].get(right_src) if right_src else None
        equi = ((
            _key_stats_with_tdom(left_scan, left_col, left_leaf),
            _key_stats_with_tdom(right_scan, right_col, right_leaf),
        ),)
        edges.append(
            JoinEdge(
                left=left_leaf,
                right=right_leaf,
                equi_keys=equi,
                extra_selectivity=1.0,
                payload=pred,
            )
        )

    if not edges:
        return None

    graph = JoinGraph(vertices=vertices, edges=edges)
    if not graph.is_connected(graph.full_mask):
        return None
    return graph
