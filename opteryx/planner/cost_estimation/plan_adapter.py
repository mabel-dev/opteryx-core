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


def _leaf_local_filter_conditions(plan: LogicalPlan, subplan_id: str) -> List[Node]:
    """Filter conditions that sit inside a leaf's subplan (already pushed down)."""
    out: List[Node] = []
    for _, node in _walk_subplan(plan, subplan_id):
        if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
            out.append(node.condition)
    return out


def _split_and(node: Optional[Node]) -> List[Node]:
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]
    return _split_and(node.left) + _split_and(node.right)


def _key_stats(scan_node, column_name: Optional[str]) -> KeyStats:
    """Resolve KeyStats for a column on a scan node, with safe fallbacks."""
    if scan_node is None or column_name is None:
        return KeyStats(ndv=None, null_fraction=None)
    manifest = getattr(scan_node, "manifest", None)
    if manifest is None:
        return KeyStats(ndv=None, null_fraction=None)
    ndv = manifest.estimate_cardinality(column_name)
    null_fraction = manifest.estimate_null_fraction(column_name)
    return KeyStats(ndv=ndv, null_fraction=null_fraction)


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
    leaf_local_above: List[Node],
) -> Optional[int]:
    """Estimate row count for a leaf.

    Sum of record counts from each scan in the leaf, multiplied by the
    selectivity of every leaf-local filter (above-chain or in-subplan).
    Returns None if no scan in the leaf carries a manifest — without a row
    count DPccp can't cost the plan and the caller bails.
    """
    if not leaf_rel_to_scan:
        return None
    base_rows = 0
    for _rel, scan in leaf_rel_to_scan.items():
        manifest = getattr(scan, "manifest", None)
        if manifest is None:
            return None
        base_rows += manifest.get_record_count()
    if base_rows <= 0:
        return 1

    # Apply selectivity from filters embedded in the leaf subplan.
    selectivity = 1.0
    for cond in _leaf_local_filter_conditions(plan, leaf_subplan_id):
        for pred in _split_and(cond):
            for _rel, scan in leaf_rel_to_scan.items():
                manifest = getattr(scan, "manifest", None)
                if manifest is None:
                    continue
                sel = manifest.estimate_selectivity(pred)
                if sel is not None:
                    selectivity *= sel
                break

    # Apply selectivity from leaf-local predicates that live in a Filter
    # *above* the chain (still apply to this leaf only).
    for pred in leaf_local_above:
        for _rel, scan in leaf_rel_to_scan.items():
            manifest = getattr(scan, "manifest", None)
            if manifest is None:
                continue
            sel = manifest.estimate_selectivity(pred)
            if sel is not None:
                selectivity *= sel
            break

    return max(1, int(base_rows * selectivity))


def _identifier_sources_in_subtree(node: Optional[Node]) -> Set[str]:
    """Collect every identifier ``source`` referenced anywhere in the subtree.

    A predicate that touches only one relation source is leaf-local. Used for
    routing single-table filters (``col op literal``, multi-conjunct OR-of-LIKE
    on one column, etc.) to the right leaf for cost-side selectivity scaling.
    """
    if node is None:
        return set()
    if node.node_type == NodeType.IDENTIFIER:
        src = getattr(node, "source", None)
        return {src} if src is not None else set()
    out: Set[str] = set()
    for attr in ("left", "right", "centre"):
        child = getattr(node, attr, None)
        if child is not None:
            out |= _identifier_sources_in_subtree(child)
    parameters = getattr(node, "parameters", None)
    if parameters:
        for p in parameters:
            out |= _identifier_sources_in_subtree(p)
    return out


def _classify_predicate(
    pred: Node, rel_to_leaf: Dict[str, int]
) -> Tuple[Optional[int], Optional[int], bool]:
    """Return ``(left_leaf, right_leaf, is_equality)`` for a predicate.

    ``left_leaf`` / ``right_leaf`` are leaf indices, or None if either side
    isn't a simple identifier bound to a tracked relation. For single-leaf
    predicates the two indices are equal.

    Falls back to subtree-wide identifier scanning when the predicate isn't a
    pure ``identifier op identifier`` comparison: any predicate whose
    identifier references all bind to a single leaf (``col op literal``,
    ``f(col) op literal``, OR-chains over one column, etc.) is leaf-local on
    that leaf.
    """
    if pred.node_type == NodeType.COMPARISON_OPERATOR:
        left_src = _identifier_source(pred.left)
        right_src = _identifier_source(pred.right)
        if left_src is not None and right_src is not None:
            left_leaf = rel_to_leaf.get(left_src)
            right_leaf = rel_to_leaf.get(right_src)
            return left_leaf, right_leaf, pred.value == "Eq"

    sources = _identifier_sources_in_subtree(pred)
    leaves = {rel_to_leaf[s] for s in sources if s in rel_to_leaf}
    if len(leaves) == 1:
        leaf = next(iter(leaves))
        return leaf, leaf, False
    return None, None, False


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

    # Partition predicates: leaf-local vs cross-leaf-equi vs other.
    leaf_local_per_leaf: List[List[Node]] = [[] for _ in leaves]
    cross_equi: List[Tuple[int, int, Node]] = []
    for pred in predicates:
        l, r, is_eq = _classify_predicate(pred, rel_to_leaf)
        if l is None or r is None:
            continue
        if l == r:
            leaf_local_per_leaf[l].append(pred)
        elif is_eq:
            cross_equi.append((l, r, pred))
        # cross-leaf non-equi: ignored for graph building (stays in upper Filter)

    # Resolve scan nodes per leaf (relation -> scan).
    per_leaf_scans: List[Dict[str, Any]] = [
        _leaf_relation_to_scan(plan, leaf.subplan_id, leaf.rel_names) for leaf in leaves
    ]

    # Build vertices.
    vertices: List[JoinVertex] = []
    for i, leaf in enumerate(leaves):
        rows = _leaf_row_count(
            plan, leaf.subplan_id, per_leaf_scans[i], leaf_local_per_leaf[i]
        )
        if rows is None:
            return None
        name = leaf.rel_names[0] if leaf.rel_names else f"leaf_{i}"
        vertices.append(JoinVertex(id=i, name=name, row_count=rows, payload=leaf))

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
        equi = ((_key_stats(left_scan, left_col), _key_stats(right_scan, right_col)),)
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
