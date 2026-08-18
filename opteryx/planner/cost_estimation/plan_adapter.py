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


def _walk_subplan_through_subqueries(
    plan: LogicalPlan, root_id: str, visited: Optional[Set[str]] = None
):
    """Like `_walk_subplan`, but descends PAST Subquery/CTE boundaries.

    `_walk_subplan` stops at a Subquery node by design (relation-NAME
    resolution must not reach past an opaque boundary -- two subqueries can
    legitimately share an inner alias). Verifying that every base source has
    a real row count is a different question with the opposite answer: a
    leaf whose `subplan_id` IS a CTE/subquery boundary (e.g. `cs_ui` in
    TPC-DS Q64) has its real Scan nodes ONLY reachable by continuing past
    it -- stopping there finds no source at all and reads as unbacked.
    """
    if visited is None:
        visited = set()
    if root_id in visited:
        return
    visited.add(root_id)
    node = plan[root_id]
    yield root_id, node
    for child_id, _, _ in plan.ingoing_edges(root_id):
        yield from _walk_subplan_through_subqueries(plan, child_id, visited)


def _subtree_sources_are_backed(plan: LogicalPlan, root_id: str) -> bool:
    """True iff every Scan/FunctionDataset reachable under `root_id` reports a
    REAL row count (manifest or schema estimate) -- never the
    `statistics_refresh._UNKNOWN_ROW_COUNT` placeholder substituted for one
    that can't report a size.

    Mirrors `result_size_guard._declared_row_count`'s precedence, which
    exists for exactly this reason: once `_UNKNOWN_ROW_COUNT` is folded into
    `.statistics.row_count`, nothing about that attribute distinguishes a
    real count from a fabricated one -- this must be checked against the
    same raw manifest/schema fields the guard reads, before that fold.
    False when no source at all is found (an empty subtree proves nothing).
    """
    saw_a_source = False
    for _, node in _walk_subplan_through_subqueries(plan, root_id):
        if node.node_type not in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset):
            continue
        saw_a_source = True
        manifest = getattr(node, "manifest", None)
        if manifest is not None:
            count = manifest.get_record_count()
            if count is not None and count > 0:
                continue
        schema = getattr(node, "schema", None)
        count = None
        if schema is not None:
            count = schema.row_count_metric or schema.row_count_estimate
        if count is None or count <= 0:
            return False
    return saw_a_source


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


def _leaf_row_count(plan: LogicalPlan, leaf_subplan_id: str) -> Optional[int]:
    """Estimate row count for a leaf by reading ITS OWN refreshed statistics.

    Reads ``plan[leaf_subplan_id].statistics.row_count`` directly rather than
    summing per-relation Scan stats. A leaf is not always a single bare Scan
    -- a CTE or subquery reference used inside a cross-join chain (e.g.
    ``cs_ui`` in TPC-DS Q64) is ONE leaf whose recorded relation names
    include internally-scoped relations that ``_find_scan_for_relation``
    cannot resolve (it stops at the opaque Subquery boundary by design).
    Summing scans that were never found left ``leaf_rel_to_scan`` empty for
    that leaf, returning None and aborting graph construction for the WHOLE
    chain rather than just that leaf -- DPccp then bailed out entirely and
    the chain fell back to naive left-deep cross-join ordering.
    ``statistics_refresh`` already computes a correct ``row_count`` for this
    exact subtree bottom-up, whatever shape it is; read that instead of
    re-deriving one from possibly-unreachable Scan nodes -- but only once
    ``_subtree_sources_are_backed`` confirms every source it was built from
    reported a real size, preserving the original "no fabricated number"
    refusal for a leaf like a manifest-less ``READ_JSONL`` FunctionDataset.
    """
    if not _subtree_sources_are_backed(plan, leaf_subplan_id):
        return None
    stats = getattr(plan[leaf_subplan_id], "statistics", None)
    if stats is None:
        return None
    return max(1, int(stats.row_count))


def _leaf_domain_row_count(plan: LogicalPlan, leaf_subplan_id: str) -> Optional[int]:
    """PRE-filter row count for a leaf — the ``_leaf_row_count`` counterpart.

    Reads ``RelationStatistics.domain_row_count`` (the base count refresh
    recorded before any selectivity was folded in) rather than ``row_count``,
    off the same leaf-subtree statistics ``_leaf_row_count`` reads. Returns
    None on the same terms so the caller's existing "no statistics → no
    graph" refusal is unchanged.
    """
    if not _subtree_sources_are_backed(plan, leaf_subplan_id):
        return None
    stats = getattr(plan[leaf_subplan_id], "statistics", None)
    if stats is None:
        return None
    return max(1, int(stats.domain_row_count))


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


def _group_equivalence_classes(
    cross_equi: List[Tuple[int, int, Node]],
) -> List[List[Tuple[int, str]]]:
    """Partition (leaf_idx, col_name) key references into equivalence classes.

    Columns transitively joined by equality (e.g. JOB's
    `t.id=mi.movie_id AND t.id=mk.movie_id AND mk.movie_id=mi.movie_id`) form
    one class — the three predicates all restate a single key identity.
    Returned in a deterministic order (by each class's smallest member) so
    class ids are stable across calls for the same input.
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

    return [sorted(members) for members in sorted(sets.values(), key=lambda m: min(m))]


def _build_equiv_tdoms(
    equivalence_classes: List[List[Tuple[int, str]]],
    per_leaf_scans: List[Dict[str, Any]],
    vertices: List[JoinVertex],
) -> Dict[Tuple[int, str], int]:
    """Compute tdom for each join column using equivalence sets (Ebergen 2022 §3.2).

    tdom for a set = max(known NDVs in set) if any NDV is available,
                     min(domain_row_count of leaves in set) otherwise.

    The fallback assumes the smallest table is the PK/dimension side, so its
    cardinality upper-bounds the number of distinct join-key values. This is
    strictly better than a magic constant because it uses actual table sizes
    from the manifest.

    ⚠️ That fallback reads the PRE-filter (``domain_row_count``) size, never the
    post-filter ``row_count``. A key domain is a property of the relation as
    stored; a filter removes ROWS, not the values the key column could hold.
    Dividing by the post-filter count instead charges the filter's selectivity
    a second time inside the divisor, and the error is not small: TPC-H Q09's
    ``p_name LIKE '%plum%'`` takes part 2,000,000 → 200,000, and the partkey
    class then priced ``part ⋈ lineitem`` at the full 59,986,052 instead of
    ~6M — the one join that had to happen first looked no cheaper than any
    other, so DPccp applied the query's only selective filter LAST and drove
    60M rows through four joins (1688ms; 567ms once this and the occupancy
    bound in ``dpccp._combine`` are both in). This mirrors the identical
    fallback in ``statistics_refresh._equi_key_classes``; the two paths must
    agree, or the tree-picker and the build-side chooser cost the same join
    differently.

    Returns a dict mapping (leaf_idx, col_name) → tdom for every column seen in
    a join predicate. Columns not present in the returned dict had no join
    predicate and are unaffected.
    """
    result: Dict[Tuple[int, str], int] = {}
    for members in equivalence_classes:
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

        tdom = (
            max(known_ndvs)
            if known_ndvs
            else min(vertices[li].domain_row_count for li in leaf_set)
        )
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
        rows = _leaf_row_count(plan, leaf.subplan_id)
        if rows is None:
            return None
        name = leaf.rel_names[0] if leaf.rel_names else f"leaf_{i}"
        vertices.append(
            JoinVertex(
                id=i,
                name=name,
                row_count=rows,
                payload=leaf,
                base_row_count=_leaf_domain_row_count(plan, leaf.subplan_id),
            )
        )

    # Compute equivalence-set tdoms from all cross-equi predicates. When NDV is
    # absent from scan statistics (common with Parquet files) the tdom falls
    # back to min(domain_row_count of leaves in the set), which is far better
    # than the flat 0.1 constant used by _key_selectivity. See Ebergen (2022)
    # §3.2, and _build_equiv_tdoms on why that is the PRE-filter count.
    equivalence_classes = _group_equivalence_classes(cross_equi)
    equiv_tdoms = _build_equiv_tdoms(equivalence_classes, per_leaf_scans, vertices)
    # Reverse lookup so edges can be tagged with the class they belong to —
    # DPccp/_combine uses this to dedupe redundant transitive-equality edges
    # when a chain of joins closes a cycle (see dpccp._combine).
    class_id_of: Dict[Tuple[int, str], int] = {
        member: class_idx
        for class_idx, members in enumerate(equivalence_classes)
        for member in members
    }

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
        class_id = class_id_of.get((left_leaf, left_col)) if left_col is not None else None
        edges.append(
            JoinEdge(
                left=left_leaf,
                right=right_leaf,
                equi_keys=equi,
                extra_selectivity=1.0,
                class_id=class_id,
                payload=pred,
            )
        )

    if not edges:
        return None

    graph = JoinGraph(vertices=vertices, edges=edges)
    if not graph.is_connected(graph.full_mask):
        return None
    return graph
