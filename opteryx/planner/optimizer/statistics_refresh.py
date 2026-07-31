# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statistics refresh pass.

Bottom-up traversal that recomputes per-node statistics on a logical plan.
Triggered by ``OptimizerVisitor.optimize`` immediately before any strategy
declared as ``optimization_technique = "cost"`` whenever the plan's
``statistics_are_stale`` flag is set.

Each visited node receives a ``statistics`` attribute holding a
``RelationStatistics``. Scans seed stats from their manifest; downstream
operators transform both row count *and* per-column stats (range, NDV)
according to their semantics:

  * Filter — row count × selectivity; predicate ranges intersect column
    ranges; NDVs cap at output row count.
  * Inner Join — join-key NDV estimate for row count; join-key ranges
    intersect; key NDV = min(left, right); histograms drop; non-key NDVs
    cap at output row count.
  * Cross Join — product of inputs; histograms drop.
  * Outer joins — bounded by the appropriate input.
  * Semi/anti — bounded by the left side; right columns dropped.
  * AggregateAndGroup — output rows = product of group-key NDVs (capped);
    only group-key columns survive; their histograms drop.
  * Aggregate (no groups) — 1 row.
  * Limit / HeapSort (OperatorFusion's fused Order+Limit) — min(input, limit);
    NDVs cap at the new row count.
  * Distinct — group-by over the columns the child actually outputs (not every
    column statistics_refresh still has attached); histograms drop; NDVs cap.
  * Union — sum of row counts; ranges widen (min lower / max upper); NDVs
    sum; histograms drop.
  * Project / pass-through — inherits child stats unchanged.

Histograms are never rebuilt — they are kept while the underlying
distribution shape is preserved (Filter, Limit) and dropped at the first
operator that distorts it (Join, Group-by output, Distinct, Union).

Consumers (JoinOrderingStrategy, JoinPlanningStrategy) currently still
read ``node.left_size`` / manifest directly; rewiring them to consume
``node.statistics`` is a follow-up.
"""

from dataclasses import replace
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import estimate_after_filter
from opteryx.planner.cost_estimation import estimate_group_by_cardinality
from opteryx.planner.cost_estimation import estimate_join_cardinality
from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics

# Fallback row count when a Scan has no manifest and no schema row estimate.
# Picked to be obviously synthetic but non-zero so downstream estimators don't
# divide-by-zero or collapse to 1-row plans.
_UNKNOWN_ROW_COUNT = 1_000_000

_PASS_THROUGH_TYPES = {
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.Exit,
    LogicalPlanStepType.Subquery,
    LogicalPlanStepType.Explain,
    LogicalPlanStepType.Show,
    LogicalPlanStepType.ShowColumns,
    LogicalPlanStepType.Set,
    LogicalPlanStepType.Comment,
    LogicalPlanStepType.Analyze,
}

# Map the local estimator vocabulary to cost_estimation.estimate_join_cardinality's
# vocabulary. The local names ("left", "right", "outer") were a System A
# convention; cost_estimation uses the planner's own join-type strings.
_JOIN_TYPE_FOR_CARDINALITY = {
    "inner": "inner",
    "left": "left outer",
    "right": "right outer",
    "outer": "full outer",
}


def _split_and_conjuncts(node):
    """Split an AND-tree into a flat list of conjuncts. Returns [node] for non-AND."""
    from opteryx.expression import NodeType  # lazy: avoid touching module-level imports

    if node is None:
        return []
    if getattr(node, "node_type", None) != NodeType.AND:
        return [node]
    return _split_and_conjuncts(node.left) + _split_and_conjuncts(node.right)


def _identifier_sources(node):
    """Collect every identifier ``source`` referenced anywhere in the subtree.

    Used to determine whether a predicate touches only one relation.
    """
    from opteryx.expression import NodeType

    if node is None:
        return set()
    if node.node_type == NodeType.IDENTIFIER:
        src = getattr(node, "source", None)
        return {src} if src is not None else set()
    out = set()
    for attr in ("left", "right", "centre"):
        child = getattr(node, attr, None)
        if child is not None:
            out |= _identifier_sources(child)
    parameters = getattr(node, "parameters", None)
    if parameters:
        for p in parameters:
            out |= _identifier_sources(p)
    return out


def _scan_relation_names(scan_node):
    """Names by which identifiers can refer to this scan: relation + alias."""
    names = set()
    rel = getattr(scan_node, "relation", None)
    if rel:
        names.add(rel)
    alias = getattr(scan_node, "alias", None)
    if alias:
        names.add(alias)
    return names


# Node types that the upward walk passes through transparently when looking
# for Filter conjuncts that constrain a particular Scan. Cross joins are
# transparent for this purpose because they don't change which conjuncts
# apply to which leaf — only relation membership does. Anything not in this
# set (Aggregate, Distinct, Subquery, post-pushdown Inner Join, etc.) stops
# the walk because the relationship between this Scan's rows and conjuncts
# above that node is no longer simple selectivity.
_UPWARD_TRANSPARENT_TYPES = frozenset({
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.HeapSort,
})


def _is_upward_transparent(node) -> bool:
    nt = node.node_type
    if nt in _UPWARD_TRANSPARENT_TYPES:
        return True
    # Unconverted cross joins are transparent for filter-binding purposes.
    if nt == LogicalPlanStepType.Join and getattr(node, "type", None) == "cross join":
        return True
    return False


def _collect_leaf_local_conjuncts(plan, scan_id, scan_names):
    """Walk upward from the Scan; return conjuncts whose identifiers all bind
    to ``scan_names``.

    Stops at any non-transparent ancestor (Aggregate, Subquery, post-pushdown
    Inner Join, Union, etc.). See _is_upward_transparent for the whitelist.
    """
    out = []
    visited = set()
    frontier = [scan_id]
    while frontier:
        nid = frontier.pop()
        for _, parent_id, _ in plan.outgoing_edges(nid):
            if parent_id in visited:
                continue
            visited.add(parent_id)
            parent = plan[parent_id]
            if not _is_upward_transparent(parent):
                continue  # do not recurse past this branch
            if parent.node_type == LogicalPlanStepType.Filter:
                cond = getattr(parent, "condition", None)
                if cond is not None:
                    for conj in _split_and_conjuncts(cond):
                        sources = _identifier_sources(conj)
                        if sources and sources <= scan_names:
                            out.append(conj)
            frontier.append(parent_id)
    return out


def _empty_stats(row_count: int = 0) -> RelationStatistics:
    return RelationStatistics(row_count=max(0, int(row_count)), columns={})


def _column_identity(col) -> Optional[bytes]:
    """Column *identity* extractor for join keys, group keys, etc.

    ``RelationStatistics.columns`` is keyed by identity, so every lookup path
    must resolve to one. Two shapes reach this:

      * Join keys arrive as raw identities already — ``bytes`` such as
        ``b'tes_c_c_r6Fy7PvB'`` — and are used verbatim.
      * Group keys arrive as column Nodes carrying ``.schema_column.identity``.

    Returns None when no identity can be resolved, in which case the caller
    treats the statistic as unknown rather than guessing by name. A name is
    never an acceptable substitute: see ``SchemaColumn.__post_init__``.
    """
    if col is None:
        return None
    if isinstance(col, bytes):
        return col
    schema_column = getattr(col, "schema_column", None)
    if schema_column is not None:
        identity = getattr(schema_column, "identity", None)
        if isinstance(identity, bytes):
            return identity
    identity = getattr(col, "identity", None)
    return identity if isinstance(identity, bytes) else None


def _predicate_note(nid, node_type, relation, condition, selectivity, stats=None) -> dict:
    """Self-contained telemetry record for one predicate's estimate.

    Cost is looked up from cost_estimation.predicate_cost -- the same model
    PredicateOrderingStrategy uses to order filters -- rather than duplicated
    here. Never raises: an unreadable condition just gets an empty rendering,
    telemetry must not be able to break query planning.

    `estimator` is diagnostic only -- which selectivity estimator tier fired
    for a LIKE-family predicate, computed by re-checking the same tier
    conditions the estimators themselves use (WITHOUT re-running estimation)
    rather than threading a return value out of them. See
    cost_estimation.selectivity.predicate_estimator_tag for the authoritative
    list of tags -- currently: "char_class_decay" | "flat_fallback" for an
    infix LIKE ('%foo%'); "ordinal_range" | "ordinal_bounds" | "flat_fallback"
    for a case-sensitive prefix LIKE ('foo%'); "char_class_prefix" |
    "flat_fallback" for a case-insensitive prefix LIKE; "char_class_suffix" |
    "flat_fallback" for a suffix LIKE ('%foo') of either case sensitivity.
    None for every other predicate kind, or when `stats` isn't available --
    omitting this field entirely reproduces prior behavior exactly, same
    contract as the rest of this module's telemetry.
    """
    from opteryx.expression import format_expression
    from opteryx.planner.cost_estimation.predicate_cost import predicate_cost

    try:
        condition_text = format_expression(condition)
    except Exception:
        condition_text = None
    try:
        cost = predicate_cost(condition)
    except Exception:
        cost = None
    estimator = None
    if stats is not None:
        try:
            from opteryx.planner.cost_estimation.selectivity import predicate_estimator_tag

            estimator = predicate_estimator_tag(condition, stats)
        except Exception:
            estimator = None
    return {
        "nid": nid,
        "node_type": node_type,
        "relation": relation,
        "condition": condition_text,
        "selectivity": selectivity,
        "cost": cost,
        "estimator": estimator,
    }


def _scan_stats(
    node: LogicalPlanNode,
    plan: Optional["LogicalPlan"] = None,
    nid: Optional[str] = None,
    predicate_notes: Optional[list] = None,
) -> RelationStatistics:
    schema = getattr(node, "schema", None)
    manifest = getattr(node, "manifest", None)

    # Row count: prefer manifest record count, fall back to schema estimates.
    row_count: Optional[int] = None
    if manifest is not None:
        try:
            row_count = manifest.get_record_count()
        except Exception:
            row_count = None
    if row_count is None and schema is not None:
        row_count = schema.row_count_metric or schema.row_count_estimate
    if row_count is None or row_count <= 0:
        row_count = _UNKNOWN_ROW_COUNT

    columns: dict = {}
    has_null_counts = (
        manifest is not None
        and any(
            (f.column_stats is not None and f.column_stats.has_any_null_counts())
            or bool(f.null_value_counts)
            for f in (getattr(manifest, "files", None) or [])
        )
    )
    if schema is not None:
        for col in schema.columns:
            col_name = getattr(col, "name", None)
            identity = getattr(col, "identity", None)
            if not col_name or not isinstance(identity, bytes):
                continue
            distinct_count = None
            value_range = ColumnRange()
            histogram = None
            null_fraction = None
            class_proportions = None
            avg_length = None
            ordinal_bounds = None
            length_bounds = None
            if manifest is not None:
                try:
                    distinct_count = manifest.estimate_cardinality(col_name)
                except Exception:
                    distinct_count = None
                try:
                    histogram = manifest.get_distogram(col_name)
                except Exception:
                    histogram = None
                if has_null_counts:
                    try:
                        null_fraction = manifest.estimate_null_fraction(col_name)
                    except Exception:
                        null_fraction = None
                try:
                    char_class_stats = manifest.get_char_class_stats(col_name)
                except Exception:
                    char_class_stats = None
                if char_class_stats is not None:
                    class_proportions, avg_length = char_class_stats
                try:
                    ordinal_bounds = manifest.get_ordinal_bounds(col_name)
                except Exception:
                    ordinal_bounds = None
                try:
                    length_bounds = manifest.get_length_bounds(col_name)
                except Exception:
                    length_bounds = None
            # Keyed by identity; the manifest accessors above are name-based
            # because manifest statistics are per-relation and unambiguous.
            columns[identity] = ColumnStatistics(
                column_name=col_name,
                data_type=str(getattr(col, "type", "")),
                distinct_count=distinct_count,
                value_range=value_range,
                histogram=histogram,
                null_fraction=null_fraction,
                class_proportions=class_proportions,
                avg_length=avg_length,
                ordinal_bounds=ordinal_bounds,
                length_bounds=length_bounds,
            )

    base = RelationStatistics(row_count=int(row_count), columns=columns)

    # Apply leaf-local filter selectivity from upward Filter ancestors.
    if plan is not None and nid is not None:
        scan_names = _scan_relation_names(node)
        if scan_names:
            conjuncts = _collect_leaf_local_conjuncts(plan, nid, scan_names)
            if conjuncts:
                from opteryx.planner.cost_estimation.selectivity import (
                    estimate_selectivity,
                )
                selectivity = 1.0
                narrowed_columns = base.columns
                for conj in conjuncts:
                    try:
                        s = float(estimate_selectivity(conj, base))
                    except Exception:
                        s = 1.0
                    selectivity *= s
                    if predicate_notes is not None:
                        predicate_notes.append(
                            _predicate_note(
                                nid, "Scan", getattr(node, "relation", None), conj, s, base
                            )
                        )
                    # Tighten column value_ranges from the filter's bounds so
                    # range-aware consumers (CorrelatedFilters, join-key
                    # intersection) see the post-filter range, not just a row count.
                    narrowed_columns = _narrow_filter_columns(narrowed_columns, conj)
                new_rows = base.row_count
                if selectivity != 1.0:
                    new_rows = max(1, int(base.row_count * selectivity))
                if new_rows != base.row_count or narrowed_columns is not base.columns:
                    base = RelationStatistics(row_count=new_rows, columns=narrowed_columns)

    # Predicates already pushed onto this scan (post-PredicatePushdown) have no
    # Filter node for the leaf-local walk to find, so apply the same selectivity
    # treatment here directly: reduce row_count and narrow column ranges. This
    # multiplies on top of whatever manifest/row-group pruning already reduced
    # `row_count` to above (min/max skipping whole row-groups) -- that pruning
    # says nothing about the match rate *within* a retained row-group, so it is
    # not a substitute for statistical selectivity, only a head start on it.
    scan_predicates = getattr(node, "predicates", None)
    if scan_predicates:
        from opteryx.planner.cost_estimation.selectivity import estimate_selectivity

        selectivity = 1.0
        narrowed_columns = base.columns
        for condition in scan_predicates:
            try:
                s = float(estimate_selectivity(condition, base))
            except Exception:
                s = 1.0
            selectivity *= s
            if predicate_notes is not None:
                predicate_notes.append(
                    _predicate_note(
                        nid, "Scan", getattr(node, "relation", None), condition, s, base
                    )
                )
            narrowed_columns = _narrow_filter_columns(narrowed_columns, condition)
        new_rows = base.row_count
        if selectivity != 1.0:
            new_rows = max(1, int(base.row_count * selectivity))
        if new_rows != base.row_count or narrowed_columns is not base.columns:
            base = RelationStatistics(row_count=new_rows, columns=narrowed_columns)

    return base


def _find_underlying_scan(plan: LogicalPlan, nid: str):
    """Walk the ingoing chain looking for a single underlying Scan node.

    Returns the Scan LogicalPlanNode when exactly one Scan is reachable
    through pass-through edges; returns None if a Join, Union, Aggregate,
    Set op, or branching is encountered (selectivity from a single-table
    fold doesn't apply cleanly in those cases).
    """
    current = nid
    seen: set = set()
    while True:
        if current in seen:
            return None
        seen.add(current)
        node = plan[current]
        if node.node_type == LogicalPlanStepType.Scan:
            return node
        if node.node_type in (
            LogicalPlanStepType.Join,
            LogicalPlanStepType.DependentJoin,
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Intersect,
            LogicalPlanStepType.Except,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Aggregate,
        ):
            return None
        edges = list(plan.ingoing_edges(current))
        if len(edges) != 1:
            return None
        current = edges[0][0]


def _filter_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
    plan: LogicalPlan,
    nid: str,
    predicate_notes: Optional[list] = None,
) -> RelationStatistics:
    """Apply selectivity for conjuncts that haven't already been folded into
    the underlying Scan stats by ``_scan_stats``.

    A conjunct is considered already-folded when both:
      * the Filter has a single underlying Scan reachable through
        pass-through nodes (``_find_underlying_scan`` returns it), and
      * every identifier in the conjunct binds to that Scan's relation.

    Conjuncts not satisfying both conditions still have an effect on row
    count and need their selectivity applied here.
    """
    base = _first_child_stats(child_stats) or _empty_stats()
    condition = getattr(node, "condition", None)
    if condition is None:
        return base

    underlying_scan = _find_underlying_scan(plan, nid)
    folded_names = (
        _scan_relation_names(underlying_scan) if underlying_scan is not None else set()
    )

    from opteryx.planner.cost_estimation.selectivity import estimate_selectivity

    selectivity = 1.0
    narrowed_columns = base.columns
    for conj in _split_and_conjuncts(condition):
        sources = _identifier_sources(conj)
        if folded_names and sources and sources <= folded_names:
            # Already folded into Scan.statistics (row count AND range) by _scan_stats.
            continue
        try:
            s = float(estimate_selectivity(conj, base))
        except Exception:
            s = 1.0
        selectivity *= s
        if predicate_notes is not None:
            predicate_notes.append(_predicate_note(nid, "Filter", None, conj, s, base))
        narrowed_columns = _narrow_filter_columns(narrowed_columns, conj)

    if selectivity == 1.0 and narrowed_columns is base.columns:
        return base
    new_rows = base.row_count
    if selectivity != 1.0:
        new_rows = estimate_after_filter(base.row_count, selectivity)
    return RelationStatistics(row_count=new_rows, columns=narrowed_columns)


def _join_note(nid, join_type, left_rows, right_rows, out_rows, key_count) -> dict:
    return {
        "nid": nid,
        "join_type": join_type,
        "left_row_count": left_rows,
        "right_row_count": right_rows,
        "row_count": out_rows,
        "key_count": key_count,
    }


def _join_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
    nid: Optional[str] = None,
    join_notes: Optional[list] = None,
) -> RelationStatistics:
    left, right = _split_join_children(child_stats)
    left = left or _empty_stats()
    right = right or _empty_stats()

    join_type = getattr(node, "type", "inner")

    if join_type == "cross join" or join_type is None:
        out_rows = max(1, left.row_count * right.row_count)
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, out_rows, 0)
            )
        merged = _drop_histograms(_merge_columns(left, right))
        return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(merged, out_rows))

    # Map planner join names to the estimator's vocabulary.
    estimator_type = "inner"
    if join_type in ("left outer", "left"):
        estimator_type = "left"
    elif join_type in ("right outer", "right"):
        estimator_type = "right"
    elif join_type in ("full outer", "outer"):
        estimator_type = "outer"
    elif join_type in ("left semi", "left anti", "left anti null-aware"):
        # Semi/anti emit only left-side columns; right contributes nothing.
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, left.row_count, 0)
            )
        return RelationStatistics(
            row_count=left.row_count,
            columns=_cap_ndvs(left.columns, left.row_count),
        )

    left_keys = _join_key_identities(getattr(node, "left_columns", None))
    right_keys = _join_key_identities(getattr(node, "right_columns", None))

    if not left_keys or not right_keys or join_type == "non equi":
        # Without a usable equi key, fall back to a cross-product upper bound;
        # JoinOrdering already guards against nested-loop blow-up by row count.
        out_rows = max(1, left.row_count * right.row_count)
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, out_rows, 0)
            )
        merged = _drop_histograms(_merge_columns(left, right))
        return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(merged, out_rows))

    # A composite equi-key (`ON a.x = b.x AND a.y = b.y`) must have EVERY key
    # pair's selectivity multiplied in -- estimate_join_cardinality already
    # does this correctly given a multi-entry equi_keys list (see its
    # independence-assumption docstring), and _intersect_join_keys below
    # already loops over every pair. Using only keys[0] silently dropped every
    # key past the first, understating a composite key's true selectivity and
    # overestimating the join (the same "signal exists, never reaches the
    # estimator" shape as the pushed-predicate and DISTINCT-NDV gaps above).
    equi_keys = []
    for left_key, right_key in zip(left_keys, right_keys):
        left_col = left.get_column(left_key)
        right_col = right.get_column(right_key)
        left_ndv = left_col.distinct_count if left_col else None
        right_ndv = right_col.distinct_count if right_col else None
        # When NDV is absent on both sides (common with Parquet files that omit
        # distinct-count row-group statistics), fall back to tdom = min(row_counts).
        # Assuming FK-PK structure, the smaller relation upper-bounds the distinct
        # key count, giving a far better estimate than a flat 0.1 selectivity.
        # Ebergen (2022) §3.2.  Only fires when BOTH sides are unknown — if one
        # side has a real NDV, _key_selectivity already uses it.
        if left_ndv is None and right_ndv is None:
            tdom = max(1, min(left.row_count, right.row_count))
            left_ndv = tdom
            right_ndv = tdom
        equi_keys.append((
            KeyStats(ndv=left_ndv, null_fraction=left_col.null_fraction if left_col else None),
            KeyStats(ndv=right_ndv, null_fraction=right_col.null_fraction if right_col else None),
        ))

    out_rows = estimate_join_cardinality(
        left_rows=left.row_count,
        right_rows=right.row_count,
        join_type=_JOIN_TYPE_FOR_CARDINALITY[estimator_type],
        equi_keys=equi_keys,
        extra_predicates_selectivity=1.0,
    )
    if join_notes is not None:
        join_notes.append(
            _join_note(nid, join_type, left.row_count, right.row_count, out_rows, len(equi_keys))
        )
    merged = _drop_histograms(_merge_columns(left, right))
    # Equi-join: matching join keys see their range intersected and NDV reduced
    # to min(left, right). Non-key columns just get NDV capped at output rows.
    merged = _intersect_join_keys(merged, left, right, left_keys, right_keys)
    return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(merged, out_rows))


def _intersect_join_keys(
    merged: Dict[bytes, ColumnStatistics],
    left: RelationStatistics,
    right: RelationStatistics,
    left_keys: List[bytes],
    right_keys: List[bytes],
) -> Dict[bytes, ColumnStatistics]:
    """Intersect ranges and reduce NDV for equi-join keys on both sides."""
    out = dict(merged)
    for lk, rk in zip(left_keys, right_keys):
        l_col = left.columns.get(lk)
        r_col = right.columns.get(rk)
        if l_col is None or r_col is None:
            continue
        intersected_range = l_col.value_range.intersect(r_col.value_range)
        new_ndv: Optional[int] = None
        if l_col.distinct_count is not None and r_col.distinct_count is not None:
            new_ndv = min(l_col.distinct_count, r_col.distinct_count)
        elif l_col.distinct_count is not None:
            new_ndv = l_col.distinct_count
        elif r_col.distinct_count is not None:
            new_ndv = r_col.distinct_count
        for key in (lk, rk):
            if key in out:
                out[key] = replace(out[key], value_range=intersected_range, distinct_count=new_ndv)
    return out


def _aggregate_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    groups = getattr(node, "groups", None) or []
    group_keys = [k for k in (_column_identity(g) for g in groups) if k]
    if not group_keys:
        return _empty_stats(row_count=1)
    ndvs = [
        base.columns[key].distinct_count if key in base.columns else None
        for key in group_keys
    ]
    out_rows = estimate_group_by_cardinality(base.row_count, ndvs)
    # Output columns are the group keys; each row is now a unique combination,
    # so a single key's distinct_count is bounded above by the output row count.
    # Histograms drop because the group-by output's value distribution differs
    # from the input's (each group reduced to one row regardless of frequency).
    out_cols: Dict[bytes, ColumnStatistics] = {}
    for key in group_keys:
        col = base.columns.get(key)
        if col is None:
            continue
        out_cols[key] = replace(col, histogram=None)
    return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(out_cols, out_rows))


def _limit_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    limit = getattr(node, "limit", None)
    if limit is None:
        return base
    try:
        capped = min(int(base.row_count), int(limit))
    except (TypeError, ValueError):
        return base
    new_rows = max(0, capped)
    # Limit doesn't change ranges or distributions of *which* values appear,
    # but it does cap how many distinct values can be present.
    return RelationStatistics(row_count=new_rows, columns=_cap_ndvs(base.columns, new_rows))


def _child_output_identities(plan: Optional["LogicalPlan"], nid: Optional[str]) -> Optional[set]:
    """Column identities the single child of ``nid`` actually outputs, if known.

    Distinct has no output-column list of its own -- it dedups whatever tuple
    its one child emits, and ProjectionPushdown narrows that child's `.columns`
    to the real output set (e.g. a Scan feeding `SELECT DISTINCT n_name` has
    `.columns == [n_name]` even though its RelationStatistics still carries
    every column in the table). Returns None when there is no single child or
    its output list is unavailable, so the caller can fall back safely.
    """
    if plan is None or nid is None:
        return None
    edges = list(plan.ingoing_edges(nid))
    if len(edges) != 1:
        return None
    child = plan[edges[0][0]]
    columns = getattr(child, "columns", None)
    if not columns:
        return None
    out = {identity for identity in (_column_identity(c) for c in columns) if identity}
    return out or None


def _distinct_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
    plan: Optional["LogicalPlan"] = None,
    nid: Optional[str] = None,
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    if not base.columns:
        return base

    # Scope the NDV product to the columns actually being distinct-ed. Without
    # this, a `SELECT DISTINCT n_name` still sees every column statistics_refresh
    # attached to the underlying Scan (Project/pass-through nodes don't narrow
    # RelationStatistics.columns), so an unrelated high-cardinality column (a
    # timestamp, an id, ...) inflates the NDV product and the result collapses
    # right back to the input row count via the cap below -- silently
    # defeating the estimate DISTINCT was supposed to produce.
    scoped_identities = _child_output_identities(plan, nid)
    columns_for_ndv = base.columns
    if scoped_identities:
        relevant = {k: v for k, v in base.columns.items() if k in scoped_identities}
        if relevant:
            columns_for_ndv = relevant

    ndvs = [col.distinct_count for col in columns_for_ndv.values()]
    out_rows = estimate_group_by_cardinality(base.row_count, ndvs)
    # Distinct collapses duplicates — distribution shape changes (histograms
    # invalid); each column's NDV is bounded by the output row count.
    out_cols = _drop_histograms(base.columns)
    return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(out_cols, out_rows))


def _union_stats(
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    """UNION ALL — sum row counts; widen each column's range (lower=min, upper=max).

    NDV is summed as a loose upper bound; histograms drop because we don't try
    to merge the two distributions. UNION (distinct) callers can apply a
    Distinct on top of this and the NDV cap will tighten.
    """
    rows = 0
    columns: Dict[bytes, ColumnStatistics] = {}
    for cs, _ in child_stats:
        if cs is None:
            continue
        rows += cs.row_count
        for k, v in cs.columns.items():
            existing = columns.get(k)
            if existing is None:
                columns[k] = replace(v, histogram=None)
                continue
            # Widen range, sum NDVs.
            new_lower = _min_or_none(existing.value_range.lower_bound, v.value_range.lower_bound)
            new_upper = _max_or_none(existing.value_range.upper_bound, v.value_range.upper_bound)
            new_ndv: Optional[int] = None
            if existing.distinct_count is not None and v.distinct_count is not None:
                new_ndv = existing.distinct_count + v.distinct_count
            columns[k] = replace(
                existing,
                value_range=ColumnRange(lower_bound=new_lower, upper_bound=new_upper),
                distinct_count=new_ndv,
                histogram=None,
            )
    return RelationStatistics(row_count=rows, columns=_cap_ndvs(columns, rows))


def _min_or_none(a, b):
    if a is None:
        return b
    if b is None:
        return a
    try:
        return min(a, b)
    except TypeError:
        return a


def _max_or_none(a, b):
    if a is None:
        return b
    if b is None:
        return a
    try:
        return max(a, b)
    except TypeError:
        return a


def _set_op_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    """INTERSECT / EXCEPT — bounded by the left input."""
    left, _ = _split_join_children(child_stats) if len(child_stats) >= 2 else (None, None)
    if left is None:
        return _first_child_stats(child_stats) or _empty_stats()
    return left


# ---- helpers -----------------------------------------------------------------


def _first_child_stats(
    child_stats: Iterable[Tuple[Optional[RelationStatistics], str]],
) -> Optional[RelationStatistics]:
    for cs, _ in child_stats:
        if cs is not None:
            return cs
    return None


def _split_join_children(
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> Tuple[Optional[RelationStatistics], Optional[RelationStatistics]]:
    left = right = None
    for cs, rel in child_stats:
        if rel == "left" and left is None:
            left = cs
        elif rel == "right" and right is None:
            right = cs
    if left is None or right is None:
        # Edge labels missing — fall back to insertion order.
        ordered = [cs for cs, _ in child_stats]
        if left is None and ordered:
            left = ordered[0]
        if right is None and len(ordered) > 1:
            right = ordered[1]
    return left, right


def _merge_columns(left: RelationStatistics, right: RelationStatistics) -> dict:
    merged = dict(left.columns)
    for k, v in right.columns.items():
        merged.setdefault(k, v)
    return merged


def _cap_ndvs(columns: Dict[bytes, ColumnStatistics], row_count: int) -> Dict[bytes, ColumnStatistics]:
    """Return a new column dict where every distinct_count is capped at ``row_count``.

    A relation cannot contain more distinct values than rows. Called after any
    operator that reduces row count (Filter, Limit, Distinct, Group-by output).
    """
    out: Dict[bytes, ColumnStatistics] = {}
    for k, c in columns.items():
        if c.distinct_count is not None and c.distinct_count > row_count:
            out[k] = replace(c, distinct_count=max(1, int(row_count)))
        else:
            out[k] = c
    return out


def _drop_histograms(columns: Dict[bytes, ColumnStatistics]) -> Dict[bytes, ColumnStatistics]:
    """Return a copy with histograms removed.

    Called after any operator that distorts the underlying distribution
    (joins, group-by on the group keys, distinct, union). We don't try to
    rebuild — the input distogram no longer reflects the output, so it would
    mislead downstream cost estimation.
    """
    return {k: replace(c, histogram=None) for k, c in columns.items()}


def _narrow_filter_columns(
    columns: Dict[bytes, ColumnStatistics], condition
) -> Dict[bytes, ColumnStatistics]:
    """Apply a filter predicate's range constraints to column ranges.

    Walks the predicate AST collecting per-column (lower, upper) constraints
    from comparisons / BETWEEN / IN / equality. AND combines constraints
    (intersect bounds); OR / NOT bail out for that branch (no narrowing) since
    safe range tightening would require disjunction logic we don't have.

    Constraints are keyed by column identity, so the bounds intersected below
    always come from the same column — the ``max``/``min`` are unguarded on
    purpose, because a type error there now means a genuine defect upstream
    rather than two unrelated columns having been merged.
    """
    if condition is None:
        return columns
    constraints: Dict[bytes, Tuple[Optional[float], Optional[float], Optional[int]]] = {}
    _collect_range_constraints(condition, constraints)
    if not constraints:
        return columns

    out: Dict[bytes, ColumnStatistics] = dict(columns)
    for identity, (lower, upper, eq_card) in constraints.items():
        if identity not in out:
            continue
        col = out[identity]
        # Intersect with current range.
        new_lower = col.value_range.lower_bound
        new_upper = col.value_range.upper_bound
        if lower is not None:
            new_lower = lower if new_lower is None else max(new_lower, lower)
        if upper is not None:
            new_upper = upper if new_upper is None else min(new_upper, upper)
        new_range = ColumnRange(lower_bound=new_lower, upper_bound=new_upper)
        new_ndv = col.distinct_count
        if eq_card is not None:
            new_ndv = eq_card if new_ndv is None else min(new_ndv, eq_card)
        out[identity] = replace(col, value_range=new_range, distinct_count=new_ndv)
    return out


def _collect_range_constraints(
    node, sink: Dict[bytes, Tuple[Optional[float], Optional[float], Optional[int]]]
) -> None:
    """Walk an AND-conjunction of comparisons and accumulate per-column bounds.

    Constraints are stored as ``(lower, upper, equality_cardinality)``. The
    cardinality slot caps NDV for ``=`` and ``IN`` predicates; bounds tighten
    ranges for comparisons and ``BETWEEN``.
    """
    from opteryx.expression import NodeType

    if node is None:
        return
    nt = getattr(node, "node_type", None)
    if nt == NodeType.AND:
        _collect_range_constraints(node.left, sink)
        _collect_range_constraints(node.right, sink)
        return
    if nt in (NodeType.OR, NodeType.NOT):
        return  # Can't safely tighten bounds across disjunction / negation.

    if nt == NodeType.BETWEEN:
        identity = _identifier_identity(node.left)
        if identity is None:
            return
        # BETWEEN ranges live on .right (lower) / .right.right? — manifest does
        # ``a = node.right; b = ...``; we read whichever attributes carry the
        # literals.
        a = _literal_value(getattr(node, "right", None))
        b = _literal_value(getattr(node, "centre", None))
        if a is None or b is None:
            return
        lo, hi = (a, b) if a <= b else (b, a)
        _merge_constraint(sink, identity, lower=lo, upper=hi)
        return

    if nt == NodeType.COMPARISON_OPERATOR:
        op = getattr(node, "value", None)
        identity = _identifier_identity(node.left)
        literal = node.right
        if identity is None:
            identity = _identifier_identity(node.right)
            literal = node.left
            op = _SWAPPED_COMPARISON.get(op, op)
        if identity is None:
            return
        lit_value = _literal_value(literal)
        if lit_value is None:
            return
        if op == "Eq":
            _merge_constraint(sink, identity, lower=lit_value, upper=lit_value, eq_card=1)
        elif op == "Lt":
            _merge_constraint(sink, identity, upper=lit_value)
        elif op == "LtEq":
            _merge_constraint(sink, identity, upper=lit_value)
        elif op == "Gt":
            _merge_constraint(sink, identity, lower=lit_value)
        elif op == "GtEq":
            _merge_constraint(sink, identity, lower=lit_value)
        elif op == "InList":
            values = _in_list_values(literal)
            if values:
                try:
                    lo, hi = min(values), max(values)
                except TypeError:
                    return
                _merge_constraint(sink, identity, lower=lo, upper=hi, eq_card=len(values))
        return


_SWAPPED_COMPARISON = {
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def _identifier_identity(node) -> Optional[bytes]:
    """Identity of an IDENTIFIER node, for ``RelationStatistics.columns`` lookup.

    Keyed on identity rather than ``source_column`` so that constraints from
    ``it1.info``, ``mi.info`` and ``mi_idx.info`` stay three separate entries
    instead of being merged into one bogus intersected range.
    """
    from opteryx.expression import NodeType

    if node is None or getattr(node, "node_type", None) != NodeType.IDENTIFIER:
        return None
    schema_column = getattr(node, "schema_column", None)
    identity = getattr(schema_column, "identity", None) if schema_column is not None else None
    return identity if isinstance(identity, bytes) else None


def _literal_value(node):
    from opteryx.expression import NodeType

    if node is None or getattr(node, "node_type", None) != NodeType.LITERAL:
        return None
    value = getattr(node, "value", None)
    if getattr(value, "item", None) is not None:
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def _in_list_values(node) -> List:
    """Best-effort extraction of literal values from an IN list expression."""
    if node is None:
        return []
    value = getattr(node, "value", None)
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    parameters = getattr(node, "parameters", None) or []
    out = []
    for p in parameters:
        v = _literal_value(p)
        if v is not None:
            out.append(v)
    return out


def _merge_constraint(
    sink: Dict[bytes, Tuple[Optional[float], Optional[float], Optional[int]]],
    identity: bytes,
    lower=None,
    upper=None,
    eq_card: Optional[int] = None,
) -> None:
    cur_lower, cur_upper, cur_eq = sink.get(identity, (None, None, None))
    if lower is not None:
        cur_lower = lower if cur_lower is None else max(cur_lower, lower)
    if upper is not None:
        cur_upper = upper if cur_upper is None else min(cur_upper, upper)
    if eq_card is not None:
        cur_eq = eq_card if cur_eq is None else min(cur_eq, eq_card)
    sink[identity] = (cur_lower, cur_upper, cur_eq)


def _join_key_identities(columns) -> List[bytes]:
    if not columns:
        return []
    out: List[bytes] = []
    for col in columns:
        identity = _column_identity(col)
        if identity:
            out.append(identity)
    return out


# ---- visitor -----------------------------------------------------------------


class StatisticsRefreshVisitor:
    """Bottom-up walker that attaches ``RelationStatistics`` to every node.

    ``telemetry``, when given, additionally records per-predicate selectivity
    and cost and per-join cardinality inputs as they're computed (see
    ``_predicate_note``/``_join_note``) -- diagnostic detail only, never
    consulted for correctness. ``run()`` also then records one row-count
    entry per node so estimated-vs-actual comparisons don't require a second
    plan walk elsewhere.
    """

    def __init__(self, plan: LogicalPlan, telemetry=None):
        self.plan = plan
        self._visited: set = set()
        self.telemetry = telemetry
        self.predicate_notes: Optional[list] = [] if telemetry is not None else None
        self.join_notes: Optional[list] = [] if telemetry is not None else None

    def run(self) -> None:
        for nid in self.plan.get_exit_points():
            self._visit(nid)
        if self.telemetry is not None:
            self._record_telemetry()

    def _record_telemetry(self) -> None:
        row_counts = []
        for nid, node in self.plan.nodes(True):
            stats = getattr(node, "statistics", None)
            if stats is None:
                continue
            row_counts.append({
                "nid": nid,
                "node_type": node.node_type.name,
                "relation": getattr(node, "relation", None),
                "row_count": stats.row_count,
            })
        self.telemetry._reading["estimated_row_counts"] = row_counts
        self.telemetry._reading["predicate_estimates"] = self.predicate_notes
        self.telemetry._reading["join_estimates"] = self.join_notes

    def _visit(self, nid: str) -> None:
        if nid in self._visited:
            return
        self._visited.add(nid)

        # Children first.
        child_stats: List[Tuple[Optional[RelationStatistics], str]] = []
        for child_id, _, relationship in self.plan.ingoing_edges(nid):
            self._visit(child_id)
            child_stats.append(
                (getattr(self.plan[child_id], "statistics", None), relationship)
            )

        node = self.plan[nid]
        node.statistics = self._compute(node, child_stats, nid)

    def _compute(
        self,
        node: LogicalPlanNode,
        child_stats: List[Tuple[Optional[RelationStatistics], str]],
        nid: str,
    ) -> RelationStatistics:
        nt = node.node_type

        if nt == LogicalPlanStepType.Scan:
            return _scan_stats(node, self.plan, nid, self.predicate_notes)
        if nt == LogicalPlanStepType.Filter:
            return _filter_stats(node, child_stats, self.plan, nid, self.predicate_notes)
        if nt in (LogicalPlanStepType.Join, LogicalPlanStepType.DependentJoin):
            return _join_stats(node, child_stats, nid, self.join_notes)
        if nt == LogicalPlanStepType.AggregateAndGroup:
            return _aggregate_stats(node, child_stats)
        if nt == LogicalPlanStepType.Aggregate:
            return _empty_stats(row_count=1)
        if nt in (LogicalPlanStepType.Limit, LogicalPlanStepType.HeapSort):
            return _limit_stats(node, child_stats)
        if nt == LogicalPlanStepType.Distinct:
            return _distinct_stats(node, child_stats, self.plan, nid)
        if nt == LogicalPlanStepType.Union:
            return _union_stats(child_stats)
        if nt in (LogicalPlanStepType.Intersect, LogicalPlanStepType.Except):
            return _set_op_stats(node, child_stats)
        if nt in _PASS_THROUGH_TYPES:
            return _first_child_stats(child_stats) or _empty_stats()

        # Unknown node type — pass through whatever the first child produced.
        return _first_child_stats(child_stats) or _empty_stats()


def refresh_statistics(plan: LogicalPlan, telemetry=None) -> LogicalPlan:
    """Recompute statistics for every node in ``plan``.

    Walks the plan bottom-up from each exit point and attaches a
    ``RelationStatistics`` to every node as ``node.statistics``. Clears the
    plan's ``statistics_are_stale`` flag on completion.

    When ``telemetry`` is given, also records the planner's per-node row-count
    estimates, per-predicate selectivity/cost, and per-join cardinality inputs
    onto ``telemetry._reading`` (``estimated_row_counts`` / ``predicate_estimates``
    / ``join_estimates``) -- diagnostic detail for comparing estimate vs actual,
    never consulted by planning itself. Omitting ``telemetry`` (the default)
    skips this entirely; existing callers are unaffected.
    """
    StatisticsRefreshVisitor(plan, telemetry).run()
    plan.statistics_are_stale = False
    return plan
