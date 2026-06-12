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
  * Limit — min(input, limit); NDVs cap at the new row count.
  * Distinct — group-by over all columns; histograms drop; NDVs cap.
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
    LogicalPlanStepType.HeapSort,
    LogicalPlanStepType.Exit,
    LogicalPlanStepType.Subquery,
    LogicalPlanStepType.CTE,
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


def _column_name(col) -> Optional[str]:
    """Best-effort column name extractor for join keys, group keys, etc."""
    if col is None:
        return None
    name = getattr(col, "source_column", None) or getattr(col, "value", None)
    if isinstance(name, str):
        return name
    return getattr(col, "name", None)


def _scan_stats(
    node: LogicalPlanNode,
    plan: Optional["LogicalPlan"] = None,
    nid: Optional[str] = None,
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
        and any(f.null_value_counts for f in (getattr(manifest, "files", None) or []))
    )
    if schema is not None:
        for col in schema.columns:
            col_name = getattr(col, "name", None)
            if not col_name:
                continue
            distinct_count = None
            value_range = ColumnRange()
            histogram = None
            null_fraction = None
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
            columns[col_name] = ColumnStatistics(
                column_name=col_name,
                data_type=str(getattr(col, "type", "")),
                distinct_count=distinct_count,
                value_range=value_range,
                histogram=histogram,
                null_fraction=null_fraction,
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
    # Filter node for the leaf-local walk to find, so narrow column ranges from
    # them directly. Row-count selectivity for these is already applied via the
    # scan's own row estimates upstream; here we only tighten the ranges.
    scan_predicates = getattr(node, "predicates", None)
    if scan_predicates:
        narrowed_columns = base.columns
        for condition in scan_predicates:
            narrowed_columns = _narrow_filter_columns(narrowed_columns, condition)
        if narrowed_columns is not base.columns:
            base = RelationStatistics(row_count=base.row_count, columns=narrowed_columns)

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
        narrowed_columns = _narrow_filter_columns(narrowed_columns, conj)

    if selectivity == 1.0 and narrowed_columns is base.columns:
        return base
    new_rows = base.row_count
    if selectivity != 1.0:
        new_rows = estimate_after_filter(base.row_count, selectivity)
    return RelationStatistics(row_count=new_rows, columns=narrowed_columns)


def _join_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    left, right = _split_join_children(child_stats)
    left = left or _empty_stats()
    right = right or _empty_stats()

    join_type = getattr(node, "type", "inner")

    if join_type == "cross join" or join_type is None:
        out_rows = max(1, left.row_count * right.row_count)
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
        return RelationStatistics(
            row_count=left.row_count,
            columns=_cap_ndvs(left.columns, left.row_count),
        )

    left_keys = _join_key_names(getattr(node, "left_columns", None))
    right_keys = _join_key_names(getattr(node, "right_columns", None))
    left_key = left_keys[0] if left_keys else None
    right_key = right_keys[0] if right_keys else None

    if left_key is None or right_key is None or join_type == "non equi":
        # Without a usable equi key, fall back to a cross-product upper bound;
        # JoinOrdering already guards against nested-loop blow-up by row count.
        out_rows = max(1, left.row_count * right.row_count)
        merged = _drop_histograms(_merge_columns(left, right))
        return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(merged, out_rows))

    left_col = left.get_column(left_key)
    right_col = right.get_column(right_key)
    left_key_stats = KeyStats(
        ndv=left_col.distinct_count if left_col else None,
        null_fraction=left_col.null_fraction if left_col else None,
    )
    right_key_stats = KeyStats(
        ndv=right_col.distinct_count if right_col else None,
        null_fraction=right_col.null_fraction if right_col else None,
    )
    out_rows = estimate_join_cardinality(
        left_rows=left.row_count,
        right_rows=right.row_count,
        join_type=_JOIN_TYPE_FOR_CARDINALITY[estimator_type],
        equi_keys=[(left_key_stats, right_key_stats)],
        extra_predicates_selectivity=1.0,
    )
    merged = _drop_histograms(_merge_columns(left, right))
    # Equi-join: matching join keys see their range intersected and NDV reduced
    # to min(left, right). Non-key columns just get NDV capped at output rows.
    merged = _intersect_join_keys(merged, left, right, left_keys, right_keys)
    return RelationStatistics(row_count=out_rows, columns=_cap_ndvs(merged, out_rows))


def _intersect_join_keys(
    merged: Dict[str, ColumnStatistics],
    left: RelationStatistics,
    right: RelationStatistics,
    left_keys: List[str],
    right_keys: List[str],
) -> Dict[str, ColumnStatistics]:
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
    group_names = [n for n in (_column_name(g) for g in groups) if n]
    if not group_names:
        return _empty_stats(row_count=1)
    ndvs = [
        base.columns[name].distinct_count if name in base.columns else None
        for name in group_names
    ]
    out_rows = estimate_group_by_cardinality(base.row_count, ndvs)
    # Output columns are the group keys; each row is now a unique combination,
    # so a single key's distinct_count is bounded above by the output row count.
    # Histograms drop because the group-by output's value distribution differs
    # from the input's (each group reduced to one row regardless of frequency).
    out_cols: Dict[str, ColumnStatistics] = {}
    for name in group_names:
        col = base.columns.get(name)
        if col is None:
            continue
        out_cols[name] = replace(col, histogram=None)
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


def _distinct_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    if not base.columns:
        return base
    ndvs = [col.distinct_count for col in base.columns.values()]
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
    columns: Dict[str, ColumnStatistics] = {}
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


def _cap_ndvs(columns: Dict[str, ColumnStatistics], row_count: int) -> Dict[str, ColumnStatistics]:
    """Return a new column dict where every distinct_count is capped at ``row_count``.

    A relation cannot contain more distinct values than rows. Called after any
    operator that reduces row count (Filter, Limit, Distinct, Group-by output).
    """
    out: Dict[str, ColumnStatistics] = {}
    for k, c in columns.items():
        if c.distinct_count is not None and c.distinct_count > row_count:
            out[k] = replace(c, distinct_count=max(1, int(row_count)))
        else:
            out[k] = c
    return out


def _drop_histograms(columns: Dict[str, ColumnStatistics]) -> Dict[str, ColumnStatistics]:
    """Return a copy with histograms removed.

    Called after any operator that distorts the underlying distribution
    (joins, group-by on the group keys, distinct, union). We don't try to
    rebuild — the input distogram no longer reflects the output, so it would
    mislead downstream cost estimation.
    """
    return {k: replace(c, histogram=None) for k, c in columns.items()}


def _narrow_filter_columns(
    columns: Dict[str, ColumnStatistics], condition
) -> Dict[str, ColumnStatistics]:
    """Apply a filter predicate's range constraints to column ranges.

    Walks the predicate AST collecting per-column (lower, upper) constraints
    from comparisons / BETWEEN / IN / equality. AND combines constraints
    (intersect bounds); OR / NOT bail out for that branch (no narrowing) since
    safe range tightening would require disjunction logic we don't have.
    """
    if condition is None:
        return columns
    constraints: Dict[str, Tuple[Optional[float], Optional[float], Optional[int]]] = {}
    _collect_range_constraints(condition, constraints)
    if not constraints:
        return columns

    out: Dict[str, ColumnStatistics] = dict(columns)
    for col_name, (lower, upper, eq_card) in constraints.items():
        if col_name not in out:
            continue
        col = out[col_name]
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
        out[col_name] = replace(col, value_range=new_range, distinct_count=new_ndv)
    return out


def _collect_range_constraints(
    node, sink: Dict[str, Tuple[Optional[float], Optional[float], Optional[int]]]
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
        col_name = _identifier_name(node.left)
        if col_name is None:
            return
        # BETWEEN ranges live on .right (lower) / .right.right? — manifest does
        # ``a = node.right; b = ...``; we read whichever attributes carry the
        # literals.
        a = _literal_value(getattr(node, "right", None))
        b = _literal_value(getattr(node, "centre", None))
        if a is None or b is None:
            return
        lo, hi = (a, b) if a <= b else (b, a)
        _merge_constraint(sink, col_name, lower=lo, upper=hi)
        return

    if nt == NodeType.COMPARISON_OPERATOR:
        op = getattr(node, "value", None)
        col_name = _identifier_name(node.left)
        literal = node.right
        if col_name is None:
            col_name = _identifier_name(node.right)
            literal = node.left
            op = _SWAPPED_COMPARISON.get(op, op)
        if col_name is None:
            return
        lit_value = _literal_value(literal)
        if lit_value is None:
            return
        if op == "Eq":
            _merge_constraint(sink, col_name, lower=lit_value, upper=lit_value, eq_card=1)
        elif op == "Lt":
            _merge_constraint(sink, col_name, upper=lit_value)
        elif op == "LtEq":
            _merge_constraint(sink, col_name, upper=lit_value)
        elif op == "Gt":
            _merge_constraint(sink, col_name, lower=lit_value)
        elif op == "GtEq":
            _merge_constraint(sink, col_name, lower=lit_value)
        elif op == "InList":
            values = _in_list_values(literal)
            if values:
                try:
                    lo, hi = min(values), max(values)
                except TypeError:
                    return
                _merge_constraint(sink, col_name, lower=lo, upper=hi, eq_card=len(values))
        return


_SWAPPED_COMPARISON = {
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def _identifier_name(node) -> Optional[str]:
    from opteryx.expression import NodeType

    if node is None or getattr(node, "node_type", None) != NodeType.IDENTIFIER:
        return None
    name = getattr(node, "source_column", None) or getattr(node, "value", None)
    return name if isinstance(name, str) else None


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
    sink: Dict[str, Tuple[Optional[float], Optional[float], Optional[int]]],
    col_name: str,
    lower=None,
    upper=None,
    eq_card: Optional[int] = None,
) -> None:
    cur_lower, cur_upper, cur_eq = sink.get(col_name, (None, None, None))
    if lower is not None:
        cur_lower = lower if cur_lower is None else max(cur_lower, lower)
    if upper is not None:
        cur_upper = upper if cur_upper is None else min(cur_upper, upper)
    if eq_card is not None:
        cur_eq = eq_card if cur_eq is None else min(cur_eq, eq_card)
    sink[col_name] = (cur_lower, cur_upper, cur_eq)


def _join_key_names(columns) -> List[str]:
    if not columns:
        return []
    out: List[str] = []
    for col in columns:
        name = _column_name(col)
        if name:
            out.append(name)
    return out


# ---- visitor -----------------------------------------------------------------


class StatisticsRefreshVisitor:
    """Bottom-up walker that attaches ``RelationStatistics`` to every node."""

    def __init__(self, plan: LogicalPlan):
        self.plan = plan
        self._visited: set = set()

    def run(self) -> None:
        for nid in self.plan.get_exit_points():
            self._visit(nid)

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
            return _scan_stats(node, self.plan, nid)
        if nt == LogicalPlanStepType.Filter:
            return _filter_stats(node, child_stats, self.plan, nid)
        if nt in (LogicalPlanStepType.Join, LogicalPlanStepType.DependentJoin):
            return _join_stats(node, child_stats)
        if nt == LogicalPlanStepType.AggregateAndGroup:
            return _aggregate_stats(node, child_stats)
        if nt == LogicalPlanStepType.Aggregate:
            return _empty_stats(row_count=1)
        if nt == LogicalPlanStepType.Limit:
            return _limit_stats(node, child_stats)
        if nt == LogicalPlanStepType.Distinct:
            return _distinct_stats(node, child_stats)
        if nt == LogicalPlanStepType.Union:
            return _union_stats(child_stats)
        if nt in (LogicalPlanStepType.Intersect, LogicalPlanStepType.Except):
            return _set_op_stats(node, child_stats)
        if nt in _PASS_THROUGH_TYPES:
            return _first_child_stats(child_stats) or _empty_stats()

        # Unknown node type — pass through whatever the first child produced.
        return _first_child_stats(child_stats) or _empty_stats()


def refresh_statistics(plan: LogicalPlan) -> LogicalPlan:
    """Recompute statistics for every node in ``plan``.

    Walks the plan bottom-up from each exit point and attaches a
    ``RelationStatistics`` to every node as ``node.statistics``. Clears the
    plan's ``statistics_are_stale`` flag on completion.
    """
    StatisticsRefreshVisitor(plan).run()
    plan.statistics_are_stale = False
    return plan
