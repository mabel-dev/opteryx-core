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
  * ASOF — EQUAL to the left side (the operator emits exactly one row per
    left row, null-filled on no match), with or without a partition key;
    both sides' columns survive.
  * AggregateAndGroup — output rows = min(input rows, product of group-key
    NDVs); a key with NO NDV (even after the manifest's range-derived
    fallback) makes the estimate the input row count — the only sound cap —
    rather than a fabricated per-key factor. Only group-key columns survive;
    their histograms drop.
  * Aggregate (no groups) — 1 row.
  * Limit / HeapSort (OperatorFusion's fused Order+Limit) — min(input, limit);
    NDVs cap at the new row count. A LIMIT pushed into a Scan (which deletes
    the Limit node) caps that scan's count the same way.
  * Distinct — group-by over the columns the child actually outputs (not every
    column statistics_refresh still has attached); histograms drop; NDVs cap.
  * Union — sum of row counts; ranges widen (min lower / max upper); NDVs
    sum; histograms drop.
  * Project / pass-through — inherits child stats unchanged.

Histograms are never rebuilt — they are kept while the underlying
distribution shape is preserved (Filter, Limit) and dropped at the first
operator that distorts it (Join, Group-by output, Distinct, Union).

Every ``RelationStatistics`` carries its row count as either
``row_count_metric`` (a number we claim to KNOW: a manifest count, or exact
arithmetic over metric inputs — cross-join product, LIMIT min, UNION ALL sum,
no-group aggregate = 1) or ``row_count_estimate`` (anything touched by a
selectivity or NDV heuristic). The plan-time result-size guard acts only on
metrics; estimates defer to the runtime row counter.

Consumers (JoinOrderingStrategy, JoinPlanningStrategy) currently still
read ``node.left_size`` / manifest directly; rewiring them to consume
``node.statistics`` is a follow-up.
"""

from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation.join_cardinality import NdvProvenance
from opteryx.planner.cost_estimation import apply_occupancy_bound
from opteryx.planner.cost_estimation import composite_key_ndv
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
    LogicalPlanStepType.Order,
    LogicalPlanStepType.Exit,
    LogicalPlanStepType.Subquery,
    LogicalPlanStepType.Explain,
    LogicalPlanStepType.Show,
    LogicalPlanStepType.ShowColumns,
    LogicalPlanStepType.ShowManifest,
    LogicalPlanStepType.ShowSnapshots,
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
    """Split an AND-tree into a flat list of conjuncts. Returns [node] for non-AND.

    Delegates to ``_inner_split`` — the one existing definition of this split
    (split_conjunctive_predicates.py), also used by compiler.py and mermaid.py —
    so the statistics pass sees the same terms the physical FILTER does.

    Writing the AND recursion here a second time is what caused the gap this
    delegation closes: PredicateOrderingStrategy folds a filter chain into a
    single n-ary AND (``NodeType.DNF``, terms in ``parameters``), which the local
    recursion returned WHOLE as one opaque conjunct. Every consumer downstream —
    the leaf-local scan fold, per-conjunct range narrowing, the predicate
    telemetry — then saw one unreadable term instead of the real conditions.
    """
    from .strategies.split_conjunctive_predicates import _inner_split  # lazy: import cycle

    if node is None:
        return []
    return _inner_split(node)


def _identifier_sources(node):
    """Collect every identifier ``source`` referenced anywhere in the subtree.

    Used to determine whether a predicate touches only one relation.
    """
    from opteryx.expression import NodeType

    if node is None:
        return set()
    if node.node_type == NodeType.IDENTIFIER:
        src = node.source
        return {src} if src is not None else set()
    out = set()
    if node.left is not None:
        out |= _identifier_sources(node.left)
    if node.right is not None:
        out |= _identifier_sources(node.right)
    if node.centre is not None:
        out |= _identifier_sources(node.centre)
    parameters = node.parameters
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


def _empty_stats(row_count: int = 0, metric: bool = False) -> RelationStatistics:
    """Stand-in statistics. ``metric=True`` only where the count is exact by
    construction (a no-group aggregate emits exactly one row); the default is
    an estimate — a missing child's stats are a fabrication, not knowledge."""
    count = max(0, int(row_count))
    if metric:
        return RelationStatistics(columns={}, row_count_metric=count)
    return RelationStatistics(columns={}, row_count_estimate=count)


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


def _referenced_scan_identities(node: LogicalPlanNode):
    """The column identities this query can consult on this scan: the scan's
    own (pushdown-pruned) output columns plus any column its pushed predicates
    read. Everything downstream of the scan — filter selectivity, join-key
    intersection, byte-size scaling — keys into this set; a column the query
    never references cannot influence an estimate, so its manifest walk is
    pure waste (a 105-column ClickBench scan touching 2 columns paid for 105).

    Returns None when the referenced set cannot be established (no seeded
    columns) — the caller then computes statistics for every schema column.
    """
    from opteryx.expression import NodeType
    from opteryx.expression import get_all_nodes_of_type

    columns = node.columns
    if not columns:
        return None
    wanted = set()
    for col in columns:
        schema_column = col.schema_column
        if schema_column is not None and isinstance(schema_column.identity, bytes):
            wanted.add(schema_column.identity)
    for predicate in node.predicates or []:
        for ident in get_all_nodes_of_type(predicate, (NodeType.IDENTIFIER,)):
            schema_column = ident.schema_column
            if schema_column is not None and isinstance(schema_column.identity, bytes):
                wanted.add(schema_column.identity)
    return frozenset(wanted) if wanted else None


def _scan_base_stats(node: LogicalPlanNode, wanted=None) -> RelationStatistics:
    """The manifest/schema-derived statistics of a scan, BEFORE any predicate
    narrowing — a per-column walk of the manifest (cardinality, distogram,
    value range, null fraction, char-class, ordinal/length bounds, bytes).

    `wanted` (a frozenset of column identities, or None for "all") limits the
    walk to the columns the query actually references — see
    _referenced_scan_identities.

    Everything read here is immutable for the life of a plan: the schema and
    manifest objects are shared by reference across plan copies (neither has a
    ``copy`` method, so Node's property copier passes them through), so the
    result is memoized by ``_scan_stats`` across statistics refreshes. The
    returned object is therefore SHARED — treat it, its column dict, and its
    ColumnStatistics entries as immutable; narrowing builds new objects.
    """
    schema = node.schema
    manifest = node.manifest

    # Row count: prefer manifest record count, fall back to schema estimates.
    # Track PROVENANCE alongside the number: a manifest record count or a
    # schema row_count_metric is a metric (we claim to know it); a schema
    # row_count_estimate is an estimate; the _UNKNOWN_ROW_COUNT stand-in is
    # the most fabricated estimate of all.
    row_count: Optional[int] = None
    row_count_is_metric = False
    if manifest is not None:
        try:
            row_count = manifest.get_record_count()
        except Exception:
            row_count = None
        row_count_is_metric = row_count is not None
    if row_count is None and schema is not None:
        row_count = schema.row_count_metric
        row_count_is_metric = row_count is not None
        if row_count is None:
            row_count = schema.row_count_estimate
    if row_count is None or row_count <= 0:
        row_count = _UNKNOWN_ROW_COUNT
        row_count_is_metric = False

    columns: dict = {}
    has_null_counts = (
        manifest is not None
        and any(
            (f.column_stats is not None and f.column_stats.has_any_null_counts())
            or bool(f.null_value_counts)
            for f in (manifest.files or [])
        )
    )
    if schema is not None:
        for col in schema.columns:
            col_name = col.name
            identity = col.identity
            if wanted is not None and identity not in wanted:
                continue
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
            total_bytes = None
            if manifest is not None:
                try:
                    distinct_count = manifest.estimate_cardinality(col_name)
                except Exception:
                    distinct_count = None
                if distinct_count is None:
                    # No KMV sketch (nothing ANALYZE'd — the norm for plain
                    # parquet). Fall back to the range-derived estimate built
                    # from per-file row counts and min/max bounds; costing
                    # only — the KMV method keeps its near-exact semantics
                    # for the execution-variant strategies that rely on it.
                    try:
                        distinct_count = manifest.estimate_range_cardinality(col_name)
                    except Exception:
                        distinct_count = None
                try:
                    histogram = manifest.get_distogram(col_name)
                except Exception:
                    histogram = None
                # Manifest min/max -> value_range. Without this the field was
                # left empty here and only ever written by
                # _narrow_filter_columns, so a column with no predicate on it
                # -- which is every join key -- had no range at all, and the
                # parquet footer's real per-row-group bounds were discarded.
                try:
                    bounds = manifest.get_value_range(col_name)
                except Exception:
                    bounds = None
                # NUMERIC ONLY, deliberately. The manifest returns bounds
                # decoded for integer columns but as RAW SERIALIZED BYTES for
                # strings and decimals (ps_supplycost comes back as
                # b'\x00...d'). value_range holds logical values that
                # _narrow_filter_columns intersects against predicate literals
                # with a bare max()/min() -- unguarded on purpose, so that a
                # type error there means a genuine upstream defect. Feeding it
                # undecoded bytes IS that defect: bytes vs str raises. Ints and
                # floats are the only bounds that arrive already decoded, and
                # they are exactly the ones the NDV-span bound can use.
                if bounds is not None and all(
                    type(b) in (int, float) for b in bounds
                ):
                    value_range = ColumnRange(lower_bound=bounds[0], upper_bound=bounds[1])
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
                try:
                    total_bytes = manifest.get_total_uncompressed_size(col_name)
                except Exception:
                    total_bytes = None
            # Byte-size estimate, in priority order: real measured manifest
            # bytes (above) > avg_length (ANALYZE'd string columns) > fixed
            # physical width (row_count * DrakenType.fixed_itemsize(), the
            # single canonical native width table -- see core/buffers.h).
            # None -- never a fabricated guess -- when none of these signals
            # are available (e.g. a variable-width column with no ANALYZE
            # pass and no manifest-level size).
            if total_bytes is None and avg_length is not None:
                total_bytes = int(avg_length * row_count)
            if total_bytes is None:
                column_type = col.column_type
                physical = None if column_type is None else column_type.physical
                if physical is not None:
                    fixed_width = physical.fixed_itemsize()
                    if fixed_width:
                        total_bytes = int(fixed_width) * int(row_count)
            # Keyed by identity; the manifest accessors above are name-based
            # because manifest statistics are per-relation and unambiguous.
            col_type = col.column_type
            columns[identity] = ColumnStatistics(
                column_name=col_name,
                data_type=str(col_type) if col_type is not None else "",
                distinct_count=distinct_count,
                value_range=value_range,
                histogram=histogram,
                null_fraction=null_fraction,
                class_proportions=class_proportions,
                avg_length=avg_length,
                ordinal_bounds=ordinal_bounds,
                length_bounds=length_bounds,
                total_bytes=total_bytes,
            )

    # `row_count` here is the relation's pre-filter size -- the domain the two
    # selectivity passes below shrink. Carry it as base_row_count so join-key
    # tdom estimates divide by the domain rather than by the filtered count.
    if row_count_is_metric:
        return RelationStatistics(
            columns=columns, row_count_metric=int(row_count), base_row_count=int(row_count)
        )
    return RelationStatistics(
        columns=columns, row_count_estimate=int(row_count), base_row_count=int(row_count)
    )


def _scan_stats(
    node: LogicalPlanNode,
    plan: Optional["LogicalPlan"] = None,
    nid: Optional[str] = None,
    predicate_notes: Optional[list] = None,
    base_stats_cache: Optional[dict] = None,
    fold_registry: Optional[Dict[int, object]] = None,
) -> RelationStatistics:
    # The base (pre-narrowing) statistics depend only on the scan's schema and
    # manifest. Both are shared by reference across plan copies and the node's
    # uuid is preserved by LogicalPlanNode.copy, so within one optimization run
    # the base is memoizable — keyed by object identity. Manifests are
    # immutable by contract: every prune (ManifestPruning/TopNManifestPruning/
    # LimitFilesPruning/statistics_only_response) is copy-on-write and assigns
    # a NEW Manifest to node.manifest (see Manifest.subset), so id(manifest)
    # misses here and the base recomputes over the pruned file set — an
    # in-place prune would have kept serving pre-pruning statistics. A
    # strategy that replaces the schema misses the same way. The narrowing
    # below is predicate- and plan-shape-dependent and always re-runs.
    wanted = _referenced_scan_identities(node)
    base = None
    cache_key = None
    if base_stats_cache is not None:
        # `wanted` is part of the key: projection pushdown prunes the scan's
        # columns mid-optimization, and a base computed for the wide set must
        # not answer for the narrow one (or vice versa).
        cache_key = (node.uuid, id(node.schema), id(node.manifest), wanted)
        base = base_stats_cache.get(cache_key)
    if base is None:
        base = _scan_base_stats(node, wanted)
        if cache_key is not None:
            base_stats_cache[cache_key] = base

    # Apply leaf-local filter selectivity from upward Filter ancestors.
    if plan is not None and nid is not None:
        scan_names = _scan_relation_names(node)
        if scan_names:
            conjuncts = _collect_leaf_local_conjuncts(plan, nid, scan_names)
            if fold_registry is not None:
                # Claim each conjunct for THIS scan node. A self-join scans the
                # same relation twice under the same name, so the name test in
                # _collect_leaf_local_conjuncts matches the identical conjunct
                # from BOTH scans — folding it into both squares its
                # selectivity. First scan visited claims it; the twin skips it.
                # Keyed by conjunct object identity: _split_and_conjuncts
                # returns references into the Filter's own condition tree, so
                # the same id() is seen when _filter_stats splits the same
                # condition later in the walk. Values are the claiming scan's
                # uuid, diagnostic only.
                conjuncts = [c for c in conjuncts if id(c) not in fold_registry]
                for conj in conjuncts:
                    fold_registry[id(conj)] = node.uuid
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
                narrowed_columns = _scale_total_bytes(
                    narrowed_columns, _ratio(new_rows, base.row_count)
                )
                if new_rows != base.row_count or narrowed_columns is not base.columns:
                    base = RelationStatistics(
                        columns=narrowed_columns,
                        row_count_estimate=new_rows,
                        base_row_count=base.domain_row_count,
                    )
                else:
                    # A predicate constrains this scan even though its
                    # estimated selectivity came out at 1.0 — the output
                    # count is no longer a number we claim to know.
                    base = base.as_estimate()

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
        narrowed_columns = _scale_total_bytes(narrowed_columns, _ratio(new_rows, base.row_count))
        if new_rows != base.row_count or narrowed_columns is not base.columns:
            base = RelationStatistics(
                columns=narrowed_columns,
                row_count_estimate=new_rows,
                base_row_count=base.domain_row_count,
            )
        else:
            # Same honesty rule as the leaf-local conjuncts above: a pushed
            # predicate makes the output count an estimate even at s == 1.0.
            base = base.as_estimate()

    # A LIMIT pushed INTO the scan (LimitPushdownStrategy removes the Limit
    # node once `connector.supports_limit_pushdown`, so there is no Limit node
    # left for _limit_stats to see) is still a hard cap on the rows this scan
    # emits. Applied last: pushdown refuses to add a limit to a scan that
    # already carries predicates, but the reverse order can happen, and
    # min(filtered, limit) is the count either way.
    scan_limit = getattr(node, "limit", None)
    if scan_limit is not None and int(scan_limit) >= 0:
        capped_rows = min(int(base.row_count), int(scan_limit))
        if capped_rows != base.row_count:
            capped_cols = _cap_ndvs(
                _scale_total_bytes(base.columns, _ratio(capped_rows, base.row_count)),
                capped_rows,
            )
            # Same provenance rule as _limit_stats: min() over a metric count
            # is exact arithmetic and stays a metric; over an estimate it
            # stays an estimate.
            if base.row_count_is_metric:
                base = RelationStatistics(
                    columns=capped_cols,
                    row_count_metric=capped_rows,
                    base_row_count=base.domain_row_count,
                )
            else:
                base = RelationStatistics(
                    columns=capped_cols,
                    row_count_estimate=capped_rows,
                    base_row_count=base.domain_row_count,
                )

    return base


def _filter_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
    plan: LogicalPlan,
    nid: str,
    predicate_notes: Optional[list] = None,
    fold_registry: Optional[Dict[int, object]] = None,
) -> RelationStatistics:
    """Apply selectivity for conjuncts that haven't already been folded into
    an underlying Scan's stats by ``_scan_stats``.

    "Already folded" is not re-derived here — it is read from
    ``fold_registry``, the record ``_scan_stats`` writes as it folds (keyed by
    conjunct object identity, values the claiming scan's uuid). A previous
    version re-derived it with a second, downward traversal that disagreed
    with the upward walk about cross-join transparency: the upward walk folded
    a single-relation conjunct through a cross join, the downward walk stopped
    at ANY join, and the same conjunct's selectivity was applied twice. One
    traversal, one record, nothing to keep in agreement.

    Conjuncts not in the registry still affect row count and have their
    selectivity applied here.
    """
    base = _first_child_stats(child_stats) or _empty_stats()
    condition = getattr(node, "condition", None)
    if condition is None:
        return base

    from opteryx.planner.cost_estimation.selectivity import estimate_selectivity

    selectivity = 1.0
    narrowed_columns = base.columns
    applied_any = False
    for conj in _split_and_conjuncts(condition):
        if fold_registry is not None and id(conj) in fold_registry:
            # Already folded into a Scan's statistics (row count AND range) by
            # _scan_stats; that reduction reached `base` via the child chain.
            continue
        applied_any = True
        try:
            s = float(estimate_selectivity(conj, base))
        except Exception:
            s = 1.0
        selectivity *= s
        if predicate_notes is not None:
            predicate_notes.append(_predicate_note(nid, "Filter", None, conj, s, base))
        narrowed_columns = _narrow_filter_columns(narrowed_columns, conj)

    if not applied_any:
        # Every conjunct was folded into the Scan's own stats; nothing new to
        # apply here — the child's count (and its provenance) stand.
        return base
    if selectivity == 1.0 and narrowed_columns is base.columns:
        # A predicate was applied but estimated no reduction — the number
        # stands, the claim to KNOW it does not.
        return base.as_estimate()
    new_rows = base.row_count
    if selectivity != 1.0:
        new_rows = estimate_after_filter(base.row_count, selectivity)
    narrowed_columns = _scale_total_bytes(narrowed_columns, _ratio(new_rows, base.row_count))
    # Filtering shrinks the cardinality, never the key domain it was drawn from.
    return RelationStatistics(
        columns=narrowed_columns, row_count_estimate=new_rows, base_row_count=base.domain_row_count
    )


def _join_note(nid, join_type, left_rows, right_rows, out_rows, key_count) -> dict:
    return {
        "nid": nid,
        "join_type": join_type,
        "left_row_count": left_rows,
        "right_row_count": right_rows,
        "row_count": out_rows,
        "key_count": key_count,
    }


def _value_range_span(col_stats) -> Optional[int]:
    """Distinct-value upper bound implied by an integer column's value range.

    ``max - min + 1`` counts every value the range could hold, so it is an
    UPPER bound on NDV -- never a substitute for a real ``distinct_count``,
    only a cap on an estimate derived from something weaker. For the dense
    surrogate keys that carry most joins it is close to exact (TPC-H
    ``ps_suppkey`` spans 1..10,000 against a true NDV of 10,000); for a sparse
    key it can overshoot badly, which is why callers cap with it rather than
    adopt it.

    Integer bounds only: a float or string range has no meaningful value
    count. ``type(x) is int`` rather than ``isinstance`` because
    ``isinstance(True, int)`` is True and a bool range spans nothing useful.
    """
    value_range = getattr(col_stats, "value_range", None)
    if value_range is None:
        return None
    lower, upper = value_range.lower_bound, value_range.upper_bound
    if type(lower) is not int or type(upper) is not int or upper < lower:
        return None
    return upper - lower + 1


def _equi_key_classes(
    left_keys: List[bytes],
    right_keys: List[bytes],
    left: RelationStatistics,
    right: RelationStatistics,
) -> List[Tuple[KeyStats, KeyStats]]:
    """Collapse a join's equi-key pairs to one KeyStats pair per equivalence class.

    A query that restates a key transitively -- JOB writes
    ``t.id = mc.movie_id AND t.id = ci.movie_id AND ci.movie_id = mc.movie_id``
    -- hands a single join several key pairs that all express the SAME key
    identity. That is not a composite key. ``estimate_join_cardinality``
    multiplies every pair's selectivity under its independence assumption, so
    passing the restated pairs squares a selectivity that should apply once and
    collapses the estimate towards nothing (JOB 10a: a 632K x 2.6M join
    estimated at 4 rows).

    Pairs are grouped by shared endpoint: ``a.x = c.z`` and ``b.y = c.z`` both
    touch ``c.z``, so they form one class and contribute one factor. A genuinely
    composite key (``a.x = b.x AND a.y = b.y``) shares no endpoint, stays two
    classes, and still has both selectivities multiplied in.

    Per class, tdom is estimated SEPARATELY for each side and the two combined
    with ``max`` (Ebergen 2022 3.2) -- so a class where only one side knows its
    NDV still uses a real domain size rather than ``_key_selectivity``'s flat
    0.1 equality fallback. A side's own estimate is the largest NDV known across
    its endpoints, capped by the narrowest value-range span across those same
    endpoints. Keeping the sides apart is what makes tdom a domain size instead
    of an intersection size; see the comment at the computation for the
    cross-product blow-up that pooling them caused. When a side knows no NDV
    (common with Parquet files that omit distinct-count statistics), it falls
    back to the smaller side's ``domain_row_count`` -- its PRE-filter size:
    under FK-PK structure the smaller relation upper-bounds the distinct key
    count, and dividing by the post-filter count instead yields exactly
    ``max(rows)``, erasing every dimension filter. Null fractions take the worst
    case per side, matching ``JoinOrderingStrategy._key_null_fraction``.

    This mirrors ``plan_adapter._build_equiv_tdoms``, which already groups the
    identical way for the edges DPccp enumerates. The two paths must agree, or
    the tree-picker and the build-side chooser cost the same join differently.
    """
    parent: Dict[Tuple[str, bytes], Tuple[str, bytes]] = {}

    def find(key):
        parent.setdefault(key, key)
        root = key
        while parent[root] != root:
            root = parent[root]
        while parent[key] != root:
            parent[key], key = root, parent[key]
        return root

    def union(a, b):
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    pairs = list(zip(left_keys, right_keys))
    for left_key, right_key in pairs:
        union(("L", left_key), ("R", right_key))

    # Insertion-ordered so the emitted key list is deterministic.
    classes: Dict[Tuple[str, bytes], List[Tuple[bytes, bytes]]] = {}
    for left_key, right_key in pairs:
        classes.setdefault(find(("L", left_key)), []).append((left_key, right_key))

    equi_keys: List[Tuple[KeyStats, KeyStats]] = []
    for members in classes.values():
        known_ndvs: Dict[str, List[int]] = {"left": [], "right": []}
        spans: Dict[str, List[int]] = {"left": [], "right": []}
        left_nulls: List[float] = []
        right_nulls: List[float] = []
        for left_key, right_key in members:
            left_col = left.get_column(left_key)
            right_col = right.get_column(right_key)
            for side, col in (("left", left_col), ("right", right_col)):
                if col is None:
                    continue
                if col.distinct_count is not None:
                    known_ndvs[side].append(col.distinct_count)
                span = _value_range_span(col)
                if span is not None:
                    spans[side].append(span)
            if left_col is not None and left_col.null_fraction is not None:
                left_nulls.append(left_col.null_fraction)
            if right_col is not None and right_col.null_fraction is not None:
                right_nulls.append(right_col.null_fraction)

        # tdom stands in for max(ndv_left, ndv_right) -- the divisor in
        # |L| x |R| / tdom -- so every input to it must describe ONE side's own
        # key column. Pooling the two sides' NDVs and ranges and then taking a
        # minimum across the pool does not: it produces the size of the
        # INTERSECTED domain while the row counts stay un-intersected, and the
        # two halves of the ratio then disagree. That is a one-way error --
        # the intersection can only shrink, so the estimate can only grow --
        # and it grows precisely when a filter narrows one side, which is the
        # opposite of what a filter does. `WHERE grp_wide = 5` left one side
        # with ndv 1 spanning [5, 5]; pooled, that pinned tdom to 1 and turned
        # a 20,000-row equi-join into the full 2,000,000,000-row cross product,
        # over the `sql_select_limit` guard, so an ordinary query was refused
        # outright. Estimated per side and combined with max(), the same query
        # divides by the OTHER side's real domain and lands on 20,000.
        fallback = min(left.domain_row_count, right.domain_row_count)
        # (tdom, is_measured) per side. A side's number is MEASURED only when
        # it is a composed distinct_count that no upper bound overrode -- the
        # domain fallback is a relation size and a range span is a bound on
        # the NDV, neither is a count of anything.
        side_tdoms: List[Tuple[int, bool]] = []
        for side in ("left", "right"):
            # A side that reports no NDV falls back to the domain bound -- the
            # smaller relation's PRE-filter size, per this function's docstring.
            # Composition across the side's endpoints is the shared helper --
            # the same one join_ordering._key_ndv uses for the build-side pick.
            side_ndv = composite_key_ndv(known_ndvs[side])
            side_measured = side_ndv is not None
            side_tdom = side_ndv if side_ndv is not None else fallback
            # A range span is an upper bound on the NDV of the column it came
            # from (see _value_range_span) -- never a substitute for a real
            # distinct_count, and never a bound on the other side's column.
            if spans[side]:
                capped = min(side_tdom, min(spans[side]))
                if capped != side_tdom:
                    # The number in play is now the span, not the count.
                    side_measured = False
                side_tdom = capped
            side_tdoms.append((side_tdom, side_measured))
        tdom = max(1, max(t for t, _ in side_tdoms))
        # tdom stands in for max(ndv_left, ndv_right); its provenance is the
        # provenance of the side that supplied that max. A measured value
        # tying the max still counts as measured.
        measured = any(is_measured for t, is_measured in side_tdoms if t == tdom)
        provenance = NdvProvenance.MEASURED if measured else NdvProvenance.DOMAIN_STANDIN
        equi_keys.append((
            KeyStats(
                ndv=tdom,
                null_fraction=max(left_nulls) if left_nulls else None,
                ndv_provenance=provenance,
            ),
            KeyStats(
                ndv=tdom,
                null_fraction=max(right_nulls) if right_nulls else None,
                ndv_provenance=provenance,
            ),
        ))

    return equi_keys


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
        merged = _scale_total_bytes_by_origin(merged, left, right, out_rows)
        # A cross-join product of two KNOWN counts is itself exact — this is
        # the "accidental cross join" shape the plan-time result-size guard
        # exists to refuse, so its metric-ness must survive.
        if left.row_count_is_metric and right.row_count_is_metric:
            return RelationStatistics(
                columns=_cap_ndvs(merged, out_rows), row_count_metric=out_rows
            )
        return RelationStatistics(
            columns=_cap_ndvs(merged, out_rows), row_count_estimate=out_rows
        )

    # Map planner join names to the estimator's vocabulary.
    estimator_type = "inner"
    if join_type in ("left outer", "left"):
        estimator_type = "left"
    elif join_type in ("right outer", "right"):
        estimator_type = "right"
    elif join_type in ("full outer", "outer"):
        estimator_type = "outer"
    elif join_type in (
        "left semi",
        "left anti",
        "left anti null-aware",
        "left semi not-distinct",
        "left anti not-distinct",
    ):
        # Semi/anti emit only left-side columns; right contributes nothing.
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, left.row_count, 0)
            )
        # Bounded by the left side, not equal to it — an estimate.
        return RelationStatistics(
            columns=_cap_ndvs(left.columns, left.row_count),
            row_count_estimate=left.row_count,
        )

    if join_type == "asof":
        # ASOF is LEFT-PRESERVING: the operator emits EXACTLY one row per left
        # row, null-filled when nothing matches (see the AsofJoinNode docstring
        # and tests/operators/test_asof_join.py). That is true with or without
        # the optional USING/ON partition key, so it is settled here, before
        # the equi-key lookup -- a no-ON ASOF has empty left_columns /
        # right_columns (the MATCH_CONDITION populates asof_left_column /
        # asof_right_column instead), and would otherwise fall into the keyless
        # cross-product upper bound below and be estimated at |L| x |R|.
        out_rows = max(0, left.row_count)
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, out_rows, 0)
            )
        # Both sides' columns survive (unlike semi/anti), but the right side
        # contributes at most one value per left row.
        merged = _drop_histograms(_merge_columns(left, right))
        merged = _scale_total_bytes_by_origin(merged, left, right, out_rows)
        # EQUAL to the left count, not bounded by it -- no selectivity or NDV
        # heuristic is involved, so a metric left count yields a metric here,
        # the same exact-arithmetic rule the cross-join product follows.
        if left.row_count_is_metric:
            return RelationStatistics(
                columns=_cap_ndvs(merged, out_rows), row_count_metric=out_rows
            )
        return RelationStatistics(
            columns=_cap_ndvs(merged, out_rows), row_count_estimate=out_rows
        )

    left_keys = _join_key_identities(getattr(node, "left_columns", None))
    right_keys = _join_key_identities(getattr(node, "right_columns", None))

    if not left_keys or not right_keys:
        # Without a usable equi key, fall back to a cross-product upper bound;
        # JoinOrdering already guards against nested-loop blow-up by row count.
        out_rows = max(1, left.row_count * right.row_count)
        if join_notes is not None:
            join_notes.append(
                _join_note(nid, join_type, left.row_count, right.row_count, out_rows, 0)
            )
        merged = _drop_histograms(_merge_columns(left, right))
        merged = _scale_total_bytes_by_origin(merged, left, right, out_rows)
        # Cross-product UPPER BOUND for a keyless non-cross join — a bound is
        # an estimate even when both inputs were metric.
        return RelationStatistics(
            columns=_cap_ndvs(merged, out_rows), row_count_estimate=out_rows
        )

    # A composite equi-key (`ON a.x = b.x AND a.y = b.y`) must have EVERY key
    # pair's selectivity multiplied in -- estimate_join_cardinality already
    # does this correctly given a multi-entry equi_keys list (see its
    # independence-assumption docstring), and _intersect_join_keys below
    # already loops over every pair. Restated transitive keys are NOT a
    # composite key and must not be multiplied; _equi_key_classes separates
    # the two cases.
    equi_keys = _equi_key_classes(left_keys, right_keys, left, right)
    # Class count BEFORE the occupancy bound: a bounded composite collapses to
    # one pair, and telemetry reporting "1 key" for a two-column join would
    # hide exactly the shape a reader needs to see to understand the estimate.
    key_class_count = len(equi_keys)
    equi_keys = apply_occupancy_bound(
        equi_keys, left.domain_row_count, right.domain_row_count
    )

    out_rows = estimate_join_cardinality(
        left_rows=left.row_count,
        right_rows=right.row_count,
        join_type=_JOIN_TYPE_FOR_CARDINALITY[estimator_type],
        equi_keys=equi_keys,
        extra_predicates_selectivity=1.0,
    )
    if join_notes is not None:
        join_notes.append(
            _join_note(nid, join_type, left.row_count, right.row_count, out_rows, key_class_count)
        )
    merged = _drop_histograms(_merge_columns(left, right))
    # Equi-join: matching join keys see their range intersected and NDV reduced
    # to min(left, right). Non-key columns just get NDV capped at output rows.
    merged = _intersect_join_keys(merged, left, right, left_keys, right_keys)
    merged = _scale_total_bytes_by_origin(merged, left, right, out_rows)
    # A later join keys off a column belonging to one of the base relations
    # under this subtree; we can't tell which, so keep the largest domain --
    # the conservative choice, since it under-claims rather than over-claims
    # the reduction at the next join.
    return RelationStatistics(
        columns=_cap_ndvs(merged, out_rows),
        row_count_estimate=out_rows,
        base_row_count=max(left.domain_row_count, right.domain_row_count),
    )


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
                out[key] = out[key].but(value_range=intersected_range, distinct_count=new_ndv)
    return out


def _aggregate_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    groups = getattr(node, "groups", None) or []
    group_keys = [k for k in (_column_identity(g) for g in groups) if k]
    if not group_keys:
        # No group keys → exactly one output row, whatever the input. Metric.
        return _empty_stats(row_count=1, metric=True)
    ndvs = [
        base.columns[key].distinct_count if key in base.columns else None
        for key in group_keys
    ]
    out_rows = estimate_group_by_cardinality(base.row_count, ndvs)
    # Output columns are the group keys; each row is now a unique combination,
    # so a single key's distinct_count is bounded above by the output row count.
    # Histograms drop because the group-by output's value distribution differs
    # from the input's (each group reduced to one row regardless of frequency).
    #
    # With exactly ONE group key that bound is an EQUALITY, not a cap: the
    # output holds one row per distinct value of that key, so its NDV *is* the
    # output row count. Leaving it None when the input never reported a
    # distinct_count throws away the one NDV a group-by always knows, and
    # downstream that omission is not neutral -- `_equi_key_classes` takes
    # `max(known_ndvs)` across a join class, so joining this aggregate to a
    # filtered relation whose key NDV *is* known (say 1, after `flag = TRUE`)
    # set tdom to 1 and estimated the join as a full cross product. That is how
    # a window function -- planned as a self-join against a grouped aggregate --
    # came to be estimated at 2,000,000,000 rows over a 200,000-row relation
    # the moment any filter was added, and refused by the `sql_select_limit`
    # guard.
    #
    # With several group keys nothing equivalent holds: out_rows counts distinct
    # COMBINATIONS, and an individual key's NDV is only capped by it (applied
    # below by _cap_ndvs), so the input's own estimate stands.
    single_key_ndv = out_rows if len(group_keys) == 1 else None
    out_cols: Dict[bytes, ColumnStatistics] = {}
    for key in group_keys:
        col = base.columns.get(key)
        if col is None:
            continue
        out_cols[key] = col.but(
            histogram=None,
            distinct_count=single_key_ndv if single_key_ndv is not None else col.distinct_count,
        )
    out_cols = _scale_total_bytes(out_cols, _ratio(out_rows, base.row_count))
    # A grouped aggregate's output count is ALWAYS an estimate — even NDV-backed
    # products rest on an independence assumption between the keys.
    return RelationStatistics(
        columns=_cap_ndvs(out_cols, out_rows), row_count_estimate=out_rows
    )


def _limit_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    limit = getattr(node, "limit", None)
    offset = getattr(node, "offset", None)
    if limit is None and offset is None:
        return base
    try:
        # OFFSET consumes rows before LIMIT counts: only the rows past the
        # offset are available, so LIMIT 10 OFFSET 1_000_000 over 1_000_005
        # rows yields 5, not 10. An OFFSET with no LIMIT emits everything past
        # the offset.
        available = int(base.row_count) - (0 if offset is None else int(offset))
        capped = available if limit is None else min(int(limit), available)
    except (TypeError, ValueError):
        return base
    new_rows = max(0, capped)
    # Limit doesn't change ranges or distributions of *which* values appear,
    # but it does cap how many distinct values can be present.
    columns = _scale_total_bytes(base.columns, _ratio(new_rows, base.row_count))
    capped_cols = _cap_ndvs(columns, new_rows)
    # min(count - offset, limit) is exact arithmetic: the output inherits the
    # INPUT's provenance — a limited metric count stays a metric ("LIMIT
    # rescues the query" from the plan-time guard by genuinely bounding it).
    if base.row_count_is_metric:
        return RelationStatistics(columns=capped_cols, row_count_metric=new_rows)
    return RelationStatistics(columns=capped_cols, row_count_estimate=new_rows)


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


# Physical casts that map distinct non-NULL inputs to distinct non-NULL outputs,
# so NDV crosses the cast EXACTLY rather than as a bound. Two families qualify:
#
#   * integer -> VARCHAR. Distinct integers always render as distinct text.
#   * value-preserving numeric widenings. The target holds every source value
#     without collision.
#
# Excluded on purpose: every narrowing (wraps or saturates, collapsing distinct
# inputs onto one output); FLOAT -> VARCHAR (0.0 and -0.0 are one value that
# renders two ways, and the NaN spellings are worse); DECIMAL and temporal
# renderings (scale/precision truncation collapses values); VARCHAR -> anything
# (a parse maps every unparseable input onto a single outcome); and every TRY_
# cast, whose whole contract is to collapse failures onto NULL.
#
# Deliberately NOT shared with predicate_rewriter._NULL_PRESERVING_WIDENING.
# The two tables agree on the numeric family today and mean different things --
# that one asks "can this turn a value into NULL", this one asks "can this turn
# two values into one". A future edit to either is not an edit to the other.
_INTEGER_PHYSICALS = frozenset({
    "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64",
})

_DISTINCTNESS_PRESERVING_WIDENING: Dict[str, frozenset] = {
    "INT8":    frozenset({"INT8", "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "INT16":   frozenset({"INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "INT32":   frozenset({"INT32", "INT64", "FLOAT64"}),  # not FLOAT32: 2^31 > 2^24
    "INT64":   frozenset({"INT64"}),
    "UINT8":   frozenset({"UINT8", "UINT16", "UINT32", "UINT64",
                          "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "UINT16":  frozenset({"UINT16", "UINT32", "UINT64",
                          "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "UINT32":  frozenset({"UINT32", "UINT64", "INT64", "FLOAT64"}),
    "UINT64":  frozenset({"UINT64"}),
    "FLOAT32": frozenset({"FLOAT32", "FLOAT64"}),
    "FLOAT64": frozenset({"FLOAT64"}),
}


def _physical_type_name(expression) -> str:
    """Physical DrakenType name bound to an expression, or '' when untyped."""
    schema_column = getattr(expression, "schema_column", None)
    column_type = getattr(schema_column, "column_type", None)
    physical = getattr(column_type, "physical", None)
    return getattr(physical, "name", "") or ""


def _cast_preserves_distinctness(cast_node) -> bool:
    """True iff this CAST cannot map two distinct values onto one.

    Distinctness, not null-preservation: the question is whether the output's
    NDV equals the input's, so that a counted distinct_count stays a counted
    distinct_count on the far side.
    """
    target_name = (getattr(cast_node, "value", "") or "").upper()
    if target_name.startswith("TRY_"):
        return False  # TRY_ exists precisely to collapse failures onto NULL
    if getattr(cast_node, "format", None) is not None:
        return False  # a FORMAT pattern can render two values identically
    source = getattr(cast_node, "left", None)
    if source is None:
        return False
    source_physical = _physical_type_name(source)
    target_physical = _physical_type_name(cast_node)
    if not source_physical or not target_physical:
        return False
    if source_physical in _INTEGER_PHYSICALS and target_physical == "VARCHAR":
        return True
    return target_physical in _DISTINCTNESS_PRESERVING_WIDENING.get(
        source_physical, frozenset()
    )


def _project_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    """Pass the child's statistics through, and give derived columns whose
    expression cannot change the distinct-value count their source's NDV.

    Project used to be pure pass-through, which silently dropped the one
    statistic a COMPUTED join key can still honestly claim. `CAST(src_addr AS
    VARCHAR)` renders a UINT32 as a dotted quad -- distinct addresses give
    distinct strings -- so the derived column's NDV *is* src_addr's NDV. But
    the derived identity carried no statistics at all, so `_equi_key_classes`
    fell through to its domain-size stand-in (the smaller side's PRE-filter
    row count) and divided by 278,985 for a key with ~5,000 distinct values.
    Measured on `home.network.netflow JOIN home.network.dns ON
    CAST(src_addr AS VARCHAR) = client`: a join emitting 2,295,861,762 rows
    was estimated at 462,275. src_addr's NDV of 10,087 was sitting in the
    scan statistics one node below, unread.

    ONLY expressions that provably preserve distinctness qualify -- see
    `_cast_preserves_distinctness`. Everything else keeps no statistics, which
    is the honest answer: any other expression gives an NDV that is merely an
    UPPER BOUND (a function cannot increase distinct values, only reduce them),
    and writing a bound into `distinct_count` is the stand-in problem again one
    level down. `_equi_key_classes` reads a present `distinct_count` as
    MEASURED, and `ColumnStatistics` has no provenance field to say otherwise.

    Only `distinct_count` and `null_fraction` cross. Value ranges, histograms,
    char-class stats and byte sizes do NOT: '10.0.0.9' and '10.0.0.10' sort the
    opposite way round to the integers behind them, and the rendered width is a
    different number from the source's.
    """
    from opteryx.expression import NodeType

    base = _first_child_stats(child_stats) or _empty_stats()
    columns = getattr(node, "columns", None)
    if not columns or not base.columns:
        return base

    derived: Dict[bytes, ColumnStatistics] = {}
    for column in columns:
        if column.node_type != NodeType.CAST:
            continue
        identity = _column_identity(column)
        # An identity already carrying statistics is the child's own column
        # re-exposed (an alias does not mint a new identity); leave it alone.
        if identity is None or identity in base.columns or identity in derived:
            continue
        source_identity = _column_identity(getattr(column, "left", None))
        if source_identity is None:
            continue
        source_stats = base.columns.get(source_identity)
        if source_stats is None or source_stats.distinct_count is None:
            continue
        if not _cast_preserves_distinctness(column):
            continue
        schema_column = getattr(column, "schema_column", None)
        derived[identity] = ColumnStatistics(
            column_name=getattr(schema_column, "name", "") or "",
            data_type=_physical_type_name(column),
            distinct_count=source_stats.distinct_count,
            null_fraction=source_stats.null_fraction,
        )

    if not derived:
        return base

    merged = dict(base.columns)
    merged.update(derived)
    # A projection changes no row counts, so the count and its provenance --
    # and the pre-filter domain size the join stand-in reads -- pass through
    # untouched. Rebuilding without base_row_count would shrink the very
    # fallback this function exists to stop being reached.
    if base.row_count_is_metric:
        return RelationStatistics(
            columns=merged,
            row_count_metric=base.row_count,
            base_row_count=base.base_row_count,
        )
    return RelationStatistics(
        columns=merged,
        row_count_estimate=base.row_count,
        base_row_count=base.base_row_count,
    )


def _distinct_stats(
    node: LogicalPlanNode,
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
    plan: Optional["LogicalPlan"] = None,
    nid: Optional[str] = None,
) -> RelationStatistics:
    base = _first_child_stats(child_stats) or _empty_stats()
    if not base.columns:
        # No column stats to estimate a reduction from, but DISTINCT still
        # collapses duplicates — the input count is now only a bound.
        return base.as_estimate()

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
    out_cols = _scale_total_bytes(out_cols, _ratio(out_rows, base.row_count))
    return RelationStatistics(
        columns=_cap_ndvs(out_cols, out_rows), row_count_estimate=out_rows
    )


def _union_stats(
    child_stats: List[Tuple[Optional[RelationStatistics], str]],
) -> RelationStatistics:
    """UNION ALL — sum row counts; widen each column's range (lower=min, upper=max).

    NDV is summed as a loose upper bound; histograms drop because we don't try
    to merge the two distributions. UNION (distinct) callers can apply a
    Distinct on top of this and the NDV cap will tighten.
    """
    rows = 0
    # UNION ALL's sum is exact arithmetic — metric only when EVERY input is
    # present and metric; one missing or estimated input makes the sum a guess.
    all_metric = bool(child_stats)
    columns: Dict[bytes, ColumnStatistics] = {}
    for cs, _ in child_stats:
        if cs is None:
            all_metric = False
            continue
        if not cs.row_count_is_metric:
            all_metric = False
        rows += cs.row_count
        for k, v in cs.columns.items():
            existing = columns.get(k)
            if existing is None:
                columns[k] = v.but(histogram=None)
                continue
            # Widen range, sum NDVs.
            new_lower = _min_or_none(existing.value_range.lower_bound, v.value_range.lower_bound)
            new_upper = _max_or_none(existing.value_range.upper_bound, v.value_range.upper_bound)
            new_ndv: Optional[int] = None
            if existing.distinct_count is not None and v.distinct_count is not None:
                new_ndv = existing.distinct_count + v.distinct_count
            # Sum total_bytes the same strict way as NDV above -- this column
            # now carries both sides' concatenated values, but a partial sum
            # from only one known side would understate the true total, so
            # require both (matches Manifest.get_total_uncompressed_size's
            # own no-partial-sums contract).
            new_total_bytes: Optional[int] = None
            if existing.total_bytes is not None and v.total_bytes is not None:
                new_total_bytes = existing.total_bytes + v.total_bytes
            columns[k] = existing.but(
                value_range=ColumnRange(lower_bound=new_lower, upper_bound=new_upper),
                distinct_count=new_ndv,
                histogram=None,
                total_bytes=new_total_bytes,
            )
    if all_metric:
        return RelationStatistics(columns=_cap_ndvs(columns, rows), row_count_metric=rows)
    return RelationStatistics(columns=_cap_ndvs(columns, rows), row_count_estimate=rows)


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
    """INTERSECT / EXCEPT — bounded by the left input; a bound is an estimate."""
    left, _ = _split_join_children(child_stats) if len(child_stats) >= 2 else (None, None)
    if left is None:
        return (_first_child_stats(child_stats) or _empty_stats()).as_estimate()
    return left.as_estimate()


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


def _ratio(new_rows: int, old_rows: int) -> float:
    """new_rows / old_rows, guarding the degenerate old_rows == 0 case.

    Shared by every operator that rescales `total_bytes` alongside row_count:
    bytes-per-row is assumed uniform across an operator's output (the same
    assumption row_count estimation itself already makes implicitly), so
    scaling total_bytes by this ratio keeps it consistent with row_count.
    """
    if old_rows <= 0:
        return 1.0 if new_rows == old_rows else 0.0
    return new_rows / old_rows


def _scale_total_bytes(
    columns: Dict[bytes, ColumnStatistics], ratio: float
) -> Dict[bytes, ColumnStatistics]:
    """Return a copy with each column's total_bytes scaled by `ratio`.

    `ratio == 1.0` is a no-op fast path returning `columns` unchanged, so
    callers can freely call this even when nothing actually changed (mirrors
    _cap_ndvs / _narrow_filter_columns' own no-op-preserving style, which
    downstream `is not` identity checks rely on).
    """
    if ratio == 1.0:
        return columns
    out: Dict[bytes, ColumnStatistics] = {}
    for k, c in columns.items():
        if c.total_bytes is None:
            out[k] = c
        else:
            out[k] = c.but(total_bytes=max(0, int(c.total_bytes * ratio)))
    return out


def _scale_total_bytes_by_origin(
    merged: Dict[bytes, ColumnStatistics],
    left: RelationStatistics,
    right: RelationStatistics,
    out_rows: int,
) -> Dict[bytes, ColumnStatistics]:
    """Join variant of `_scale_total_bytes`: each column scales by ITS OWN
    side's row-count ratio, not a single shared ratio -- a join can multiply
    left and right row counts by different factors (e.g. a 10-row left table
    joined 100-to-1 against a right table), so a left-sourced column's bytes
    must scale by out_rows/left.row_count while a right-sourced column scales
    by out_rows/right.row_count. `_merge_columns` (left takes priority via
    setdefault) already tells us which side a given identity came from.
    """
    ratio_left = _ratio(out_rows, left.row_count)
    ratio_right = _ratio(out_rows, right.row_count)
    if ratio_left == 1.0 and ratio_right == 1.0:
        return merged
    out: Dict[bytes, ColumnStatistics] = {}
    for k, c in merged.items():
        if c.total_bytes is None:
            out[k] = c
            continue
        ratio = ratio_left if k in left.columns else ratio_right
        out[k] = c.but(total_bytes=max(0, int(c.total_bytes * ratio)))
    return out


def _cap_ndvs(columns: Dict[bytes, ColumnStatistics], row_count: int) -> Dict[bytes, ColumnStatistics]:
    """Return a new column dict where every distinct_count is capped at ``row_count``.

    A relation cannot contain more distinct values than rows. Called after any
    operator that reduces row count (Filter, Limit, Distinct, Group-by output).
    """
    out: Dict[bytes, ColumnStatistics] = {}
    for k, c in columns.items():
        if c.distinct_count is not None and c.distinct_count > row_count:
            out[k] = c.but(distinct_count=max(1, int(row_count)))
        else:
            out[k] = c
    return out


def _drop_histograms(columns: Dict[bytes, ColumnStatistics]) -> Dict[bytes, ColumnStatistics]:
    """Return a copy with histograms removed.

    Called after any operator that distorts the underlying distribution
    (joins, group-by on the group keys, distinct, union). We don't try to
    rebuild — the input distogram no longer reflects the output, so it would
    mislead downstream cost estimation.

    No-op preserving: returns `columns` unchanged when no column carries a
    histogram (the common case), matching _scale_total_bytes / _cap_ndvs.
    """
    if all(c.histogram is None for c in columns.values()):
        return columns
    return {k: (c if c.histogram is None else c.but(histogram=None)) for k, c in columns.items()}


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
        out[identity] = col.but(value_range=new_range, distinct_count=new_ndv)
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
        a = _orderable_bound(_literal_value(getattr(node, "right", None)))
        b = _orderable_bound(_literal_value(getattr(node, "centre", None)))
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
        if op == "InList":
            values = _in_list_values(literal)
            if not values:
                return
            # An IN-list caps NDV whatever the member type; only the RANGE it
            # implies needs members that can be ordered against a stored bound.
            bounds = [b for b in (_orderable_bound(v) for v in values) if b is not None]
            _merge_constraint(
                sink,
                identity,
                lower=min(bounds) if len(bounds) == len(values) else None,
                upper=max(bounds) if len(bounds) == len(values) else None,
                eq_card=len(values),
            )
            return
        bound = _orderable_bound(lit_value)
        if op == "Eq":
            _merge_constraint(sink, identity, lower=bound, upper=bound, eq_card=1)
        elif bound is None:
            return
        elif op in ("Lt", "LtEq"):
            _merge_constraint(sink, identity, upper=bound)
        elif op in ("Gt", "GtEq"):
            _merge_constraint(sink, identity, lower=bound)
        return


_SWAPPED_COMPARISON = {
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def _orderable_bound(value):
    """`value` if it can be ordered against a stored `value_range` bound, else None.

    `value_range` holds NUMBERS and nothing else. `_scan_stats` enforces that on
    the way in — it records a manifest bound only when both ends are int or float,
    because the manifest returns strings and decimals as RAW SERIALIZED BYTES and
    feeding those to the unguarded `max`/`min` in `_narrow_filter_columns` would
    raise. This is the same gate on the other inlet: a bound harvested from a
    PREDICATE lands in the same field and is intersected by the same bare
    comparison on a later pass.

    It was not gated, and the two spellings a VARCHAR literal has in this engine
    then met in that comparison — a parsed IN-list literal is `bytes`, a
    constant-folded `INITCAP('item')` is `str` — as
    `min('Item', b'zeta')`: TypeError, from an ordinary two-predicate query. The
    representation split is real and lives upstream of the cost model; this keeps
    the cost model out of it rather than adopting a side in passing.

    Dropping a bound only widens an estimate, so nothing here can make a plan
    wrong. `bool` is excluded the way `_value_range_span` excludes it: True is an
    int to `isinstance` and a range over it means nothing. Equality CARDINALITY is
    unaffected — it counts values rather than ordering them, so `=` and `IN` still
    cap NDV for every type.
    """
    return value if type(value) in (int, float) else None


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
    consulted for correctness. ``run()`` also then records one row-count and
    one total-byte-size entry per node so estimated-vs-actual comparisons
    don't require a second plan walk elsewhere.
    """

    def __init__(self, plan: LogicalPlan, telemetry=None, scan_stats_cache: Optional[dict] = None):
        self.plan = plan
        self._visited: set = set()
        self.telemetry = telemetry
        self.scan_stats_cache = scan_stats_cache
        self.predicate_notes: Optional[list] = [] if telemetry is not None else None
        self.join_notes: Optional[list] = [] if telemetry is not None else None
        # id(conjunct) -> claiming scan uuid, written by _scan_stats as it
        # folds leaf-local Filter conjuncts, read by _filter_stats to skip
        # exactly those conjuncts. Per-refresh: conjunct object ids are only
        # stable while this visitor holds the plan alive.
        self.fold_registry: Dict[int, object] = {}

    def run(self) -> None:
        for nid in self.plan.get_exit_points():
            self._visit(nid)
        if self.telemetry is not None:
            self._record_telemetry()

    def _record_telemetry(self) -> None:
        row_counts = []
        total_bytes_by_node = []
        for nid, node in self.plan.nodes(True):
            stats = node.statistics
            if stats is None:
                continue
            row_counts.append({
                "nid": nid,
                "node_type": node.node_type.name,
                "relation": node.relation,
                "row_count": stats.row_count,
                # Provenance, following the metric/estimate lingo on
                # RelationStatistics: "metric" is a number we claim to KNOW,
                # "estimate" passed through a selectivity/NDV heuristic. The
                # estimate-vs-actual harness needs this to score only the
                # numbers the estimators actually produced.
                "row_count_kind": "metric" if stats.row_count_is_metric else "estimate",
            })
            # Node-level total, summing only the columns with a known
            # total_bytes -- a variable-width column with no ANALYZE pass and
            # no manifest size (see ColumnStatistics.total_bytes) contributes
            # nothing rather than forcing the whole node to "unknown". None
            # (not 0) when NOT ONE column has a known estimate, so a consumer
            # can tell "genuinely unknown" from "known to be empty".
            known = [c.total_bytes for c in stats.columns.values() if c.total_bytes is not None]
            total_bytes_by_node.append({
                "nid": nid,
                "node_type": node.node_type.name,
                "relation": node.relation,
                "total_bytes": sum(known) if known else None,
            })
        self.telemetry._reading["estimated_row_counts"] = row_counts
        self.telemetry._reading["estimated_total_bytes"] = total_bytes_by_node
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
            return _scan_stats(
                node,
                self.plan,
                nid,
                self.predicate_notes,
                self.scan_stats_cache,
                self.fold_registry,
            )
        if nt == LogicalPlanStepType.MaterializedCteRef:
            # A reference to a shared CTE is a leaf with no manifest of its own;
            # its cardinality is the shared body's output estimate, stamped by
            # do_optimizer before the main plan is optimized (see shared_cte.py).
            # Absent a stamp, UNKNOWN — the same posture as a scan with no
            # manifest counts. Zero would be a claim of provable emptiness and
            # propagates multiplicatively: any join against a 0-row side
            # collapses to ~1 row, poisoning every cost decision above it.
            stamped = getattr(node, "cte_statistics", None)
            return stamped if stamped is not None else _empty_stats(_UNKNOWN_ROW_COUNT)
        if nt == LogicalPlanStepType.Filter:
            return _filter_stats(
                node, child_stats, self.plan, nid, self.predicate_notes, self.fold_registry
            )
        if nt in (LogicalPlanStepType.Join, LogicalPlanStepType.DependentJoin):
            return _join_stats(node, child_stats, nid, self.join_notes)
        if nt == LogicalPlanStepType.AggregateAndGroup:
            return _aggregate_stats(node, child_stats)
        if nt == LogicalPlanStepType.Aggregate:
            return _empty_stats(row_count=1, metric=True)
        if nt in (LogicalPlanStepType.Limit, LogicalPlanStepType.HeapSort):
            return _limit_stats(node, child_stats)
        if nt == LogicalPlanStepType.Distinct:
            return _distinct_stats(node, child_stats, self.plan, nid)
        if nt == LogicalPlanStepType.Project:
            return _project_stats(node, child_stats)
        if nt == LogicalPlanStepType.Union:
            return _union_stats(child_stats)
        if nt in (LogicalPlanStepType.Intersect, LogicalPlanStepType.Except):
            return _set_op_stats(node, child_stats)
        if nt in _PASS_THROUGH_TYPES:
            return _first_child_stats(child_stats) or _empty_stats()

        # Unknown node type — pass through whatever the first child produced.
        return _first_child_stats(child_stats) or _empty_stats()


def refresh_statistics(
    plan: LogicalPlan, telemetry=None, scan_stats_cache: Optional[dict] = None
) -> LogicalPlan:
    """Recompute statistics for every node in ``plan``.

    Walks the plan bottom-up from each exit point and attaches a
    ``RelationStatistics`` to every node as ``node.statistics``. Clears the
    plan's ``statistics_are_stale`` flag on completion.

    When ``telemetry`` is given, also records the planner's per-node row-count
    and total-byte-size estimates, per-predicate selectivity/cost, and
    per-join cardinality inputs onto ``telemetry._reading``
    (``estimated_row_counts`` / ``estimated_total_bytes`` /
    ``predicate_estimates`` / ``join_estimates``) -- diagnostic detail for
    comparing estimate vs actual, never consulted by planning itself.
    Omitting ``telemetry`` (the default) skips this entirely; existing
    callers are unaffected.
    """
    StatisticsRefreshVisitor(plan, telemetry, scan_stats_cache).run()
    plan.statistics_are_stale = False
    return plan
