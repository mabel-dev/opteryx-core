# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The planner's estimates that EXECUTION consumes, computed when the physical plan
is built.

Three native sinks size themselves from a planner estimate:

  * Join2BuildSink reads the join's expected OUTPUT rows to decide whether to
    consolidate its retained build payload into one block (one copy, then codes
    per output row) or keep gathering the payload per output row. Only worth it
    when the join emits more rows than its build side holds.
  * GroupBySink and DistinctSink read the expected distinct-group count to arm
    their fixed-capacity parvi front maps (kGBParviGateNDV / kDistinctParviGateNDV).
  * GroupBySink also takes its integer keys' manifest bounds and the scans' row
    count to lay out its direct-array path (group_key_ranges).

Each is a pure function of the final logical plan, its PlanContext statistics
and the scans' manifests, so it is computed here, at the one place that hands it
to the operator — nothing is stamped on a plan node and carried to the physical
planner. (These lived in JoinBuildShapeStrategy and HashMapVariantStrategy, which
wrote them onto nodes as the optimizer's last passes; architect ruling
2026-09-25.)

Unknown is always None, never a fabricated number: every sink keeps its default
behaviour when it gets no estimate.
"""

from typing import Optional

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import PlanStep
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import LogicalCategory

# Ceiling on the NDV product so a many-key GROUP BY cannot leave int64 (the sink
# takes the estimate as int64_t; _estimate_to_int64 refuses anything larger). The
# product is a REAL estimate now (GROUP BY redesign R4, docs/GROUPBY_REDESIGN_2026_10_12.md):
# it used to stop multiplying at 64, the parvi gate, which left the sink with a
# truncated lower bound it could use for exactly one decision.
_NDV_PRODUCT_CAP = 1 << 62
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
# A scan this small IS its own group-count bound (the parvi gate's reach).
_ROWS_ARE_THE_ESTIMATE = 64

# Join types whose probe gathers a build-side payload per output row. SEMI/ANTI
# emit probe rows collapsed to existence and drop the build payload (compiler.py's
# `semi_no_payload`), so they have no build gather to size and get no estimate.
_BUILD_PAYLOAD_JOINS = frozenset(
    {
        "inner",
        "left outer",
        "right outer",
        "full outer",
        "cross",
        "cross join",
        "nested loop",
        "nested_loop",
        "asof",
    }
)


def join_output_rows_estimate(nid: int, node: PlanStep, plan_context: PlanContext) -> Optional[int]:
    """The join's estimated output row count, or None when unknown or when the
    join type has no build payload to size.

    Read off the final plan's statistics — the same number JoinAlgorithmStrategy
    costs its trees with. A join the refresh never reached returns None.
    """
    if node.type not in _BUILD_PAYLOAD_JOINS:
        return None
    return plan_context.statistics.row_count(nid)


def group_count_estimate(plan: LogicalPlan, nid: str, node: PlanStep) -> Optional[int]:
    """Best available distinct-group-count bound for a GROUP BY / DISTINCT, or
    None when neither signal resolves.

    Two signals: the total record count of every upstream scan (output groups
    cannot exceed input rows — a provable bound), and the product of the group
    columns' manifest NDVs. The estimate is the smaller of the two when both
    resolve. The product OVERESTIMATES correlated keys (SearchEngineID x ClientIP
    is far more than the pairs that occur), so a consumer sizing anything from it
    must treat it as an upper bound; the sink's own runtime sketch is the truth.
    """
    scans = [
        plan[source]
        for source, _target, _relation in plan.breadth_first_search(nid, reverse=True)
        if plan[source].node_type == LogicalPlanStepType.Scan
    ]
    if not scans:
        return None

    # The row count alone is an estimate only when it is tiny (a 40-row table has at
    # most 40 groups); a large row count says nothing about the group count, and a
    # consumer could not tell "100M rows, product unknown" from "100M distinct keys".
    estimate = None
    total_rows = _total_record_count(scans)
    if total_rows is not None and total_rows <= _ROWS_ARE_THE_ESTIMATE:
        estimate = total_rows
    ndv_product = _ndv_product(node, scans)
    if ndv_product is not None and total_rows is not None:
        ndv_product = min(ndv_product, total_rows)
    if ndv_product is not None:
        estimate = ndv_product if estimate is None else min(estimate, ndv_product)
    return estimate


def group_key_ranges(plan: LogicalPlan, nid: str, node: PlanStep) -> Optional[tuple]:
    """Manifest bounds for a GROUP BY's keys, for the native sink's direct-array
    layout: ``([(min, max) or None per key], total_rows or None)``, or None when no
    scan is upstream.

    A key gets bounds only when it is a plain INTEGER column every upstream scan's
    manifest bounds in every file; the bounds are the union over files and scans.
    They are a layout HINT: the sink range-checks every morsel and hashes whatever
    falls outside, so a loose or stale bound costs speed, never an answer.
    """
    scans = [
        plan[source]
        for source, _target, _relation in plan.breadth_first_search(nid, reverse=True)
        if plan[source].node_type == LogicalPlanStepType.Scan
    ]
    if not scans:
        return None
    return [_column_int_range(column, scans) for column in node.groups or []], _total_record_count(scans)


def _column_int_range(column, scans: list) -> Optional[tuple]:
    name = _source_column_name(column)
    if name is None or column.schema_column.column_type.category != LogicalCategory.INTEGER:
        return None
    lo = hi = None
    for scan in scans:
        manifest = scan.manifest
        if manifest is None:
            return None
        bounds = manifest.file_value_bounds(name)
        if not bounds:
            return None
        for low, high in bounds:
            if low[0] != "int" or high[0] != "int":
                return None
            lo = low[1] if lo is None else min(lo, low[1])
            hi = high[1] if hi is None else max(hi, high[1])
    # The sink's layout hint is int64; a UINT64 bound above INT64_MAX has no
    # representation there, so the key is simply unbounded.
    if lo < _INT64_MIN or hi > _INT64_MAX:
        return None
    return lo, hi


def _total_record_count(scans: list) -> Optional[int]:
    total = 0
    for scan in scans:
        manifest = scan.manifest
        if manifest is None:
            return None
        count = manifest.get_record_count()
        if count is None:
            return None
        total += int(count)
    return total


def _ndv_product(node: PlanStep, scans: list) -> Optional[int]:
    columns = node.groups if node.node_type == LogicalPlanStepType.AggregateAndGroup else node.on
    if not columns:
        # An EMPTY column list means two different things. GROUP BY () is a scalar
        # aggregate that provably yields exactly ONE row. A plain `SELECT DISTINCT`
        # has no `on`: its keys are the projected columns, which the logical node
        # does not carry here — UNKNOWN, not 1 (1 armed the parvi front set for
        # every DISTINCT, high-cardinality ones included).
        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            return 1
        return None

    # Only a plain column resolves to a manifest NDV; any expression key (GROUP BY
    # UPPER(x)) defeats the signal.
    product = 1
    for column in columns:
        name = _source_column_name(column)
        if name is None:
            return None
        ndv = _column_ndv(name, scans)
        if ndv is None:
            return None
        product *= max(1, ndv)
        if product > _NDV_PRODUCT_CAP:
            return _NDV_PRODUCT_CAP
    return product


def _source_column_name(column) -> Optional[str]:
    if column.node_type != NodeType.IDENTIFIER:
        return None
    schema_column = column.schema_column
    if schema_column is None:
        return None
    return schema_column.name or None


def _column_ndv(name: str, scans: list) -> Optional[int]:
    """Sum of per-scan NDV estimates; any unknown gives up."""
    total = 0
    for scan in scans:
        manifest = scan.manifest
        if manifest is None:
            return None
        estimate = manifest.estimate_cardinality(name)
        if estimate is None:
            return None
        total += int(estimate)
    return total if total > 0 else None
