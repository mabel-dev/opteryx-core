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
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_context import PlanContext

# The highest group count any consumer compares against (the native GroupBySink's
# gate). The NDV product below stops multiplying once past it: a larger number is
# only ever compared as "above the gate".
NATIVE_GB_GATE = 64

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


def join_output_rows_estimate(node: LogicalPlanNode, plan_context: PlanContext) -> Optional[int]:
    """The join's estimated output row count, or None when unknown or when the
    join type has no build payload to size.

    Read off the final plan's statistics — the same number JoinAlgorithmStrategy
    costs its trees with. A join the refresh never reached returns None.
    """
    if node.type not in _BUILD_PAYLOAD_JOINS:
        return None
    stats = plan_context.statistics(node)
    if stats is None:
        return None
    return int(stats.row_count)


def group_count_estimate(plan: LogicalPlan, nid: str, node: LogicalPlanNode) -> Optional[int]:
    """Best available distinct-group-count bound for a GROUP BY / DISTINCT, or
    None when neither signal resolves.

    Two signals: the total record count of every upstream scan (output groups
    cannot exceed input rows — a provable bound, used only when it is small), and
    the product of the group columns' manifest NDVs. Values above NATIVE_GB_GATE
    may be truncated lower bounds (the product stops early) — only comparisons at
    or below the gate are valid, which is all any sink does.
    """
    scans = [
        plan[source]
        for source, _target, _relation in plan.breadth_first_search(nid, reverse=True)
        if plan[source].node_type == LogicalPlanStepType.Scan
    ]
    if not scans:
        return None

    estimate = None
    total_rows = _total_record_count(scans)
    if total_rows is not None and total_rows <= NATIVE_GB_GATE:
        estimate = total_rows
    ndv_product = _ndv_product(node, scans)
    if ndv_product is not None:
        estimate = ndv_product if estimate is None else min(estimate, ndv_product)
    return estimate


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


def _ndv_product(node: LogicalPlanNode, scans: list) -> Optional[int]:
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
        if product > NATIVE_GB_GATE:
            return product
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
