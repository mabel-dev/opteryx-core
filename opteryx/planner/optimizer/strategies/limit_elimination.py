# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Limit Elimination

Type: Heuristic
Goal: Reduce Work

Drops a LIMIT node when it is provably a no-op. A no-op LIMIT costs nothing by
itself, but its mere presence in the plan blocks other strategies that require
a LIMIT-free plan to fire — notably StatisticsOnlyResponseStrategy, which
cannot answer `SELECT COUNT(*) FROM t LIMIT n` from the manifest while a LIMIT
node sits between the aggregate and the exit, even though an ungrouped
aggregate always returns exactly one row and the LIMIT can never bind.

Runs before StatisticsOnlyResponseStrategy so a query eliminated here is
answered from statistics in the same optimizer pass.

Three provably-safe cases. All require a LIMIT with no OFFSET (an OFFSET can
still discard rows down to zero even when the total is under the limit, so a
LIMIT with an OFFSET is never eliminated here):

1. Ungrouped aggregate — a `LogicalPlanStepType.Aggregate` node (GROUP BY
   produces `AggregateAndGroup` instead, never `Aggregate`) always emits
   exactly one row, unconditionally: it is a hard collapse of every row
   underneath it, no matter what runs below (filters, unnests, joins). One
   row is <= any LIMIT >= 1.

2. `$no_table` scan — the synthetic single-row source behind a FROM-less
   `SELECT <expr>` (e.g. `SELECT 1 LIMIT 1`). Always exactly one row.

3. Manifest-bounded scan — the Scan's manifest record count is the exact,
   filter-free row count the connector would read. A WHERE clause can only
   remove rows from that count, never add to it, so if the manifest count is
   already <= the LIMIT the query can't exceed the LIMIT — PROVIDED nothing
   else in the plan can change row count. Unlike case 1, this reasoning
   breaks the moment anything row-count-changing sits between the Scan and
   the LIMIT: a Join (including a `CROSS JOIN UNNEST`, which compiles to a
   Join/Unnest node, not a second Scan) or a Union can multiply the manifest
   count into far more output rows. Cases 2 and 3 therefore additionally
   require every node in the plan to be drawn from a small allow-list of
   node types that are provably row-count non-increasing relative to the
   scan (Filter, Project, Distinct, Order, HeapSort) — deny-by-default
   rather than trying to enumerate every row-multiplying node type that
   might exist now or be added later.
"""

from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Node types that, alone or in combination, never increase row count relative
# to the Scan they sit above. Anything not in this set (Join, Unnest, Union,
# Intersect, Except, FunctionDataset, Window, ...) disqualifies cases 2 and 3.
_ROW_COUNT_SAFE_TYPES = {
    LogicalPlanStepType.Scan,
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Distinct,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.HeapSort,
    LogicalPlanStepType.Limit,
    LogicalPlanStepType.Exit,
    # EXPLAIN wraps the real plan in its own node purely to route output to
    # the plan renderer; it neither reads nor changes row count, so its
    # presence must not make the EXPLAIN preview diverge from what the same
    # query does when actually run.
    LogicalPlanStepType.Explain,
}


def _limit_sees_only_aggregate_row(plan, limit_nid) -> bool:
    """True when everything feeding `limit_nid` collapses to a single row.

    Descends the plan from the LIMIT (edges run child -> parent, so
    `ingoing_edges` walks toward the sources). An ungrouped Aggregate is a hard
    collapse to one row, so the walk stops there rather than descending past it;
    anything that can INCREASE row count between the LIMIT and that aggregate
    (Join, Unnest, Union, ... — deny-by-default) or a grouped aggregate feeding
    the LIMIT disqualifies it.

    This has to be asked PER LIMIT. Asking it of the whole plan — "is there an
    ungrouped aggregate anywhere?" — eliminated LIMITs that the aggregate does
    not sit under at all: `SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM t
    LIMIT 5)` dropped the SUBQUERY's LIMIT because the outer COUNT(*) made the
    plan look single-row, silently widening the IN-list to every row and
    returning 9 instead of 5. Silent wrong answers, no error.
    """
    stack = [source for source, _, _ in plan.ingoing_edges(limit_nid)]
    seen = set()
    found_aggregate = False
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        node_type = plan[current].node_type
        if node_type == LogicalPlanStepType.Aggregate:
            # Collapses every row beneath it to exactly one — stop here, what is
            # below can no longer affect how many rows the LIMIT sees.
            found_aggregate = True
            continue
        if node_type == LogicalPlanStepType.AggregateAndGroup:
            return False
        if node_type not in _ROW_COUNT_SAFE_TYPES:
            return False
        stack.extend(source for source, _, _ in plan.ingoing_edges(current))
    return found_aggregate


class LimitEliminationStrategy(OptimizationStrategy):
    """Drop LIMIT nodes that are provably no-ops."""

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        # This strategy operates globally in `complete` and does not need to
        # inspect nodes during the traversal phase.
        return context

    def should_i_run(self, plan) -> bool:  # pragma: no cover - trivial
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))) > 0

    def complete(self, plan, context: OptimizerContext) -> object:
        scan_nodes = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Scan,))

        # Case 1 (ungrouped aggregate) is decided PER LIMIT below, by walking the
        # plan down from that LIMIT — see _limit_sees_only_aggregate_row. It used
        # to be a single plan-wide flag, which dropped LIMITs the aggregate did
        # not sit above.

        # Cases 2 and 3 require a single Scan and an all-safe node-type plan;
        # anything else (a second Scan, a Join, an Unnest, ...) means row
        # count downstream of the Scan is no longer bounded by its manifest.
        row_count_bound_to_scan = len(scan_nodes) == 1 and all(
            n.node_type in _ROW_COUNT_SAFE_TYPES for _, n in plan.nodes(data=True)
        )

        no_table_scan = False
        manifest_row_count = None
        if row_count_bound_to_scan:
            scan_node = scan_nodes[0][1]
            no_table_scan = getattr(scan_node, "relation", None) == "$no_table"
            manifest = getattr(scan_node, "manifest", None)
            manifest_row_count = manifest.get_record_count() if manifest is not None else None

        for nid, limit_node in get_nodes_of_type_from_logical_plan(
            plan, (LogicalPlanStepType.Limit,)
        ):
            if limit_node.offset not in (None, 0):
                continue
            if limit_node.limit in (None, 0):
                continue

            eliminable = (
                _limit_sees_only_aggregate_row(plan, nid)
                or no_table_scan
                or (manifest_row_count is not None and manifest_row_count <= limit_node.limit)
            )
            if not eliminable:
                continue

            plan.remove_node(nid, heal=True)
            if self.telemetry is not None:
                self.telemetry.optimization_limit_elimination += 1

        return plan
