# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Aggregate Scan Pushdown

Type: Heuristic
Goal: Answer an aggregate at the source instead of shipping every row

    Aggregate / AggregateAndGroup           SCAN [POSTGRES] (t)
    └─ SCAN [POSTGRES] (t)          →          AGGREGATE [COUNT(*), SUM(x)] GROUP BY [k]

An Aggregate whose only input is a Scan on a connector that promises "one read
of this relation IS the complete aggregate" (`supports_aggregate_pushdown`,
see AggregatePushable) has nothing left to do locally once the connector
computes it: the scan emits one row per group (one row, ungrouped) under the
aggregate outputs' own identities and bound types, and the Aggregate node is
REMOVED. That removal is exact only under the connector's promise — a source
that splits the relation across files or partitions would return partials that
still need combining, and must not set the flag.

The connector's `can_push_aggregate` is the semantic gate: every key must be a
column of the relation and every aggregate must have a remote spelling whose
result is the type the binder bound (see postgres_connector._remote_aggregate).
Anything it declines leaves the plan untouched.

Declined by shape here, before the connector is asked:
  - GROUP BY ROLLUP/CUBE/GROUPING SETS (`grouping_sets`): the grouping-id lane
    and GROUPING() are engine-side machinery;
  - HAVING (`having_condition`): not rendered remotely yet;
  - a scan already carrying a pushed LIMIT, top-N or DISTINCT;
  - an Aggregate that does not read DIRECTLY from the Scan.

Ordering: after PredicatePushdown (the Filter is absorbed into the scan and
HAVING has been folded into `having_condition`) and ProjectionPushdown; after
ProjectFusion/RedundantOperations so the Project the planner leaves between the
aggregate and the Exit is already gone.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import PlanStep
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

_AGGREGATE_TYPES = (LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup)


class AggregateScanPushdownStrategy(OptimizationStrategy):
    requires = ("projection-pushed", "predicates-pushed", "project-fused")
    provides = ("aggregates-pushed",)

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type not in _AGGREGATE_TYPES:
            return context
        if node.node_type == LogicalPlanStepType.AggregateAndGroup and (
            node.grouping_sets is not None or node.having_condition is not None
        ):
            return context

        ingoing = context.optimized_plan.ingoing_edges(context.node_id)
        if len(ingoing) != 1:
            return context
        scan_nid = ingoing[0][0]
        scan = context.optimized_plan[scan_nid]
        if scan is None or scan.node_type != LogicalPlanStepType.Scan:
            return context
        connector = getattr(scan, "connector", None)
        if connector is None or not connector.supports_aggregate_pushdown:
            return context
        if (
            scan.pushed_aggregates is not None
            or scan.pushed_distinct
            or scan.topn_limit is not None
            or scan.limit is not None
        ):
            return context

        groups = list(node.groups or [])
        aggregates = list(node.aggregates or [])
        if not aggregates and not groups:
            return context
        if not connector.can_push_aggregate(groups, aggregates):
            return context

        # The scan now emits the group keys and the aggregate outputs — the
        # aggregate node's own bound columns, so everything above resolves by
        # the identities it already holds. `columns` becomes the keys: the
        # aggregate operands are consumed on the server and never arrive.
        scan.pushed_groups = groups
        scan.pushed_aggregates = aggregates
        scan.columns = list(groups)
        context.optimized_plan[scan_nid] = scan
        context.optimized_plan.remove_node(context.node_id, heal=True)
        self.telemetry.optimization_aggregate_scan_pushdown += 1
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(get_nodes_of_type_from_logical_plan(plan, _AGGREGATE_TYPES)) > 0
