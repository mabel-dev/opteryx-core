# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Distinct Scan Pushdown

Type: Heuristic
Goal: Deduplicate at the source instead of shipping every row

    DISTINCT                          SCAN [POSTGRES] (t) [a, b] DISTINCT
    └─ SCAN [POSTGRES] (t) [a, b]  →

A plain DISTINCT (no `ON`) whose only input is a Scan on a connector that
promises "one read of this relation IS the deduplicated row set"
(`supports_distinct_pushdown`, see DistinctPushable) is absorbed into the
scan and the Distinct node is REMOVED. The scan's projection is exactly the
DISTINCT set — projection pushdown has already narrowed it to what the
Distinct emits, and the planner refuses an ORDER BY column that is not in
the SELECT list under DISTINCT.

`DISTINCT ON` is not pushed: the server keeps the first row per key in sort
order where the engine's DistinctSink keeps an arbitrary survivor, and
changing which row survives is a behaviour change to rule on, not to slip in.

Same ordering as AggregateScanPushdownStrategy, for the same reasons.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class DistinctScanPushdownStrategy(OptimizationStrategy):
    requires = ("projection-pushed", "predicates-pushed", "project-fused")
    provides = ("distinct-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.Distinct or node.on:
            return context

        ingoing = context.optimized_plan.ingoing_edges(context.node_id)
        if len(ingoing) != 1:
            return context
        scan_nid = ingoing[0][0]
        scan = context.optimized_plan[scan_nid]
        if scan is None or scan.node_type != LogicalPlanStepType.Scan:
            return context
        connector = getattr(scan, "connector", None)
        if connector is None or not connector.supports_distinct_pushdown:
            return context
        if (
            scan.pushed_aggregates is not None
            or scan.pushed_distinct
            or scan.topn_limit is not None
            or scan.limit is not None
        ):
            return context

        columns = list(scan.columns or [])
        if not connector.can_push_distinct(columns):
            return context

        scan.pushed_distinct = True
        context.optimized_plan[scan_nid] = scan
        context.optimized_plan.remove_node(context.node_id, heal=True)
        self.telemetry.optimization_distinct_scan_pushdown += 1
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Distinct,))) > 0
