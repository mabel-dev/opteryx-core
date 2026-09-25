# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Top-N Scan Pushdown

Type: Heuristic
Goal: Reduce Rows read for ORDER BY ... LIMIT n

When a HeapSort (a fused Order+Limit) reads directly from a Scan whose
connector can honour the sort spec, the spec is stamped on the Scan:

    scan.topn_order_by = [(schema_column, ascending), ...]
    scan.topn_limit    = n

and, for a single key, the physical-name form the parquet reader consumes
(`topn_sort_name` / `topn_sort_identity` / `topn_descending`). The parquet
reader uses it to cut pass-2 work to rows that can be in the top-n; a SQL
connector renders it as ORDER BY ... LIMIT on the server.

The downstream HeapSort is left in place and makes the final, canonical cut.
The reader may return a superset of the true top-n; the HeapSort result is
identical to the un-pushed plan regardless of what the reader did with the
spec. So the spec is an optimisation, never the answer.

Whether a scan can take the spec is the CONNECTOR's decision
(`supports_topn_pushdown` + `can_push_topn`, see TopNPushable): the parquet
reader sorts on one physical column, a SQL server takes any key list. This
strategy only checks the plan shape — HeapSort directly over a single Scan
(no intervening node; a Filter or Join between them would make the spec
describe a different row set), positive LIMIT, no OFFSET (fusion already
excludes it).

Ordering: runs AFTER RedundantOperations/ProjectFusion have removed the
Project a SELECT list leaves between the HeapSort and the Scan — before them
the HeapSort is adjacent to a Scan only for `SELECT *`, and the stamp is never
applied to a real query.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import PlanStep
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class TopNScanPushdownStrategy(OptimizationStrategy):
    """Attach a top-N sort spec to a scan feeding a HeapSort."""

    # the HeapSort it targets is created by OperatorFusionStrategy; the Project
    # between it and the Scan is removed by ProjectFusion (and RedundantOperations)
    requires = ("heapsort-fused", "project-fused")
    provides = ("topn-scan-pushdown",)

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.HeapSort:
            return context

        limit = node.limit
        order_by = node.order_by
        if not limit or limit <= 0 or not order_by:
            return context

        # The HeapSort must read directly from a single Scan.
        ingoing = context.optimized_plan.ingoing_edges(context.node_id)
        if len(ingoing) != 1:
            return context
        source_nid = ingoing[0][0]
        source_node = context.optimized_plan[source_nid]
        if source_node is None or source_node.node_type != LogicalPlanStepType.Scan:
            return context
        # A scan that already absorbed an aggregate or DISTINCT emits rows that
        # are not the relation's rows; its connector's `can_push_topn` reasons
        # about relation columns, so the spec is not offered.
        if source_node.pushed_aggregates is not None or source_node.pushed_distinct:
            return context

        connector = source_node.connector
        if connector is None or not connector.supports_topn_pushdown:
            return context
        if not connector.can_push_topn(order_by):
            return context

        source_node.topn_order_by = [
            (expression.schema_column, bool(ascending)) for expression, ascending in order_by
        ]
        source_node.topn_limit = int(limit)
        if len(order_by) == 1 and order_by[0][0].node_type == NodeType.IDENTIFIER:
            # The physical-name form the parquet reader and TopNManifestPruning
            # consume. Stamped only for the single-key shape they understand.
            schema_column = order_by[0][0].schema_column
            source_node.topn_sort_name = schema_column.name
            source_node.topn_sort_identity = schema_column.identity
            source_node.topn_descending = not order_by[0][1]
        context.optimized_plan[source_nid] = source_node
        self.telemetry.optimization_topn_scan_pushdown += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(
            get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.HeapSort,))
        ) > 0
