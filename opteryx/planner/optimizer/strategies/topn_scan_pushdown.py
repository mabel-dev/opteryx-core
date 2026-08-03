# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Top-N Scan Pushdown (WP-2)

Type: Heuristic
Goal: Reduce late-materialization work for `ORDER BY <col> LIMIT n` queries.

When a HeapSort (a fused Order+Limit) reads directly from a parquet Scan, the
scan currently materializes every projected column for every filter-surviving
row, even though only `n` rows survive the sort. This rule stamps a top-N spec
onto the scan so its late-materialization path can:

  1. decode the sort column in pass 1 (alongside the filter columns),
  2. select only the rows whose sort key is at-least-as-good as the n-th best
     value (n plus any ties at the boundary), and
  3. materialize the remaining (projection-only) columns for just those rows.

The downstream HeapSort is left in place and makes the final, canonical cut.
Because the scan only ever drops rows that are strictly worse than the true
top-n, the HeapSort result is identical to the un-pushed plan regardless of
tie-breaking.

Scope of this first cut (deliberately narrow, mirrors the conservative WP-1
gate): single-column ORDER BY where the key is a plain column reference that is
physically present in the scanned relation, no OFFSET, and the HeapSort reads
directly from the Scan. Anything else falls through unchanged.

Note this also excludes vector (nearest-neighbour) top-k without a special case:
`OperatorFusionStrategy.vector_topk_candidate` requires the sort key to be a
COSINE_DISTANCE/COSINE_SIMILARITY FUNCTION node, and the plain-column-reference
check below admits only IDENTIFIER — so a flagged node can never reach the
stamping code.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class TopNScanPushdownStrategy(OptimizationStrategy):
    """Attach a top-N sort spec to a parquet scan feeding a HeapSort."""

    # the HeapSort it targets is created by OperatorFusionStrategy
    requires = ("heapsort-fused",)
    provides = ("topn-scan-pushdown",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type != LogicalPlanStepType.HeapSort:
            return context

        limit = getattr(node, "limit", None)
        order_by = getattr(node, "order_by", None)
        if not limit or limit <= 0 or not order_by or len(order_by) != 1:
            return context

        expression, ascending = order_by[0]
        # Single-cut scope: the sort key must be a plain column reference.
        if expression.node_type != NodeType.IDENTIFIER:
            return context
        schema_column = getattr(expression, "schema_column", None)
        if schema_column is None:
            return context
        sort_name = getattr(schema_column, "name", None)
        sort_identity = getattr(schema_column, "identity", None)
        if not sort_name or not sort_identity:
            return context

        # The HeapSort must read directly from a single Scan (no intervening
        # Project/Join in this first cut).
        ingoing = context.optimized_plan.ingoing_edges(context.node_id)
        if len(ingoing) != 1:
            return context
        source_nid = ingoing[0][0]
        source_node = context.optimized_plan[source_nid]
        if source_node is None or source_node.node_type != LogicalPlanStepType.Scan:
            return context

        # The scan must be able to honour the spec: the sort column has to be a
        # real column of the scanned relation (the read path resolves it by
        # physical name). Computed/derived sort keys are out of scope.
        source_node.topn_sort_name = sort_name
        source_node.topn_sort_identity = sort_identity
        source_node.topn_descending = not ascending
        source_node.topn_limit = int(limit)
        context.optimized_plan[source_nid] = source_node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(
            get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.HeapSort,))
        ) > 0
