# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Operator Fusion

Type: Heuristic
Goal: Chose more efficient physical implementations.

Some operators can be fused to be faster.

'Fused' opertors are when physical operations perform multiple logical operations.

Initially we fused Limit and Order operators, this allows us to use a heap sort
algorithm (basically we dicard records we know aren't going to be kept early).

Note that predicate and projection pushdowns may also fuse operators. Most commonly
we fuse the READ operator with SELECTION and PROJECTION operators, we also push into
JOINs, this is sometimes as part of the join condition, but we also push SELECTIONs
into joins.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory
from opteryx.vectors.vector_types import (
    get_vector_source_identifier,
    node_is_numeric_vector,
    node_is_vector_query_expression,
)

from .optimization_strategy import OptimizationStrategy, OptimizerContext


class OperatorFusionStrategy(OptimizationStrategy):
    @staticmethod
    def _is_vector_topk_candidate(order_by) -> bool:
        if len(order_by) != 1:
            return False

        expression, ascending = order_by[0]
        if expression.node_type != NodeType.FUNCTION:
            return False
        if expression.value not in ("COSINE_SIMILARITY", "COSINE_DISTANCE"):
            return False
        if len(expression.parameters) != 2:
            return False
        if get_vector_source_identifier(expression.parameters[0]) is None:
            return False
        if not node_is_numeric_vector(expression.parameters[0]):
            return False
        if not node_is_vector_query_expression(expression.parameters[1]):
            return False

        descending = not ascending
        return (expression.value == "COSINE_DISTANCE" and not descending) or (
            expression.value == "COSINE_SIMILARITY" and descending
        )

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Order:
            edges = context.optimized_plan.outgoing_edges(context.node_id)
            if len(edges) == 1:
                next_node_id = edges[0][1]
                next_node = context.optimized_plan[next_node_id]
                if next_node.node_type == LogicalPlanStepType.Limit and not next_node.offset:
                    new_node = LogicalPlanNode(node_type=LogicalPlanStepType.HeapSort)
                    new_node.limit = next_node.limit
                    new_node.order_by = node.order_by
                    new_node.vector_topk_candidate = self._is_vector_topk_candidate(node.order_by)
                    context.optimized_plan[next_node_id] = new_node
                    context.optimized_plan.remove_node(context.node_id, heal=True)
                    self.telemetry.optimization_fuse_operators_heap_sort += 1
                    if new_node.vector_topk_candidate:
                        self.telemetry.optimization_fuse_operators_vector_heap_sort += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
