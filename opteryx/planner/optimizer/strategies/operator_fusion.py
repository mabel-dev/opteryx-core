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

from opteryx.planner.logical_planner import LogicalPlan, PlanStep, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext, get_nodes_of_type_from_logical_plan
from opteryx.compiled.structures.plan_steps import HeapSortStep


class OperatorFusionStrategy(OptimizationStrategy):
    provides = ("heapsort-fused",)

    def should_i_run(self, plan: LogicalPlan) -> bool:
        # visit() fuses an Order into the Limit above it; no Order, nothing to fuse.
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Order,))) > 0

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Order:
            edges = context.optimized_plan.outgoing_edges(context.node_id)
            if len(edges) == 1:
                next_node_id = edges[0][1]
                next_node = context.optimized_plan[next_node_id]
                if next_node.node_type == LogicalPlanStepType.Limit and next_node.limit is not None:
                    offset = int(next_node.offset or 0)
                    new_node = HeapSortStep()
                    # LIMIT l OFFSET o reads the first l + o rows of the ordered
                    # stream and discards o of them, so the top l + o is all the
                    # sort ever has to keep.
                    new_node.limit = int(next_node.limit) + offset
                    new_node.order_by = node.order_by
                    # This strategy runs AFTER projection pushdown, so the fused node
                    # is the only place the Order's active-column set can come from —
                    # a fresh PlanStep has none, and without it the HeapSort
                    # would gather every column including an ORDER BY key nothing
                    # above it reads. The ORDER's set, not the LIMIT's: the fused node
                    # emits what the Order emitted, and a LIMIT adds no columns of its
                    # own so the two sets are the same set anyway.
                    new_node.pre_update_columns = node.pre_update_columns
                    if offset:
                        # The HeapSort replaces the Order and the Limit STAYS above
                        # it to skip the offset — HeapSortStep has no offset, and the
                        # Limit already applies one natively over the sorted stream
                        # (both run on one ordered dop-1 pipeline).
                        context.optimized_plan[context.node_id] = new_node
                        self.telemetry.optimization_fuse_operators_heap_sort_offset += 1
                    else:
                        context.optimized_plan[next_node_id] = new_node
                        context.optimized_plan.remove_node(context.node_id, heal=True)
                    self.telemetry.optimization_fuse_operators_heap_sort += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
