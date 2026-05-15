# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - SUM rewriter

Type: Heuristic
Goal: sum(e + const) → sum(e) + const × count(e), integers only.

IEEE 754 floating-point addition is non-associative, so this rewrite is only
sound when both the column and the constant are integer-typed. Runs post-binding
so that identifier.type is known.
"""

from orso.types import OrsoTypes

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext

_AGGREGATE_TYPES = (LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup)


class SumRewriteStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        if node.node_type not in _AGGREGATE_TYPES:
            return context

        aggregate_set = {}
        result_aggregates = []
        result_projection = list(node.projection or [])
        modified = False

        for aggregate in node.aggregates or []:
            if aggregate.value != "SUM":
                result_aggregates.append(aggregate)
                continue

            param = aggregate.parameters[0]
            if param.node_type != NodeType.BINARY_OPERATOR:
                result_aggregates.append(aggregate)
                continue

            identifier = param.left
            operator = param.value
            literal = param.right

            if (
                identifier.node_type != NodeType.IDENTIFIER
                or literal.node_type != NodeType.LITERAL
                or operator not in ("Plus", "Minus")
                or identifier.type != OrsoTypes.INTEGER
                or literal.type != OrsoTypes.INTEGER
            ):
                result_aggregates.append(aggregate)
                continue

            modified = True
            sum_key = f"SUM_{identifier.qualified_name}"
            count_key = f"COUNT_{identifier.qualified_name}"

            if sum_key not in aggregate_set:
                sum_node = Node(node_type=NodeType.AGGREGATOR, value="SUM", parameters=[identifier])
                result_aggregates.append(sum_node)
                aggregate_set[sum_key] = sum_node
            else:
                sum_node = aggregate_set[sum_key]

            if count_key not in aggregate_set:
                count_node = Node(
                    node_type=NodeType.AGGREGATOR, value="COUNT", parameters=[identifier]
                )
                result_aggregates.append(count_node)
                aggregate_set[count_key] = count_node
            else:
                count_node = aggregate_set[count_key]

            scaling_node = Node(
                node_type=NodeType.BINARY_OPERATOR, value="Multiply", left=count_node, right=literal
            )
            calculation_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value=operator,
                left=sum_node,
                right=scaling_node,
                alias=aggregate.alias or aggregate.qualified_name,
            )

            result_projection = [p for p in result_projection if p != aggregate]
            result_projection.append(calculation_node)

        if modified:
            node.aggregates = result_aggregates
            node.projection = result_projection
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
