# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# AS IS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""
Optimization Strategy: Cross Join Filter Pushdown

Converts CROSS JOINs with join-like WHERE conditions into INNER JOINs.

Example:
  FROM A CROSS JOIN B WHERE A.id = B.id
  →
  FROM A INNER JOIN B ON A.id = B.id

This can provide 100,000× speedup for large cartesian products by avoiding
intermediate materialization of the full cross product.
"""

from typing import List, Optional, Tuple

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext, OptimizationStrategy
from opteryx.utils import random_string


def _split_and_conditions(node: Optional[Node]) -> List[Node]:
    """Recursively split AND nodes into a list of predicates."""
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]
    return _split_and_conditions(node.left) + _split_and_conditions(node.right)


def _build_and_condition_tree(predicates: List[Node]) -> Optional[Node]:
    """Build AND tree from list of predicates."""
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]

    result = predicates[0]
    for pred in predicates[1:]:
        and_node = Node(node_type=NodeType.AND)
        and_node.left = result
        and_node.right = pred
        result = and_node
    return result


def _extract_join_predicates(
    where_condition: Optional[Node],
    left_relations: List[str],
    right_relations: List[str],
) -> Tuple[List[Node], List[Node]]:
    """
    Extract join predicates (equalities spanning both sides) from WHERE conditions.

    Returns:
        Tuple of (join_predicates, remaining_predicates)
    """
    if where_condition is None:
        return [], []

    predicates = _split_and_conditions(where_condition)
    join_preds = []
    remaining = []

    for pred in predicates:
        # Look for equality comparisons between columns from different tables
        if (
            pred.node_type == NodeType.COMPARISON_OPERATOR
            and pred.value == "Eq"
            and pred.left is not None
            and pred.right is not None
        ):
            left_table = _get_table_from_identifier(pred.left)
            right_table = _get_table_from_identifier(pred.right)

            # Check if this spans left and right sides of the join
            if (
                left_table in left_relations
                and right_table in right_relations
            ):
                join_preds.append(pred)
                continue
            elif (
                left_table in right_relations
                and right_table in left_relations
            ):
                # Reversed - still a join predicate
                join_preds.append(pred)
                continue

        # Not a join predicate
        remaining.append(pred)

    return join_preds, remaining


def _get_table_from_identifier(node: Optional[Node]) -> Optional[str]:
    """Extract table name from an identifier node."""
    if node is None:
        return None
    if node.node_type == NodeType.IDENTIFIER:
        # Return the source (table name) if explicitly qualified
        return node.source
    return None


class CrossJoinFilterPushdownStrategy(OptimizationStrategy):
    """
    Optimization Rule - Cross Join Filter Pushdown

    Converts CROSS JOINs with equalities in WHERE clause to INNER JOINs.

    Pattern:
        FROM A CROSS JOIN B WHERE A.id = B.id

    Converts to:
        FROM A INNER JOIN B ON A.id = B.id

    Impact:
        Avoids full cartesian product materialization, reducing intermediate data size.
        Potential 100,000× speedup for large tables.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Traverse plan and collect CROSS JOINs and FILTER nodes.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        # Collect cross joins and filters
        if (
            node.node_type == LogicalPlanStepType.Join
            and node.type == "cross join"
            and not getattr(node, "on", None)
            and not getattr(node, "using", None)
        ):
            if not hasattr(context, "collected_joins"):
                context.collected_joins = []
            context.collected_joins.append(node)

        if node.node_type == LogicalPlanStepType.Filter:
            if not hasattr(context, "collected_filters"):
                context.collected_filters = []
            context.collected_filters.append(node)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Rewrite CROSS JOINs with extractable predicates to INNER JOINs.
        """
        if not hasattr(context, "collected_joins") or not hasattr(context, "collected_filters"):
            return plan

        if not context.collected_joins or not context.collected_filters:
            return plan

        # For each cross join, check if the next node is a filter
        for join_node in context.collected_joins:
            # Find this node in the plan
            join_id = None
            for node_id, node in plan._nodes.items():
                if node is join_node:
                    join_id = node_id
                    break

            if join_id is None:
                continue

            # Get successor nodes
            successors = plan.successors(join_id)
            if not successors:
                continue

            # Check if successor is a filter node
            successor_id = list(successors)[0]
            successor_node = plan[successor_id]

            if successor_node.node_type != LogicalPlanStepType.Filter:
                continue

            # Try to extract join predicates from the filter
            join_preds, remaining_preds = _extract_join_predicates(
                successor_node.condition,
                join_node.left_relation_names or [],
                join_node.right_relation_names or [],
            )

            if not join_preds:
                # No join predicates found, keep as cross join
                continue

            # Convert to inner join
            join_node.type = "inner"
            join_node.on = _build_and_condition_tree(join_preds)

            # Update or remove filter node
            if remaining_preds:
                successor_node.condition = _build_and_condition_tree(remaining_preds)
            else:
                # No remaining filters, remove the filter node
                plan.remove_node(successor_id, heal=True)

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Only run if there are cross joins in the plan."""
        for node in plan._nodes.values():
            if (
                node.node_type == LogicalPlanStepType.Join
                and node.type == "cross join"
                and not getattr(node, "on", None)
            ):
                return True
        return False
