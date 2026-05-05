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

from typing import List, Optional, Set, Tuple

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.binder.common import extract_join_fields
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


def _collect_cross_joins(
    plan: LogicalPlan,
    node_id: str,
    result: List[Tuple[str, LogicalPlanNode]],
    visited: Optional[Set[str]] = None,
) -> None:
    """
    Descend the input subtree of `node_id` collecting cross-join nodes that are
    still candidates for conversion (no ON / USING). Subqueries are not
    descended into — a CTE/subquery is an opaque relation at this scope.
    """
    if visited is None:
        visited = set()
    if node_id in visited:
        return
    visited.add(node_id)

    for child_id, _, _ in plan.ingoing_edges(node_id):
        child_node = plan[child_id]
        if child_node.node_type == LogicalPlanStepType.Subquery:
            continue
        if (
            child_node.node_type == LogicalPlanStepType.Join
            and child_node.type == "cross join"
            and not getattr(child_node, "on", None)
            and not getattr(child_node, "using", None)
        ):
            result.append((child_id, child_node))
        _collect_cross_joins(plan, child_id, result, visited)


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
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        For every Filter in the plan, walk down the input subtree to find any
        unconverted cross joins, and try to extract join predicates from the
        filter's condition for each one. Cross joins below intermediate inner
        joins are reachable this way — earlier predicate pushdown can interpose
        such joins between the filter and the remaining cross joins.
        """
        filter_ids = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.Filter
        ]

        for filter_id in filter_ids:
            filter_node = plan[filter_id]
            if filter_node.condition is None:
                continue

            cross_joins: List[Tuple[str, LogicalPlanNode]] = []
            _collect_cross_joins(plan, filter_id, cross_joins)
            if not cross_joins:
                continue

            remaining_condition = filter_node.condition
            any_converted = False

            for join_id, join_node in cross_joins:
                join_preds, remaining_preds = _extract_join_predicates(
                    remaining_condition,
                    join_node.left_relation_names or [],
                    join_node.right_relation_names or [],
                )
                if not join_preds:
                    continue

                on_condition = _build_and_condition_tree(join_preds)
                join_node.type = "inner"
                join_node.on = on_condition
                join_node.left_columns, join_node.right_columns = extract_join_fields(
                    on_condition,
                    join_node.left_relation_names or [],
                    join_node.right_relation_names or [],
                )
                join_node.columns = get_all_nodes_of_type(on_condition, (NodeType.IDENTIFIER,))
                plan[join_id] = join_node
                remaining_condition = _build_and_condition_tree(remaining_preds)
                any_converted = True
                if remaining_condition is None:
                    break

            if any_converted:
                if remaining_condition is None:
                    plan.remove_node(filter_id, heal=True)
                else:
                    filter_node.condition = remaining_condition
                    plan[filter_id] = filter_node

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
