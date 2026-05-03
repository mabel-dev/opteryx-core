# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Elimination

Type: Heuristic
Goal: Rewrite INNER JOINs to LEFT SEMI JOINs when the right relation contributes
      no columns to the output

When no column from the right-hand relation appears above the JOIN node in the
plan, the join exists only to test for existence of a matching row.  An INNER
JOIN in that position is semantically equivalent to a LEFT SEMI JOIN:

    SELECT title FROM film JOIN language ON film.language_id = language.language_id

becomes:

    SELECT title FROM film LEFT SEMI JOIN language ON film.language_id = language.language_id

The semi-join returns at most one row per left row regardless of how many right
rows match, which is the correct deduplication behaviour.  It also allows the
execution engine to short-circuit after the first match per probe row.
"""

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext, get_nodes_of_type_from_logical_plan


def _right_columns_used_above(plan: LogicalPlan, join_nid: str, right_relations: set) -> bool:
    """Return True if any identifier sourced from right_relations appears above the JOIN."""
    visited = {join_nid}
    queue = list(plan.outgoing_edges(join_nid))
    while queue:
        _, consumer_nid, _ = queue.pop()
        if consumer_nid in visited:
            continue
        visited.add(consumer_nid)
        node = plan[consumer_nid]

        for attr in ("condition", "order_by", "groups", "having"):
            expr = getattr(node, attr, None)
            if expr is None:
                continue
            exprs = expr if isinstance(expr, list) else [expr]
            for e in exprs:
                for ident in get_all_nodes_of_type(e, (NodeType.IDENTIFIER,)):
                    if getattr(ident, "source", None) in right_relations:
                        return True

        for col in node.columns or []:
            if getattr(col, "source", None) in right_relations:
                return True

        queue.extend(plan.outgoing_edges(consumer_nid))

    return False


class JoinEliminationStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        if (
            node.node_type == LogicalPlanStepType.Join
            and node.type == "inner"
            and not getattr(node, "using", None)
            and getattr(node, "left_columns", None)
        ):
            context.collected_joins.append((context.node_id, node))

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        for join_nid, join_node in context.collected_joins:
            right_relations = set(join_node.right_relation_names or [])
            if not right_relations:
                continue

            if _right_columns_used_above(plan, join_nid, right_relations):
                continue

            plan[join_nid].type = "left semi"
            self.telemetry.optimization_join_elimination += 1

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.type == "inner"
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        )
