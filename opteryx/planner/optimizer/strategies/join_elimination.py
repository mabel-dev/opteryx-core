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

        # `on` covers Join consumers — projection_pushdown rewrites Join.columns to
        # only the passthrough projection set, dropping the join keys themselves, so
        # we must inspect the consumer's join condition explicitly.
        for attr in ("condition", "order_by", "groups", "having", "on"):
            expr = getattr(node, attr, None)
            if expr is None:
                continue
            exprs = expr if isinstance(expr, list) else [expr]
            for e in exprs:
                # order_by items are (column_node, ascending_bool) tuples
                if isinstance(e, tuple):
                    e = e[0]
                for ident in get_all_nodes_of_type(e, (NodeType.IDENTIFIER,)):
                    if getattr(ident, "source", None) in right_relations:
                        return True

        for col in node.columns or []:
            if getattr(col, "source", None) in right_relations:
                return True

        queue.extend(plan.outgoing_edges(consumer_nid))

    return False


# Node types that pass rows through without increasing their multiplicity, so
# uniqueness established below them is preserved as we walk down toward the source.
_UNIQUENESS_PRESERVING = {
    LogicalPlanStepType.Subquery,
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.HeapSort,
    LogicalPlanStepType.Limit,
    LogicalPlanStepType.Exit,
}


def _subtree_covers_relations(plan: LogicalPlan, nid: str, relations: set) -> bool:
    """True if any node in the subtree rooted at nid is sourced from `relations`."""
    queue = [nid]
    seen = set()
    while queue:
        cur = queue.pop()
        if cur in seen:
            continue
        seen.add(cur)
        node = plan[cur]
        if getattr(node, "alias", None) in relations or getattr(node, "relation", None) in relations:
            return True
        queue.extend(src for src, _, _ in plan.ingoing_edges(cur))
    return False


def _right_is_provably_unique(plan: LogicalPlan, join_nid: str, right_relations: set) -> bool:
    """Conservatively decide whether the right input yields at most one row per join key.

    INNER → LEFT SEMI is only sound when each left row matches at most one right
    row (otherwise the inner join multiplies left rows but the semi-join collapses
    them, corrupting COUNT/SUM). That holds when the right side is unique on the
    join key. The only proof we trust without alias-resolved key matching is a
    `Distinct` below the right input (full-row uniqueness ⇒ unique on any subset),
    reached through row-count-preserving nodes. Anything else (GROUP BY whose key
    we cannot reliably match, a base scan, a join) is treated as not-provable.
    """
    # Identify the right child subtree of the join.
    right_child = None
    for src, _, _ in plan.ingoing_edges(join_nid):
        if _subtree_covers_relations(plan, src, right_relations):
            right_child = src
            break
    if right_child is None:
        return False

    # Walk down through uniqueness-preserving nodes looking for a Distinct.
    cur = right_child
    while True:
        node = plan[cur]
        if node.node_type == LogicalPlanStepType.Distinct:
            return True
        if node.node_type not in _UNIQUENESS_PRESERVING:
            return False
        children = list(plan.ingoing_edges(cur))
        if len(children) != 1:
            # A passthrough should have exactly one input; bail out conservatively.
            return False
        cur = children[0][0]


class JoinEliminationStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
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

            # Only sound when the right side has unique join keys; otherwise the
            # semi-join drops the extra matches an inner join would emit, changing
            # row counts (and thus COUNT/SUM above the join).
            if not _right_is_provably_unique(plan, join_nid, right_relations):
                continue

            join_node = plan[join_nid]
            join_node.type = "left semi"
            # Write back through the plan: the write-back is what materializes
            # the copy-on-write working plan and what tells the optimizer the
            # pass changed something — see OptimizationStrategy's contract.
            plan[join_nid] = join_node
            self.telemetry.optimization_join_elimination += 1

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.type == "inner"
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        )
