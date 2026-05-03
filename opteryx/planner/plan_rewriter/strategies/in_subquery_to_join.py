# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites IN (<subquery>) predicates as LEFT SEMI JOINs.

    WHERE col IN (SELECT key FROM T WHERE ...)

is semantically identical to:

    LEFT SEMI JOIN (SELECT key FROM T WHERE ...) AS $in-xxx ON outer.col = $in-xxx.key

The rewrite is correct because:
- A semi-join returns left rows where at least one matching right row exists — exactly
  IN semantics.
- Returns only left-side columns (no duplication from the right side).
- Enables the filter-join operator to build a hash set over the right side once, then
  probe it per morsel on the left — far more efficient than per-row subquery evaluation.

NOT rewritten:
  - NOT IN (<subquery>) — SQL null semantics for NOT IN are non-trivial: if the subquery
    returns any NULL value, the entire NOT IN result is empty (UNKNOWN propagation). A
    plain anti-join does not implement this. Use NOT EXISTS instead.
  - IN (<subquery>) with more than one projected column — undefined join key.
  - IN (<subquery>) inside OR branches — cannot be expressed as a single semi-join when
    combined with OR; only top-level conjunctive IN predicates are rewritten.

Multiple IN subqueries in the same WHERE clause are handled by the fixed-point rewriter
loop: each pass rewrites one IN predicate, and the loop repeats until none remain.
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteStrategy
from opteryx.utils import random_string


def _find_projected_columns(plan: LogicalPlan, start_nid: str):
    """
    Walk from start_nid toward leaves, returning the first non-empty columns list found.
    Handles cases where the exit node is a Distinct or other pass-through with no columns.
    """
    nid = start_nid
    visited = set()
    while nid and nid not in visited:
        visited.add(nid)
        node = plan[nid]
        cols = getattr(node, "columns", None)
        if cols:
            return cols
        children = plan.ingoing_edges(nid)
        if not children:
            break
        nid = children[0][0]
    return []


def _has_in_subquery(condition) -> bool:
    """True if the condition tree contains an InSubQuery comparison."""
    if condition is None:
        return False
    return bool(get_all_nodes_of_type(condition, (NodeType.SUBQUERY,)))


def _extract_in_subquery(condition):
    """
    Walk an AND tree and pull out the first InSubQuery comparison node.

    Returns (in_node, remaining) where:
      - in_node    is the COMPARISON_OPERATOR(InSubQuery, ...) node
      - remaining  is the rest of the AND tree with in_node removed, or None if nothing is left

    Returns (None, condition) when no InSubQuery node is present.
    """
    if condition is None:
        return None, None

    if (
        condition.node_type == NodeType.COMPARISON_OPERATOR
        and condition.value == "InSubQuery"
    ):
        return condition, None

    if condition.node_type == NodeType.AND:
        left_in, left_rest = _extract_in_subquery(condition.left)
        if left_in is not None:
            if left_rest is None:
                return left_in, condition.right
            rebuilt = Node(node_type=NodeType.AND, do_not_create_column=True)
            rebuilt.left = left_rest
            rebuilt.right = condition.right
            return left_in, rebuilt

        right_in, right_rest = _extract_in_subquery(condition.right)
        if right_in is not None:
            if right_rest is None:
                return right_in, condition.left
            rebuilt = Node(node_type=NodeType.AND, do_not_create_column=True)
            rebuilt.left = condition.left
            rebuilt.right = right_rest
            return right_in, rebuilt

    return None, condition


class InSubqueryToJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type == LogicalPlanStepType.Filter and _has_in_subquery(node.condition)
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if node.node_type == LogicalPlanStepType.Filter and _has_in_subquery(node.condition):
            in_node, remaining = _extract_in_subquery(node.condition)
            if in_node is not None:
                context.bag.setdefault("candidates", []).append(
                    (context.node_id, in_node, remaining)
                )

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for filter_nid, in_node, remaining in context.bag.get("candidates", []):
            outer_col = in_node.left
            subquery_plan = in_node.right.value  # LogicalPlan; Exit node already removed

            # Validate: subquery must project exactly one column
            top_nid = subquery_plan.get_exit_points()[0]
            projected_cols = _find_projected_columns(subquery_plan, top_nid)
            op = "NOT IN" if getattr(in_node, "negated", False) else "IN"
            if not projected_cols:
                raise UnsupportedSyntaxError(
                    f"{op} (<subquery>) requires the subquery to project at least one column."
                )
            if len(projected_cols) > 1:
                raise UnsupportedSyntaxError(
                    f"{op} (<subquery>) requires the subquery to project exactly one column; "
                    "found multiple. Use a single-column subquery."
                )

            subquery_col_node = projected_cols[0]
            subquery_col_name = (
                getattr(subquery_col_node, "alias", None)
                or getattr(subquery_col_node, "source_column", None)
                or getattr(subquery_col_node, "value", None)
            )
            if not subquery_col_name:
                raise UnsupportedSyntaxError(
                    "IN (<subquery>) could not determine the subquery column name. "
                    "Add an alias: SELECT expr AS col FROM ..."
                )

            subquery_alias = f"$in-{random_string(6)}"

            # Wrap the subquery plan in a Subquery node (same pattern as FROM-clause subqueries)
            subquery_wrapper = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
            subquery_wrapper.alias = subquery_alias
            subquery_wrapper.columns = projected_cols

            # Merge the subquery plan nodes and edges into the main plan
            plan += subquery_plan

            # Add the wrapper node and connect it above the subquery's top node
            subquery_wrapper_nid = random_string()
            plan.add_node(subquery_wrapper_nid, subquery_wrapper)
            plan.add_edge(top_nid, subquery_wrapper_nid)

            # Build the ON condition: outer_col = subquery_alias.subquery_col
            on_eq = Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value="Eq",
                do_not_create_column=True,
            )
            on_eq.left = outer_col
            on_eq.right = LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source=subquery_alias,
                source_column=subquery_col_name,
            )

            # NOT IN → null-aware anti-join; IN → semi-join
            join_type = "left anti null-aware" if getattr(in_node, "negated", False) else "left semi"

            join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
            join_node.type = join_type
            join_node.on = on_eq
            join_node.using = None
            join_node.left_relation_names = None   # inferred by visit_join after binding
            join_node.right_relation_names = [subquery_alias]
            join_node.columns = []

            plan[filter_nid] = join_node

            # Wire the subquery wrapper as the right input of the join
            plan.add_edge(subquery_wrapper_nid, filter_nid)

            # If predicates remain (e.g. col IN (...) AND b > 5), push them above the join
            if remaining is not None:
                remaining_filter = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
                remaining_filter.condition = remaining
                remaining_filter_nid = random_string()
                plan.insert_node_after(remaining_filter_nid, remaining_filter, filter_nid)

        return plan
