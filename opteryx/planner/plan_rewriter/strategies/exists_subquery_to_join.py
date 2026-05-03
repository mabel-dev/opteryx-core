# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites EXISTS (<subquery>) predicates as LEFT SEMI or LEFT ANTI JOINs.

    WHERE EXISTS (SELECT 1 FROM T WHERE T.key = outer.key)
→   LEFT SEMI JOIN (SELECT T.key FROM T) AS $exists-xxx ON outer.key = $exists-xxx.key

    WHERE NOT EXISTS (SELECT 1 FROM T WHERE T.key = outer.key)
→   LEFT ANTI JOIN (SELECT T.key FROM T) AS $exists-xxx ON outer.key = $exists-xxx.key

The correlation predicate (T.key = outer.key) is extracted from the subquery's WHERE
clause and becomes the JOIN ON condition.  The subquery's projection is replaced with
the inner-side correlation key column(s) so the FilterJoinNode can build its hash set
over the correct values.

NOT rewritten:
  - Uncorrelated EXISTS (no equality predicate linking outer to inner table).
  - EXISTS with OR-branched correlation predicates.
  - Multiple EXISTS subqueries are handled one-at-a-time by the fixed-point rewriter loop.

NULL semantics:
  - EXISTS  → left semi: NULL outer keys excluded when right side has NULLs
              (NULL = NULL is UNKNOWN so EXISTS evaluates to FALSE, outer row excluded).
  - NOT EXISTS → left anti: uses plain anti-join.
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteStrategy
from opteryx.utils import random_string


# ---------------------------------------------------------------------------
# Condition-tree helpers
# ---------------------------------------------------------------------------

def _has_exists_subquery(condition) -> bool:
    if condition is None:
        return False
    if (condition.node_type == NodeType.UNARY_OPERATOR
            and getattr(condition, "value", None) == "Exists"):
        return True
    if condition.node_type in (NodeType.AND, NodeType.OR):
        return (
            _has_exists_subquery(getattr(condition, "left", None))
            or _has_exists_subquery(getattr(condition, "right", None))
        )
    return False


def _extract_exists_node(condition):
    """
    Walk an AND tree and pull out the first Exists unary node.

    Returns (exists_node, remaining) where remaining is the rest of the conjunction,
    or (None, condition) if no Exists node is present.
    """
    if condition is None:
        return None, None

    if (condition.node_type == NodeType.UNARY_OPERATOR
            and getattr(condition, "value", None) == "Exists"):
        return condition, None

    if condition.node_type == NodeType.AND:
        left_ex, left_rest = _extract_exists_node(getattr(condition, "left", None))
        if left_ex is not None:
            if left_rest is None:
                return left_ex, condition.right
            rebuilt = Node(node_type=NodeType.AND, do_not_create_column=True)
            rebuilt.left = left_rest
            rebuilt.right = condition.right
            return left_ex, rebuilt

        right_ex, right_rest = _extract_exists_node(getattr(condition, "right", None))
        if right_ex is not None:
            if right_rest is None:
                return right_ex, condition.left
            rebuilt = Node(node_type=NodeType.AND, do_not_create_column=True)
            rebuilt.left = condition.left
            rebuilt.right = right_rest
            return right_ex, rebuilt

    return None, condition


# ---------------------------------------------------------------------------
# Subquery plan helpers
# ---------------------------------------------------------------------------

def _get_inner_relations(subquery_plan: LogicalPlan) -> set:
    """
    Collect the names by which inner tables are referenced in the subquery plan.

    When a table has an explicit alias (e.g. FROM T AS t), only the alias is
    visible to column references — the base relation name is not.  Using both
    would incorrectly classify outer-query columns whose source happens to match
    the inner table's base name as inner columns.
    """
    relations = set()
    for _, node in subquery_plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Scan:
            continue
        relation = getattr(node, "relation", None)
        alias = getattr(node, "alias", None)
        if alias and alias != relation:
            # Explicit alias — only the alias is visible to column references
            relations.add(alias)
        elif relation:
            relations.add(relation)
        elif alias:
            relations.add(alias)
    return relations


def _split_filter_condition(condition, inner_relations: set):
    """
    Recursively split a conjunction into (correlation_list, remaining).

    correlation_list: equi-predicates referencing exactly one inner and one outer table.
    remaining: the non-correlation predicates (kept inside the subquery), or None.
    """
    if condition is None:
        return [], None

    if _is_equi_correlation(condition, inner_relations):
        return [condition], None

    if condition.node_type == NodeType.AND:
        l_corr, l_rem = _split_filter_condition(
            getattr(condition, "left", None), inner_relations
        )
        r_corr, r_rem = _split_filter_condition(
            getattr(condition, "right", None), inner_relations
        )
        corr = l_corr + r_corr
        if l_rem is None:
            remaining = r_rem
        elif r_rem is None:
            remaining = l_rem
        else:
            rebuilt = Node(node_type=NodeType.AND, do_not_create_column=True)
            rebuilt.left = l_rem
            rebuilt.right = r_rem
            remaining = rebuilt
        return corr, remaining

    return [], condition


def _is_equi_correlation(node, inner_relations: set) -> bool:
    """True if node is an equality with exactly one inner-table and one outer-table column."""
    if (node.node_type != NodeType.COMPARISON_OPERATOR
            or getattr(node, "value", None) != "Eq"):
        return False
    left_src = getattr(getattr(node, "left", None), "source", None)
    right_src = getattr(getattr(node, "right", None), "source", None)
    left_inner = left_src in inner_relations if left_src else False
    right_inner = right_src in inner_relations if right_src else False
    return left_inner != right_inner


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class ExistsSubqueryToJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type == LogicalPlanStepType.Filter
            and _has_exists_subquery(node.condition)
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if node.node_type == LogicalPlanStepType.Filter and _has_exists_subquery(node.condition):
            exists_node, remaining = _extract_exists_node(node.condition)
            if exists_node is not None:
                context.bag.setdefault("candidates", []).append(
                    (context.node_id, exists_node, remaining)
                )

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for filter_nid, exists_node, remaining in context.bag.get("candidates", []):
            negated = getattr(exists_node, "negated", False)
            subquery_plan = exists_node.parameters[0].value  # LogicalPlan (exit already removed)

            inner_relations = _get_inner_relations(subquery_plan)

            # --- Extract correlation predicates from the subquery's Filter node -------
            correlation_preds = []
            for filt_nid, filt_node in list(subquery_plan.nodes(True)):
                if filt_node.node_type != LogicalPlanStepType.Filter:
                    continue
                corr, sub_remaining = _split_filter_condition(
                    filt_node.condition, inner_relations
                )
                if corr:
                    correlation_preds.extend(corr)
                    if sub_remaining is None:
                        subquery_plan.remove_node(filt_nid, heal=True)
                    else:
                        filt_node.condition = sub_remaining
                    break  # one Filter node per EXISTS subquery

            if not correlation_preds:
                raise UnsupportedSyntaxError(
                    "EXISTS requires a correlated equality predicate linking the subquery "
                    "to the outer query (e.g. EXISTS (SELECT 1 FROM T WHERE T.k = outer.k)). "
                    "Uncorrelated EXISTS is not supported."
                )

            # --- Identify inner (subquery-side) columns from correlation predicates ---
            inner_cols = []
            for pred in correlation_preds:
                left_src = getattr(getattr(pred, "left", None), "source", None)
                if left_src in inner_relations:
                    inner_cols.append(pred.left)
                else:
                    inner_cols.append(pred.right)

            # Replace the subquery projection with the inner correlation columns so
            # the hash set is built over the join key, not a literal.
            for _, proj_node in subquery_plan.nodes(True):
                if proj_node.node_type == LogicalPlanStepType.Project:
                    proj_node.columns = inner_cols
                    break

            subquery_alias = f"$exists-{random_string(6)}"

            # --- Wrap the subquery plan in a Subquery node (same as IN subquery) ------
            top_nid = subquery_plan.get_exit_points()[0]

            subquery_wrapper = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
            subquery_wrapper.alias = subquery_alias
            subquery_wrapper.columns = inner_cols

            plan += subquery_plan

            subquery_wrapper_nid = random_string()
            plan.add_node(subquery_wrapper_nid, subquery_wrapper)
            plan.add_edge(top_nid, subquery_wrapper_nid)

            # --- Build the ON condition: outer.col = $exists-xxx.col per predicate ----
            on_parts = []
            for pred in correlation_preds:
                left_src = getattr(getattr(pred, "left", None), "source", None)
                if left_src in inner_relations:
                    outer_col = pred.right
                    inner_col = pred.left
                else:
                    outer_col = pred.left
                    inner_col = pred.right

                inner_col_name = (
                    getattr(inner_col, "source_column", None)
                    or getattr(inner_col, "value", None)
                )

                eq = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    do_not_create_column=True,
                )
                eq.left = outer_col
                eq.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=subquery_alias,
                    source_column=inner_col_name,
                )
                on_parts.append(eq)

            if len(on_parts) == 1:
                on_condition = on_parts[0]
            else:
                on_condition = on_parts[0]
                for part in on_parts[1:]:
                    and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                    and_node.left = on_condition
                    and_node.right = part
                    on_condition = and_node

            # EXISTS → left semi; NOT EXISTS → left anti
            join_type = "left anti" if negated else "left semi"

            join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
            join_node.type = join_type
            join_node.on = on_condition
            join_node.using = None
            join_node.left_relation_names = None
            join_node.right_relation_names = [subquery_alias]
            join_node.columns = []

            plan[filter_nid] = join_node
            plan.add_edge(subquery_wrapper_nid, filter_nid)

            # Push any remaining predicates (e.g. EXISTS(...) AND b > 5) above the join
            if remaining is not None:
                remaining_filter = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
                remaining_filter.condition = remaining
                remaining_filter_nid = random_string()
                plan.insert_node_after(remaining_filter_nid, remaining_filter, filter_nid)

        return plan
