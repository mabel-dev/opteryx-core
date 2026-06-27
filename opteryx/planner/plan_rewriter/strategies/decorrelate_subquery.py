# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites correlated scalar subqueries as regular joins (Phase 1: simple unnesting).

    WHERE col = (SELECT AGG(x) FROM T WHERE T.k = outer.k)
→   INNER JOIN (SELECT k, AGG(x) AS $agg_xxx FROM T GROUP BY k) AS $scalar-xxx
    ON outer.k = $scalar-xxx.k
    WHERE col = $scalar-xxx.$agg_xxx

The correlation predicate (T.k = outer.k) is extracted from the subquery's WHERE
clause and lifted as the JOIN ON condition.  The inner plan's aggregate is extended
with the join key column so the join can match rows.

Phase 1 (this file): handles equi-correlated scalar subqueries.
Phase 2 (future): D-based general unnesting for non-equi or OR-branched correlations.

NOT rewritten:
  - Uncorrelated scalar subqueries.
  - Scalar subqueries with OR-branched or non-equi correlation predicates.
  - Scalar subqueries whose inner plan has no aggregate node (use EXISTS/IN instead).
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies.exists_subquery_to_join import (
    _get_inner_relations,
    _is_equi_correlation,
    _split_filter_condition,
)
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import (
    PlanRewriteContext,
    PlanRewriteStrategy,
)
from opteryx.utils import random_string


# ---------------------------------------------------------------------------
# Condition-tree helpers
# ---------------------------------------------------------------------------

def _is_exists_or_in_subquery(node) -> bool:
    """EXISTS and IN-subquery wrap a SUBQUERY node but are not scalar subqueries —
    they are owned by ExistsSubqueryToJoinStrategy / InSubqueryToJoinStrategy."""
    if node.node_type == NodeType.UNARY_OPERATOR and node.value == "Exists":
        return True
    if node.node_type == NodeType.COMPARISON_OPERATOR and node.value == "InSubQuery":
        return True
    return False


def _has_scalar_subquery(condition) -> bool:
    """True if the condition tree contains a *scalar* subquery — a NodeType.SUBQUERY
    used directly as a value (e.g. `col = (SELECT ...)`).

    EXISTS / IN subqueries also embed a SUBQUERY node, but those are not scalar and
    are rewritten by their own strategies earlier in the fixed-point loop.  Descending
    into them would mis-classify a still-unrewritten EXISTS/IN — for example the
    surviving sibling of a multi-EXISTS filter, which is rewritten one per pass — as
    an unsupported scalar subquery and raise prematurely.
    """
    if condition is None:
        return False
    if condition.node_type == NodeType.SUBQUERY:
        return True
    if _is_exists_or_in_subquery(condition):
        return False
    for child_attr in ("left", "right", "centre"):
        child = getattr(condition, child_attr, None)
        if child is not None and _has_scalar_subquery(child):
            return True
    params = getattr(condition, "parameters", None)
    if params:
        return any(_has_scalar_subquery(p) for p in params)
    return False


def _extract_scalar_subquery(condition):
    """
    Walk condition tree and extract the first NodeType.SUBQUERY node.

    Returns (subquery_plan, replacement_placeholder, replace_fn) where:
      - subquery_plan  is the LogicalPlan embedded in the SUBQUERY node
      - replace_fn     is a callable(new_col) -> new_condition that performs
                       the replacement of the SUBQUERY node with new_col

    Returns (None, None, None) if no SUBQUERY node is present.
    """
    if condition is None:
        return None, None, None

    if condition.node_type == NodeType.SUBQUERY:
        # The condition itself IS the subquery — caller replaces it entirely
        def _replace(new_col):
            return new_col
        return condition.value, condition, _replace

    for child_attr in ("left", "right", "centre"):
        child = getattr(condition, child_attr, None)
        if child is None:
            continue
        plan, _, replace_child = _extract_scalar_subquery(child)
        if plan is not None:
            attr = child_attr

            def _replace(new_col, _cond=condition, _attr=attr, _rc=replace_child):
                setattr(_cond, _attr, _rc(new_col))
                return _cond

            return plan, child, _replace

    params = getattr(condition, "parameters", None)
    if params:
        for i, param in enumerate(params):
            plan, _, replace_param = _extract_scalar_subquery(param)
            if plan is not None:
                def _replace(new_col, _cond=condition, _i=i, _rc=replace_param):
                    _cond.parameters[_i] = _rc(new_col)
                    return _cond
                return plan, param, _replace

    return None, None, None


# ---------------------------------------------------------------------------
# Inner plan helpers
# ---------------------------------------------------------------------------

def _find_aggregate_node(inner_plan: LogicalPlan):
    """Return (nid, node) for the first Aggregate or AggregateAndGroup node."""
    for nid, node in inner_plan.nodes(True):
        if node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
        ):
            return nid, node
    return None, None


def _add_join_key_to_projects(inner_plan: LogicalPlan, inner_join_col: Node, col_alias: str):
    """
    Append the join key column to every Project node in the inner plan so that
    the column survives the projection above the aggregate and is visible to the
    outer JOIN ON condition.
    """
    key_col = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source=getattr(inner_join_col, "source", None),
        source_column=getattr(inner_join_col, "source_column", None) or getattr(inner_join_col, "value", None),
    )
    for _nid, node in inner_plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Project:
            if node.columns is None:
                node.columns = []
            node.columns = list(node.columns) + [key_col]


def _add_join_key_to_aggregate(inner_plan: LogicalPlan, inner_join_col: Node, col_alias: str):
    """
    Extend the aggregate node to group by the join key and include it in the output.

    - If an Aggregate node (no GROUP BY) is found: convert to AggregateAndGroup
      and add inner_join_col as the sole group key.
    - If an AggregateAndGroup node is found: append inner_join_col to groups.
    - Appends a copy of inner_join_col (with col_alias) to the node's projection.
    """
    agg_nid, agg_node = _find_aggregate_node(inner_plan)
    if agg_nid is None:
        raise UnsupportedSyntaxError(
            "Correlated scalar subquery could not be decorrelated: inner plan has no "
            "aggregate node. Rewrite using EXISTS or IN instead."
        )

    key_col = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source=getattr(inner_join_col, "source", None),
        source_column=getattr(inner_join_col, "source_column", None) or getattr(inner_join_col, "value", None),
        alias=col_alias,
    )

    if agg_node.node_type == LogicalPlanStepType.Aggregate:
        # Convert to AggregateAndGroup
        agg_node.node_type = LogicalPlanStepType.AggregateAndGroup
        agg_node.groups = [key_col]
    else:
        if agg_node.groups is None:
            agg_node.groups = []
        agg_node.groups.append(key_col)

    # Add the key column to projection so it appears in the subquery output
    if agg_node.projection is None:
        agg_node.projection = []
    agg_node.projection = list(agg_node.projection) + [key_col]


def _get_agg_output_col_name(inner_plan: LogicalPlan, agg_alias: str) -> str:
    """
    Set a stable alias on the aggregate expression and return it.
    Walks the aggregate node's aggregates list and sets agg_alias on the first one.
    """
    _, agg_node = _find_aggregate_node(inner_plan)
    if agg_node is None:
        return agg_alias

    aggregates = getattr(agg_node, "aggregates", None)
    if aggregates:
        agg_expr = aggregates[0]
        agg_expr.alias = agg_alias
        agg_expr.query_column = agg_alias
        # Also stamp any copy of the aggregate expression sitting in PROJECT node columns —
        # the PROJECT and the AGGREGATE both hold a reference to the MAX(...) Node but they
        # are separate objects built by the logical planner.  The PROJECT binder re-binds its
        # copy independently, so we must update it too or it produces a 'MAX(mass)' schema
        # column that shadows and replaces '$agg_xxx'.
        for _nid, pnode in inner_plan.nodes(True):
            if pnode.node_type == LogicalPlanStepType.Project:
                for col in (pnode.columns or []):
                    if col.node_type == NodeType.AGGREGATOR:
                        col.alias = agg_alias
                        col.query_column = agg_alias
        # Also update projection if present
        if agg_node.projection:
            for col in agg_node.projection:
                if col.node_type == NodeType.AGGREGATOR:
                    col.alias = agg_alias
                    col.query_column = agg_alias
                    break

    return agg_alias


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class DecorrelateSubqueryStrategy(PlanRewriteStrategy):

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type == LogicalPlanStepType.Filter
            and _has_scalar_subquery(node.condition)
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if (
            node.node_type == LogicalPlanStepType.Filter
            and _has_scalar_subquery(node.condition)
        ):
            context.bag.setdefault("candidates", []).append(context.node_id)

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        if context.bag.get("candidates"):
            raise UnsupportedSyntaxError(
                "Correlated scalar subqueries are not supported. "
                "Rewrite using EXISTS, IN, or an explicit JOIN."
            )
        return plan


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------

def _decorrelate_one(plan: LogicalPlan, filter_nid: str) -> LogicalPlan:
    filter_node = plan[filter_nid]
    condition = filter_node.condition

    # --- Extract the SUBQUERY expression node from the condition ---------------
    inner_plan, _, replace_fn = _extract_scalar_subquery(condition)
    if inner_plan is None:
        return plan  # nothing to do

    inner_relations = _get_inner_relations(inner_plan)

    # --- Find and extract correlation predicates from inner plan's Filter ------
    correlation_preds = []
    for filt_nid, filt_node in list(inner_plan.nodes(True)):
        if filt_node.node_type != LogicalPlanStepType.Filter:
            continue
        corr, sub_remaining = _split_filter_condition(filt_node.condition, inner_relations)
        if corr:
            correlation_preds.extend(corr)
            if sub_remaining is None:
                inner_plan.remove_node(filt_nid, heal=True)
            else:
                filt_node.condition = sub_remaining
            break

    if not correlation_preds:
        raise UnsupportedSyntaxError(
            "Correlated scalar subquery requires a correlated equality predicate linking "
            "the subquery to the outer query. "
            "Uncorrelated scalar subqueries are not supported."
        )

    # --- Identify inner and outer join key columns ----------------------------
    inner_join_cols = []
    outer_join_cols = []
    for pred in correlation_preds:
        left_src = getattr(getattr(pred, "left", None), "source", None)
        if left_src in inner_relations:
            inner_join_cols.append(pred.left)
            outer_join_cols.append(pred.right)
        else:
            inner_join_cols.append(pred.right)
            outer_join_cols.append(pred.left)

    # --- Give the aggregate output column a stable alias ----------------------
    agg_alias = f"$agg_{random_string(6)}"
    _get_agg_output_col_name(inner_plan, agg_alias)

    # --- Add join key columns to the inner aggregate node ---------------------
    inner_col_names = []
    for inner_col in inner_join_cols:
        col_name = (
            getattr(inner_col, "source_column", None)
            or getattr(inner_col, "value", None)
            or f"$key_{random_string(4)}"
        )
        inner_col_names.append(col_name)
        _add_join_key_to_aggregate(inner_plan, inner_col, col_name)
        _add_join_key_to_projects(inner_plan, inner_col, col_name)

    # --- Generate subquery alias and reference column -------------------------
    subquery_alias = f"$scalar-{random_string(6)}"
    ref_col = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source=subquery_alias,
        source_column=agg_alias,
    )

    # --- Replace SUBQUERY node in outer condition with the reference ----------
    filter_node.condition = replace_fn(ref_col)

    # --- Build ON condition: outer_col = $scalar-xxx.inner_col_name -----------
    on_parts = []
    for outer_col, inner_col_name in zip(outer_join_cols, inner_col_names):
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

    # --- Wrap inner plan in Subquery node -------------------------------------
    top_nid = inner_plan.get_exit_points()[0]
    subquery_wrapper = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
    subquery_wrapper.alias = subquery_alias
    subquery_wrapper.columns = [Node(node_type=NodeType.WILDCARD)]  # expose all inner columns

    plan += inner_plan

    subquery_wrapper_nid = random_string()
    plan.add_node(subquery_wrapper_nid, subquery_wrapper)
    plan.add_edge(top_nid, subquery_wrapper_nid)

    # --- Create inner join and wire it below the filter -----------------------
    join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join_node.type = "inner"
    join_node.on = on_condition
    join_node.using = None
    join_node.left_relation_names = None
    join_node.right_relation_names = [subquery_alias]
    join_node.columns = []

    join_nid = random_string()

    # insert_node_before wires: (all inputs of filter_nid) → join_nid → filter_nid
    plan.insert_node_before(join_nid, join_node, filter_nid)

    # Add the subquery as a second input to the join
    plan.add_edge(subquery_wrapper_nid, join_nid)

    return plan
