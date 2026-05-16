# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites Window logical nodes (OVER (PARTITION BY ...)) as inner joins.

    SELECT agg(e) OVER (PARTITION BY c) FROM t

is rewritten at the plan level to:

    WITH _win AS (SELECT c, agg(e) AS $win_xxx FROM t GROUP BY c)
    SELECT $win_xxx FROM t INNER JOIN _win ON t.c = _win.c

The Window node carries the aggregate expressions, the partition columns, and a copy
of the base Scan node. The rewriter builds a CTE sub-plan (Scan → AggregateAndGroup),
wraps it in a Subquery node, replaces the Window node with an inner Join, and wires
the subquery as the right-hand input of the join.

Limitations (Phase 1):
- PARTITION BY only — no ORDER BY, no frame specs.
- Single source table (no pre-existing joins before the window).
- NULL partition keys: uses Eq (= not IS NOT DISTINCT FROM); NULL partitions are excluded.
"""

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import (
    PlanRewriteContext,
    PlanRewriteStrategy,
)
from opteryx.utils import random_string


def _build_eq_condition(left_col: Node, right_col: Node) -> Node:
    eq = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True)
    eq.left = left_col
    eq.right = right_col
    return eq


def _and_conditions(conditions: list) -> Node:
    result = conditions[0]
    for cond in conditions[1:]:
        and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
        and_node.left = result
        and_node.right = cond
        result = and_node
    return result


def _rewrite_one_window(plan: LogicalPlan, win_nid: str) -> LogicalPlan:
    win_node = plan[win_nid]
    partition_by = win_node.partition_by  # list of LogicalColumn/Node
    agg_nodes = win_node.aggregates       # list of agg Node (each has .alias set)
    source_scan = win_node.source_scan    # copy of the base Scan LogicalPlanNode

    subquery_alias = f"$win-{random_string(6)}"
    # Give the CTE scan a unique alias so the binder doesn't see it as a duplicate
    # of the outer scan referencing the same relation.
    cte_src_alias = f"$win_src-{random_string(6)}"

    # --- Build partition column refs for the CTE inner plan ---
    # These carry cte_src_alias so the binder resolves them within the CTE scope.
    inner_partition_by = []
    for pb in partition_by:
        col_name = getattr(pb, "source_column", None) or getattr(pb, "value", None)
        inner_pb = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=cte_src_alias,
            source_column=col_name,
        )
        inner_pb.query_column = col_name
        inner_partition_by.append(inner_pb)

    # --- Build the CTE inner plan: Scan → AggregateAndGroup ---
    inner_plan = LogicalPlan()

    scan_copy = source_scan.copy()
    scan_copy.alias = cte_src_alias  # unique alias; relation unchanged (same data source)
    scan_nid = random_string()
    inner_plan.add_node(scan_nid, scan_copy)

    # AggregateAndGroup: GROUP BY partition columns, compute window aggregates.
    # projection exposes both partition cols and aggregate results for the join.
    agg_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_step.groups = inner_partition_by
    agg_step.aggregates = list(agg_nodes)
    agg_step.projection = inner_partition_by + list(agg_nodes)
    agg_nid = random_string()
    inner_plan.add_node(agg_nid, agg_step)
    inner_plan.add_edge(scan_nid, agg_nid)

    # Project node above the AggAndGroup — required so the binder renames $derived to
    # $project before the Subquery wrapper's visit_exit runs. Without it, aggregate
    # columns added to $derived by the AggAndGroup binder are popped by visit_exit and
    # never appear in the subquery's output schema.
    project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    project_step.columns = list(inner_partition_by) + list(agg_nodes)
    project_step.order_by_columns = []
    project_step.except_columns = None
    project_nid = random_string()
    inner_plan.add_node(project_nid, project_step)
    inner_plan.add_edge(agg_nid, project_nid)

    # Wrap in a Subquery node so the binder treats it as a named relation.
    subquery_wrapper = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
    subquery_wrapper.alias = subquery_alias
    subquery_wrapper.columns = [Node(node_type=NodeType.WILDCARD)]
    subquery_wrapper_nid = random_string()

    # Merge CTE inner plan into main plan.
    plan += inner_plan
    plan.add_node(subquery_wrapper_nid, subquery_wrapper)
    plan.add_edge(project_nid, subquery_wrapper_nid)

    # --- Build ON condition: outer_scan.c = $win.c for each partition column ---
    # Left side explicitly references the outer scan alias to avoid post-join ambiguity.
    source_alias = source_scan.alias
    on_parts = []
    for pb in partition_by:
        col_name = getattr(pb, "source_column", None) or getattr(pb, "value", None)
        outer_col = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=source_alias,
            source_column=col_name,
        )
        inner_col = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=subquery_alias,
            source_column=col_name,
        )
        on_parts.append(_build_eq_condition(outer_col, inner_col))

    on_condition = _and_conditions(on_parts) if on_parts else None

    # --- Replace Window node with an inner Join ---
    join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join_node.type = "inner"
    join_node.on = on_condition
    join_node.using = None
    join_node.left_relation_names = None
    join_node.right_relation_names = [subquery_alias]
    join_node.columns = []
    join_node.is_window_join = True

    plan[win_nid] = join_node
    plan.add_edge(subquery_wrapper_nid, win_nid)

    # --- Insert a filter Project above the join ---
    # After the join, both the outer scan and the subquery expose the partition column(s)
    # under the same name, causing AmbiguousIdentifierError in the parent Project.
    # We insert a thin Project that expands only the outer scan (via a qualified wildcard)
    # plus the window aggregate result columns from the subquery.  The subquery's copy of
    # the partition column is never projected, so the parent sees each name exactly once.
    filter_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    filter_step.order_by_columns = []
    filter_step.except_columns = None

    outer_wildcard = Node(node_type=NodeType.WILDCARD)
    outer_wildcard.value = [source_alias]  # qualified wildcard: outer_scan.*

    win_refs = []
    for agg in agg_nodes:
        win_ref = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=subquery_alias,
            source_column=agg.alias,
            alias=agg.alias,
        )
        win_ref.query_column = agg.alias
        win_refs.append(win_ref)

    filter_step.columns = [outer_wildcard] + win_refs
    filter_nid = random_string()
    plan.insert_node_after(filter_nid, filter_step, win_nid)

    return plan


class WindowToJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type == LogicalPlanStepType.Window for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if node.node_type == LogicalPlanStepType.Window:
            context.bag.setdefault("candidates", []).append(context.node_id)

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for win_nid in context.bag.get("candidates", []):
            plan = _rewrite_one_window(plan, win_nid)
        return plan
