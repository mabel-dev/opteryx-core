# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

r"""
Rewrites INTERSECT ALL / EXCEPT ALL using ROW_NUMBER + semi/anti join.

Multiset semantics need occurrence counts, which a plain semi/anti join (existence
only) cannot express. Tagging each row with its occurrence index within its
partition (all projected columns) and joining on (cols AND rn) turns the multiset
problem into an existence problem:

    INTERSECT ALL  ->  emit min(count_left, count_right) copies  (SEMI JOIN on rn)
    EXCEPT ALL     ->  emit max(count_left - count_right, 0)      (ANTI JOIN on rn)

The rn-th left copy matches a right row only if the right side has >= rn copies.

Plan transform for  L INTERSECT ALL R  (EXCEPT ALL is identical with anti join):

         SetOp(ALL)                       Project (drop $row_number)
        /          \                              |
       L            R          ==>          Join (semi/anti)
                                          ON  L.cols = R.cols AND L.rn = R.rn
                                          /                    \
                                  Window(rn over cols)   Window(rn over cols)
                                       |                       |
                                       L                       R

ROW_NUMBER here has NO ORDER BY — within a partition of identical rows any distinct
numbering is correct, which is exactly what the streaming WindowNode produces.

INTERSECT ALL / EXCEPT ALL with wildcard or unresolvable column names are left for
a later stage (the column names are needed to build the partition / join keys).
"""

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies._set_op_join_common import live_relations
from opteryx.planner.plan_rewriter.strategies.intersect_to_inner_join import _column_names
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteStrategy
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, mint_column_identity
from opteryx.utils import random_string

_ROW_NUMBER_NAME = "$row_number"


def _and(conditions: list) -> Node:
    """Left-deep AND tree over a non-empty list of conditions."""
    node = conditions[0]
    for cond in conditions[1:]:
        and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
        and_node.left = node
        and_node.right = cond
        node = and_node
    return node


def _eq(left_rel: str, left_col: str, right_rel: str, right_col: str) -> Node:
    eq = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True)
    eq.left = LogicalColumn(
        node_type=NodeType.IDENTIFIER, source=left_rel, source_column=left_col
    )
    eq.right = LogicalColumn(
        node_type=NodeType.IDENTIFIER, source=right_rel, source_column=right_col
    )
    return eq


def _subtree_scans(plan: LogicalPlan, start_nid: str) -> set:
    """Collect the scan aliases reachable upward from a node (its branch's relations)."""
    out: set = set()
    seen: set = set()
    stack = [start_nid]
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        node = plan[cur]
        if node.node_type == LogicalPlanStepType.Scan and node.alias:
            out.add(node.alias)
        for upstream_nid, _, _ in plan.ingoing_edges(cur):
            stack.append(upstream_nid)
    return out


def _make_window(partition_relation: str, col_names: list):
    """Build a ranking Window node that appends ROW_NUMBER over `col_names`."""
    rn_relation = f"$rownum-{random_string(6)}"
    rn_column = SchemaColumn(
        name=_ROW_NUMBER_NAME,
        column_type=_lt.INT64,
        identity=mint_column_identity(rn_relation, _ROW_NUMBER_NAME),
    )
    window = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
    window.partition_by = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER, source=partition_relation, source_column=c
        )
        for c in col_names
    ]
    window.order_by = []  # internal ROW_NUMBER has no ORDER BY (streaming path)
    window.outputs = [("ROW_NUMBER", rn_column)]
    window.output_relation = rn_relation
    window.columns = []
    return window, rn_relation


def _insert_window_on_edge(plan: LogicalPlan, leg_nid: str, setop_nid: str, window) -> str:
    """Splice a Window node onto the edge leg_nid -> setop_nid."""
    window_nid = random_string()
    plan.add_node(window_nid, window)
    plan.remove_edge(leg_nid, setop_nid, None)
    plan.add_edge(leg_nid, window_nid)
    plan.add_edge(window_nid, setop_nid)
    return window_nid


class IntersectExceptAllToWindowJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.node_type in (LogicalPlanStepType.Intersect, LogicalPlanStepType.Except)
            and node.modifier == "All"
            and _column_names(node.columns) is not None
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if (
            node.node_type in (LogicalPlanStepType.Intersect, LogicalPlanStepType.Except)
            and node.modifier == "All"
            and _column_names(node.columns) is not None
        ):
            context.bag.setdefault("all_nodes", []).append(context.node_id)

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for nid in context.bag.get("all_nodes", []):
            setop = plan[nid]
            is_except = setop.node_type == LogicalPlanStepType.Except
            col_names = _column_names(setop.columns)

            live_left = live_relations(plan, nid, setop.left_relation_names)
            live_right = live_relations(plan, nid, setop.right_relation_names)

            # Identify which incoming edge is the left leg and which is the right.
            left_set = set(setop.left_relation_names)
            left_leg = right_leg = None
            for child, _, _ in plan.ingoing_edges(nid):
                if _subtree_scans(plan, child) & left_set:
                    left_leg = child
                else:
                    right_leg = child

            # ROW_NUMBER over all projected columns on each leg.
            left_window, left_rn_rel = _make_window(live_left[0], col_names)
            right_window, right_rn_rel = _make_window(live_right[0], col_names)
            _insert_window_on_edge(plan, left_leg, nid, left_window)
            _insert_window_on_edge(plan, right_leg, nid, right_window)

            # Join: cols equal across sides AND occurrence indices equal.
            conditions = []
            for left_rel in live_left:
                for right_rel in live_right:
                    for col in col_names:
                        conditions.append(_eq(left_rel, col, right_rel, col))
            conditions.append(_eq(left_rn_rel, _ROW_NUMBER_NAME, right_rn_rel, _ROW_NUMBER_NAME))

            join = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
            # not-distinct on BOTH the value columns and $row_number: the ALL forms
            # compare rows the same way the DISTINCT forms do, they just count
            # occurrences as well. $row_number is never NULL, so the rule only ever
            # bites on the value columns — which is exactly where it is needed.
            join.type = ("left anti not-distinct" if is_except
                         else "left semi not-distinct")
            join.on = _and(conditions)
            join.using = None
            join.left_relation_names = list(live_left) + [left_rn_rel]
            join.right_relation_names = list(live_right) + [right_rn_rel]
            join.columns = []
            plan[nid] = join

            # Drop the row-number column from the output.
            project = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
            project.columns = [
                LogicalColumn(node_type=NodeType.IDENTIFIER, source=None, source_column=c)
                for c in col_names
            ]
            project.passthrough_columns = []
            plan.insert_node_after(random_string(), project, nid)

        return plan
