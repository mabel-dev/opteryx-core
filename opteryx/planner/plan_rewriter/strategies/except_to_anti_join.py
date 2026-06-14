# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites EXCEPT set operations as LEFT ANTI JOINs.

EXCEPT returns the distinct rows from the left relation that are absent from the right.
That is semantically identical to:

    LEFT ANTI JOIN on all projected columns, followed by DISTINCT

The logical planner already places a DISTINCT node above every non-ALL EXCEPT node, so
this rewrite only needs to swap the EXCEPT node itself for a JOIN node. The DISTINCT is
preserved unchanged.

NOT rewritten:
  - EXCEPT ALL — multiset difference cannot be expressed as a plain anti-join. An anti-join
    excludes every left row that has any match on the right, whereas EXCEPT ALL removes
    exactly one left occurrence per right occurrence. It is not yet implemented: the Except
    node survives to physical planning, which fails fast with `InvalidInternalStateError`
    (there is no Except physical operator). should_i_run() must exclude it so the fixed-point
    plan rewriter does not spin forever on an un-rewritable node.
  - Wildcard projections (SELECT * EXCEPT SELECT *) — column names are not yet available
    pre-bind. The binder expands wildcards and handles those nodes directly.

Known limitation:
  Column matching is by name from the left-side projection. Queries where the left and
  right sides use different column names (e.g. SELECT a ... EXCEPT SELECT x ...) will
  raise a column-not-found error at bind time. Positional matching requires the schema
  information that only becomes available in the binder.
"""

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.plan_rewriter.strategies._set_op_join_common import live_relations
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteStrategy


def _column_names(columns) -> list | None:
    """
    Extract source column names from a projection column list.

    Returns None when the projection is a wildcard or any column name cannot be
    determined — both cases require schema information from the binder.
    """
    if not columns:
        return None
    if len(columns) == 1 and columns[0].node_type == NodeType.WILDCARD:
        return None
    names = []
    for col in columns:
        name = getattr(col, "source_column", None)
        if name is None:
            return None
        names.append(name)
    return names or None


def _build_on_condition(
    left_relations: list,
    right_relations: list,
    col_names: list,
) -> Node:
    """
    Build an AND-tree of equality conditions covering every (left_rel, right_rel, col)
    triple. The result is equivalent to what convert_using_to_on produces but does not
    set node.using, avoiding the $shared-nnn schema creation in the binder's join handler.
    """
    conditions = []
    for left_rel in left_relations:
        for right_rel in right_relations:
            for col_name in col_names:
                eq = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    do_not_create_column=True,
                )
                eq.left = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=left_rel,
                    source_column=col_name,
                )
                eq.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=right_rel,
                    source_column=col_name,
                )
                conditions.append(eq)

    while len(conditions) > 1:
        paired = []
        for i in range(0, len(conditions), 2):
            if i + 1 < len(conditions):
                and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                and_node.left = conditions[i]
                and_node.right = conditions[i + 1]
                paired.append(and_node)
            else:
                paired.append(conditions[i])
        conditions = paired

    return conditions[0]


class ExceptToAntiJoinStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        # Only claim work for nodes complete() will actually rewrite. The plan
        # rewriter loops to a fixed point: a strategy whose should_i_run() stays
        # True without removing the triggering node spins forever. EXCEPT ALL and
        # wildcard/unresolvable projections are skipped in complete(), so they
        # must not register here (the binder handles those directly).
        return any(
            node.node_type == LogicalPlanStepType.Except
            and node.modifier != "All"
            and _column_names(node.columns) is not None
            for _, node in plan.nodes(True)
        )

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if node.node_type == LogicalPlanStepType.Except:
            context.bag.setdefault("except_nodes", []).append((context.node_id, node))

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for nid, except_node in context.bag.get("except_nodes", []):
            if except_node.modifier == "All":
                # Multiset semantics — cannot be expressed as a plain anti-join.
                continue

            col_names = _column_names(except_node.columns)
            if col_names is None:
                # Wildcard or unresolvable column names — binder handles this node.
                continue

            # Reduce each side to the relations that actually survive at this node:
            # a nested set op / semi-anti join below collapses its legs into one.
            live_left = live_relations(plan, nid, except_node.left_relation_names)
            live_right = live_relations(plan, nid, except_node.right_relation_names)

            on_condition = _build_on_condition(
                live_left,
                live_right,
                col_names,
            )

            join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
            join_node.type = "left anti"
            join_node.on = on_condition
            join_node.using = None
            join_node.left_relation_names = live_left
            join_node.right_relation_names = live_right
            join_node.columns = []

            plan[nid] = join_node

        return plan
