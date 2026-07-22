# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rewrites FULL OUTER JOIN as a UNION ALL of a LEFT OUTER JOIN and a LEFT ANTI JOIN.

There is no native FULL OUTER operator (the engine only implements LEFT OUTER, LEFT
SEMI and LEFT ANTI — see `opteryx/managers/execution/compiler.py::_compile_join`).
FULL OUTER is expressed as:

    A FULL OUTER JOIN B ON <cond>
        ≡
    (A LEFT OUTER JOIN B ON <cond>)                              -- all of A, matched B or NULL
        UNION ALL
    (SELECT NULL, ..., NULL, b.* FROM B LEFT ANTI JOIN A ON <cond>)  -- B rows with no match in A

The LEFT OUTER branch reuses the engine's own NULL-padding for B's columns on
unmatched rows — nothing is synthesized there. The LEFT ANTI branch only ever emits
the preserved (B) side, so A's columns are synthesized as NULL literals above it.

Restricted scope
-----------------
This only fires when the FULL OUTER join's sole consumer is a Project with an
EXPLICIT, non-wildcard column list where every column is a bare identifier
attributable to one side of the join (`col.source` is a left- or right-relation
alias). This is required because, pre-bind, we do not know a relation's column
names in general (a bare table scan's schema is a Binder/catalog concern) — the
NULL literals for A's columns in the anti-join branch must line up 1:1 with the
LEFT OUTER branch's output, and the only place a concrete column list exists
pre-bind is an explicit projection.

NOT rewritten (falls through to full-outer's existing fail-loud NotSupportedError
at physical planning):
  - `SELECT *` / wildcard projections over the join
  - Any projected expression that isn't a bare identifier (function calls, casts,
    arithmetic, literals, columns combining both sides, ...)
  - A projected identifier whose source can't be attributed to exactly one side
  - `... EXCEPT (col)` or an ORDER BY column pulled in by the projection
  - `USING (...)` joins whose `on` condition hasn't been synthesized yet
  - Either side rooted in a Subquery (`(SELECT ...) AS x`) rather than a bare
    Scan — `rename_relations` (relation_resolver) only renames Scan aliases,
    not Subquery aliases, so a Subquery-wrapped side would collide with itself
    between the LEFT OUTER leg (original aliases) and the cloned LEFT ANTI leg
    (meant to carry fresh ones)

Multiple FULL OUTER joins in one query are handled by the fixed-point rewriter loop.
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies.optimization_strategy import flip_join_leg_labels
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteStrategy
from opteryx.planner.relation_resolver import rename_relations
from opteryx.planner.relation_resolver import copy_sub_plan
from opteryx.types.logical_type import NULL as _CT_NULL
from opteryx.utils import random_string


def _classify_projection(join_node: LogicalPlanNode, project_node: LogicalPlanNode):
    """
    Validate the Project directly above a FULL OUTER join is in the restricted
    scope this rewrite supports, and classify each of its columns by side.

    Returns a list of ("left" | "right", column_node) pairs in original order, or
    None if the projection is out of scope (wildcard, expression, ambiguous source,
    EXCEPT/order-by carry-through, ...).
    """
    columns = project_node.columns
    if not columns:
        return None
    if project_node.except_columns or project_node.order_by_columns:
        return None

    left_relations = set(join_node.left_relation_names or [])
    right_relations = set(join_node.right_relation_names or [])
    if not left_relations or not right_relations:
        return None

    classified = []
    for col in columns:
        if col.node_type == NodeType.WILDCARD:
            return None
        if col.node_type != NodeType.IDENTIFIER:
            return None
        source = getattr(col, "source", None)
        in_left = source in left_relations
        in_right = source in right_relations
        if in_left == in_right:  # neither, or (shouldn't happen) both
            return None
        classified.append(("left" if in_left else "right", col))

    return classified


def _has_subquery_leg(plan: LogicalPlan, join_nid: str) -> bool:
    """True if any node feeding this join (down to its scans) is a Subquery."""
    stack = [join_nid]
    seen = set()
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if plan[cur].node_type == LogicalPlanStepType.Subquery:
            return True
        for child_nid, _tgt, _label in plan.ingoing_edges(cur):
            stack.append(child_nid)
    return False


def _find_consuming_project(plan: LogicalPlan, join_nid: str):
    """Find the Project that determines the join's output shape.

    Usually the join's sole consumer directly. A single Filter (WHERE clause)
    between them is also allowed and stepped over unchanged: it re-attaches
    above the synthesized UNION exactly where it was — applying the SAME
    predicate uniformly to both legs is semantically correct regardless of
    what it references (NULLs propagate through it identically either way),
    so nothing about the join's restricted-scope shape requirements changes.
    Returns None if no such Project can be found within that one hop.
    """
    consumers = plan.outgoing_edges(join_nid)
    if len(consumers) != 1:
        return None
    consumer_nid = consumers[0][1]
    consumer_node = plan[consumer_nid]
    if consumer_node.node_type == LogicalPlanStepType.Project:
        return consumer_nid
    if consumer_node.node_type == LogicalPlanStepType.Filter:
        above = plan.outgoing_edges(consumer_nid)
        if len(above) != 1:
            return None
        above_node = plan[above[0][1]]
        if above_node.node_type == LogicalPlanStepType.Project:
            return above[0][1]
    return None


class FullOuterToUnionStrategy(PlanRewriteStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        for join_nid, join_node in plan.nodes(True):
            if join_node.node_type != LogicalPlanStepType.Join or join_node.type != "full outer":
                continue
            if not join_node.on:
                continue
            project_nid = _find_consuming_project(plan, join_nid)
            if project_nid is None:
                continue
            project_node = plan[project_nid]
            if _classify_projection(join_node, project_node) is None:
                continue
            if _has_subquery_leg(plan, join_nid):
                continue
            return True
        return False

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        if not context.rewritten_plan:
            context.rewritten_plan = context.pre_rewrite_tree.copy()

        if node.node_type == LogicalPlanStepType.Join and node.type == "full outer":
            context.bag.setdefault("full_outer_joins", []).append((context.node_id, node))

        return context

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        for join_nid, join_node in context.bag.get("full_outer_joins", []):
            if not join_node.on:
                continue

            project_nid = _find_consuming_project(plan, join_nid)
            if project_nid is None:
                continue
            project_node = plan[project_nid]

            classified = _classify_projection(join_node, project_node)
            if classified is None:
                continue
            if _has_subquery_leg(plan, join_nid):
                continue

            # ---- Leg 1: A LEFT OUTER JOIN B, unchanged Project. -------------------
            # The engine's own outer-join NULL padding covers B's unmatched columns —
            # nothing to synthesize here. Mutate the existing nodes in place.
            join_node.type = "left outer"
            plan[join_nid] = join_node

            # ---- Leg 2 template: (join subtree) -> synthetic Project. -------------
            # Build the synthetic Project's columns BEFORE cloning, referencing the
            # ORIGINAL (pre-rename) aliases, so rename_relations's generic property
            # walker remaps the surviving (right-side) identifier references for us
            # exactly like every other reference in the cloned subtree.
            leg2_columns = []
            for side, col in classified:
                if side == "left":
                    null_lit = Node(node_type=NodeType.LITERAL, type=_CT_NULL, value=None)
                    null_lit.alias = getattr(col, "alias", None) or getattr(
                        col, "source_column", None
                    )
                    leg2_columns.append(null_lit)
                else:
                    leg2_columns.append(col.copy())

            synthetic_project = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
            synthetic_project.columns = leg2_columns
            synthetic_project.order_by_columns = []
            synthetic_project.except_columns = None

            template = LogicalPlan()
            stack = [join_nid]
            seen = set()
            while stack:
                cur = stack.pop()
                if cur in seen:
                    continue
                seen.add(cur)
                template.add_node(cur, plan[cur])
                for child_nid, _tgt, label in plan.ingoing_edges(cur):
                    template.add_edge(child_nid, cur, label)
                    stack.append(child_nid)

            synthetic_project_nid = random_string()
            template.add_node(synthetic_project_nid, synthetic_project)
            template.add_edge(join_nid, synthetic_project_nid)

            cloned = copy_sub_plan(template)
            cloned = rename_relations(cloned, prefix="$full-outer-anti-")

            cloned_project_nid = cloned.get_exit_points()[0]
            cloned_join_nid = cloned.ingoing_edges(cloned_project_nid)[0][0]
            cloned_join_node = cloned[cloned_join_nid]

            # B LEFT ANTI JOIN A — preserved side (B, originally "right") becomes the
            # anti-join's "left"/probe side; mirrors the RIGHT OUTER -> LEFT ANTI
            # canonicalisation in optimizer/strategies/join_rewriter.py.
            cloned_join_node.type = "left anti"
            cloned_join_node.left_relation_names, cloned_join_node.right_relation_names = (
                cloned_join_node.right_relation_names,
                cloned_join_node.left_relation_names,
            )
            left_readers = getattr(cloned_join_node, "left_readers", None)
            right_readers = getattr(cloned_join_node, "right_readers", None)
            cloned_join_node.left_readers, cloned_join_node.right_readers = (
                right_readers,
                left_readers,
            )
            flip_join_leg_labels(cloned, cloned_join_nid)
            cloned[cloned_join_nid] = cloned_join_node

            plan += cloned

            # ---- Union ALL over leg 1 (unchanged) and leg 2 (cloned). --------------
            union_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
            union_node.modifier = "All"
            union_node.columns = project_node.columns
            union_node.left_relation_names = sorted(
                set(join_node.left_relation_names or []) | set(join_node.right_relation_names or [])
            )
            union_node.right_relation_names = sorted(
                set(cloned_join_node.left_relation_names or [])
                | set(cloned_join_node.right_relation_names or [])
            )

            union_nid = random_string()
            plan.insert_node_after(union_nid, union_node, project_nid)
            plan.add_edge(cloned_project_nid, union_nid)

            self.telemetry.optimization_full_outer_to_union += 1

        return plan
