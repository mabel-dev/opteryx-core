# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Shared helpers for rewriting INTERSECT ALL / EXCEPT ALL into window + semi/anti joins.

The DISTINCT forms are not rewritten here. They are rewritten at BIND time, by
`binder/set_ops._rewrite_setop_to_join`, because their ON condition pairs the legs'
output columns positionally and which column a leg produces at each position is
knowable only once the legs are bound. The pre-bind rewrites that used to do it
(`intersect_to_inner_join`, `except_to_anti_join`) were deleted with that move; the
ALL forms stayed because they insert a ROW_NUMBER Window into the plan, which is a
structural change the binder cannot make mid-traversal.

`left_relation_names` / `right_relation_names` on a set-op node over-report: they
are collected by walking down to every scan in the branch. When a branch contains
a *nested* set operation, that nested op collapses its legs into a single surviving
relation (the others are consumed on the right side of the resulting semi/anti join,
or popped by the binder for a nested UNION). Building the join ON-condition against
the over-reported list references a relation that is no longer available, raising
`UnexpectedDatasetReferenceError` at bind time for chained INTERSECT/EXCEPT.

`live_relations` reduces a side's relation list to the relations that actually
survive at the set-op node: it drops any relation consumed by a set operation or
semi/anti join that lives *below* the node in that branch.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType


def column_names(columns) -> list | None:
    """
    Extract source column names from a projection column list.

    Returns None when the projection is a wildcard or any column name cannot be
    determined — both cases require schema information from the binder, which is
    why the ALL rewrite (this is the last pre-bind set-op rewrite) declines them.
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


def _consumed_below(plan: LogicalPlan, set_op_nid: str) -> set:
    """Collect relation names consumed by set ops / semi-anti joins below a node.

    Walks the subtree strictly below `set_op_nid` (its own right side is *not*
    counted — that relation survives at this node). A nested INTERSECT/EXCEPT/UNION
    consumes its right side; the same node may already have been rewritten into a
    `left semi` / `left anti` Join by the time this runs, so those are handled too.
    """
    consumed: set = set()
    seen: set = set()
    stack = [child_nid for child_nid, _, _ in plan.ingoing_edges(set_op_nid)]
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        node = plan[cur]
        is_set_op = node.node_type in (
            LogicalPlanStepType.Intersect,
            LogicalPlanStepType.Except,
            LogicalPlanStepType.Union,
        )
        is_semi_anti_join = node.node_type == LogicalPlanStepType.Join and node.type in (
            "left semi",
            "left anti",
            # The set-op rewrites emit the not-distinct forms (NULL equals NULL, as
            # INTERSECT/EXCEPT require). Omitting them here would stop a NESTED set op
            # being recognised as having consumed its right side, so `live_relations`
            # would keep a collapsed relation and the ON condition would be built over
            # a relation that no longer exists at that node.
            "left semi not-distinct",
            "left anti not-distinct",
        )
        if (is_set_op or is_semi_anti_join) and node.right_relation_names:
            consumed.update(node.right_relation_names)
        for upstream_nid, _, _ in plan.ingoing_edges(cur):
            stack.append(upstream_nid)
    return consumed


def live_relations(plan: LogicalPlan, set_op_nid: str, relation_names: list) -> list:
    """Reduce a set-op side's relation list to the relations that survive here.

    Relations collapsed by a nested set op / semi-anti join below this node are
    dropped. The branch's leftmost leg is never consumed, so the result is always
    non-empty when `relation_names` is.
    """
    consumed = _consumed_below(plan, set_op_nid)
    live = [rel for rel in relation_names if rel not in consumed]
    return live or list(relation_names)
