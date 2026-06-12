# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Shared helper for rewriting INTERSECT/EXCEPT set operations into semi/anti joins.

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

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType


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
