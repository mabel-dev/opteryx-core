# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A sort led by COSINE_DISTANCE never returns a row that has no embedding.

Ruled 2026-10-03: a row whose distance is NULL (null text) or NaN is dropped by any sort
whose LEADING key is COSINE_DISTANCE - with or without a vector index, with or without a
LIMIT - so an index never changes an answer. This is the query's meaning, not an
optimization, so it is decided here, straight after binding, and not by an optimizer
strategy (which a flag, or DISABLE_OPTIMIZER, can switch off). The Order steps are marked
`drops_unsearchable`; the HeapSort OperatorFusion makes of one carries the mark, and the
compiler arms the native sort with it.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlanStepType

FUNCTION = "COSINE_DISTANCE"


def distance_calls(expressions):
    """Every COSINE_DISTANCE call in `expressions`."""
    return [
        node
        for node in get_all_nodes_of_type(list(expressions), (NodeType.FUNCTION,))
        if str(node.value).upper() == FUNCTION
    ]


def leading_call(plan, key):
    """The COSINE_DISTANCE call a sort key IS, or that computes the column it names
    (identities are unique in a plan), else None."""
    if key.node_type == NodeType.FUNCTION:
        return key if str(key.value).upper() == FUNCTION else None
    if key.schema_column is None:
        return None
    identity = key.schema_column.identity
    for _, node in plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Project:
            continue
        for expression in node.expressions():
            if (
                expression.schema_column is not None
                and expression.schema_column.identity == identity
                and expression.node_type == NodeType.FUNCTION
            ):
                return expression if str(expression.value).upper() == FUNCTION else None
    return None


def mark_unsearchable_sorts(plan) -> None:
    """Mark every Order of `plan` whose leading key is COSINE_DISTANCE."""
    for nid, node in plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Order:
            continue
        if leading_call(plan, node.order_by[0][0]) is not None:
            node.drops_unsearchable = True
            plan[nid] = node
