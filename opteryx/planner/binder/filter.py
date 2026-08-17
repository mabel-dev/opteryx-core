# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.logical_type import LogicalCategory


def visit_filter(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # We don't update the context, otherwise we'd be adding the predicates as columns
    original_context = context.copy()
    node.condition, context = inner_binder(node.condition, context)
    # AGGREGATOR alongside IDENTIFIER: a WHERE condition can never legally contain
    # one (aggregates aren't allowed pre-GROUP BY), so this only bites a HAVING that
    # stayed a standalone Filter instead of fusing onto its aggregate — e.g. one that
    # also needs a column from a later JOIN (a decorrelated `HAVING SUM(x) > (SELECT
    # ...)`). The compiler treats an AGGREGATOR node exactly like an IDENTIFIER —
    # "already resolved, load its own identity from the stream" (see compiler.py's
    # array-hoist gate: IDENTIFIER/EVALUATED/AGGREGATOR all "already lower to
    # BC_LOAD_COL"), never "recompute from its operand". Collecting only IDENTIFIER
    # leaves walked PAST the aggregate into its pre-aggregation operand (e.g. `mass`
    # under `SUM(mass)`), so downstream column-liveness (projection_pushdown) never
    # saw the aggregate's OWN identity as demanded and pruned it — the leg's own
    # aggregate output vanished before the filter could read it.
    node.columns = get_all_nodes_of_type(
        node.condition, (NodeType.IDENTIFIER, NodeType.AGGREGATOR)
    )
    node.relations = node.condition.relations or {}

    # Verify the predicate evaluates to a boolean — non-boolean expressions (e.g.
    # bitwise arithmetic) are not valid WHERE conditions without an explicit comparison.
    _condition_sc = getattr(node.condition, "schema_column", None)
    if _condition_sc is not None:
        _condition_type = _condition_sc.category
    else:
        _ct = getattr(node.condition, "type", None)
        _condition_type = _ct.category if _ct is not None else None
    if _condition_type not in (
        None,
        LogicalCategory.BOOLEAN,
        LogicalCategory.NULL,
    ):
        from opteryx.expression import format_expression

        expr = format_expression(node.condition)
        raise UnsupportedSyntaxError(
            f"**WHERE** condition `{expr}` returns {_condition_type} instead of BOOLEAN. "
            f"To filter on this value, compare it explicitly (e.g. `{expr} != 0`)."
        )

    return node, original_context
