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
from opteryx.types import SqlType


def visit_filter(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # We don't update the context, otherwise we'd be adding the predicates as columns
    original_context = context.copy()
    node.condition, context = inner_binder(node.condition, context)
    node.columns = get_all_nodes_of_type(node.condition, (NodeType.IDENTIFIER,))
    node.relations = node.condition.relations or {}

    # Verify the predicate evaluates to a boolean — non-boolean expressions (e.g.
    # bitwise arithmetic) are not valid WHERE conditions without an explicit comparison.
    _condition_sc = getattr(node.condition, "schema_column", None)
    _condition_type = (
        _condition_sc.type if _condition_sc is not None else getattr(node.condition, "type", None)
    )
    if _condition_type not in (
        None,
        0,
        SqlType.BOOLEAN,
        SqlType.NULL,
        SqlType._MISSING_TYPE,
    ):
        from opteryx.expression import format_expression

        expr = format_expression(node.condition)
        raise UnsupportedSyntaxError(
            f"WHERE condition `{expr}` returns {_condition_type} instead of BOOLEAN. "
            f"To filter on this value, compare it explicitly (e.g. `{expr} != 0`)."
        )

    return node, original_context
