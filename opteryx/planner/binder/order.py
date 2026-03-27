# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext


def visit_order(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    order_by = []
    columns = []
    for column, direction in node.order_by:
        bound_column, context = inner_binder(column, context)

        order_by.append(
            (
                bound_column,
                "ascending" if direction else "descending",
            )
        )
        columns.append(bound_column)

    node.order_by = order_by
    node.columns = columns
    return node, context
