# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext


def _resolve_wildcard_order_position(node: Node, context: BindingContext) -> Node:
    """Resolve an ORDER BY position logical_planner deferred over a bare `SELECT *`.

    `SELECT *` has no fixed column list at plan time -- it is one WILDCARD
    placeholder that only becomes real columns once the source schema is bound
    (see `visit_exit`'s bare-wildcard branch in `binder/project.py`) -- so
    `inner_query_planner` cannot validate or resolve `ORDER BY <position>`
    against it and leaves the integer literal in place, flagged
    `is_wildcard_order_position`. By the time this runs, the schema IS bound,
    so the position is resolved the same way `*` itself is: flatten every
    bound relation's columns in schema order, skipping `$derived` (binder
    scratch space, never part of a real `*`) and deduping on
    (identity, name) the same way a column reachable through more than one
    schema key (shared/view schemas) is deduped there.
    """
    position = int(node.value)
    seen_identities = set()
    ordinal = 0
    for name, schema in context.schemas.items():
        if name == "$derived":
            continue
        for schema_col in schema.columns:
            if (schema_col.identity, schema_col.name) in seen_identities:
                continue
            seen_identities.add((schema_col.identity, schema_col.name))
            ordinal += 1
            if ordinal == position:
                return LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=schema_col.name,
                    source=None,
                    alias=schema_col.name,
                    schema_column=schema_col,
                )
    raise UnsupportedSyntaxError(
        f"**ORDER BY** position {position} is out of range — **SELECT** has {ordinal} column(s). "
        "Positions count the **SELECT** columns and start at 1."
    )


def visit_order(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    order_by = []
    columns = []
    for column, ascending in node.order_by:
        if column.is_wildcard_order_position:
            column = _resolve_wildcard_order_position(column, context)
        bound_column, context = inner_binder(column, context)

        order_by.append((bound_column, bool(ascending)))
        columns.append(bound_column)

    node.order_by = order_by
    node.columns = columns
    return node, context
