# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Binder for ranking-window nodes (ROW_NUMBER / RANK / DENSE_RANK).

A whole-partition aggregate window (`SUM(x) OVER (PARTITION BY p)`, no ORDER BY) is
lowered to a join by the plan rewriter and never reaches the binder. A FRAMED
aggregate window (ORDER BY and/or a ROWS/RANGE frame present) is a different node
type — `LogicalPlanStepType.FramedWindow` — with its own binder,
`opteryx.planner.binder.framed_window.visit_framed_window`; it is never rewritten
to a join. Ranking windows survive as `LogicalPlanStepType.Window` nodes carrying an
`outputs` list; they come from two producers — the logical planner for user-facing
ranking windows (PARTITION BY ... ORDER BY ...), and the INTERSECT/EXCEPT ALL
rewrite (no ORDER BY, single ROW_NUMBER). Both pre-mint the output schema columns
and the relation to register them under. This visitor:

  1. binds the PARTITION BY columns so the physical operator can resolve their
     identities, and
  2. registers the pre-minted output column in its own relation so the downstream
     join / projection can resolve it.

It deliberately does not pop the input relations — the window passes every input
column through and only appends the row-number column.
"""

from typing import Tuple

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_window(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Bind the partition-by expressions against the input relations.
    bound_partitions = []
    for col in node.partition_by or []:
        bound, context = inner_binder(col, context)
        bound_partitions.append(bound)
    node.partition_by = bound_partitions

    # Bind the order-by expressions (user-facing ranking windows; empty internally).
    bound_order = []
    for col, ascending in node.order_by or []:
        bound, context = inner_binder(col, context)
        bound_order.append((bound, ascending))
    node.order_by = bound_order

    # `outputs` is a list of (kind, pre-minted SchemaColumn, params). Ranking
    # functions carry no params and their pre-minted INT64 type is final.
    # LAG/LEAD carry (argument expression[, offset literal]): the argument is
    # bound here, and the output column TAKES THE ARGUMENT'S TYPE — the INT64 the
    # planner minted is a placeholder, overwritten before the schema is
    # registered so everything downstream sees the true type.
    bound_outputs = []
    for kind, sc, params in node.outputs:
        arg_node = None
        offset = 1
        if params:
            arg_node, context = inner_binder(params[0], context)
            if len(params) > 1:
                offset = int(params[1].value)
            sc.column_type = arg_node.schema_column.column_type
        bound_outputs.append((kind, sc, arg_node, offset))
    node.outputs = [(kind, sc, [a] if a is not None else []) for kind, sc, a, _ in bound_outputs]

    # Register the outputs under a dedicated relation so the downstream
    # projection / join condition resolves, and hand the operator
    # (kind, output identity, bound argument node or None, offset) per function.
    schema_columns = [sc for _, sc, _params in node.outputs]
    context.schemas[node.output_relation] = RelationSchema(
        name=node.output_relation, columns=schema_columns
    )
    node.window_functions = [
        (kind, sc.identity, arg_node, offset) for kind, sc, arg_node, offset in bound_outputs
    ]

    node.columns = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=node.output_relation,
            source_column=sc.name,
            schema_column=sc,
        )
        for _, sc, _params in node.outputs
    ]

    return node, context
