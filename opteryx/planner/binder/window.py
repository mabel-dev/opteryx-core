# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Binder for ranking-window nodes (ROW_NUMBER / RANK / DENSE_RANK).

Aggregate windows are lowered to joins by the plan rewriter and never reach the
binder. Ranking windows survive as `LogicalPlanStepType.Window` nodes carrying a
`window_functions` list; they are produced today only by the INTERSECT/EXCEPT ALL
rewrite, which pre-mints the output (row-number) schema column and the relation to
register it under. This visitor:

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

    # `outputs` is a list of (kind, pre-minted SchemaColumn). Register them under a
    # dedicated relation so the downstream projection / join condition resolves, and
    # hand the operator the (kind, identity) pairs it executes.
    schema_columns = [sc for _, sc in node.outputs]
    context.schemas[node.output_relation] = RelationSchema(
        name=node.output_relation, columns=schema_columns
    )
    node.window_functions = [(kind, sc.identity) for kind, sc in node.outputs]

    node.columns = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=node.output_relation,
            source_column=sc.name,
            schema_column=sc,
        )
        for _, sc in node.outputs
    ]

    return node, context
