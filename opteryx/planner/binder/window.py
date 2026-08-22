# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Binder for ranking-window nodes (every function in WINDOW_FUNCTIONS:
ROW_NUMBER / RANK / DENSE_RANK / NTILE / PERCENT_RANK / CUME_DIST /
LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE).

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
from opteryx.operators.window.helpers import FLOAT_VALUED
from opteryx.operators.window.helpers import GATHERED_FUNCTIONS
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types import logical_type as _plt
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

    # `outputs` is a list of (kind, pre-minted SchemaColumn, params). What the
    # params MEAN, and what the output's type is, both depend on the kind — the
    # planner mints every one of them as INT64 because the true type is not always
    # knowable before binding, so this is where each is settled:
    #
    #   ROW_NUMBER/RANK/DENSE_RANK/NTILE  no bound argument; INT64 is final.
    #     NTILE's single param is its bucket COUNT — a constant validated by the
    #     builder, carried in the offset slot, NEVER bound as an expression.
    #   PERCENT_RANK/CUME_DIST            no params; the output is a FRACTION of
    #     the partition, so the minted INT64 is corrected to FLOAT64 here.
    #   LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE
    #     params[0] is an ARGUMENT EXPRESSION, bound here, and the output column
    #     TAKES ITS TYPE. params[1], where present, is the constant offset
    #     (LAG/LEAD) or 1-based position (NTH_VALUE).
    #
    # Every overwrite happens before the schema is registered below, so everything
    # downstream sees the true type.
    bound_outputs = []
    rebuilt_outputs = []
    for kind, sc, params in node.outputs:
        arg_node = None
        offset = 1
        if kind in GATHERED_FUNCTIONS:
            arg_node, context = inner_binder(params[0], context)
            if len(params) > 1:
                offset = int(params[1].value)
            sc.column_type = arg_node.schema_column.column_type
        elif kind == "NTILE":
            offset = int(params[0].value)
        elif kind in FLOAT_VALUED:
            sc.column_type = _plt.FLOAT64
        bound_outputs.append((kind, sc, arg_node, offset))
        # `outputs` is rebuilt with the argument expression REPLACED BY ITS BOUND
        # form, and every other param left exactly as it was — the constants
        # (LAG/LEAD's offset, NTILE's bucket count, NTH_VALUE's position) are not
        # expressions to bind, and dropping them here would silently reset them to
        # their defaults for anything that reads `outputs` after this point.
        rebuilt_outputs.append(
            (kind, sc, ([arg_node] + list(params[1:])) if arg_node is not None else list(params))
        )
    node.outputs = rebuilt_outputs

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
