# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Binder for FRAMED aggregate window nodes: SUM/COUNT/AVG/MIN/MAX
OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE BETWEEN ...).

A separate node type from ranking windows (`opteryx.planner.binder.window`) —
see native_window_frame.hpp's header comment for why the two are different
computations with different native sinks. This visitor mirrors that one closely
(bind PARTITION BY, bind ORDER BY, register the pre-minted outputs under their
own relation) with one real difference: a framed aggregate's output TYPE is not
fixed (ranking windows are always INT64; LAG/LEAD take the argument's type
verbatim) — SUM(int) is INT64, SUM(float) is FLOAT64, AVG is always FLOAT64,
SUM/MIN/MAX(DECIMAL128) stays DECIMAL128, and so on. That inference is exactly
what `_aggregate_return_type` already does for a plain (non-windowed) aggregate,
so it is reused here rather than re-derived — a framed window function is an
ordinary aggregate call in every respect except WHEN it runs (per-row, over a
sliding frame, instead of once per group).
"""

from typing import Tuple

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import _aggregate_return_type
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_framed_window(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Bind the partition-by expressions against the input relations.
    bound_partitions = []
    for col in node.partition_by or []:
        bound, context = inner_binder(col, context)
        bound_partitions.append(bound)
    node.partition_by = bound_partitions

    # Bind the window's own ORDER BY. A framed window always has one — the
    # logical planner refuses a FRAME with no ORDER BY before this node exists.
    bound_order = []
    for col, ascending in node.order_by or []:
        bound, context = inner_binder(col, context)
        bound_order.append((bound, ascending))
    node.order_by = bound_order

    # `outputs` is a list of (kind, pre-minted SchemaColumn, params, frame). `params`
    # is the aggregate's argument list — `[]` or `[WILDCARD]` for COUNT(*), `[expr]`
    # for SUM/COUNT/AVG/MIN/MAX otherwise. The argument is bound here, and — same
    # move as the ranking binder's LAG/LEAD — the pre-minted INT64 placeholder type
    # is overwritten with the aggregate's TRUE result type before the schema is
    # registered, so everything downstream sees it.
    bound_outputs = []
    for kind, sc, params, frame in node.outputs:
        arg_node = None
        if params and params[0].node_type != NodeType.WILDCARD:
            arg_node, context = inner_binder(params[0], context)
        _probe = Node(
            node_type=NodeType.AGGREGATOR,
            value=kind,
            parameters=[arg_node] if arg_node is not None else [],
        )
        result_type = _aggregate_return_type(_probe)
        if result_type is not None:
            sc.column_type = result_type
        bound_outputs.append((kind, sc, arg_node, frame))
    node.outputs = [(kind, sc, [a] if a is not None else [], frame) for kind, sc, a, frame in bound_outputs]

    # Register the outputs under a dedicated relation so the downstream
    # projection / join condition resolves, and hand the operator
    # (kind, output identity, bound argument node or None, frame) per function.
    schema_columns = [sc for _, sc, _params, _frame in node.outputs]
    context.schemas[node.output_relation] = RelationSchema(
        name=node.output_relation, columns=schema_columns
    )
    node.window_functions = [
        (kind, sc.identity, arg_node, frame) for kind, sc, arg_node, frame in bound_outputs
    ]

    node.columns = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=node.output_relation,
            source_column=sc.name,
            schema_column=sc,
        )
        for _, sc, _params, _frame in node.outputs
    ]

    return node, context
