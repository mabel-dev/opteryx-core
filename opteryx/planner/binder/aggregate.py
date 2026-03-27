# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.virtual_datasets import derived


def visit_aggregate_and_group(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """
    Handles the binding logic for aggregate and group nodes.

    This function maps the field to the existing schema fields, disposes of the existing
    schemas, and replaces it with a new '$group-by' schema.

    Parameters:
        node: Node
            The node containing the aggregate and group data.
        context: Optional[Dict[str, Any]]
            The current binding context, defaults to None.

    Returns:
        Tuple[Node, Dict[str, Any]]
        The modified node and the updated context.
    """
    tmp_aggregates: tuple = tuple()
    if node.aggregates:
        tmp_aggregates, _ = zip(
            *(inner_binder(aggregate, context) for aggregate in node.aggregates)
        )
        # Deduplicate aggregates by schema_column.identity
        aggregates_by_identity = {
            agg.schema_column.identity: agg
            for agg in tmp_aggregates
            if agg.schema_column.identity is not None
        }
        node.aggregates = list(aggregates_by_identity.values())

    for agg in node.aggregates:
        if agg.condition:
            agg.condition, context = inner_binder(agg.condition, context)

    # We're going to trim down the schemas to just the columns used in the GROUP BY.
    # 1) the easy one - the columns explictly in the GROUP BY
    columns_to_keep = set()
    if node.groups:
        tmp_groups, _ = zip(*(inner_binder(group, context) for group in node.groups))
        columns_to_keep = {col.schema_column.identity for col in tmp_groups}
    # remove literals in the GROUP BY clause, they form one group
    node.groups = [g for g in node.groups if g.node_type != NodeType.LITERAL]
    # 2) the columns referenced in the SELECT
    identifier_columns = get_all_nodes_of_type(
        node.aggregates + node.groups, select_nodes=(NodeType.IDENTIFIER,)
    )
    node.columns = list(node.aggregates) + identifier_columns
    all_identifiers = [node.schema_column.identity for node in node.columns]
    columns_to_keep = columns_to_keep.union(all_identifiers)

    for name, schema in list(context.schemas.items()):
        schema_columns = [
            column for column in schema.columns if column.identity in columns_to_keep
        ]
        if schema_columns:
            context.schemas[name].columns = schema_columns
        else:
            context.schemas.pop(name)

    for array_agg in [agg for agg in tmp_aggregates if agg.value == "ARRAY_AGG"]:
        if not node.groups:
            raise UnsupportedSyntaxError(
                "ARRAY_AGG requires a GROUP BY clause, and cannot GROUP BY a literal value."
            )
        if array_agg.order:
            if len(array_agg.order) > 1:
                raise UnsupportedSyntaxError(
                    "ARRAY_AGG can only ORDER BY the aggregated column."
                )
            if array_agg.order[0][0].current_name != array_agg.parameters[0].current_name:
                raise UnsupportedSyntaxError(
                    "ARRAY_AGG can only ORDER BY the aggregated column."
                )

    # we should always have a derived schema
    if "$derived" not in context.schemas:
        context.schemas["$derived"] = derived.schema()

    # the aggregates and any calculated expressions in the SELECT should be in $derived
    context.schemas["$derived"].columns.extend(col.schema_column for col in node.aggregates)
    node.schema = context.schemas["$derived"]
    return node, context


# Alias for backward compatibility
visit_aggregate = visit_aggregate_and_group


def visit_distinct(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.columns = []
    if node.on:
        # Bind the local columns to physical columns
        node.on, group_contexts = zip(*(inner_binder(col, context) for col in node.on))
        from opteryx.planner.binder.binder import merge_schemas
        context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])
        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))

    return node, context
