# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.schema import ConstantColumn, FlatColumn, RelationSchema
from opteryx.types import OrsoTypes


def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.variables = context.execution_context.variables
    node.columns = []
    return node, context


def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "union" for n in node.left_relation_names}

    if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,  # column type
                        source_column=schema_column.name,  # the source column
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


def visit_unnest(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.columns = []

    # we create a new schema for the unnested column
    unnest_schema = node.alias

    # this is the column which is being unnested
    if node.unnest_column.node_type == NodeType.LITERAL:
        schema_column = ConstantColumn(name=node.unnest_alias)
        schema_column.type = node.unnest_column.type
        schema_column.value = node.unnest_column.value
        schema_column.element_type = node.unnest_column.element_type
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )
        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])
        # reference the new column in the node
        node.columns.append(node.unnest_target)
    else:
        from opteryx.planner.binder.binder import inner_binder

        node.unnest_column, context = inner_binder(node.unnest_column, context)
        node.columns += [node.unnest_column]

        # we can only UNNEST an ARRAY type column, we need to find it before we know its type
        if node.unnest_column.schema_column.type not in (
            0,
            OrsoTypes.ARRAY,
            OrsoTypes.VECTOR,
            OrsoTypes.NULL,
        ):
            from opteryx.exceptions import IncorrectTypeError

            raise IncorrectTypeError(
                f"CROSS JOIN UNNEST requires an ARRAY or VECTOR type column, not {node.unnest_column.schema_column.type}."
            )

        # this is the column that is being created
        element_type = OrsoTypes.VARCHAR
        if (
            node.unnest_column.schema_column
            and node.unnest_column.schema_column.type == OrsoTypes.VECTOR
        ):
            element_type = OrsoTypes.DOUBLE
        elif node.unnest_column.schema_column and node.unnest_column.schema_column.element_type:
            element_type = node.unnest_column.schema_column.element_type

        schema_column = FlatColumn(name=node.unnest_alias, type=element_type)
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )

        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])

        # reference the new column in the node
        node.columns.append(node.unnest_target)

    return node, context
