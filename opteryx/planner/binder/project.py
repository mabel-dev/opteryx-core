# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from orso.schema import RelationSchema

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.managers.virtual_datasets import derived
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext


def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # clear the derived schema
    context.schemas.pop("$derived", None)

    def name_column(column):
        for projection_column in node.columns:
            if (
                projection_column.schema_column
                and projection_column.schema_column.identity == column.identity
            ):
                if projection_column.alias:
                    return projection_column.alias

                if projection_column.query_column:
                    return str(projection_column.query_column)
                if projection_column.current_name:
                    return projection_column.current_name

        return column.name

    def keep_column(column, identities):
        if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
            if node.columns[0].value:
                if isinstance(column.origin, str):
                    column.origin = [column.origin]
                if node.columns[0].value[0] in column.origin:
                    identities.append(column.identity)
                    return True
                else:
                    return False
            identities.append(column.identity)
            return True
        return column.identity in identities

    identities = []
    for column in (col for col in node.columns if col.node_type != NodeType.WILDCARD):
        new_col, _ = inner_binder(column, context)
        identities.append(new_col.schema_column.identity)

    for select_column in (
        col for col in node.columns if col.node_type == NodeType.WILDCARD and col.value is not None
    ):
        for column in context.schemas[select_column.value[0]].columns:
            # new_col, _ = inner_binder(column, context)
            identities.append(column.identity)

    columns = []
    seen_identities = set()
    for _, schema in context.schemas.items():
        for column in schema.columns:
            if column.identity in seen_identities:
                continue
            if keep_column(column, identities):
                column_name = name_column(column=column)
                column_reference = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=column_name,
                    source=None,
                    alias=column_name,
                    schema_column=column,
                )
                columns.append(column_reference)
                seen_identities.add(column.identity)

    # we bound as we came across items in schemas, not the order the user wants them
    desired_order = {id: index for index, id in enumerate(identities)}
    node.columns = sorted(columns, key=lambda item: desired_order[item.schema_column.identity])

    context.schemas["$derived"] = derived.schema()

    return node, context


def visit_project(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    columns = []
    projected_column_count = 0

    # Handle wildcards, including qualified wildcards.
    for column in list(node.columns):
        if column.node_type != NodeType.WILDCARD:
            columns.append(column)
        elif column.value is None:
            # we're just a wildcard (not qualified), we're probably here because of an EXCEPT modifier
            except_columns = {c.source_column for c in node.except_columns}
            all_columns = []

            for name, schema in list(context.schemas.items()):
                for schema_column in schema.columns:
                    if schema_column.name in except_columns:
                        except_columns.remove(schema_column.name)
                        continue

                    all_columns.append(schema_column.name)

                    column_reference = LogicalColumn(
                        node_type=NodeType.IDENTIFIER,  # column type
                        source_column=schema_column.name,  # the source column
                        source=name,  # the source relation
                        schema_column=schema_column,
                    )
                    columns.append(column_reference)
                if name.startswith("$shared") and f"^{name}#" in schema.name:
                    context.schemas.pop(name)

                context.schemas[name] = RelationSchema(
                    name=name, columns=[col.schema_column for col in columns]
                )

            if len(except_columns) > 0:
                from opteryx.exceptions import ColumnNotFoundError

                message = f"EXCEPT references mulitple columns that cannot be found - " + ", ".join(
                    f"'{c}'" for c in except_columns
                )

                if len(except_columns) == 1:
                    from opteryx.utils import suggest_alternative

                    column = except_columns.pop()
                    suggestion = suggest_alternative(column, candidates=all_columns)
                    message = f"EXCEPT references column that cannot be found - '{column}'."
                    if suggestion is not None:
                        message += f" Did you mean '{suggestion}'?."

                raise ColumnNotFoundError(message=message)

        else:
            # Handle qualified wildcards
            # Ensure column.value is a list/tuple for qualified references
            table_name = (
                column.value[0] if isinstance(column.value, (list, tuple)) else column.value
            )

            for name, schema in list(context.schemas.items()):
                if (
                    name == table_name
                    or name.startswith("$shared")
                    and f"^{table_name}#" in schema.name
                ):
                    for schema_column in schema.columns:
                        column_reference = LogicalColumn(
                            node_type=NodeType.IDENTIFIER,  # column type
                            source_column=schema_column.name,  # the source column
                            source=table_name,  # the source relation
                            schema_column=schema_column,
                        )
                        columns.append(column_reference)
                if name.startswith("$shared") and f"^{table_name}#" in schema.name:
                    context.schemas.pop(name)

                context.schemas[table_name] = RelationSchema(
                    name=name, columns=[col.schema_column for col in columns]
                )

    projected_column_count = len(columns)

    for column in list(node.order_by_columns):
        if column.node_type != NodeType.WILDCARD:
            columns.append(column)
            continue
        raise UnsupportedSyntaxError("ORDER BY does not support wildcard projections.")

    # Bind the local columns to physical columns
    node.columns, group_contexts = zip(*(inner_binder(col, context) for col in columns))
    bound_columns = list(node.columns)
    node.columns = list(bound_columns[:projected_column_count])
    node.order_by_columns = list(bound_columns[projected_column_count:])
    context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])

    # Check for duplicates
    all_top_level_identities = [
        c.schema_column.identity for c in list(node.columns) + list(node.order_by_columns)
    ]
    if len(set(all_top_level_identities)) != len(all_top_level_identities):
        from collections import Counter

        from opteryx.exceptions import AmbiguousIdentifierError

        duplicates = [
            column for column, count in Counter(all_top_level_identities).items() if count > 1
        ]
        matches = {c.value for c in node.columns if c.schema_column.identity in duplicates}
        raise AmbiguousIdentifierError(
            message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
        )

    # Remove columns not being projected from the schemas, and remove empty schemas
    columns = []
    for relation, schema in list(context.schemas.items()):
        schema_columns = [
            column for column in schema.columns if column.identity in all_top_level_identities
        ]
        if len(schema_columns) == 0:
            context.schemas.pop(relation)
        else:
            for column in schema_columns:
                # for each column in the schema, try to find the node's columns
                node_column = next(
                    (
                        n
                        for n in list(node.columns) + list(node.order_by_columns)
                        if n.schema_column.identity == column.identity
                    ),
                    None,
                )
                # update the column reference with any AS aliases
                if node_column and node_column.alias:
                    node_column.schema_column.aliases.append(node_column.alias)
                    column.aliases.append(node_column.alias)
            # update the schema with columns we have references to, removing redundant columns
            schema.columns = schema_columns
            for column in list(node.columns) + list(node.order_by_columns):
                if column.schema_column.identity in [i.identity for i in schema_columns]:
                    columns.append(column)

    # We always have a $derived schema, even if it's empty
    if "$derived" in context.schemas:
        context.schemas["$project"] = context.schemas.pop("$derived")
        context.schemas["$project"].name = "$project"
    if "$derived" not in context.schemas:
        context.schemas["$derived"] = derived.schema()

    # update the columns attribute, preserving order
    bound_columns = {c.schema_column.identity: c for c in columns}
    node.columns = [bound_columns[c.schema_column.identity] for c in node.columns]
    node.order_by_columns = [bound_columns[c.schema_column.identity] for c in node.order_by_columns]

    return node, context
