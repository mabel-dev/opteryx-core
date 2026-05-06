# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.managers.virtual_datasets import derived
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import inner_binder, merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # clear the derived schema
    context.schemas.pop("$derived", None)

    def _output_name_for_projection(proj_col, schema_col):
        """User-visible name for an explicitly-projected column."""
        if proj_col.alias:
            return proj_col.alias
        if proj_col.query_column:
            return str(proj_col.query_column)
        if proj_col.current_name:
            return proj_col.current_name
        return schema_col.name

    output_columns = []

    for column in node.columns:
        if column.node_type == NodeType.WILDCARD:
            # Wildcard expansion — schema-driven. Each underlying column produces
            # exactly one output column.
            if column.value is not None:
                # Qualified wildcard: only columns whose origin matches the qualifier.
                qualifier = column.value[0]
                seen_identities = set()
                for schema in context.schemas.values():
                    for schema_col in schema.columns:
                        if schema_col.identity in seen_identities:
                            continue
                        origin = schema_col.origin
                        if isinstance(origin, str):
                            origin = [origin]
                            schema_col.origin = origin
                        if origin and qualifier in origin:
                            output_columns.append(
                                LogicalColumn(
                                    node_type=NodeType.IDENTIFIER,
                                    source_column=schema_col.name,
                                    source=None,
                                    alias=schema_col.name,
                                    schema_column=schema_col,
                                )
                            )
                            seen_identities.add(schema_col.identity)
            else:
                # Bare wildcard: every column from every schema (deduped by identity).
                seen_identities = set()
                for schema in context.schemas.values():
                    for schema_col in schema.columns:
                        if schema_col.identity in seen_identities:
                            continue
                        output_columns.append(
                            LogicalColumn(
                                node_type=NodeType.IDENTIFIER,
                                source_column=schema_col.name,
                                source=None,
                                alias=schema_col.name,
                                schema_column=schema_col,
                            )
                        )
                        seen_identities.add(schema_col.identity)
            continue

        # Explicit projection: emit one output per `node.columns` entry, even when
        # multiple entries resolve to the same underlying schema_column (identity).
        # Earlier nodes may have folded same-identity columns into one — EXIT
        # unfolds them back into the user's distinct output names.
        new_col, _ = inner_binder(column, context)
        schema_col = new_col.schema_column
        column_name = _output_name_for_projection(new_col, schema_col)
        output_columns.append(
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=column_name,
                source=None,
                alias=column_name,
                schema_column=schema_col,
            )
        )

    node.columns = output_columns

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

            found_match = False
            shared_schema_names = []

            for name, schema in list(context.schemas.items()):
                # Check if this schema matches the qualified wildcard
                # Match by:
                # 1. Exact key match (e.g., "supplier" == "supplier")
                # 2. Ends with .table_name (e.g., "testdata.tpch_001.supplier" ends with ".supplier")
                # 3. Shared schema pattern (e.g., "$view-ABC" with matching schema.name)
                is_exact_match = name == table_name
                is_qualified_match = name.endswith(f".{table_name}") or (
                    name.startswith("$view") and schema.name.endswith(f"/{table_name}.parquet")
                )
                is_shared_match = (
                    name.startswith("$shared")
                    and f"^{table_name}#" in schema.name
                )

                if is_exact_match or is_qualified_match or is_shared_match:
                    found_match = True
                    # Expand all columns from this schema
                    for schema_column in schema.columns:
                        column_reference = LogicalColumn(
                            node_type=NodeType.IDENTIFIER,  # column type
                            source_column=schema_column.name,  # the source column
                            source=table_name,  # the source relation
                            schema_column=schema_column,
                        )
                        columns.append(column_reference)

                    # Track shared schemas for cleanup after loop
                    if is_shared_match:
                        shared_schema_names.append(name)

            # Clean up shared schemas after processing
            for shared_name in shared_schema_names:
                context.schemas.pop(shared_name)

            # Update the schema mapping if we found a match
            if found_match and columns:
                context.schemas[table_name] = RelationSchema(
                    name=table_name, columns=[col.schema_column for col in columns]
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

    # Check for duplicates.
    # Two columns sharing the same underlying identity are still distinct
    # outputs when their user-visible names differ — e.g. `SELECT a AS x, a AS y`,
    # or `SELECT supp_nation, cust_nation` over a self-join where both resolve
    # to the same `n_name` identity. We compare on (identity, lower(name)) so
    # case-variant references like `SELECT id, ID` are still flagged.
    def _output_key(c):
        name = c.alias or getattr(c, "value", None)
        if isinstance(name, str):
            name = name.lower()
        return (c.schema_column.identity, name)

    all_top_level_identities = [
        c.schema_column.identity for c in list(node.columns) + list(node.order_by_columns)
    ]
    all_top_level_keys = [
        _output_key(c) for c in list(node.columns) + list(node.order_by_columns)
    ]
    if len(set(all_top_level_keys)) != len(all_top_level_keys):
        from collections import Counter

        from opteryx.exceptions import AmbiguousIdentifierError

        duplicates = [
            key for key, count in Counter(all_top_level_keys).items() if count > 1
        ]
        matches = {c.value for c in node.columns if _output_key(c) in duplicates}
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
                    if column.aliases:
                        column.aliases.append(node_column.alias)
                    else:
                        column.aliases = [node_column.alias]
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
