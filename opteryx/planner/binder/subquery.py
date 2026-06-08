# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.managers.virtual_datasets import derived
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_comment(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the COMMENT node to determine which connector should handle
    storing the comment on the view/table.

    This is a pass-through binder - COMMENT nodes don't need schema resolution,
    but we do need to determine the connector for storage.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.virtual_datasets import derived

    # Get connector gateway (cached by prefix)
    node.connector = connector_factory(node.object_name, telemetry=context.telemetry)

    # Ensure this user can write to the object location
    if not can_perform_action(context.execution_context, node.object_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to comment on {node.object_name}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables

    # COMMENT nodes don't have columns (non-tabular result)
    node.columns = []
    return node, context


def visit_subquery(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)

    # Extract the column names to check for duplicates
    column_names = (n.current_name for n in node.columns)
    seen = set()
    duplicates = [name for name in column_names if name in seen or seen.add(name)]  # type: ignore

    # Now you can check if there are any duplicates and take action accordingly
    if duplicates:
        from opteryx.exceptions import AmbiguousIdentifierError

        raise AmbiguousIdentifierError(
            identifier=duplicates,
            message=f"Column name collision in subquery '{node.alias}'; Column(s) {', '.join(duplicates)} is ambiguous in the outer query, use AS to provide unique names for these columns.",
        )

    # we sack all the tables we previously knew and create a new set of schemas here
    columns: list = []
    source_relations: list = []
    for name, schema in context.schemas.items():
        for schema_column in schema.columns:
            # Find ALL projection columns matching this schema_column's identity.
            # When the user aliases the same underlying column with multiple
            # output names (e.g. `n1.n_name AS supp, n2.n_name AS cust` in a
            # self-join, or `id AS x, id AS y`), every alias must remain
            # resolvable from the outer query.
            projection_matches = [
                column
                for column in node.columns
                if column.schema_column.identity == schema_column.identity
            ]
            projection_column = projection_matches[0] if projection_matches else None
            if not schema_column.origin:
                schema_column.origin = []
            source_relations.extend(schema_column.origin or [])
            if projection_column:
                projection_column.source = node.alias
            schema_column.origin += [node.alias]

            schema_column.name = (
                projection_column.current_name if projection_column else schema_column.name
            )

            if "." in schema_column.name:
                # If the column is not in the projection, it should retain its name without any prefix
                schema_column.name = schema_column.name.split(".")[-1]

            # Carry every additional alias from sibling projection columns
            # so the outer query can resolve any of the user's output names.
            extra_aliases = []
            for extra in projection_matches[1:]:
                extra_name = extra.current_name
                if "." in (extra_name or ""):
                    extra_name = extra_name.split(".")[-1]
                if extra_name and extra_name != schema_column.name and extra_name not in extra_aliases:
                    extra_aliases.append(extra_name)
            schema_column.aliases = extra_aliases
            columns.append(schema_column)
        if name[0] != "$" and name in context.relations:
            context.relations.pop(name)
    context.relations[node.alias] = "subquery"

    schema = RelationSchema(name=node.alias, columns=columns)

    context.schemas = {"$derived": derived.schema(), node.alias: schema}
    context.relations[node.alias] = "subquery"
    node.schema = schema
    node.source_relations = set(source_relations)
    return node, context
