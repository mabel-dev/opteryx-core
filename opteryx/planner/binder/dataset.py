# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import (
    AmbiguousDatasetError,
    InvalidFunctionParameterError,
    UnsupportedSyntaxError,
)
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn, RelationSchema
from opteryx.utils import random_string


def visit_function_dataset(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    # We need to build the schema and add it to the schema collection.
    if node.function == "VALUES":
        relation_name = node.alias or f"$values-{random_string()}"
        types = {}
        element_types = {}
        if len(node.values) > 0:
            for i, column in enumerate(node.columns):
                if len(node.values[0]) >= i:
                    value = node.values[0][i]
                    types[column] = value.type
                    if value.type in (OrsoTypes.ARRAY, OrsoTypes.VECTOR):
                        element_type = getattr(value, "element_type", None)
                        if element_type in (None, OrsoTypes._MISSING_TYPE):
                            schema_column = getattr(value, "schema_column", None)
                            element_type = (
                                getattr(schema_column, "element_type", None)
                                if schema_column is not None
                                else None
                            )
                        element_types[column] = element_type
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=column,
                source=relation_name,
                schema_column=FlatColumn(
                    name=column,
                    type=types.get(column, OrsoTypes.NULL),
                    element_type=element_types.get(column),
                ),
            )
            for column in node.columns
        ]
        schema = RelationSchema(
            name=relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[relation_name] = schema
        node.columns = columns
        node.schema = schema
    elif node.function == "UNNEST":
        # this is strictly SELECT * FROM UNNEST(literal) AS alias(column)
        relation_name = node.alias

        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=node.unnest_target,
                source=relation_name,
                schema_column=FlatColumn(name=node.unnest_target, type=0),
            )
        ]
        schema = RelationSchema(name=relation_name, columns=[c.schema_column for c in columns])
        context.schemas[relation_name] = schema
        # ensure origin is set so later passes (projection pushdown, etc.)
        for column in schema.columns:
            column.origin = [relation_name]
        node.columns = columns
        node.schema = schema
    elif node.function == "GENERATE_SERIES":
        element_type = OrsoTypes._MISSING_TYPE
        first_arg = node.args[0]
        if first_arg.node_type == NodeType.NESTED:
            first_arg = first_arg.centre
        if first_arg.type.is_numeric():
            types = {n.type for n in node.args}
            if len(types) == 1:
                element_type = list(types)[0]
            elif types == {OrsoTypes.INTEGER, OrsoTypes.DOUBLE}:
                element_type = OrsoTypes.DOUBLE
            else:
                raise InvalidFunctionParameterError(
                    "GENERATE_SERIES for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters."
                )
        if first_arg.type.is_temporal():
            element_type = OrsoTypes.TIMESTAMP

        node.relation_name = node.alias
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=node.alias,
                source=node.relation_name,
                schema_column=FlatColumn(name=node.alias, type=element_type),
            )
        ]
        schema = RelationSchema(
            name=node.relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[node.relation_name] = schema
        # tag generated columns with their origin relation name so downstream
        # binder/optimizer logic can detect their source
        for column in schema.columns:
            column.origin = [node.relation_name]
        node.columns = columns
        node.schema = schema
    elif node.function == "FAKE":
        from opteryx.types.schema import ColumnDisposition

        node.relation_name = node.alias
        node.rows = int(node.args[0].value)

        if len(node.args) < 2:
            raise InvalidFunctionParameterError(
                "FAKE function expects at least two parameters, the number of rows, and then either the number of columns, or an array of the column types."
            )

        if node.args[1].node_type == NodeType.NESTED:
            column_definition = [node.args[1].centre]
        else:
            column_definition = node.args[1].value

        special_handling = {
            "NAME": (OrsoTypes.VARCHAR, ColumnDisposition.NAME),
            "AGE": (OrsoTypes.INTEGER, ColumnDisposition.AGE),
        }

        columns = []
        if isinstance(column_definition, tuple):
            for i, column_type in enumerate(column_definition):
                name = node.columns[i] if i < len(node.columns) else f"column_{i}"
                column_type = str(column_type).upper()
                if column_type in special_handling:
                    actual_type, disposition = special_handling[column_type]
                    schema_column = FlatColumn(name=name, type=actual_type, disposition=disposition)
                else:
                    schema_column = FlatColumn(name=name, type=column_type)
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=schema_column.name,
                        source=node.alias,
                        schema_column=schema_column,
                    )
                )
            schema = RelationSchema(
                name=node.alias,
                columns=[c.schema_column for c in columns],
            )
            node.columns = columns
            node.schema = schema
        else:
            try:
                column_definition = int(column_definition)  # type: ignore
            except TypeError:
                raise InvalidFunctionParameterError(
                    "Expected number of rows for 'FAKE' function or list of column types. Are you missing parenthesis?"
                )
            names = node.columns + tuple(
                f"column_{i}"
                for i in range(len(node.columns), column_definition)  # type: ignore
            )
            node.columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=names[i],
                    source=node.alias,
                    schema_column=FlatColumn(name=names[i], type=OrsoTypes.INTEGER),
                )
                for i in range(column_definition)  # type: ignore
            ]

        schema = RelationSchema(
            name=node.relation_name,
            columns=[c.schema_column for c in node.columns],
        )
        context.schemas[node.relation_name] = schema
        node.schema = schema
    else:
        raise UnsupportedSyntaxError(f"{node.function} cannot be used in place of a table.")
    node.connector = None
    return node, context


def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    from opteryx.connectors import connector_factory
    from opteryx.exceptions import DatabaseError
    from opteryx.managers.permissions import can_perform_action

    node.relation = node.relation.lower()

    if node.alias in context.relations:
        raise AmbiguousDatasetError(dataset=node.alias)

    # Get connector gateway (cached by prefix)
    gateway = connector_factory(node.relation, telemetry=context.telemetry)

    # Extract the dataset name (remove prefix if configured)
    dataset_name = node.relation

    # Create table-specific engine
    engine_kwargs = {}
    if hasattr(gateway, "variables"):
        engine_kwargs["variables"] = context.execution_context.variables
    if gateway.supports_diachronic:
        engine_kwargs["at_date"] = node.at_date

    node.connector = gateway.table_engine(
        dataset_name, telemetry=context.telemetry, **engine_kwargs
    )

    # ensure this user can read the table
    if not can_perform_action(context.execution_context, node.relation, action="READ"):
        raise PermissionError(f"User does not have permission to read {node.relation}")

    if hasattr(node.connector, "variables"):
        node.connector.variables = context.execution_context.variables
    if gateway.supports_diachronic:
        node.connector.start_date = node.start_date
        node.connector.end_date = node.end_date
    try:
        # Get dataset schema and build manifest (if supported by connector)
        # For Opteryx catalog connectors, this creates a Manifest with file-level stats
        if hasattr(node.connector, "get_dataset_metadata"):
            node.schema, node.manifest = node.connector.get_dataset_metadata()
            # Propagate dataset commit timestamp from the connector to the
            # logical node so it becomes available to physical nodes
            # (and ultimately shown as `committed_at` in telemetry).
            try:
                dc = getattr(node.connector, "dataset_committed_at", None)
                if dc is not None:
                    node.dataset_committed_at = dc
            except (AttributeError, TypeError):
                pass
        else:
            # Fallback for connectors that don't have manifest support yet
            node.schema = node.connector.get_dataset_schema()
            node.manifest = None
        context.schemas[node.alias] = node.schema
        for column in node.schema.columns:
            column.origin = [node.alias]

        context.relations[node.alias] = node.connector.__mode__
    except DatabaseError as err:
        raise err
    except Exception as e:
        from opteryx.exceptions import DatasetReadError

        raise DatasetReadError(f"Cannot read information for dataset '{node.relation}': {e}") from e

    return node, context
