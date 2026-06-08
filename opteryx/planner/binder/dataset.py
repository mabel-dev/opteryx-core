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
from opteryx.types.logical_type import LogicalCategory, ColumnType, _NUMERIC_TYPES, _TEMPORAL_TYPES
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema
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
                    types[column] = value.type  # ColumnType
                    # Phase 2: element is embedded in ARRAY/VECTOR ColumnType.
                    _val_cat = value.type.category if isinstance(value.type, ColumnType) else value.type
                    if _val_cat in (LogicalCategory.ARRAY, LogicalCategory.VECTOR):
                        _elem = value.type.element if isinstance(value.type, ColumnType) else None
                        if _elem is None:
                            schema_column = getattr(value, "schema_column", None)
                            if schema_column is not None and isinstance(getattr(schema_column, "column_type", None), ColumnType):
                                _elem = schema_column.column_type.element
                        element_types[column] = _elem
        def _build_value_column(column):
            ct = types.get(column)  # ColumnType or None
            if isinstance(ct, ColumnType):
                return SchemaColumn(name=column, column_type=ct)
            from opteryx.types import logical_type as _lt2
            return SchemaColumn(name=column, column_type=_lt2.NULL)
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=column,
                source=relation_name,
                schema_column=_build_value_column(column),
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
                schema_column=SchemaColumn(name=node.unnest_target),
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
        element_type = None
        first_arg = node.args[0]
        if first_arg.node_type == NodeType.NESTED:
            first_arg = first_arg.centre
        # Phase 2: first_arg.type is ColumnType; compare via .category
        first_arg_cat = first_arg.type.category if isinstance(first_arg.type, ColumnType) else first_arg.type
        if first_arg_cat is not None and first_arg_cat in _NUMERIC_TYPES:
            arg_cts = {n.type for n in node.args}
            arg_cats = {t.category if isinstance(t, ColumnType) else t for t in arg_cts}
            if len(arg_cts) == 1:
                element_type = list(arg_cts)[0]  # ColumnType
            elif arg_cats == {LogicalCategory.INTEGER, LogicalCategory.FLOAT}:
                element_type = _lt.FLOAT64
            else:
                raise InvalidFunctionParameterError(
                    "GENERATE_SERIES for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters."
                )
        if first_arg_cat is not None and first_arg_cat in _TEMPORAL_TYPES:
            element_type = _lt.TIMESTAMP()

        node.relation_name = node.alias
        _gs_schema_col = SchemaColumn(name=node.alias, column_type=element_type if isinstance(element_type, ColumnType) else None)
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=node.alias,
                source=node.relation_name,
                schema_column=_gs_schema_col,
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
    if "variables" in dir(gateway):
        engine_kwargs["variables"] = context.execution_context.variables
    if gateway.supports_diachronic:
        engine_kwargs["at_date"] = node.at_date

    node.connector = gateway.table_engine(
        dataset_name, telemetry=context.telemetry, **engine_kwargs
    )

    # ensure this user can read the table
    if not can_perform_action(context.execution_context, node.relation, action="READ"):
        raise PermissionError(f"User does not have permission to read {node.relation}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables
    if gateway.supports_diachronic:
        node.connector.start_date = node.start_date
        node.connector.end_date = node.end_date
    try:
        # Get dataset schema and build manifest (if supported by connector)
        # For Opteryx catalog connectors, this creates a Manifest with file-level stats
        if getattr(node.connector, "get_dataset_metadata", None) is not None:
            node.schema, node.manifest = node.connector.get_dataset_metadata()
            # Propagate dataset commit timestamp from the connector to the
            # logical node so it becomes available to physical nodes
            # (and ultimately shown as `committed_at` in telemetry).
            dc = getattr(node.connector, "dataset_committed_at", None)
            if dc is not None:
                node.dataset_committed_at = dc
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
