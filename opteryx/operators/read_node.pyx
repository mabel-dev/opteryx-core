# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Read Node

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader),
normalizes the data into the format for internal processing.
"""

import datetime
import logging
import time

_logger = logging.getLogger(__name__)
from collections import defaultdict
from typing import Generator

import pyarrow
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.utils.json_compat import dumps as json_dumps
from opteryx.types.schema import RelationSchema
from opteryx.types.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "arrow,draken"


def struct_to_jsonb(table: pyarrow.Table) -> pyarrow.Table:
    """
    Converts any STRUCT columns in a PyArrow Table to JSON strings and replaces them
    in the same column position.

    Parameters:
        table (pa.Table): The PyArrow Table to process.

    Returns:
        pa.Table: A new PyArrow Table with STRUCT columns converted to JSON strings.
    """
    for i in range(table.num_columns):
        field = table.schema.field(i)

        # Check if the column is a STRUCT
        if pyarrow.types.is_struct(field.type):
            # Convert each row in the STRUCT column to a JSON string
            # Use list comprehension over the column directly for better performance than to_pylist()
            json_array = pyarrow.array(
                [None if row is None else json_dumps(row) for row in table.column(i)],
                type=pyarrow.binary(),
            )

            # Drop the original STRUCT column
            table = table.drop_columns(field.name)

            # Insert the new JSON column at the same position
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=pyarrow.binary()), json_array
            )

        # Check for LIST<STRUCT>
        if pyarrow.types.is_list(field.type) and pyarrow.types.is_struct(field.type.value_type):
            # Use list comprehension over the column directly
            converted_data = [
                None if item is None else [
                    None if struct is None else json_dumps(struct) for struct in item
                ] for item in table.column(i)
            ]

            # Build the new array
            jsonb_array = pyarrow.array(converted_data, type=pyarrow.list_(pyarrow.binary()))

            # Drop original column and insert new one at same position
            table = table.drop_columns(field.name)
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=jsonb_array.type), jsonb_array
            )

    return table


def normalize_morsel(schema: RelationSchema, morsel: pyarrow.Table) -> pyarrow.Table:
    if morsel.column_names == ["$COUNT(*)"]:
        return morsel
    if len(schema.columns) == 0 and morsel.column_names != ["*"]:
        one_column = pyarrow.array([True] * morsel.num_rows, type=pyarrow.bool_())
        morsel = morsel.append_column("*", one_column)
        return morsel.select(["*"])

    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = set()

    # Find which columns to drop and which columns we already have
    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.add(i)
        else:
            target_column_names.append(column_name.identity)

    # Remove from the end otherwise we'll remove the wrong columns after we've removed one
    if droppable_columns:
        keep_indices = [i for i in range(len(morsel.columns)) if i not in droppable_columns]
        morsel = morsel.select(keep_indices)

    # remane columns to the internal names (identities)
    morsel = morsel.rename_columns(target_column_names)

    # add columns we don't have, populate with nulls but try to get the correct type
    for column in schema.columns:
        if column.identity not in target_column_names:
            null_column = pyarrow.nulls(morsel.num_rows, type=column.arrow_field.type)
            field = pyarrow.field(name=column.identity, type=column.arrow_field.type)
            morsel = morsel.append_column(field, null_column)

    # ensure the columns are in the right order
    return morsel.select([col.identity for col in schema.columns])


def merge_schemas(
    orso_schema: RelationSchema, arrow_schema: pyarrow.Schema
) -> pyarrow.Schema:
    """
    Merge the Orso schema and the Arrow schema.
    """
    # ensure the columns are in the right order
    return pyarrow.schema(
        [
            pyarrow.field(
                name=col.identity,
                type=arrow_schema.field(col.identity).type,
                nullable=col.arrow_field.nullable,
                metadata=col.arrow_field.metadata,
            )
            for col in orso_schema.columns
        ]
    )


class ReaderNode(BasePlanNode):
    """
    The Reader Node is responsible for reading the relevant datasets.
    """

    def __init__(self, properties: QueryProperties, **parameters):
        """Initialize ReaderNode."""
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.alias = parameters.get("alias")
        self.dataset = parameters.get("dataset")
        self.connector = parameters.get("connector")
        self.predicates = parameters.get("predicates", [])
        self.limit = parameters.get("limit")
        self.schema = parameters.get("schema")

    def to_mermaid(self, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        dataset_name = str(self.dataset)

        mermaid = f'NODE_{nid}["**READ** ({dataset_name})<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'

    @property
    def name(self):  # pragma: no cover
        """Friendly name for this step"""
        return "Reader"

    def sensors(self):
        """Additional details for this step"""
        return {
            "dataset": self.dataset,
            "alias": self.alias,
        }

    @property
    def config(self):
        """Additional details for this step"""
        dataset_name = str(self.dataset)
        if self.alias:
            return f"{dataset_name} AS {self.alias}"
        return dataset_name

    def plan_config(self, plan):
        """Additional details for this step"""
        from opteryx.expression import NodeType
        from opteryx.planner.logical_planner import LogicalPlanStepType

        def _is_numeric(node):
            return node.node_type == NodeType.LITERAL and isinstance(
                node.value, (int, float)
            )

        def _is_string(node):
            return node.node_type == NodeType.LITERAL and isinstance(node.value, str)

        def _is_boolean(node):
            return node.node_type == NodeType.LITERAL and isinstance(node.value, bool)

        def _get_literal_value(node):
            return node.value

        def _get_column_name(node):
            return node.value

        def _get_function_name(node):
            return node.value

        # can we push selections (WHERE) into this reader
        if self.connector and self.connector.can_push_selection:
            # get the selections from the plan
            selections = plan.get_nodes_of_type(LogicalPlanStepType.Filter)
            # if we have selections, push them into the reader
            for selection in selections:
                if selection.condition:
                    self.predicates.append(selection.condition)

        # can we push projections (SELECT) into this reader
        if self.connector and self.connector.can_push_projection:
            # get the projections from the plan
            projections = plan.get_nodes_of_type(LogicalPlanStepType.Project)
            # if we have projections, push them into the reader
            for projection in projections:
                if projection.columns:
                    self.columns.extend(projection.columns)

        # can we push limits (LIMIT) into this reader
        if self.connector and self.connector.can_push_limit:
            # get the limits from the plan
            limits = plan.get_nodes_of_type(LogicalPlanStepType.Limit)
            # if we have limits, push them into the reader
            for limit in limits:
                if limit.limit:
                    self.limit = limit.limit

    def execute(self, morsel: Morsel) -> Generator:
        """Execute the ReaderNode."""
        if morsel == EOS:
            yield EOS
            return

        if not self.connector:
            raise UnsupportedSyntaxError(
                "ReaderNode is restricted to internal virtual datasets. "
                "Use ParquetReadNode for external table scans."
            )

        morsel_table = None
        orso_schema = self.schema
        orso_schema_cols = []

        # Filter columns
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)

        orso_schema.columns = orso_schema_cols
        arrow_schema = None
        start_clock = time.monotonic_ns()
        reader = self.connector.read_dataset(
            columns=self.columns,
            predicates=self.predicates,
        )

        records_to_read = self.limit if self.limit is not None else float("inf")

        for raw in reader:
            # Connectors yield Morsel; extract Arrow table for schema-alignment preprocessing.
            morsel_table = raw.to_arrow()

            if records_to_read < morsel_table.num_rows:
                morsel_table = morsel_table.slice(0, int(records_to_read))
                records_to_read = 0
            else:
                records_to_read -= morsel_table.num_rows

            morsel_table = struct_to_jsonb(morsel_table)
            morsel_table = normalize_morsel(orso_schema, morsel_table)

            if arrow_schema is None:
                arrow_schema = merge_schemas(self.schema, morsel_table.schema)

            if arrow_schema.names:
                morsel_table = morsel_table.cast(arrow_schema)

            self.telemetry.time_reading_blobs += time.monotonic_ns() - start_clock
            self.telemetry.blobs_read += 1
            self.telemetry.rows_read += morsel_table.num_rows
            self.telemetry.bytes_processed += morsel_table.nbytes

            result_morsel = Morsel.from_arrow(morsel_table)

            yield result_morsel
            start_clock = time.monotonic_ns()

            if records_to_read <= 0:
                break

        if morsel_table:
            self.telemetry.columns_read += morsel_table.num_columns
        else:
            self.telemetry.columns_read += len(orso_schema.columns)
