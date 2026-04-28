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

from draken.vectors.date32_vector import Date32Vector
from draken.vectors.timestamp_vector import TimestampVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.interval_vector import IntervalVector
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.types.schema import RelationSchema
from opteryx.types import OrsoTypes

from opteryx import EOS

from . import BasePlanNode


def _create_null_vector(column, num_rows):
    """Create a null vector of the correct type for a schema column."""
    col_type = column.type

    if col_type == OrsoTypes.INT8 or col_type == OrsoTypes.INT16 or col_type == OrsoTypes.INT32:
        return Int64Vector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.INT64:
        return Int64Vector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.FLOAT32 or col_type == OrsoTypes.FLOAT64:
        return Float64Vector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.VARCHAR or col_type == OrsoTypes.TEXT or col_type == OrsoTypes.BLOB:
        return StringVector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.BOOLEAN:
        return BoolVector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.DATE:
        return Date32Vector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.TIMESTAMP:
        return TimestampVector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.TIME:
        return TimeVector.from_constant(None, num_rows)
    elif col_type == OrsoTypes.INTERVAL:
        return IntervalVector.from_constant(None, num_rows)
    else:
        return StringVector.from_constant(None, num_rows)


def normalize_morsel(schema: RelationSchema, morsel: Morsel) -> Morsel:
    """Normalize a Morsel to match the expected schema.

    Handles:
    - Selecting columns that match the schema
    - Adding missing columns as nulls
    - Reordering columns to match schema order
    """
    if morsel.column_names == [b"$COUNT(*)"]:
        return morsel

    if len(schema.columns) == 0:
        if morsel.column_names != [b"*"]:
            all_true = BoolVector.from_constant(True, morsel.num_rows)
            morsel.append_vector(b"*", all_true)
        return morsel.select([b"*"])

    # Build lists of vectors and names in schema order
    names = []
    vectors = []

    for column in schema.columns:
        col_identity = column.identity
        col_name_bytes = column.name.encode() if isinstance(column.name, str) else column.name

        try:
            vector = morsel.column(col_identity, col_name_bytes)
            names.append(col_identity)
            vectors.append(vector)
        except (KeyError, ValueError):
            null_vector = _create_null_vector(column, morsel.num_rows)
            names.append(col_identity)
            vectors.append(null_vector)

    return Morsel.from_vectors(names, vectors)


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

    def execute(self, morsel) -> Generator:
        """Execute the ReaderNode."""
        if morsel == EOS:
            yield EOS
            return

        if not self.connector:
            raise UnsupportedSyntaxError(
                "ReaderNode is restricted to internal virtual datasets. "
                "Use ParquetReadNode for external table scans."
            )

        orso_schema = self.schema
        orso_schema_cols = []

        # Filter columns based on projection
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)

        orso_schema.columns = orso_schema_cols
        start_clock = time.monotonic_ns()
        reader = self.connector.read_dataset(
            columns=self.columns,
            predicates=self.predicates,
        )

        records_to_read = self.limit if self.limit is not None else float("inf")
        result_morsel = None

        for raw in reader:
            if records_to_read < raw.num_rows:
                raw = raw.slice(0, int(records_to_read))
                records_to_read = 0
            else:
                records_to_read -= raw.num_rows

            result_morsel = normalize_morsel(orso_schema, raw)

            self.telemetry.time_reading_blobs += time.monotonic_ns() - start_clock
            self.telemetry.blobs_read += 1
            self.telemetry.rows_read += result_morsel.num_rows
            self.telemetry.bytes_processed += result_morsel.nbytes

            yield result_morsel
            start_clock = time.monotonic_ns()

            if records_to_read <= 0:
                break

        if result_morsel:
            self.telemetry.columns_read += result_morsel.num_columns
        else:
            self.telemetry.columns_read += len(orso_schema.columns)
