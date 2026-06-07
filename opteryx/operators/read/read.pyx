# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

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

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.types.schema import RelationSchema
from opteryx.types.logical_type import LogicalCategory

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.

# Null vector factory: maps LogicalCategory → draken_native constructor for null constants.
# Built once at module import. All constructors accept (value=None, length).
cdef dict _NULL_CONSTRUCTORS = {}

def _build_null_constructors():
    """Build null vector constructor table: LogicalCategory → callable(n) → Vector."""
    from draken.draken_native import (
        vector_from_constant,
        vector_float64_from_constant,
        vector_varchar_from_constant,
        vector_from_bool_constant,
        vector_null_from_length,
    )
    # All lambdas take (n: int) and return a null vector of appropriate type
    table = [
        ("INTEGER",   lambda n: vector_from_constant(None, n)),
        ("INT8",      lambda n: vector_from_constant(None, n)),
        ("INT16",     lambda n: vector_from_constant(None, n)),
        ("INT32",     lambda n: vector_from_constant(None, n)),
        ("INT64",     lambda n: vector_from_constant(None, n)),
        ("DOUBLE",    lambda n: vector_float64_from_constant(None, n)),
        ("FLOAT32",   lambda n: vector_float64_from_constant(None, n)),
        ("FLOAT64",   lambda n: vector_float64_from_constant(None, n)),
        ("VARCHAR",   lambda n: vector_varchar_from_constant(None, n)),
        ("TEXT",      lambda n: vector_varchar_from_constant(None, n)),
        ("BLOB",      lambda n: vector_varchar_from_constant(None, n)),
        ("BOOLEAN",   lambda n: vector_from_bool_constant(None, n)),
        ("DATE",      lambda n: vector_null_from_length(n)),
        ("TIMESTAMP", lambda n: vector_null_from_length(n)),
        ("TIME",      lambda n: vector_null_from_length(n)),
        ("INTERVAL",  lambda n: vector_null_from_length(n)),
    ]
    out = {}
    for name, ctor in table:
        member = getattr(LogicalCategory, name, None)
        if member is not None:
            out[member] = ctor
    return out

_NULL_CONSTRUCTORS = _build_null_constructors()


cdef inline object _create_null_vector(object column, Py_ssize_t num_rows):
    """Create a null vector of the correct type for a schema column."""
    ctor = _NULL_CONSTRUCTORS.get(column.category)
    if ctor is None:
        return _draken_native.vector_null_from_length(<uint32_t>num_rows)
    return ctor(<uint32_t>num_rows)


cdef Morsel normalize_morsel(object schema, Morsel morsel):
    """Normalize a Morsel to match the expected schema.

    Handles:
    - Selecting columns that match the schema
    - Adding missing columns as nulls
    - Reordering columns to match schema order
    """
    if morsel.column_names == [b"$COUNT(*)"]:
        return morsel

    cdef Py_ssize_t num_rows = morsel.num_rows

    if len(schema.columns) == 0:
        if morsel.column_names != [b"*"]:
            all_true = _draken_native.vector_from_bool_constant(True, <uint32_t>num_rows)
            morsel.append_vector(b"*", all_true)
        return morsel.select([b"*"])

    # Build lists of vectors and names in schema order
    cdef list names = []
    cdef list vectors = []
    cdef object col_identity
    cdef object col_name
    cdef bytes col_name_bytes

    for column in schema.columns:
        col_identity = column.identity
        col_name = column.name
        if isinstance(col_name, str):
            col_name_bytes = col_name.encode()
        else:
            col_name_bytes = col_name

        try:
            vector = morsel.column(col_identity, col_name_bytes)
            names.append(col_identity)
            vectors.append(vector)
        except (KeyError, ValueError):
            names.append(col_identity)
            vectors.append(_create_null_vector(column, num_rows))

    return Morsel.from_vectors(names, vectors)


cdef class ReaderNode(BasePlanNode):
    """
    The Reader Node is responsible for reading the relevant datasets.
    """

    cdef public object alias
    cdef public object dataset
    cdef public object connector
    cdef public object predicates
    cdef public object limit
    cdef public object schema

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

    def read_morsels(self):
        """Source-side morsel iterator used by the push pipeline engine.

        Yields raw morsels; the engine pushes each one into the chain and
        sends a terminal EOS after the iterator exhausts."""
        if not self.connector:
            raise UnsupportedSyntaxError(
                "ReaderNode is restricted to internal virtual datasets. "
                "Use ParquetReadNode for external table scans."
            )

        relation_schema = self.schema
        relation_schema_cols = []
        for col in relation_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                relation_schema_cols.append(col)
        relation_schema.columns = relation_schema_cols
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

            result_morsel = normalize_morsel(relation_schema, raw)

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
            self.telemetry.columns_read += len(relation_schema.columns)
