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

from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.types.schema import RelationSchema

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


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

        # Fail clean (§1): this path only ever serves internal virtual datasets,
        # where schema() and read() come from the same provider — every schema
        # column is present in the source morsel. A miss here is therefore an
        # internal inconsistency (e.g. a binder/optimizer rename that detached a
        # scan column from its physical name), NOT a "missing column" to be padded.
        # Silently substituting a NULL placeholder of the schema's default width
        # masked exactly such a bug as wrong data; surface it instead.
        try:
            vector = morsel.column(col_identity, col_name_bytes)
        except (KeyError, ValueError):
            raise InvalidInternalStateError(
                f"Reader could not map schema column identity={col_identity!r} "
                f"name={col_name_bytes!r} (schema '{getattr(schema, 'name', None)}') "
                f"to the source data; available columns: {list(morsel.column_names)}. "
                "The schema and the data disagree — an upstream planning step has "
                "corrupted this column's identity/name."
            )
        names.append(col_identity)
        vectors.append(vector)

    return Morsel.from_vectors(names, vectors)


cdef class ReaderNode(BasePlanNode):
    """
    The Reader Node is responsible for reading the relevant datasets.
    """

    cdef public object alias
    cdef public object dataset
    cdef public object relation
    cdef public object connector
    cdef public object predicates
    cdef public object limit
    cdef public object schema

    def __init__(self, properties: QueryProperties, **parameters):
        """Initialize ReaderNode."""
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.alias = parameters.get("alias")
        self.dataset = parameters.get("dataset")
        # Only set for a plain Scan (catalog/filesystem table) -- READ_JSONL/
        # READ_PARQUET FunctionDataset nodes carry the source path in `dataset`
        # instead and never set `relation`.
        self.relation = parameters.get("relation")
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

    @property
    def honours_scan_overrides(self):
        """Whether this reader READS the per-scan `WITH(name = value)` settings.

        False here, and overridden to True only by a reader that actually
        consumes them. The planner refuses a hint on a relation whose reader
        does not, rather than accepting a setting that would quietly do
        nothing — an inert knob is indistinguishable from a broken one.
        """
        return False

    def sensors(self):
        """Reader-specific details, merged onto the base counters (calls,
        execution_time, self_time, records/bytes) so scans report timing too —
        the scan's execution_time is populated by drive_scan timing
        next_morsel()."""
        base = BasePlanNode.sensors(self)
        base["dataset"] = self.dataset
        base["alias"] = self.alias
        return base

    @property
    def config(self):
        """Additional details for this step"""
        # A plain Scan never populates `dataset` (only READ_JSONL/READ_PARQUET
        # FunctionDataset nodes do) -- fall back to the relation name so the
        # table isn't rendered as the literal string "None".
        dataset_name = str(self.dataset) if self.dataset is not None else str(self.relation)
        if self.alias and self.alias != self.relation:
            return f"{dataset_name} AS {self.alias}"
        return dataset_name

    def plan_config(self):
        """What this reader ABSORBED from the plan.

        Pushdown removes operators: a predicate pushed into a parquet scan
        leaves no Filter node behind, so without this the plan view showed a
        bare relation name and the predicate simply vanished from the render.

        This reports the node's OWN state, which is the record of what was
        pushed. It does not re-derive the decision: by the time a physical plan
        exists the optimizer has already made it, and `self.predicates` /
        `self.columns` / `self.limit` ARE the answer. The previous body asked
        the physical plan for `LogicalPlanStepType` nodes and appended what it
        found back onto those same fields -- it answered the wrong question and
        mutated the reader while rendering it.

        Keys carrying nothing are omitted, so a reader with no pushdown renders
        as the relation alone rather than as a row of empty lists.
        """
        from opteryx.expression import format_expression

        config = {"relation": self.config}

        if self.columns:
            config["columns"] = [format_expression(column) for column in self.columns]
        if self.predicates:
            config["predicates"] = [format_expression(predicate) for predicate in self.predicates]
        if self.limit is not None:
            config["limit"] = self.limit

        return config

    def read_morsels(self):
        """Source-side morsel iterator used by the push pipeline engine.

        Yields raw morsels; the engine pushes each one into the chain and
        sends a terminal EOS after the iterator exhausts."""
        if not self.connector:
            raise InvalidInternalStateError(
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
            # Per-node only (sensors/mermaid, remapped to bytes_in). This is the
            # morsel's MATERIALIZED in-memory size, not a quantity the billing
            # meter can use — `billing_bytes` on the shared telemetry is dense
            # logical bytes measured at plan time (planner/data_processed.py),
            # and adding this to it mixed two quantities in one number.
            self.readings["bytes_processed"] += result_morsel.nbytes

            yield result_morsel
            start_clock = time.monotonic_ns()

            if records_to_read <= 0:
                break

        if result_morsel:
            self.telemetry.columns_read += result_morsel.num_columns
        else:
            self.telemetry.columns_read += len(relation_schema.columns)
