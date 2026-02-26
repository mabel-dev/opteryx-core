# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet Read Node

SQL Query Execution Plan Node that reads Parquet files using the column-chunk
range-read design (docs/parquet-column-reads-design.md).

Instead of downloading whole blobs into a shared-memory ring, this node:

  1. Fetches the Parquet footer for each file (two small range reads each).
  2. Fans out (file × row-group) work units to a thread pool.
  3. For each unit, batches all projected column ranges into one read_ranges()
     call, decodes with rugo, and yields the assembled row group.

The filesystem layer is taken directly from the connector (every catalog-backed
connector already exposes ``self.filesystem``), so this node works identically
for local disk, GCS, and S3.

Row groups are yielded in completion order — the thread pool handles overlap
between I/O and decode across all files and row groups simultaneously.
"""

from __future__ import annotations

import time
from typing import Generator

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties
from opteryx.parquet_io import InMemoryParquetCache
from opteryx.parquet_io import fetch_footer
from opteryx.parquet_io import iter_row_groups
from opteryx.utils.file_decoders import get_decoder

from .read_node import ReaderNode


class ParquetReadNode(ReaderNode):
    """Read node backed by column-chunk range reads via ``parquet_io``.

    Activated for filesystem-backed connectors (GCS, S3, local) when the
    manifest contains only ``.parquet`` files.  Falls back to the existing
    ``IopsReadNode`` / ``ReaderNode`` paths for mixed or non-Parquet manifests.
    """

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.predicates = parameters.get("predicates")

    @property
    def name(self) -> str:  # pragma: no cover
        return "Parquet Read"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.connector.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    def execute(self, morsel, **kwargs) -> Generator:
        if morsel == EOS:
            yield None
            return

        orso_schema = self.parameters["schema"]

        # ── Empty manifest ────────────────────────────────────────────────────
        if not self.manifest or self.manifest.get_file_count() == 0:
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=orso_schema).arrow()
            renames = [orso_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow
            return

        # ── Project schema to requested columns only ──────────────────────────
        orso_schema_cols = [
            col
            for col in orso_schema.columns
            if col.identity in {c.schema_column.identity for c in self.columns}
        ]
        orso_schema.columns = orso_schema_cols
        self.readings["columns_read"] += len(orso_schema.columns)

        records_to_read = self.limit if self.limit is not None else float("inf")

        filesystem = self.connector.filesystem
        # Column names as they appear in the Parquet file (Parquet uses the
        # original names, not identity aliases).
        column_names = [col.name for col in orso_schema.columns]
        # Map data-file column name → query-engine identity for Morsel construction.
        name_to_identity = {col.name: col.identity for col in orso_schema.columns}
        blob_paths = self.manifest.get_file_paths()

        # One cache per execute() call: footers shared across all row groups of
        # the same file; column chunks cached for reuse across row groups with
        # identical content (rare but free).
        cache = InMemoryParquetCache()
        result_morsel = None

        decode_start = time.monotonic_ns()
        try:
            for row_group in iter_row_groups(filesystem, blob_paths, column_names, cache):
                path = row_group.pop("__path__")
                rg_idx = row_group.pop("__row_group__")

                # Assemble the projected columns into a Draken Morsel directly.
                # Each value is a DrakenVector; we map data-file names to identity
                # names so the morsel arrives downstream already correctly labelled.
                identity_names = [name_to_identity[col] for col in row_group]
                vectors = list(row_group.values())
                result_morsel = Morsel.from_vectors(identity_names, vectors)

                num_rows = result_morsel.num_rows
                self.readings["rows_seen"] += num_rows
                self.readings["blobs_seen"] += 1

                # ── LIMIT enforcement ─────────────────────────────────────────
                if records_to_read < num_rows:
                    result_morsel = result_morsel.slice(0, int(records_to_read))
                    records_to_read = 0
                else:
                    records_to_read -= num_rows

                self.readings["blobs_read"] += 1
                self.telemetry.blobs_read += 1
                self.readings["rows_read"] += result_morsel.num_rows
                self.telemetry.rows_read += result_morsel.num_rows
                self.readings["bytes_processed"] += result_morsel.nbytes
                self.telemetry.bytes_processed += result_morsel.nbytes

                yield result_morsel

                if records_to_read <= 0:
                    break

        finally:
            decode_ns = time.monotonic_ns() - decode_start
            self.readings["time_decoding_blobs"] = (
                self.readings.get("time_decoding_blobs", 0) + decode_ns
            )
            self.telemetry.time_decoding_blobs += decode_ns

        # ── Empty result guard ────────────────────────────────────────────────
        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in arrow_schema], schema=arrow_schema
            )
