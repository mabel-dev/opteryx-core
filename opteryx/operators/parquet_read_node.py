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

import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties
from opteryx.parquet_io import InMemoryParquetCache
from opteryx.parquet_io import fetch_footer
from opteryx.parquet_io import iter_row_groups
from opteryx.parquet_io.predicates import extract_predicate_stats
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
        self._parquet_files_seen: set = set()

    @property
    def name(self) -> str:  # pragma: no cover
        return "Parquet Read"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.connector.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    def sensors(self):
        base = super().sensors()
        base["row_groups_read"] = self.readings.get("row_groups_read", 0)
        base["row_groups_pruned"] = self.readings.get("parquet_row_groups_pruned", 0)
        base["files_read"] = self.readings.get("files_read", 0)
        decode_ns = self.readings.get("time_decoding_blobs", 0)
        if decode_ns > 0 and base["row_groups_read"] > 0:
            base["rowgroups_completed_per_s"] = base["row_groups_read"] / (
                decode_ns / 1_000_000_000
            )
        range_requests = self.readings.get("parquet_range_request_count", 0)
        range_bytes = self.readings.get("parquet_range_bytes_requested", 0)
        if range_requests:
            base["parquet_avg_range_bytes"] = int(range_bytes / range_requests)
        cache_hits = self.readings.get("parquet_column_cache_hits", 0)
        cache_misses = self.readings.get("parquet_column_cache_misses", 0)
        cache_lookups = cache_hits + cache_misses
        if cache_lookups:
            base["parquet_column_cache_hit_ratio"] = cache_hits / cache_lookups
        return base

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
        self.readings["parquet_range_request_count"] += 0
        self.readings["parquet_range_bytes_requested"] += 0
        self.readings["parquet_footer_bytes"] += 0
        self.readings["parquet_column_cache_hits"] += 0
        self.readings["parquet_column_cache_misses"] += 0
        self.readings["time_parquet_read_ranges_ns"] += 0
        self.readings["time_parquet_decode_columns_ns"] += 0
        self.readings["time_parquet_task_queue_wait_ns"] += 0
        self.readings["time_parquet_task_total_ns"] += 0
        self.readings["time_parquet_footer_fetch_ns"] += 0
        self.readings["time_parquet_scheduler_wait_ns"] += 0
        self.readings["time_parquet_rowgroup_completion_ns"] += 0
        self.readings["parquet_rowgroup_peak_in_flight_max"] += 0
        self.readings["parquet_ranges_in_flight_peak"] += 0
        self.readings["parquet_active_files_peak"] += 0
        self.readings["parquet_active_rowgroups_peak"] += 0
        self.readings["time_to_first_rowgroup_ns"] += 0
        self.readings["parquet_row_groups_pruned"] += 0

        # Phase 1 predicate pushdown: extract (col, op, value) triples from pushed-down
        # predicates so the reader can prune row groups using footer min/max stats.
        predicate_stats = extract_predicate_stats(self.predicates or [])

        records_to_read = self.limit if self.limit is not None else float("inf")
        arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)

        blob_paths = self.manifest.get_file_paths()
        file_sizes = {}
        files = getattr(self.manifest, "files", None)
        if files:
            for file_entry in files:
                size = getattr(file_entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(file_entry.file_path, size)

        # Resolve the filesystem: connectors that own a pre-configured filesystem
        # (FileSystemConnector subclasses) expose it directly.  For connectors that
        # don't (e.g. OpteryxConnector/IopsReadNode path), derive it from the
        # storage protocol embedded in the file paths.
        if hasattr(self.connector, "filesystem"):
            filesystem = self.connector.filesystem
        else:
            from opteryx.connectors.io_systems import create_filesystem

            first_path = blob_paths[0] if blob_paths else ""
            protocol = first_path.split("://")[0] if "://" in first_path else ""
            filesystem = create_filesystem(protocol)
        # Column names as they appear in the Parquet file (Parquet uses the
        # original names, not identity aliases).
        column_names = [col.name for col in orso_schema.columns]
        # Map data-file column name → query-engine identity for Morsel construction.
        name_to_identity = {col.name: col.identity for col in orso_schema.columns}

        # One cache per execute() call: footers shared across all row groups of
        # the same file; column chunks cached for reuse across row groups with
        # identical content (rare but free).
        cache = InMemoryParquetCache()
        result_morsel = None

        decode_start = time.monotonic_ns()
        try:
            for row_group in iter_row_groups(
                filesystem,
                blob_paths,
                column_names,
                cache,
                predicates=predicate_stats,
                file_sizes=file_sizes or None,
            ):
                path = row_group.pop("__path__")
                _ = row_group.pop("__row_group__")
                self.readings["parquet_row_groups_pruned"] = row_group.pop(
                    "__row_groups_pruned__", 0
                )
                self.bytes_in += row_group.pop("__bytes_fetched__", 0)
                self.readings["parquet_footer_bytes"] += row_group.pop("__footer_bytes__", 0)
                self.readings["parquet_range_request_count"] += row_group.pop(
                    "__range_request_count__", 0
                )
                self.readings["parquet_range_bytes_requested"] += row_group.pop(
                    "__range_bytes_requested__", 0
                )
                self.readings["time_parquet_read_ranges_ns"] += row_group.pop(
                    "__time_read_ranges_ns__", 0
                )
                self.readings["time_parquet_decode_columns_ns"] += row_group.pop(
                    "__time_decode_columns_ns__", 0
                )
                self.readings["parquet_column_cache_hits"] += row_group.pop(
                    "__cache_column_hits__", 0
                )
                self.readings["parquet_column_cache_misses"] += row_group.pop(
                    "__cache_column_misses__", 0
                )
                self.readings["time_parquet_task_queue_wait_ns"] += row_group.pop(
                    "__task_queue_wait_ns__", 0
                )
                self.readings["time_parquet_task_total_ns"] += row_group.pop("__task_total_ns__", 0)
                self.readings["time_parquet_footer_fetch_ns"] += row_group.pop(
                    "__footer_fetch_ns__", 0
                )
                self.readings["time_parquet_scheduler_wait_ns"] += row_group.pop(
                    "__scheduler_wait_ns__", 0
                )
                self.readings["time_parquet_rowgroup_completion_ns"] += row_group.pop(
                    "__rowgroup_completion_latency_ns__", 0
                )
                self.readings["parquet_rowgroup_peak_in_flight_max"] = max(
                    self.readings.get("parquet_rowgroup_peak_in_flight_max", 0),
                    row_group.pop("__rowgroup_peak_in_flight__", 0),
                )
                self.readings["parquet_ranges_in_flight_peak"] = max(
                    self.readings.get("parquet_ranges_in_flight_peak", 0),
                    row_group.pop("__ranges_in_flight_peak__", 0),
                )
                self.readings["parquet_active_files_peak"] = max(
                    self.readings.get("parquet_active_files_peak", 0),
                    row_group.pop("__active_files_peak__", 0),
                )
                self.readings["parquet_active_rowgroups_peak"] = max(
                    self.readings.get("parquet_active_rowgroups_peak", 0),
                    row_group.pop("__active_rowgroups_peak__", 0),
                )
                time_to_first_rowgroup_ns = row_group.pop("__time_to_first_rowgroup_ns__", 0)
                if time_to_first_rowgroup_ns:
                    existing = self.readings.get("time_to_first_rowgroup_ns", 0)
                    if existing == 0 or time_to_first_rowgroup_ns < existing:
                        self.readings["time_to_first_rowgroup_ns"] = time_to_first_rowgroup_ns

                # Assemble the projected columns into a Draken Morsel directly.
                # Each value is a DrakenVector; we map data-file names to identity
                # names so the morsel arrives downstream already correctly labelled.
                identity_names = [name_to_identity[col] for col in row_group]
                vectors = list(row_group.values())
                if not identity_names:
                    # Zero-projection query (e.g. COUNT(*) with a pushed-down WHERE
                    # predicate that stripped all columns).  The reader cannot build a
                    # Morsel with no columns; row-level filtering for this case is an
                    # architectural concern for the planner, not the reader.
                    continue
                result_morsel = Morsel.from_vectors(identity_names, vectors)

                num_rows = result_morsel.num_rows
                self.readings["rows_seen"] += num_rows
                self.readings["row_groups_read"] = self.readings.get("row_groups_read", 0) + 1

                # Track distinct files (rg_idx==0 is the first row group of each file)
                if path not in self._parquet_files_seen:
                    self._parquet_files_seen.add(path)
                    self.readings["files_read"] = len(self._parquet_files_seen)
                    self.readings["blobs_seen"] += 1

                # ── LIMIT enforcement ─────────────────────────────────────────
                if records_to_read < num_rows:
                    result_morsel = result_morsel.slice(0, int(records_to_read))
                    records_to_read = 0
                else:
                    records_to_read -= num_rows

                self.readings["blobs_read"] = len(self._parquet_files_seen)
                self.telemetry.blobs_read = len(self._parquet_files_seen)
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
