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
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from copy import deepcopy
from typing import Generator

import numpy
import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema
from orso.tools import random_string

from opteryx import EOS
from opteryx import config
from opteryx.draken.morsels.morsel import Morsel
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.parquet_io import InMemoryParquetCache
from opteryx.parquet_io import fetch_footer
from opteryx.parquet_io import iter_row_groups
from opteryx.parquet_io.predicates import extract_predicate_stats
from opteryx.utils.parquet_decoder import parquet_decoder

from .read_node import ReaderNode
from .read_node import normalize_morsel
from .read_node import struct_to_jsonb


class ParquetReadNode(ReaderNode):
    """Read node backed by column-chunk range reads via ``parquet_io``.

    Activated for filesystem-backed connectors (GCS, S3, local) when the
    manifest contains only ``.parquet`` files.  Non-parquet external scans are
    rejected by the planner.
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
        base["parquet_filter_columns_read"] = self.readings.get("parquet_filter_columns_read", 0)
        base["parquet_projection_columns_read"] = self.readings.get(
            "parquet_projection_columns_read", 0
        )
        base["parquet_rows_before_filter"] = self.readings.get("parquet_rows_before_filter", 0)
        base["parquet_rows_after_filter"] = self.readings.get("parquet_rows_after_filter", 0)
        rows_before_filter = base["parquet_rows_before_filter"]
        if rows_before_filter > 0:
            base["parquet_filter_selectivity"] = (
                base["parquet_rows_after_filter"] / rows_before_filter
            )
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
        if base["row_groups_read"] > 0:
            base["parquet_avg_emit_wait_ns"] = (
                self.readings.get("time_parquet_emit_wait_ns", 0) / base["row_groups_read"]
            )
        return base

    @staticmethod
    def _has_repeated_projection(meta: dict, column_names: list[str]) -> bool:
        row_groups = meta.get("row_groups") or []
        if not row_groups:
            return False
        name_to_stats = {col["name"]: col for col in row_groups[0].get("columns", [])}
        for col_name in column_names:
            stats = name_to_stats.get(col_name)
            if stats and int(stats.get("max_repetition_level") or 0) > 0:
                return True
        return False

    @staticmethod
    def _extract_filter_identities(predicates) -> set[str]:
        if not predicates:
            return set()
        identities = set()
        for predicate in predicates:
            identifiers = get_all_nodes_of_type(predicate, select_nodes=(NodeType.IDENTIFIER,))
            for identifier in identifiers:
                schema_column = getattr(identifier, "schema_column", None)
                identity = getattr(schema_column, "identity", None)
                if identity:
                    identities.add(identity)
        return identities

    @staticmethod
    def _compose_predicates(predicates):
        if not predicates:
            return None

        predicate_nodes = [predicate.copy() for predicate in predicates if predicate is not None]
        if not predicate_nodes:
            return None

        root = predicate_nodes.pop()
        while predicate_nodes:
            right = predicate_nodes.pop()
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
        return root

    @staticmethod
    def _mask_to_arrow(mask):
        if isinstance(mask, pyarrow.BooleanArray):
            return mask
        if isinstance(mask, pyarrow.ChunkedArray):
            return mask.combine_chunks()
        if isinstance(mask, numpy.ndarray):
            return pyarrow.array(mask, type=pyarrow.bool_())
        if isinstance(mask, list):
            return pyarrow.array(mask, type=pyarrow.bool_())
        return pyarrow.array(numpy.asarray(mask, dtype=numpy.bool_), type=pyarrow.bool_())

    def _apply_predicates_to_morsel(self, morsel: Morsel, predicate_root, eval_schema=None):
        if predicate_root is None:
            return morsel, morsel.num_rows, morsel.num_rows

        source_table = morsel.to_arrow()
        rows_before_filter = source_table.num_rows
        eval_table = source_table

        if eval_schema is not None and eval_table.column_names != ["*"]:
            eval_table = self._cast_table_to_schema(eval_table, eval_schema)
        if any(getattr(column, "num_chunks", 0) > 1 for column in eval_table.columns):
            eval_table = eval_table.combine_chunks()

        function_nodes = get_all_nodes_of_type(predicate_root, select_nodes=(NodeType.FUNCTION,))
        if function_nodes:
            eval_table = evaluate_and_append(function_nodes, eval_table)

        mask = evaluate(predicate_root, eval_table)
        filtered = source_table.filter(self._mask_to_arrow(mask))
        if filtered.num_rows == 0:
            return morsel.slice(0, 0), rows_before_filter, 0

        # Arrow filter commonly yields chunked arrays. Draken Morsel conversion expects
        # single-chunk arrays, so coalesce chunks before conversion for correctness.
        if any(getattr(column, "num_chunks", 0) > 1 for column in filtered.columns):
            filtered = filtered.combine_chunks()
        return Morsel.from_arrow(filtered), rows_before_filter, filtered.num_rows

    @staticmethod
    def _cast_table_to_schema(table: pyarrow.Table, schema: pyarrow.Schema) -> pyarrow.Table:
        if table.column_names == ["*"]:
            return table
        if len(schema) == 0:
            # Preserve row count for zero-column outputs (for example COUNT(*) pipelines)
            # by dropping all columns from the current table instead of constructing a
            # fresh empty table, which would have zero rows.
            if table.num_columns == 0:
                return table
            return table.drop(table.column_names)

        arrays = []
        names = []
        for field in schema:
            if field.name not in table.column_names:
                continue
            column = table.column(field.name)
            source_type = column.type
            target_type = field.type
            if source_type.equals(target_type):
                arrays.append(column)
                names.append(field.name)
                continue

            source_value_type = source_type
            if pyarrow.types.is_dictionary(source_type):
                source_value_type = source_type.value_type
                # Avoid materializing dictionary columns if the value type matches the target.
                # Rugo preserves dictionary encoding for performance (Draken Phase 5);
                # the logical content is correct even if physical representation differs.
                if source_value_type.equals(target_type):
                    arrays.append(column)
                    names.append(field.name)
                    continue

            # Rugo decodes Parquet DATE logical values as integer day counts. Arrow's
            # direct int64->date64 cast treats integers as milliseconds, so convert
            # through date32 (days) first to preserve semantics.
            if pyarrow.types.is_date64(target_type) and (
                pyarrow.types.is_int64(source_value_type)
                or pyarrow.types.is_int32(source_value_type)
            ):
                casted = pyarrow.compute.cast(column, pyarrow.int32())
                casted = pyarrow.compute.cast(casted, pyarrow.date32())
                casted = pyarrow.compute.cast(casted, pyarrow.date64())
            elif pyarrow.types.is_date32(target_type) and (
                pyarrow.types.is_int64(source_value_type)
                or pyarrow.types.is_int32(source_value_type)
            ):
                casted = pyarrow.compute.cast(column, pyarrow.int32())
                casted = pyarrow.compute.cast(casted, pyarrow.date32())
            else:
                casted = pyarrow.compute.cast(column, target_type)
            arrays.append(casted)
            names.append(field.name)

        return pyarrow.Table.from_arrays(arrays, names=names)

    @staticmethod
    def _cast_morsel_to_schema(morsel: Morsel, schema: pyarrow.Schema) -> Morsel:
        if morsel.num_rows == 0:
            return morsel
        if len(schema) == 0:
            # Zero-projection pipelines (for example COUNT(*) with filters) still
            # need row-bearing morsels for downstream operators to count rows.
            return morsel

        table = morsel.to_arrow()
        if table.column_names != ["*"]:
            table = ParquetReadNode._cast_table_to_schema(table, schema)
        if any(getattr(column, "num_chunks", 0) > 1 for column in table.columns):
            table = table.combine_chunks()
        return Morsel.from_arrow(table)

    def _execute_full_file_fallback(
        self,
        filesystem,
        blob_paths: list[str],
        file_sizes: dict,
        read_orso_schema,
        read_arrow_schema,
        output_arrow_schema,
        output_identity_order,
        predicate_root,
        records_to_read,
    ):
        result_morsel = None
        for blob_name in blob_paths:
            known_size = file_sizes.get(blob_name)
            if not isinstance(known_size, int) or known_size <= 0:
                known_size = filesystem.get_file_info(blob_name).size

            read_start_ns = time.monotonic_ns()
            (payload,) = filesystem.read_ranges(blob_name, [(0, known_size)])
            self.readings["time_parquet_read_ranges_ns"] += time.monotonic_ns() - read_start_ns
            self.bytes_in += len(payload)

            decode_start_ns = time.monotonic_ns()
            num_rows, _, raw_bytes, result_table = parquet_decoder(
                memoryview(payload),
                projection=None,
                selection=None,
            )
            decode_ns = time.monotonic_ns() - decode_start_ns
            self.readings["time_decoding_blobs"] += decode_ns
            self.telemetry.time_decoding_blobs += decode_ns

            result_table = struct_to_jsonb(result_table)
            result_table = normalize_morsel(read_orso_schema, result_table)
            if result_table.column_names != ["*"]:
                result_table = result_table.cast(read_arrow_schema)
            if any(getattr(column, "num_chunks", 0) > 1 for column in result_table.columns):
                result_table = result_table.combine_chunks()
            result_morsel = Morsel.from_arrow(result_table)

            rows_before_filter = result_morsel.num_rows
            rows_after_filter = rows_before_filter
            if predicate_root is not None:
                result_morsel, rows_before_filter, rows_after_filter = (
                    self._apply_predicates_to_morsel(
                        result_morsel,
                        predicate_root,
                        eval_schema=read_arrow_schema,
                    )
                )

            self.readings["parquet_rows_before_filter"] += rows_before_filter
            self.readings["parquet_rows_after_filter"] += rows_after_filter
            if self.readings["parquet_rows_before_filter"] > 0:
                self.readings["parquet_filter_selectivity"] = (
                    self.readings["parquet_rows_after_filter"]
                    / self.readings["parquet_rows_before_filter"]
                )

            if output_identity_order:
                result_morsel = result_morsel.select(output_identity_order)
            result_morsel = self._cast_morsel_to_schema(result_morsel, output_arrow_schema)

            if records_to_read < result_morsel.num_rows:
                result_morsel = result_morsel.slice(0, int(records_to_read))
                records_to_read = 0
            else:
                records_to_read -= result_morsel.num_rows

            self.readings["rows_seen"] += result_morsel.num_rows

            self.readings["blobs_read"] += 1
            self.telemetry.blobs_read += 1
            self.readings["rows_read"] += result_morsel.num_rows
            self.telemetry.rows_read += result_morsel.num_rows
            self.readings["bytes_processed"] += result_morsel.nbytes
            self.telemetry.bytes_processed += result_morsel.nbytes
            self.readings["bytes_raw"] += raw_bytes
            self.telemetry.bytes_raw = getattr(self.telemetry, "bytes_raw", 0) + raw_bytes

            yield result_morsel
            if records_to_read <= 0:
                break

        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in output_arrow_schema], schema=output_arrow_schema
            )

    def execute(self, morsel, **kwargs) -> Generator:
        if morsel == EOS:
            yield None
            return

        base_schema = self.parameters["schema"]
        projected_identities = [column.schema_column.identity for column in (self.columns or [])]
        projected_identity_set = set(projected_identities)

        filter_identity_set = self._extract_filter_identities(self.predicates)
        required_identity_set = projected_identity_set.union(filter_identity_set)
        if not required_identity_set and base_schema.columns:
            # Zero-projection/no-filter scans still need one physical column so row
            # counts flow through the pipeline. Keep output projection empty.
            required_identity_set = {base_schema.columns[0].identity}

        read_schema = deepcopy(base_schema)
        read_schema.columns = [
            column for column in base_schema.columns if column.identity in required_identity_set
        ]

        output_schema = deepcopy(base_schema)
        output_schema.columns = [
            column for column in base_schema.columns if column.identity in projected_identity_set
        ]
        output_identity_order = [column.identity for column in output_schema.columns]
        predicate_root = self._compose_predicates(self.predicates or [])

        # ── Empty manifest ────────────────────────────────────────────────────
        if not self.manifest or self.manifest.get_file_count() == 0:
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=output_schema).arrow()
            renames = [output_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow
            return

        self.readings["columns_read"] += len(read_schema.columns)
        self.readings["parquet_filter_columns_read"] += len(filter_identity_set)
        self.readings["parquet_projection_columns_read"] += len(projected_identity_set)
        self.readings["parquet_rows_before_filter"] += 0
        self.readings["parquet_rows_after_filter"] += 0
        self.readings["parquet_filter_selectivity"] += 0
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
        self.readings["time_parquet_emit_wait_ns"] += 0
        self.readings["time_parquet_scheduler_empty_wait_ns"] += 0
        self.readings["parquet_scheduler_empty_wait_events"] += 0
        self.readings["parquet_rowgroup_peak_in_flight_max"] += 0
        self.readings["parquet_ranges_in_flight_peak"] += 0
        self.readings["parquet_active_files_peak"] += 0
        self.readings["parquet_active_rowgroups_peak"] += 0
        self.readings["parquet_emit_queue_depth_at_ready_max"] += 0
        self.readings["time_to_first_rowgroup_ns"] += 0
        self.readings["parquet_row_groups_pruned"] += 0
        self.readings["io_ring_slot_bytes"] += 0
        self.readings["io_ring_slot_count"] += 0
        self.readings["io_ring_total_bytes"] += 0
        self.readings["io_ring_producer_full_wait_ns"] += 0
        self.readings["io_ring_producer_full_wait_events"] += 0
        self.readings["io_ring_consumer_empty_wait_ns"] += 0
        self.readings["io_ring_consumer_empty_wait_events"] += 0
        self.readings["io_transfer_ready_backlog_peak"] += 0
        self.readings["io_transfer_emit_wait_ns"] += 0
        self.readings["io_transfer_fragment_count_p50"] += 0
        self.readings["io_transfer_fragment_count_p95"] += 0
        self.readings["io_transfer_fragment_count_max"] += 0
        self.readings["io_transfer_payload_bytes_p50"] += 0
        self.readings["io_transfer_payload_bytes_p95"] += 0
        self.readings["io_transfer_payload_bytes_max"] += 0
        self.readings["io_rowgroup_slice_count"] += 0
        self.readings["io_deserialize_ns"] += 0
        self.readings["io_serialize_ns"] += 0

        # Phase 1 predicate pushdown: extract (col, op, value) triples from pushed-down
        # predicates so the reader can prune row groups using footer min/max stats.
        predicate_stats = extract_predicate_stats(self.predicates or [])

        records_to_read = self.limit if self.limit is not None else float("inf")
        read_arrow_schema = convert_orso_schema_to_arrow_schema(read_schema, use_identities=True)
        arrow_schema = convert_orso_schema_to_arrow_schema(output_schema, use_identities=True)

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
        # don't (e.g. OpteryxConnector), derive it from the
        # storage protocol embedded in the file paths.
        if hasattr(self.connector, "filesystem"):
            filesystem = self.connector.filesystem
            connector_type = (
                getattr(self.connector, "storage_type", None) or self.connector.__type__
            )
        else:
            from opteryx.connectors.io_systems import create_filesystem

            first_path = blob_paths[0] if blob_paths else ""
            protocol = first_path.split("://")[0] if "://" in first_path else ""
            filesystem = create_filesystem(protocol)
            connector_type = protocol.upper() if protocol else "FILESYSTEM"
        # Column names as they appear in the Parquet file (Parquet uses the
        # original names, not identity aliases).
        column_names = [col.name for col in read_schema.columns]
        # Map data-file column name → query-engine identity for Morsel construction.
        name_to_identity = {col.name: col.identity for col in read_schema.columns}

        # One cache per execute() call: footers shared across all row groups of
        # the same file; column chunks cached for reuse across row groups with
        # identical content (rare but free).
        cache = InMemoryParquetCache()

        # Range-read decoder currently returns flattened leaf values for some
        # repeated/list columns. Also, heterogeneous multi-file datasets may not
        # contain every projected/predicate column in every file. Detect either
        # case up front and route to full-file fallback for correctness.
        has_repeated_projection = False
        has_missing_required_columns = False
        prefetched_footers: dict[str, dict] = {}

        unique_blob_paths = list(dict.fromkeys(blob_paths))
        footer_workers = max(1, int(config.PARQUET_PREFETCH_FOOTER_WORKERS))
        if unique_blob_paths:
            with ThreadPoolExecutor(
                max_workers=min(footer_workers, len(unique_blob_paths)),
                thread_name_prefix="parquet-footer-prefetch",
            ) as footer_pool:
                future_to_path = {
                    footer_pool.submit(
                        fetch_footer,
                        filesystem,
                        blob_name,
                        None,
                        file_sizes.get(blob_name),
                    ): blob_name
                    for blob_name in unique_blob_paths
                }
                for future in as_completed(future_to_path):
                    blob_name = future_to_path[future]
                    prefetched_footers[blob_name] = future.result()

        for blob_name in blob_paths:
            footer = prefetched_footers[blob_name]
            has_repeated_projection = has_repeated_projection or self._has_repeated_projection(
                footer, column_names
            )
            row_groups = footer.get("row_groups") or []
            if row_groups:
                available_columns = {col.get("name") for col in row_groups[0].get("columns", [])}
                if any(col_name not in available_columns for col_name in column_names):
                    has_missing_required_columns = True

        if has_repeated_projection or has_missing_required_columns:
            yield from self._execute_full_file_fallback(
                filesystem=filesystem,
                blob_paths=blob_paths,
                file_sizes=file_sizes,
                read_orso_schema=read_schema,
                read_arrow_schema=read_arrow_schema,
                output_arrow_schema=arrow_schema,
                output_identity_order=output_identity_order,
                predicate_root=predicate_root,
                records_to_read=records_to_read,
            )
            return

        result_morsel = None

        decode_start = time.monotonic_ns()
        total_rows_before_filter = 0
        total_rows_after_filter = 0
        try:
            for row_group in iter_row_groups(
                filesystem,
                blob_paths,
                column_names,
                cache,
                predicates=predicate_stats,
                file_sizes=file_sizes or None,
                connector=connector_type,
                query_id=getattr(self.properties, "query_id", None),
                prefetched_footers=prefetched_footers,
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
                self.readings["parquet_rowgroups_in_flight_cap"] = max(
                    self.readings.get("parquet_rowgroups_in_flight_cap", 0),
                    row_group.pop("__rowgroups_in_flight_cap__", 0),
                )
                self.readings["time_parquet_emit_wait_ns"] += row_group.pop("__emit_wait_ns__", 0)
                self.readings["parquet_emit_queue_depth_at_ready_max"] = max(
                    self.readings.get("parquet_emit_queue_depth_at_ready_max", 0),
                    row_group.pop("__emit_queue_depth_at_ready__", 0),
                )
                self.readings["time_parquet_scheduler_empty_wait_ns"] += row_group.pop(
                    "__scheduler_empty_wait_ns__", 0
                )
                self.readings["parquet_scheduler_empty_wait_events"] += row_group.pop(
                    "__scheduler_empty_wait_events__", 0
                )
                self.readings["io_ring_slot_bytes"] = max(
                    self.readings.get("io_ring_slot_bytes", 0),
                    row_group.pop("__io_ring_slot_bytes__", 0),
                )
                self.readings["io_ring_slot_count"] = max(
                    self.readings.get("io_ring_slot_count", 0),
                    row_group.pop("__io_ring_slot_count__", 0),
                )
                self.readings["io_ring_total_bytes"] = max(
                    self.readings.get("io_ring_total_bytes", 0),
                    row_group.pop("__io_ring_total_bytes__", 0),
                )
                self.readings["io_ring_producer_full_wait_ns"] += row_group.pop(
                    "__io_ring_producer_full_wait_ns__", 0
                )
                self.readings["io_ring_producer_full_wait_events"] += row_group.pop(
                    "__io_ring_producer_full_wait_events__", 0
                )
                self.readings["io_ring_consumer_empty_wait_ns"] += row_group.pop(
                    "__io_ring_consumer_empty_wait_ns__", 0
                )
                self.readings["io_ring_consumer_empty_wait_events"] += row_group.pop(
                    "__io_ring_consumer_empty_wait_events__", 0
                )
                self.readings["io_transfer_ready_backlog_peak"] = max(
                    self.readings.get("io_transfer_ready_backlog_peak", 0),
                    row_group.pop("__io_transfer_ready_backlog_peak__", 0),
                )
                self.readings["io_transfer_emit_wait_ns"] += row_group.pop(
                    "__io_transfer_emit_wait_ns__", 0
                )
                self.readings["io_transfer_fragment_count_p50"] = max(
                    self.readings.get("io_transfer_fragment_count_p50", 0),
                    row_group.pop("__io_transfer_fragment_count_p50__", 0),
                )
                self.readings["io_transfer_fragment_count_p95"] = max(
                    self.readings.get("io_transfer_fragment_count_p95", 0),
                    row_group.pop("__io_transfer_fragment_count_p95__", 0),
                )
                self.readings["io_transfer_fragment_count_max"] = max(
                    self.readings.get("io_transfer_fragment_count_max", 0),
                    row_group.pop("__io_transfer_fragment_count_max__", 0),
                )
                self.readings["io_transfer_payload_bytes_p50"] = max(
                    self.readings.get("io_transfer_payload_bytes_p50", 0),
                    row_group.pop("__io_transfer_payload_bytes_p50__", 0),
                )
                self.readings["io_transfer_payload_bytes_p95"] = max(
                    self.readings.get("io_transfer_payload_bytes_p95", 0),
                    row_group.pop("__io_transfer_payload_bytes_p95__", 0),
                )
                self.readings["io_transfer_payload_bytes_max"] = max(
                    self.readings.get("io_transfer_payload_bytes_max", 0),
                    row_group.pop("__io_transfer_payload_bytes_max__", 0),
                )
                self.readings["io_rowgroup_slice_count"] += row_group.pop(
                    "__io_rowgroup_slice_count__", 0
                )
                self.readings["io_deserialize_ns"] += row_group.pop("__io_deserialize_ns__", 0)
                self.readings["io_serialize_ns"] += row_group.pop("__io_serialize_ns__", 0)
                time_to_first_rowgroup_ns = row_group.pop("__time_to_first_rowgroup_ns__", 0)
                if time_to_first_rowgroup_ns:
                    existing = self.readings.get("time_to_first_rowgroup_ns", 0)
                    if existing == 0 or time_to_first_rowgroup_ns < existing:
                        self.readings["time_to_first_rowgroup_ns"] = time_to_first_rowgroup_ns

                # Drop any future scheduler metadata keys without breaking
                # the row payload contract expected below.
                for key in [k for k in row_group if k.startswith("__")]:
                    row_group.pop(key, None)

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
                rows_before_filter = result_morsel.num_rows
                rows_after_filter = rows_before_filter
                if predicate_root is not None:
                    result_morsel, rows_before_filter, rows_after_filter = (
                        self._apply_predicates_to_morsel(
                            result_morsel,
                            predicate_root,
                            eval_schema=read_arrow_schema,
                        )
                    )
                total_rows_before_filter += rows_before_filter
                total_rows_after_filter += rows_after_filter
                if output_identity_order:
                    result_morsel = result_morsel.select(output_identity_order)
                result_morsel = self._cast_morsel_to_schema(result_morsel, arrow_schema)

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
            self.readings["parquet_rows_before_filter"] += total_rows_before_filter
            self.readings["parquet_rows_after_filter"] += total_rows_after_filter
            if total_rows_before_filter > 0:
                self.readings["parquet_filter_selectivity"] = (
                    total_rows_after_filter / total_rows_before_filter
                )

        # ── Empty result guard ────────────────────────────────────────────────
        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in arrow_schema], schema=arrow_schema
            )
