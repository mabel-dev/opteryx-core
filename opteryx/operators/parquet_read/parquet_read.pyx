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
for local disk and GCS.

Row groups are yielded in completion order — the thread pool handles overlap
between I/O and decode across all files and row groups simultaneously.
"""

import time
from copy import deepcopy
from typing import Generator

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io import fetch_columns
from opteryx.connectors.parquet_io import iter_row_groups
from opteryx.connectors.parquet_io.pool_reader import iter_pass2_row_groups_ipc
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.utils import random_string
from opteryx.types import OrsoTypes

# Hoisted out of the per-row-group hot path. Previously these imports happened
# 3× per row group via `from ... import ...` inside the loop body.
import draken.draken_native as _draken_native_parquet
_int64_to_decimal  = _draken_native_parquet.vector_reinterpret_as_decimal
_int64_to_date32   = _draken_native_parquet.vector_reinterpret_as_date32
_int64_to_timestamp = _draken_native_parquet.vector_reinterpret_as_timestamp64

# Predicate evaluation is the bytecode VM only — no alternative paths. The
# compiler lowers the predicate AST to a typed CompiledBytecode at bind time;
# the executor iterates a C struct array with stack-based dispatch.
#
from opteryx.expression.evaluator import execute_bytecode
from opteryx.compiled.expression.compiled_expression import build_bytecode as _build_bytecode
from opteryx.compiled.expression.compiled_expression import lower as _lower_expr
from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.
from opteryx import config

_FOOTER_CACHE = ParquetFooterBytesCache()


cdef class ScanReadings:
    """Zero-overhead telemetry accumulator for the Parquet scan hot path.

    Replaces per-row-group Python dict writes (self.readings) with direct
    C-level field assignments. flush_into() transfers everything to the Python
    dict once at scan completion so external consumers see the same interface.
    """
    # ── Additive: from _extract_row_group_metadata ───────────────────────────
    cdef public int64_t parquet_row_groups_pruned
    cdef public int64_t parquet_footer_bytes
    cdef public int64_t parquet_range_request_count
    cdef public int64_t parquet_range_bytes_requested
    cdef public int64_t time_parquet_read_ranges_ns
    cdef public int64_t time_parquet_decode_columns_ns
    cdef public int64_t time_parquet_task_queue_wait_ns
    cdef public int64_t time_parquet_task_total_ns
    cdef public int64_t time_parquet_footer_fetch_ns
    cdef public int64_t time_parquet_scheduler_wait_ns
    cdef public int64_t time_parquet_rowgroup_completion_ns
    cdef public int64_t time_parquet_emit_wait_ns
    cdef public int64_t time_parquet_scheduler_empty_wait_ns
    cdef public int64_t parquet_scheduler_empty_wait_events
    cdef public int64_t io_ring_producer_full_wait_ns
    cdef public int64_t io_ring_producer_full_wait_events
    cdef public int64_t io_ring_consumer_empty_wait_ns
    cdef public int64_t io_ring_consumer_empty_wait_events
    cdef public int64_t io_transfer_emit_wait_ns
    cdef public int64_t io_rowgroup_slice_count
    cdef public int64_t io_deserialize_ns
    cdef public int64_t io_serialize_ns

    # ── Peak/max: from _extract_row_group_metadata ───────────────────────────
    cdef public int64_t parquet_rowgroup_peak_in_flight_max
    cdef public int64_t parquet_ranges_in_flight_peak
    cdef public int64_t parquet_active_files_peak
    cdef public int64_t parquet_active_rowgroups_peak
    cdef public int64_t parquet_rowgroups_in_flight_cap
    cdef public int64_t parquet_emit_queue_depth_at_ready_max
    cdef public int64_t io_ring_slot_bytes
    cdef public int64_t io_ring_slot_count
    cdef public int64_t io_ring_total_bytes
    cdef public int64_t io_transfer_ready_backlog_peak
    cdef public int64_t io_transfer_fragment_count_p50
    cdef public int64_t io_transfer_fragment_count_p95
    cdef public int64_t io_transfer_fragment_count_max
    cdef public int64_t io_transfer_payload_bytes_p50
    cdef public int64_t io_transfer_payload_bytes_p95
    cdef public int64_t io_transfer_payload_bytes_max

    # ── Special: set-once fields ─────────────────────────────────────────────
    cdef public object parquet_scan_strategy       # str, set once
    cdef public int64_t time_to_first_rowgroup_ns  # int64, keep minimum

    # ── Additive: from the scan loop ─────────────────────────────────────────
    cdef public int64_t parquet_latmat_pass1_row_groups
    cdef public int64_t parquet_latmat_skipped_row_groups
    cdef public int64_t parquet_latmat_pass2_bytes
    cdef public int64_t parquet_latmat_pass2_row_groups
    cdef public int64_t rows_seen
    cdef public int64_t row_groups_read
    cdef public int64_t files_read
    cdef public int64_t blobs_seen
    cdef public int64_t blobs_read
    cdef public int64_t rows_read
    cdef public int64_t bytes_processed
    cdef public int64_t time_decoding_blobs
    cdef public int64_t parquet_rows_before_filter
    cdef public int64_t parquet_rows_after_filter
    cdef public double  parquet_filter_selectivity
    cdef public int64_t empty_datasets

    # ── Mutation API ─────────────────────────────────────────────────────────
    # All accumulation goes through the methods below rather than direct field
    # writes. This narrows the mutation surface to one class for future thread
    # safety (best-effort under concurrent writers, per the telemetry contract).

    cpdef int64_t merge_row_group_metadata(self, dict row_group):
        """Consume __*__ telemetry keys from `row_group`. Returns __bytes_fetched__.

        bytes_fetched is reported back to the caller because it accumulates on
        BasePlanNode.bytes_in, not on ScanReadings. All other metadata fields
        are absorbed here.
        """
        cdef int64_t val
        cdef int64_t bytes_fetched = row_group.pop("__bytes_fetched__", 0)
        cdef object scan_strategy

        # Additive metrics
        self.parquet_row_groups_pruned        = row_group.pop("__row_groups_pruned__", 0)
        self.parquet_footer_bytes            += row_group.pop("__footer_bytes__", 0)
        self.parquet_range_request_count     += row_group.pop("__range_request_count__", 0)
        self.parquet_range_bytes_requested   += row_group.pop("__range_bytes_requested__", 0)
        self.time_parquet_read_ranges_ns     += row_group.pop("__time_read_ranges_ns__", 0)
        self.time_parquet_decode_columns_ns  += row_group.pop("__time_decode_columns_ns__", 0)
        self.time_parquet_task_queue_wait_ns += row_group.pop("__task_queue_wait_ns__", 0)
        self.time_parquet_task_total_ns      += row_group.pop("__task_total_ns__", 0)
        self.time_parquet_footer_fetch_ns    += row_group.pop("__footer_fetch_ns__", 0)
        self.time_parquet_scheduler_wait_ns  += row_group.pop("__scheduler_wait_ns__", 0)
        self.time_parquet_rowgroup_completion_ns += row_group.pop("__rowgroup_completion_latency_ns__", 0)
        self.time_parquet_emit_wait_ns       += row_group.pop("__emit_wait_ns__", 0)
        self.time_parquet_scheduler_empty_wait_ns += row_group.pop("__scheduler_empty_wait_ns__", 0)
        self.parquet_scheduler_empty_wait_events  += row_group.pop("__scheduler_empty_wait_events__", 0)
        self.io_ring_producer_full_wait_ns   += row_group.pop("__io_ring_producer_full_wait_ns__", 0)
        self.io_ring_producer_full_wait_events += row_group.pop("__io_ring_producer_full_wait_events__", 0)
        self.io_ring_consumer_empty_wait_ns  += row_group.pop("__io_ring_consumer_empty_wait_ns__", 0)
        self.io_ring_consumer_empty_wait_events += row_group.pop("__io_ring_consumer_empty_wait_events__", 0)
        self.io_transfer_emit_wait_ns        += row_group.pop("__io_transfer_emit_wait_ns__", 0)
        self.io_rowgroup_slice_count         += row_group.pop("__io_rowgroup_slice_count__", 0)
        self.io_deserialize_ns               += row_group.pop("__io_deserialize_ns__", 0)
        self.io_serialize_ns                 += row_group.pop("__io_serialize_ns__", 0)

        # Peak/max metrics
        val = row_group.pop("__rowgroup_peak_in_flight__", 0)
        if val > self.parquet_rowgroup_peak_in_flight_max:
            self.parquet_rowgroup_peak_in_flight_max = val
        val = row_group.pop("__ranges_in_flight_peak__", 0)
        if val > self.parquet_ranges_in_flight_peak:
            self.parquet_ranges_in_flight_peak = val
        val = row_group.pop("__active_files_peak__", 0)
        if val > self.parquet_active_files_peak:
            self.parquet_active_files_peak = val
        val = row_group.pop("__active_rowgroups_peak__", 0)
        if val > self.parquet_active_rowgroups_peak:
            self.parquet_active_rowgroups_peak = val
        val = row_group.pop("__rowgroups_in_flight_cap__", 0)
        if val > self.parquet_rowgroups_in_flight_cap:
            self.parquet_rowgroups_in_flight_cap = val
        val = row_group.pop("__emit_queue_depth_at_ready__", 0)
        if val > self.parquet_emit_queue_depth_at_ready_max:
            self.parquet_emit_queue_depth_at_ready_max = val
        val = row_group.pop("__io_ring_slot_bytes__", 0)
        if val > self.io_ring_slot_bytes:
            self.io_ring_slot_bytes = val
        val = row_group.pop("__io_ring_slot_count__", 0)
        if val > self.io_ring_slot_count:
            self.io_ring_slot_count = val
        val = row_group.pop("__io_ring_total_bytes__", 0)
        if val > self.io_ring_total_bytes:
            self.io_ring_total_bytes = val
        val = row_group.pop("__io_transfer_ready_backlog_peak__", 0)
        if val > self.io_transfer_ready_backlog_peak:
            self.io_transfer_ready_backlog_peak = val
        val = row_group.pop("__io_transfer_fragment_count_p50__", 0)
        if val > self.io_transfer_fragment_count_p50:
            self.io_transfer_fragment_count_p50 = val
        val = row_group.pop("__io_transfer_fragment_count_p95__", 0)
        if val > self.io_transfer_fragment_count_p95:
            self.io_transfer_fragment_count_p95 = val
        val = row_group.pop("__io_transfer_fragment_count_max__", 0)
        if val > self.io_transfer_fragment_count_max:
            self.io_transfer_fragment_count_max = val
        val = row_group.pop("__io_transfer_payload_bytes_p50__", 0)
        if val > self.io_transfer_payload_bytes_p50:
            self.io_transfer_payload_bytes_p50 = val
        val = row_group.pop("__io_transfer_payload_bytes_p95__", 0)
        if val > self.io_transfer_payload_bytes_p95:
            self.io_transfer_payload_bytes_p95 = val
        val = row_group.pop("__io_transfer_payload_bytes_max__", 0)
        if val > self.io_transfer_payload_bytes_max:
            self.io_transfer_payload_bytes_max = val

        # Scan strategy: set once
        scan_strategy = row_group.pop("__parquet_scan_strategy__", None)
        if scan_strategy and self.parquet_scan_strategy is None:
            self.parquet_scan_strategy = scan_strategy

        # Time to first row group: keep minimum non-zero
        val = row_group.pop("__time_to_first_rowgroup_ns__", 0)
        if val and (self.time_to_first_rowgroup_ns == 0 or val < self.time_to_first_rowgroup_ns):
            self.time_to_first_rowgroup_ns = val

        # Drain any remaining __*__ keys so downstream sees only data columns.
        for key in list(row_group):
            if key.startswith("__"):
                row_group.pop(key, None)

        return bytes_fetched

    cpdef void record_pass1_evaluated(self):
        self.parquet_latmat_pass1_row_groups += 1

    cpdef void record_pass1_skipped(self):
        self.parquet_latmat_skipped_row_groups += 1
        self.row_groups_read += 1

    cpdef void record_pass2_decoded(self, int64_t bytes_fetched):
        self.parquet_latmat_pass2_bytes += bytes_fetched
        self.parquet_latmat_pass2_row_groups += 1

    cpdef void record_row_group_complete(self, int64_t rows_in_morsel):
        self.rows_seen += rows_in_morsel
        self.row_groups_read += 1

    cpdef void record_morsel_yielded(
        self,
        int64_t num_rows,
        int64_t num_bytes,
        int64_t files_seen_count,
    ):
        self.blobs_read = files_seen_count
        self.rows_read += num_rows
        self.bytes_processed += num_bytes

    cpdef void record_decode_time(self, int64_t ns):
        self.time_decoding_blobs += ns

    cpdef void record_filter_totals(self, int64_t rows_before, int64_t rows_after):
        self.parquet_rows_before_filter += rows_before
        self.parquet_rows_after_filter += rows_after
        if self.parquet_rows_before_filter > 0:
            self.parquet_filter_selectivity = (
                <double>self.parquet_rows_after_filter / <double>self.parquet_rows_before_filter
            )

    cpdef void flush_into(self, object readings):
        readings["parquet_row_groups_pruned"]          = self.parquet_row_groups_pruned
        readings["parquet_footer_bytes"]               = self.parquet_footer_bytes
        readings["parquet_range_request_count"]        = self.parquet_range_request_count
        readings["parquet_range_bytes_requested"]      = self.parquet_range_bytes_requested
        readings["time_parquet_read_ranges_ns"]        = self.time_parquet_read_ranges_ns
        readings["time_parquet_decode_columns_ns"]     = self.time_parquet_decode_columns_ns
        readings["time_parquet_task_queue_wait_ns"]    = self.time_parquet_task_queue_wait_ns
        readings["time_parquet_task_total_ns"]         = self.time_parquet_task_total_ns
        readings["time_parquet_footer_fetch_ns"]       = self.time_parquet_footer_fetch_ns
        readings["time_parquet_scheduler_wait_ns"]     = self.time_parquet_scheduler_wait_ns
        readings["time_parquet_rowgroup_completion_ns"]= self.time_parquet_rowgroup_completion_ns
        readings["time_parquet_emit_wait_ns"]          = self.time_parquet_emit_wait_ns
        readings["time_parquet_scheduler_empty_wait_ns"] = self.time_parquet_scheduler_empty_wait_ns
        readings["parquet_scheduler_empty_wait_events"]= self.parquet_scheduler_empty_wait_events
        readings["io_ring_producer_full_wait_ns"]      = self.io_ring_producer_full_wait_ns
        readings["io_ring_producer_full_wait_events"]  = self.io_ring_producer_full_wait_events
        readings["io_ring_consumer_empty_wait_ns"]     = self.io_ring_consumer_empty_wait_ns
        readings["io_ring_consumer_empty_wait_events"] = self.io_ring_consumer_empty_wait_events
        readings["io_transfer_emit_wait_ns"]           = self.io_transfer_emit_wait_ns
        readings["io_rowgroup_slice_count"]            = self.io_rowgroup_slice_count
        readings["io_deserialize_ns"]                  = self.io_deserialize_ns
        readings["io_serialize_ns"]                    = self.io_serialize_ns
        readings["parquet_rowgroup_peak_in_flight_max"]= self.parquet_rowgroup_peak_in_flight_max
        readings["parquet_ranges_in_flight_peak"]      = self.parquet_ranges_in_flight_peak
        readings["parquet_active_files_peak"]          = self.parquet_active_files_peak
        readings["parquet_active_rowgroups_peak"]      = self.parquet_active_rowgroups_peak
        readings["parquet_rowgroups_in_flight_cap"]    = self.parquet_rowgroups_in_flight_cap
        readings["parquet_emit_queue_depth_at_ready_max"] = self.parquet_emit_queue_depth_at_ready_max
        readings["io_ring_slot_bytes"]                 = self.io_ring_slot_bytes
        readings["io_ring_slot_count"]                 = self.io_ring_slot_count
        readings["io_ring_total_bytes"]                = self.io_ring_total_bytes
        readings["io_transfer_ready_backlog_peak"]     = self.io_transfer_ready_backlog_peak
        readings["io_transfer_fragment_count_p50"]     = self.io_transfer_fragment_count_p50
        readings["io_transfer_fragment_count_p95"]     = self.io_transfer_fragment_count_p95
        readings["io_transfer_fragment_count_max"]     = self.io_transfer_fragment_count_max
        readings["io_transfer_payload_bytes_p50"]      = self.io_transfer_payload_bytes_p50
        readings["io_transfer_payload_bytes_p95"]      = self.io_transfer_payload_bytes_p95
        readings["io_transfer_payload_bytes_max"]      = self.io_transfer_payload_bytes_max
        if self.parquet_scan_strategy is not None:
            readings["parquet_scan_strategy"]          = self.parquet_scan_strategy
        if self.time_to_first_rowgroup_ns:
            readings["time_to_first_rowgroup_ns"]      = self.time_to_first_rowgroup_ns
        readings["parquet_latmat_pass1_row_groups"]    = self.parquet_latmat_pass1_row_groups
        readings["parquet_latmat_skipped_row_groups"]  = self.parquet_latmat_skipped_row_groups
        readings["parquet_latmat_pass2_bytes"]         = self.parquet_latmat_pass2_bytes
        readings["parquet_latmat_pass2_row_groups"]    = self.parquet_latmat_pass2_row_groups
        readings["rows_seen"]                          = self.rows_seen
        readings["row_groups_read"]                    = self.row_groups_read
        readings["files_read"]                         = self.files_read
        readings["blobs_seen"]                         = self.blobs_seen
        readings["blobs_read"]                         = self.blobs_read
        readings["rows_read"]                          = self.rows_read
        readings["bytes_processed"]                    = self.bytes_processed
        readings["time_decoding_blobs"]                = self.time_decoding_blobs
        readings["parquet_rows_before_filter"]         = self.parquet_rows_before_filter
        readings["parquet_rows_after_filter"]          = self.parquet_rows_after_filter
        if self.parquet_rows_before_filter > 0:
            readings["parquet_filter_selectivity"]     = self.parquet_filter_selectivity
        readings["empty_datasets"]                     = self.empty_datasets


cdef inline void _coerce_logical_types(
    dict row_group,
    dict decimal_col_map,
    set date_col_set,
    set timestamp_col_set,
):
    """Coerce Integer64Vector physical columns to their logical types (DATE/TIMESTAMP/DECIMAL).

    The C++ parquet pipeline serialises DATE/TIMESTAMP/DECIMAL as TAG_INT64 (the
    physical type) and the IPC format carries no logical type info, so we apply
    the schema-driven coercion here. Hot-path helper: called once per row group.
    """
    cdef str col_name
    cdef int precision
    cdef int scale

    if decimal_col_map:
        for col_name, dec in decimal_col_map.items():
            v = row_group.get(col_name)
            if v is not None and getattr(v, "type", None) == _draken_native_parquet.INT64:
                precision = dec[0]
                scale = dec[1]
                row_group[col_name] = _int64_to_decimal(v, precision, scale)
    if date_col_set:
        for col_name in date_col_set:
            v = row_group.get(col_name)
            if v is not None and getattr(v, "type", None) == _draken_native_parquet.INT64:
                row_group[col_name] = _int64_to_date32(v)
    if timestamp_col_set:
        for col_name in timestamp_col_set:
            v = row_group.get(col_name)
            if v is not None and getattr(v, "type", None) == _draken_native_parquet.INT64:
                row_group[col_name] = _int64_to_timestamp(v)


cdef class _Pass1Result:
    """Outcome of evaluating one row group's pass-1 filter columns.

    Pure value object. Produced by `_evaluate_pass1_row_group()` (no side effects);
    consumed by `ParquetReadNode._record_pass1_*` (all mutation lives there). This
    split is what would let pass-1 evaluation run on a worker thread in future:
    the worker produces results, the caller serially funnels them into shared
    state.
    """
    cdef public object path           # str
    cdef public object rg_idx         # int
    cdef public int64_t rows_before_filter
    cdef public bint survived          # True iff at least one row passes the predicate
    cdef public bint empty             # True iff row_group had no recognised columns
    # Populated only when survived == True:
    cdef public object p1_filtered     # Morsel filtered by the mask
    cdef public list   p1_identity_names
    cdef public object mask_bytes      # bytes — serialised survival mask for pass-2


cdef _Pass1Result _evaluate_pass1_row_group(
    object path,
    object rg_idx,
    dict row_group,
    CompiledBytecode compiled_predicate,
    dict pass1_name_to_identity,
    dict decimal_col_map,
    set date_col_set,
    set timestamp_col_set,
):
    """Pure pass-1 evaluation for a single row group.

    Reads only from its arguments; performs no mutation on `self`, on shared
    work-lists, or on telemetry. The caller threads the outcome into shared
    state via `_record_pass1_survivor` / `_record_pass1_skip`.

    Predicate evaluation goes through the bytecode VM — the sole evaluation
    engine. Function calls inside predicates compile to BC_FUNCTION opcodes,
    so there is no separate "append function columns first" preamble.
    """
    cdef _Pass1Result result = _Pass1Result()
    result.path = path
    result.rg_idx = rg_idx
    result.survived = False
    result.empty = False
    result.rows_before_filter = 0

    _coerce_logical_types(row_group, decimal_col_map, date_col_set, timestamp_col_set)

    cdef list p1_identity_names = [pass1_name_to_identity[col] for col in row_group]
    cdef list p1_vectors = list(row_group.values())
    if not p1_identity_names:
        result.empty = True
        return result

    p1_morsel = Morsel.from_vectors(p1_identity_names, p1_vectors)
    result.rows_before_filter = p1_morsel.num_rows

    mask = execute_bytecode(compiled_predicate, p1_morsel)

    if not mask.any():
        return result

    result.survived = True
    result.p1_filtered = p1_morsel.filter_mask(mask)
    result.p1_identity_names = p1_identity_names
    result.mask_bytes = bytes(mask.to_byte_array())
    return result


cdef class ParquetReadNode(ReaderNode):
    """Read node backed by column-chunk range reads via ``parquet_io``.

    Activated for filesystem-backed connectors (GCS, S3, local) when the
    manifest contains only ``.parquet`` files.  Non-parquet external scans are
    rejected by the planner.
    """

    cdef public set _parquet_files_seen
    cdef CompiledBytecode _compiled_predicate
    cdef public object _planner_name_to_identity_cached
    cdef public object _filter_column_names_cached
    cdef public ScanReadings scan_readings

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.predicates = parameters.get("predicates")
        self._parquet_files_seen = set()
        self._compiled_predicate = None  # CompiledBytecode, bound once at execute() time
        self._planner_name_to_identity_cached = None  # Cache name-to-identity mapping
        self._filter_column_names_cached = None  # Cache filter column names extraction
        self.scan_readings = ScanReadings()

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
        if base["row_groups_read"] > 0:
            base["parquet_avg_emit_wait_ns"] = (
                self.readings.get("time_parquet_emit_wait_ns", 0) / base["row_groups_read"]
            )
        if self.readings.get("parquet_scan_strategy"):
            base["parquet_scan_strategy"] = self.readings["parquet_scan_strategy"]
        lm_pass1 = self.readings.get("parquet_latmat_pass1_row_groups", 0)
        if lm_pass1 > 0:
            base["parquet_latmat_pass1_row_groups"] = lm_pass1
            base["parquet_latmat_pass2_row_groups"] = self.readings.get(
                "parquet_latmat_pass2_row_groups", 0
            )
            base["parquet_latmat_skipped_row_groups"] = self.readings.get(
                "parquet_latmat_skipped_row_groups", 0
            )
            base["parquet_latmat_abandoned_files"] = self.readings.get(
                "parquet_latmat_abandoned_files", 0
            )
            base["parquet_latmat_pass2_bytes"] = self.readings.get("parquet_latmat_pass2_bytes", 0)
            base["parquet_latmat_skipped_pages"] = self.readings.get(
                "parquet_latmat_skipped_pages", 0
            )
            base["parquet_latmat_decoded_pages"] = self.readings.get(
                "parquet_latmat_decoded_pages", 0
            )
            lm_total_pages = (
                base["parquet_latmat_skipped_pages"] + base["parquet_latmat_decoded_pages"]
            )
            if lm_total_pages > 0:
                base["parquet_latmat_page_skip_ratio"] = (
                    base["parquet_latmat_skipped_pages"] / lm_total_pages
                )
            base["parquet_latmat_skip_ratio"] = (
                self.readings.get("parquet_latmat_skipped_row_groups", 0) / lm_pass1
            )
        return base

    @staticmethod
    def _extract_filter_column_names(predicates) -> set:
        """Extract the physical column names referenced in pushed-down predicates."""
        if not predicates:
            return set()
        names = set()
        for predicate in predicates:
            identifiers = get_all_nodes_of_type(predicate, select_nodes=(NodeType.IDENTIFIER,))
            for identifier in identifiers:
                schema_column = getattr(identifier, "schema_column", None)
                name = getattr(schema_column, "name", None) or getattr(identifier, "source_column", None)
                if name:
                    names.add(name)
        return names

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

    def _apply_predicates_to_morsel(self, morsel: Morsel):
        """Apply the compiled predicate to a Draken Morsel.

        All evaluation goes through the bytecode VM — the sole evaluation
        engine. The predicate is compiled once at execute() time; this method
        just executes it.
        """
        if self._compiled_predicate is None:
            return morsel, morsel.num_rows, morsel.num_rows

        rows_before_filter = morsel.num_rows
        mask = execute_bytecode(self._compiled_predicate, morsel)
        filtered = morsel.filter_mask(mask)
        if filtered.num_rows == 0:
            return morsel.slice(0, 0), rows_before_filter, 0
        return filtered, rows_before_filter, filtered.num_rows

    cdef bint _mark_file_seen(self, object path):
        """Record `path` as newly seen. Returns True iff this is the first sighting.

        Encapsulates the check-then-add on `_parquet_files_seen` and the derived
        `files_read`/`blobs_seen` accounting. A single named site is what a
        future lock would protect; today it just collapses three identical
        copies of the same idiom into one.
        """
        if path in self._parquet_files_seen:
            return False
        self._parquet_files_seen.add(path)
        self.scan_readings.files_read = len(self._parquet_files_seen)
        self.scan_readings.blobs_seen += 1
        return True

    cdef void _record_morsel_emitted(self, object morsel):
        """Apply per-emit accounting to both scan_readings and the telemetry mirror.

        Keeps the dual-write pattern in one place so callers don't repeat the
        six-line ScanReadings + telemetry update before every `yield`.
        """
        cdef int64_t num_rows = morsel.num_rows
        cdef int64_t num_bytes = morsel.nbytes
        cdef int64_t files_seen = len(self._parquet_files_seen)
        self.scan_readings.record_morsel_yielded(num_rows, num_bytes, files_seen)
        self.telemetry.blobs_read = files_seen
        self.telemetry.rows_read += num_rows
        self.telemetry.bytes_processed += num_bytes

    cdef void _record_pass1_survivor(self, _Pass1Result r, list pass2_work, dict p1_cache):
        """Funnel a surviving pass-1 row group into shared work-state.

        All mutation of `pass2_work`, `p1_cache`, and pass-1 telemetry happens
        here so the evaluator function can stay pure.
        """
        self.scan_readings.record_pass1_evaluated()
        p1_cache[(r.path, r.rg_idx)] = (r.p1_filtered, r.p1_identity_names)
        pass2_work.append((r.path, r.rg_idx, r.mask_bytes))

    cdef void _record_pass1_skip(self, _Pass1Result r):
        """Funnel a pruned (mask-all-false) pass-1 row group into shared state."""
        self.scan_readings.record_pass1_evaluated()
        self.scan_readings.record_pass1_skipped()
        self._mark_file_seen(r.path)

    cdef tuple _extract_row_group_metadata(self, dict row_group):
        """Pop (path, rg_idx) and funnel the __*__ telemetry into ScanReadings.

        bytes_fetched lives on BasePlanNode.bytes_in (not ScanReadings) so we
        receive it back from the merge call and apply it here.
        Returns (path, rg_idx); all __*__ metadata keys are consumed.
        """
        cdef object path = row_group.pop("__path__")
        cdef object rg_idx = row_group.pop("__row_group__")
        self.bytes_in += self.scan_readings.merge_row_group_metadata(row_group)
        return path, rg_idx

    def read_morsels(self):
        """Source-side morsel iterator driven by the push pipeline engine."""
        base_schema = self.parameters["schema"]

        # Build name → planner identity map once and cache. Avoids repeated dict
        # comprehensions that would recompute this for every execute() call.
        if self._planner_name_to_identity_cached is None:
            self._planner_name_to_identity_cached = {
                col.schema_column.name: col.schema_column.identity
                for col in (self.columns or [])
            }
        _planner_name_to_identity = self._planner_name_to_identity_cached

        # Cache filter column names extraction (called once, reused at line 559).
        if self._filter_column_names_cached is None:
            self._filter_column_names_cached = self._extract_filter_column_names(self.predicates)
        filter_column_names = self._filter_column_names_cached
        required_names = set(_planner_name_to_identity.keys()) | filter_column_names

        # Select physical columns to read by NAME, not by identity.
        read_schema = deepcopy(base_schema)
        read_schema.columns = [c for c in base_schema.columns if c.name in required_names]
        if not read_schema.columns and base_schema.columns:
            # Zero-projection/no-filter scans still need one physical column for row counts.
            read_schema.columns = [base_schema.columns[0]]

        # output_identity_order: planner identities in self.columns order.
        output_identity_order = [
            _planner_name_to_identity[col.schema_column.name]
            for col in (self.columns or [])
            if col.schema_column.name in _planner_name_to_identity
        ]

        # Build DECIMAL column map: col_name → (precision, scale) for any DECIMAL column
        # in the schema.  Used to coerce Integer64Vector → DecimalVector after IPC deserialization,
        # since the C++ pipeline serializes DECIMAL as TAG_INT64 (the physical type) and
        # the IPC format carries no logical type info.
        from opteryx.types import OrsoTypes as _OrsoTypes
        _decimal_col_map = {
            col.name: (col.precision or 18, col.scale or 0)
            for col in base_schema.columns
            if col.type == _OrsoTypes.DECIMAL
        }
        _date_col_set = {
            col.name for col in base_schema.columns if col.type == _OrsoTypes.DATE
        }
        _timestamp_col_set = {
            col.name for col in base_schema.columns if col.type == _OrsoTypes.TIMESTAMP
        }
        predicate_root = self._compose_predicates(self.predicates or [])

        # Compile the predicate to bytecode once per execute() call. Every row
        # group then iterates a typed C struct array — no Python AST walking,
        # no per-row-group dispatch decisions.
        if self._compiled_predicate is None and predicate_root is not None:
            self._compiled_predicate = _build_bytecode(_lower_expr(predicate_root))

        # ── Two# Two-pass late materialization column split ────────────────────────
        # Use physical column names throughout — base_schema and the parquet file
        # share the same names; identities are assigned afterpass.
        # pass1_colun_names: filter columns only — fetched for every row group.
        # pass2_column_names: projection-only columns — fetched only for row groups
        # that have at least one row surviving the Pass 1 predicate evaluation.
        # Two-pass is skipped when predicates are absent, there are no projection-
        # only columns (e.g. SELECT url WHERE url LIKE …), or the feature is off.
        # Use physical column names throughout to avoid identity-space mismatch
        # between self.columns (planner) and base_schema.columns (schema loader).
        _filter_names = filter_column_names  # Use cached value instead of extracting again
        _projected_names = {col.schema_column.name for col in (self.columns or [])}
        _pass2_names = _projected_names - _filter_names
        two_pass_eligible = (
            config.features.parquet_late_materialization
            and bool(predicate_root)
            and bool(_filter_names)
            and bool(_pass2_names)
        )
        pass1_column_names: list = []
        pass2_column_names: list = []
        pass1_name_to_identity: dict = {}
        pass2_name_to_identity: dict = {}
        if two_pass_eligible:
            _p1_cols = [c for c in base_schema.columns if c.name in _filter_names]
            _p2_cols = [c for c in base_schema.columns if c.name in _pass2_names]
            pass1_column_names = [c.name for c in _p1_cols]
            pass2_column_names = [c.name for c in _p2_cols]
            pass1_name_to_identity = {name: _planner_name_to_identity.get(name, name.encode("utf-8")) for name in pass1_column_names}
            pass2_name_to_identity = {name: _planner_name_to_identity.get(name, name.encode("utf-8")) for name in pass2_column_names}

        # ── Empty manifest ────────────────────────────────────────────────────
        if not self.manifest or self.manifest.get_file_count() == 0:
            # Yield empty Morsel with the correct column names
            empty_morsel = Morsel()
            yield empty_morsel
            return

        self.readings["columns_read"] += len(read_schema.columns)
        self.readings["parquet_filter_columns_read"] += len(filter_column_names)
        self.readings["parquet_projection_columns_read"] += len(_planner_name_to_identity)

        # Phase 1 predicate pushdown: extract (col, op, value) triples from pushed-down
        # predicates so the reader can prune row groups using footer min/max stats.
        predicate_stats = extract_predicate_stats(self.predicates or [])

        records_to_read = self.limit if self.limit is not None else float("inf")

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
        filesystem = getattr(self.connector, "filesystem", None)
        if filesystem is not None:
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
        name_to_identity = {
            col.name: _planner_name_to_identity.get(col.name, col.name.encode("utf-8"))
            for col in read_schema.columns
        }

        result_morsel = None

        # Hoist properties.query_id out of the per-row-group loop; was previously
        # called via getattr() three times per iteration in iter_row_groups kwargs.
        query_id = getattr(self.properties, "query_id", None)

        decode_start = time.monotonic_ns()
        total_rows_before_filter = 0
        total_rows_after_filter = 0
        try:
            if two_pass_eligible:
                # ── Phase 1: stream pass-1 row groups, evaluate predicate, collect survivors ──
                pass2_work = []   # list of (path, rg_idx, mask_bytes)
                p1_cache = {}     # (path, rg_idx) -> p1_filtered Morsel

                for row_group in iter_row_groups(
                    filesystem,
                    blob_paths,
                    pass1_column_names,
                    decode_workers=config.PARQUET_GCS_IO_WORKERS if connector_type in ("GCS", "GS") else config.PARQUET_LOCAL_IO_WORKERS,
                    predicates=predicate_stats,
                    file_sizes=file_sizes or None,
                    connector=connector_type,
                    query_id=query_id,
                    footer_bytes_cache=_FOOTER_CACHE,
                ):
                    path, rg_idx = self._extract_row_group_metadata(row_group)

                    result = _evaluate_pass1_row_group(
                        path,
                        rg_idx,
                        row_group,
                        self._compiled_predicate,
                        pass1_name_to_identity,
                        _decimal_col_map,
                        _date_col_set,
                        _timestamp_col_set,
                    )
                    if result.empty:
                        continue

                    total_rows_before_filter += result.rows_before_filter

                    if result.survived:
                        self._record_pass1_survivor(result, pass2_work, p1_cache)
                    else:
                        self._record_pass1_skip(result)

                # ── Phase 2: parallel pass-2 decode via C++ pipeline ──────────────────────
                for row_group in iter_pass2_row_groups_ipc(
                    filesystem,
                    pass2_work,
                    pass2_column_names,
                    file_sizes=file_sizes or None,
                    connector=connector_type,
                    query_id=query_id,
                    footer_bytes_cache=_FOOTER_CACHE,
                ):
                    path = row_group.get('__path__')
                    rg_idx = row_group.get('__row_group__')

                    p2_bytes = row_group.pop('__bytes_fetched__', 0)
                    self.scan_readings.record_pass2_decoded(p2_bytes)
                    self.bytes_in += p2_bytes
                    # Clean up metadata keys.
                    for _k in [k for k in row_group if k.startswith("__")]:
                        row_group.pop(_k)

                    # Coerce DATE/TIMESTAMP/DECIMAL in pass-2 projection columns.
                    _coerce_logical_types(row_group, _decimal_col_map, _date_col_set, _timestamp_col_set)

                    p1_filtered, p1_identity_names = p1_cache.pop((path, rg_idx))

                    p1_vectors_by_identity = {n: p1_filtered.column(n) for n in p1_identity_names}
                    p2_vectors_by_identity = {
                        pass2_name_to_identity[col]: vec
                        for col, vec in row_group.items()
                    }

                    if output_identity_order:
                        combined_identity_names = []
                        combined_vectors = []
                        for identity in output_identity_order:
                            if identity in p1_vectors_by_identity:
                                combined_identity_names.append(identity)
                                combined_vectors.append(p1_vectors_by_identity[identity])
                            elif identity in p2_vectors_by_identity:
                                combined_identity_names.append(identity)
                                combined_vectors.append(p2_vectors_by_identity[identity])
                    else:
                        combined_identity_names = list(p1_identity_names)
                        combined_identity_names.extend(p2_vectors_by_identity.keys())
                        combined_vectors = list(p1_vectors_by_identity.values())
                        combined_vectors.extend(p2_vectors_by_identity.values())

                    result_morsel = Morsel.from_vectors(combined_identity_names, combined_vectors)
                    rows_after_filter = result_morsel.num_rows
                    total_rows_after_filter += rows_after_filter

                    self.scan_readings.record_row_group_complete(rows_after_filter)
                    self._mark_file_seen(path)

                    # Already assembled in output_identity_order — no select() needed.
                    num_rows = result_morsel.num_rows
                    if records_to_read < num_rows:
                        result_morsel = result_morsel.slice(0, int(records_to_read))
                        records_to_read = 0
                    else:
                        records_to_read -= num_rows

                    self._record_morsel_emitted(result_morsel)
                    yield result_morsel

                    if records_to_read <= 0:
                        break

            else:
                # ── Single-pass path: existing behaviour ─────────────────────────────────
                for row_group in iter_row_groups(
                    filesystem,
                    blob_paths,
                    column_names,
                    decode_workers=config.PARQUET_GCS_IO_WORKERS if connector_type in ("GCS", "GS") else config.PARQUET_LOCAL_IO_WORKERS,
                    predicates=predicate_stats,
                    file_sizes=file_sizes or None,
                    connector=connector_type,
                    query_id=query_id,
                    footer_bytes_cache=_FOOTER_CACHE,
                ):
                    path, rg_idx = self._extract_row_group_metadata(row_group)

                    _coerce_logical_types(row_group, _decimal_col_map, _date_col_set, _timestamp_col_set)

                    identity_names = [name_to_identity[col] for col in row_group]
                    vectors = list(row_group.values())
                    if not identity_names:
                        continue
                    result_morsel = Morsel.from_vectors(identity_names, vectors)
                    rows_before_filter = result_morsel.num_rows
                    rows_after_filter = rows_before_filter
                    if self._compiled_predicate is not None:
                        result_morsel, rows_before_filter, rows_after_filter = (
                            self._apply_predicates_to_morsel(result_morsel)
                        )
                    total_rows_before_filter += rows_before_filter
                    total_rows_after_filter += rows_after_filter

                    if output_identity_order:
                        result_morsel = result_morsel.select(output_identity_order)
                    else:
                        # No output columns (e.g. COUNT(*) with a filter-only read).
                        surviving_rows = result_morsel.num_rows
                        if surviving_rows == 0:
                            continue
                        result_morsel = Morsel.from_vectors(
                            [b'*'],
                            [_draken_native_parquet.vector_from_bool_constant(True, <uint32_t>surviving_rows)],
                        )

                    num_rows = result_morsel.num_rows
                    self.scan_readings.record_row_group_complete(num_rows)
                    self._mark_file_seen(path)

                    if records_to_read < num_rows:
                        result_morsel = result_morsel.slice(0, int(records_to_read))
                        records_to_read = 0
                    else:
                        records_to_read -= num_rows

                    self._record_morsel_emitted(result_morsel)
                    yield result_morsel

                    if records_to_read <= 0:
                        break

        finally:
            decode_ns = time.monotonic_ns() - decode_start
            self.scan_readings.record_decode_time(decode_ns)
            self.telemetry.time_decoding_blobs += decode_ns
            self.scan_readings.record_filter_totals(
                total_rows_before_filter,
                total_rows_after_filter,
            )
            self.scan_readings.flush_into(self.readings)

        # ── Empty result guard ────────────────────────────────────────────────
        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            # Yield empty Morsel without Arrow intermediate
            yield Morsel()
