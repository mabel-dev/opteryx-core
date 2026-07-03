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
from opteryx.connectors.parquet_io.pool_reader import open_ipc_source
from opteryx.connectors.parquet_io.pool_reader import open_pass2_source
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.utils import random_string
from opteryx.types.logical_type import LogicalCategory

# Hoisted out of the per-row-group hot path. Previously these imports happened
# 3× per row group via `from ... import ...` inside the loop body.
import draken.draken_native as _draken_native_parquet
_int64_to_decimal  = _draken_native_parquet.vector_reinterpret_as_decimal
_int64_to_date32   = _draken_native_parquet.vector_reinterpret_as_date32
# Zero-copy retag (moves the just-decoded int64 buffer into a TIMESTAMP64 view).
# Safe here because the decoded column is exclusively owned by the reader and is
# immediately replaced by the retagged result — no other reference survives.
_int64_to_timestamp = _draken_native_parquet.vector_retag_int64_as_timestamp64

# TimestampUnit enum name -> the unit string vector_reinterpret_as_timestamp64
# expects. Used to retag int64-stored timestamp columns with the schema's unit
# (previously this path forced "us"; a stale-unit latent bug).
_TS_UNIT_BY_NAME = {
    "SECONDS": "s",
    "MILLISECONDS": "ms",
    "MICROSECONDS": "us",
    "NANOSECONDS": "ns",
}

# Producer-side typed-sequence dispatcher for the schema-evolution NULL fill.
from draken.interop.vector_sequence import vector_from_sequence as _vector_from_sequence_typed
from draken.core.buffers cimport DrakenType, DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY


def _null_filler_for(column_type):
    """Return a callable ``n -> Vector`` that builds an all-NULL column of
    ``column_type``'s physical type and length ``n``.

    Used to fill a projected column a given file lacks (schema evolution): the
    union schema declares the type, the file does not carry it. The typed fill
    (vs an untyped NULL-type column) lets the result concatenate cleanly with the
    same column from files that DO carry it when small morsels are combined.
    Length-parametric so one filler serves every row group."""
    if column_type is None:
        return lambda n: _draken_native_parquet.vector_null_from_length(n)
    name = column_type.physical.name
    if name == "DECIMAL128" or name == "DECIMAL":
        prec = 18
        scale = 6
        if column_type.logical is not None:
            prec = column_type.logical.precision
            scale = column_type.logical.scale
        if name == "DECIMAL128":
            return lambda n: _draken_native_parquet.vector_decimal128_from_sequence([None] * n, prec, scale)
        return lambda n: _draken_native_parquet.vector_decimal_from_sequence([None] * n, prec, scale)
    if name == "TIME64":
        return lambda n: _draken_native_parquet.vector_time64_from_sequence([None] * n)
    # The producer-side dispatcher covers ARRAY / BOOL / DATE32 / floats / ints /
    # INTERVAL / NVARCHAR / TIME32 / TIMESTAMP64 / VARBINARY / VARCHAR directly.
    return lambda n: _vector_from_sequence_typed([None] * n, dtype=name)


def _string_type_for(column_type):
    """Return the declared DrakenType tag (VARCHAR/NVARCHAR/VARBINARY) for a
    string-family column, so the scan wraps/deserializes it as the schema
    declares rather than always defaulting to VARCHAR — all three share the
    exact same DrakenStringSlot/arena byte layout, so this only changes the
    type tag, never how bytes are read. Non-string / untyped columns get
    VARCHAR back (ignored, since only string tags 6/7 consult this value)."""
    if column_type is None:
        return DRAKEN_VARCHAR
    name = column_type.physical.name
    if name == "NVARCHAR":
        return DRAKEN_NVARCHAR
    if name == "VARBINARY":
        return DRAKEN_VARBINARY
    return DRAKEN_VARCHAR

# Predicate evaluation is the bytecode VM only — no alternative paths. The
# compiler lowers the predicate AST to a typed CompiledBytecode at bind time;
# the executor iterates a C struct array with stack-based dispatch.
#
from opteryx.expression.evaluator import execute_bytecode
from opteryx.expression.evaluator import predicate_filter_and_mask_c_native as _predicate_filter_and_mask_c_native
from opteryx.compiled.expression.compiled_expression import build_bytecode as _build_bytecode
from opteryx.compiled.expression.compiled_expression import lower as _lower_expr
from opteryx.compiled.expression.compiled_expression cimport CompiledBytecode
from draken.vectors.vector cimport Vector as _DrakenShimVector

# Native mutex for thread-safe concurrent pull (M4 / no-GIL target). Guards the
# per-scan accounting committed in _single_pass_next. Held only over the
# GIL-release-free commit (counters + _commit_morsel); morsel assembly and the
# decode pull happen outside it, so N workers assemble in parallel.
cdef extern from "<mutex>" namespace "std" nogil:
    cppclass cpp_mutex "std::mutex":
        cpp_mutex()
        void lock()
        void unlock()

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

    cpdef int64_t merge_row_group_metadata(self, object scan_rg):
        """Consume telemetry from typed ScanRowGroup. Returns __bytes_fetched__.

        bytes_fetched is reported back to the caller because it accumulates on
        BasePlanNode.bytes_in, not on ScanReadings. All other metadata fields
        are absorbed here by reading attributes from the typed ScanRowGroup object.
        """
        cdef int64_t val
        cdef int64_t bytes_fetched = getattr(scan_rg, 'bytes_fetched', 0)
        cdef object scan_strategy

        # Additive metrics — read from typed ScanRowGroup attributes
        self.parquet_row_groups_pruned        = getattr(scan_rg, 'row_groups_pruned', 0)
        self.parquet_footer_bytes            += getattr(scan_rg, 'footer_bytes', 0)
        self.parquet_range_request_count     += getattr(scan_rg, 'range_request_count', 0)
        self.parquet_range_bytes_requested   += getattr(scan_rg, 'range_bytes_requested', 0)
        self.time_parquet_read_ranges_ns     += getattr(scan_rg, 'time_read_ranges_ns', 0)
        self.time_parquet_decode_columns_ns  += getattr(scan_rg, 'time_decode_columns_ns', 0)
        self.time_parquet_task_queue_wait_ns += getattr(scan_rg, 'task_queue_wait_ns', 0)
        self.time_parquet_task_total_ns      += getattr(scan_rg, 'task_total_ns', 0)
        self.time_parquet_footer_fetch_ns    += getattr(scan_rg, 'footer_fetch_ns', 0)
        self.time_parquet_scheduler_wait_ns  += getattr(scan_rg, 'scheduler_wait_ns', 0)
        self.time_parquet_rowgroup_completion_ns += getattr(scan_rg, 'rowgroup_completion_latency_ns', 0)
        self.time_parquet_emit_wait_ns       += getattr(scan_rg, 'emit_wait_ns', 0)
        self.time_parquet_scheduler_empty_wait_ns += getattr(scan_rg, 'scheduler_empty_wait_ns', 0)
        self.parquet_scheduler_empty_wait_events  += getattr(scan_rg, 'scheduler_empty_wait_events', 0)
        self.io_ring_producer_full_wait_ns   += getattr(scan_rg, 'io_ring_producer_full_wait_ns', 0)
        self.io_ring_producer_full_wait_events += getattr(scan_rg, 'io_ring_producer_full_wait_events', 0)
        self.io_ring_consumer_empty_wait_ns  += getattr(scan_rg, 'io_ring_consumer_empty_wait_ns', 0)
        self.io_ring_consumer_empty_wait_events += getattr(scan_rg, 'io_ring_consumer_empty_wait_events', 0)
        self.io_transfer_emit_wait_ns        += getattr(scan_rg, 'io_transfer_emit_wait_ns', 0)
        self.io_rowgroup_slice_count         += getattr(scan_rg, 'io_rowgroup_slice_count', 0)
        self.io_deserialize_ns               += getattr(scan_rg, 'io_deserialize_ns', 0)
        self.io_serialize_ns                 += getattr(scan_rg, 'io_serialize_ns', 0)

        # Peak/max metrics
        val = getattr(scan_rg, 'rowgroup_peak_in_flight', 0)
        if val > self.parquet_rowgroup_peak_in_flight_max:
            self.parquet_rowgroup_peak_in_flight_max = val
        val = getattr(scan_rg, 'ranges_in_flight_peak', 0)
        if val > self.parquet_ranges_in_flight_peak:
            self.parquet_ranges_in_flight_peak = val
        val = getattr(scan_rg, 'active_files_peak', 0)
        if val > self.parquet_active_files_peak:
            self.parquet_active_files_peak = val
        val = getattr(scan_rg, 'active_rowgroups_peak', 0)
        if val > self.parquet_active_rowgroups_peak:
            self.parquet_active_rowgroups_peak = val
        val = getattr(scan_rg, 'rowgroups_in_flight_cap', 0)
        if val > self.parquet_rowgroups_in_flight_cap:
            self.parquet_rowgroups_in_flight_cap = val
        val = getattr(scan_rg, 'emit_queue_depth_at_ready', 0)
        if val > self.parquet_emit_queue_depth_at_ready_max:
            self.parquet_emit_queue_depth_at_ready_max = val
        val = getattr(scan_rg, 'io_ring_slot_bytes', 0)
        if val > self.io_ring_slot_bytes:
            self.io_ring_slot_bytes = val
        val = getattr(scan_rg, 'io_ring_slot_count', 0)
        if val > self.io_ring_slot_count:
            self.io_ring_slot_count = val
        val = getattr(scan_rg, 'io_ring_total_bytes', 0)
        if val > self.io_ring_total_bytes:
            self.io_ring_total_bytes = val
        val = getattr(scan_rg, 'io_transfer_ready_backlog_peak', 0)
        if val > self.io_transfer_ready_backlog_peak:
            self.io_transfer_ready_backlog_peak = val
        val = getattr(scan_rg, 'io_transfer_fragment_count_p50', 0)
        if val > self.io_transfer_fragment_count_p50:
            self.io_transfer_fragment_count_p50 = val
        val = getattr(scan_rg, 'io_transfer_fragment_count_p95', 0)
        if val > self.io_transfer_fragment_count_p95:
            self.io_transfer_fragment_count_p95 = val
        val = getattr(scan_rg, 'io_transfer_fragment_count_max', 0)
        if val > self.io_transfer_fragment_count_max:
            self.io_transfer_fragment_count_max = val
        val = getattr(scan_rg, 'io_transfer_payload_bytes_p50', 0)
        if val > self.io_transfer_payload_bytes_p50:
            self.io_transfer_payload_bytes_p50 = val
        val = getattr(scan_rg, 'io_transfer_payload_bytes_p95', 0)
        if val > self.io_transfer_payload_bytes_p95:
            self.io_transfer_payload_bytes_p95 = val
        val = getattr(scan_rg, 'io_transfer_payload_bytes_max', 0)
        if val > self.io_transfer_payload_bytes_max:
            self.io_transfer_payload_bytes_max = val

        # Scan strategy: set once
        scan_strategy = getattr(scan_rg, 'scan_strategy', None)
        if scan_strategy and self.parquet_scan_strategy is None:
            self.parquet_scan_strategy = scan_strategy

        # Time to first row group: keep minimum non-zero (optional field in ScanRowGroup)
        val = getattr(scan_rg, 'time_to_first_rowgroup_ns', 0)
        if val and (self.time_to_first_rowgroup_ns == 0 or val < self.time_to_first_rowgroup_ns):
            self.time_to_first_rowgroup_ns = val

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
    dict timestamp_unit_map,
):
    """Coerce Integer64Vector physical columns to their logical types (DATE/TIMESTAMP/DECIMAL).

    The C++ parquet pipeline serialises DATE/TIMESTAMP/DECIMAL as TAG_INT64 (the
    physical type) and the IPC format carries no logical type info, so we apply
    the schema-driven coercion here. Hot-path helper: called once per row group.
    """
    cdef bytes col_name   # dict keys are bytes; coercion maps are keyed by bytes
    cdef int precision
    cdef int scale

    # Unwrap Cython-shim Vectors to their nanobind handle before passing to
    # the nanobind-side reinterpret functions, which require the raw nb type.
    # The shim's `_nb` field is the public access point per E.24's shim
    # design. Pass-through if `v` is already a nanobind Vector.
    if decimal_col_map:
        for col_name, dec in decimal_col_map.items():
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type == _draken_native_parquet.INT64:
                precision = dec[0]
                scale = dec[1]
                row_group[col_name] = _int64_to_decimal(v_nb, precision, scale)
    if date_col_set:
        for col_name in date_col_set:
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type == _draken_native_parquet.INT64:
                row_group[col_name] = _int64_to_date32(v_nb)
    if timestamp_col_set:
        for col_name in timestamp_col_set:
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type == _draken_native_parquet.INT64:
                row_group[col_name] = _int64_to_timestamp(v_nb, timestamp_unit_map.get(col_name, "us"))


cdef list _set_bit_positions(bytes mask_bytes):
    """Return the ascending positions of set bits in a pass-1 survival mask.

    Bit i lives in byte i>>3 at bit i&7 (LSB-first), matching the packing the
    C++ masked decoder consumes (pool_reader.submit_work_native_masked). The
    k-th set position is the original row index of the k-th pass-1 survivor, so
    `_set_bit_positions(mask)[survivor_idx]` maps a survivor back to its row.
    """
    cdef list out = []
    cdef Py_ssize_t byte_i
    cdef Py_ssize_t base
    cdef int k
    cdef unsigned char b
    for byte_i in range(len(mask_bytes)):
        b = mask_bytes[byte_i]
        if b:
            base = byte_i * 8
            for k in range(8):
                if (b >> k) & 1:
                    out.append(base + k)
    return out


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
    dict timestamp_unit_map,
    list pass1_column_names,
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

    _coerce_logical_types(row_group, decimal_col_map, date_col_set, timestamp_col_set, timestamp_unit_map)

    # Positional pairing: column order in the data dict matches pass1_column_names order.
    # C++ preserves column order; dict keys (bytes) are not used for identity lookup.
    cdef list p1_identity_names = [pass1_name_to_identity[col] for col in pass1_column_names]
    cdef list p1_vectors = list(row_group.values())
    if not p1_identity_names:
        result.empty = True
        return result

    p1_morsel = Morsel.from_vectors(p1_identity_names, p1_vectors)
    result.rows_before_filter = p1_morsel.num_rows

    # All-c-native predicate: filter + survival mask in ONE nogil span — no GIL
    # Morsel VM. Only a non-c-native predicate falls through to execute_bytecode.
    _fam = _predicate_filter_and_mask_c_native(compiled_predicate, p1_morsel)
    if _fam is not None:
        p1_filtered, mask_bytes = _fam
        if not any(mask_bytes):
            return result
        result.survived = True
        result.p1_filtered = p1_filtered
        result.p1_identity_names = p1_identity_names
        result.mask_bytes = mask_bytes
        return result

    mask = execute_bytecode(compiled_predicate, p1_morsel)

    if not mask.any():
        return result

    result.survived = True
    result.p1_filtered = p1_morsel.filter_mask(mask)
    result.p1_identity_names = p1_identity_names
    result.mask_bytes = bytes(mask.to_byte_array())
    return result


cdef enum:
    # Scan mode chosen once by _ensure_scan_started.
    #   _SCAN_SINGLE   — native single-pass state machine (next_morsel pulls
    #                    one morsel per call from _rg_iter; zero outer generator).
    #   _SCAN_LATMAT   — native two-pass late-materialization state machine.
    #   _SCAN_FALLBACK — empty manifest; driven by the preserved read_morsels
    #                    generator (single empty Morsel).
    _SCAN_UNSET = 0
    _SCAN_SINGLE = 1
    _SCAN_FALLBACK = 2
    _SCAN_LATMAT = 3


cdef class ParquetReadNode(ReaderNode):
    """Read node backed by column-chunk range reads via ``parquet_io``.

    Activated for filesystem-backed connectors (GCS, S3, local) when the
    manifest contains only ``.parquet`` files.  Non-parquet external scans are
    rejected by the planner.
    """

    cdef public set _parquet_files_seen
    # ── Stage 1: native single-pass scan state machine ────────────────────────
    # Setup runs once in _ensure_scan_started; next_morsel then pulls one morsel
    # per call. These fields hold the hoisted once-per-scan plan + cursor state.
    cdef bint _scan_started
    cdef int _scan_mode
    cdef cpp_mutex* _scan_mtx             # guards per-scan accounting under concurrent pull
    cdef object _ipc_source               # IpcRowGroupSource native driver (single-pass)
    cdef list _sp_coerce_ops              # per-column (kind, arg) logical-type coercions
    cdef bint _sp_needs_coerce
    # ── Stage 6: native two-pass latmat state machine ─────────────────────────
    cdef object _lm_pass1_src             # IpcRowGroupSource over filter columns
    cdef object _lm_pass2_src             # masked IpcRowGroupSource over projection columns
    cdef bint _lm_pass1_done              # pass-1 drained + pass-2 source opened
    cdef list _lm_pass2_work              # [(path, rg_idx, mask_bytes)] survivors
    cdef dict _lm_p1_cache                # (path, rg_idx) -> (p1_filtered Morsel, identity names)
    cdef object _lm_topn_winners          # {(path,rg_idx): [survivor_idx]} or None
    cdef list _lm_pass1_names_bytes
    cdef list _lm_pass2_names_bytes
    cdef int64_t _decode_start_ns
    cdef int64_t _total_rows_before_filter
    cdef int64_t _total_rows_after_filter
    cdef bint _scan_finished              # decode-telemetry flushed once
    cdef bint _emitted_any                # any morsel returned (empty-result guard)
    cdef bint _empty_guard_done           # the single empty morsel already returned
    cdef int64_t _sp_claims_pending       # row groups claimed via next_vectors() but not
                                           # yet fully processed by this worker (see
                                           # _single_pass_next's race-fix comment)
    # Hoisted single-pass plan (read by _single_pass_next; latmat reads the
    # _sp_* it shares via read_morsels).
    cdef object _sp_filesystem
    cdef object _sp_connector_type
    cdef object _sp_blob_paths
    cdef dict _sp_file_sizes
    cdef object _sp_query_id
    cdef list _sp_column_names
    cdef list _sp_identity_names           # invariant identity order, precomputed once
    cdef dict _sp_name_to_identity
    cdef dict _sp_planner_identity
    cdef list _sp_output_identity_order
    cdef dict _sp_decimal_col_map
    cdef set _sp_date_col_set
    cdef set _sp_timestamp_col_set
    cdef dict _sp_timestamp_unit_map
    cdef object _sp_predicate_stats
    cdef list _sp_pass1_column_names
    cdef list _sp_pass2_column_names
    cdef dict _sp_pass1_name_to_identity
    cdef dict _sp_pass2_name_to_identity
    cdef dict _sp_null_filler_by_name     # schema-evolution typed NULL-fill, by physical column name
    cdef dict _sp_string_type_by_name     # declared DrakenType (VARCHAR/NVARCHAR/VARBINARY), by physical column name
    cdef bint _sp_topn_active
    cdef bint _sp_two_pass_eligible
    cdef CompiledBytecode _compiled_predicate
    # LIMIT counter for the source. Lives on the node (not a generator local) so
    # the single _commit_morsel mutation seam owns it — the granularity a future
    # per-scan lock guards (Stage 5). _records_unlimited mirrors the old float
    # "inf" sentinel: when true the LIMIT never trips and no slice is taken.
    cdef int64_t _records_to_read
    cdef bint _records_unlimited
    cdef public object _planner_name_to_identity_cached
    cdef public object _filter_column_names_cached
    cdef public ScanReadings scan_readings
    # WP-2 top-N scan pushdown spec (set by TopNScanPushdownStrategy via node properties).
    cdef public object _topn_sort_name
    cdef public bint _topn_descending
    cdef public object _topn_limit

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.predicates = parameters.get("predicates")
        self._parquet_files_seen = set()
        self._records_to_read = 0
        self._records_unlimited = True
        self._scan_started = False
        self._scan_mode = _SCAN_UNSET
        self._ipc_source = None
        self._sp_coerce_ops = None
        self._sp_needs_coerce = False
        self._lm_pass1_src = None
        self._lm_pass2_src = None
        self._lm_pass1_done = False
        self._lm_pass2_work = None
        self._lm_p1_cache = None
        self._lm_topn_winners = None
        self._lm_pass1_names_bytes = None
        self._lm_pass2_names_bytes = None
        self._decode_start_ns = 0
        self._total_rows_before_filter = 0
        self._total_rows_after_filter = 0
        self._scan_finished = False
        self._emitted_any = False
        self._empty_guard_done = False
        self._sp_claims_pending = 0
        self._compiled_predicate = None  # CompiledBytecode, bound once at execute() time
        self._planner_name_to_identity_cached = None  # Cache name-to-identity mapping
        self._filter_column_names_cached = None  # Cache filter column names extraction
        self.scan_readings = ScanReadings()
        # WP-2: physical sort column name, direction, and N. None unless the
        # optimizer matched ORDER BY <physical col> LIMIT n directly over this scan.
        self._topn_sort_name = parameters.get("topn_sort_name")
        self._topn_descending = bool(parameters.get("topn_descending", False))
        self._topn_limit = parameters.get("topn_limit")
        self._scan_mtx = new cpp_mutex()

    def __dealloc__(self):
        if self._scan_mtx != NULL:
            del self._scan_mtx
            self._scan_mtx = NULL

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

    cdef shared_ptr[CxxMorsel] _cxx_apply_predicate(self, shared_ptr[CxxMorsel] m):
        """S-B.2 prereq #2: apply the compiled predicate to a CxxMorsel, returning the
        filtered NATIVE carrier (no PyObject column materialization).

        All-c-native predicates take the one-nogil-span path (filter_morsel_c_native:
        predicate result DV* feeds straight into cxx_mask_c, no Python BoolVector);
        anything else falls back to the VM + filter_mask over a thin Cxx-backed shim.
        The intermediate Morsels are Cxx-backed wrappers (no data copy); morsel_to_cxx
        is a shallow copy. Lets the scan filter without leaving the CxxMorsel substrate.
        """
        if self._compiled_predicate is None:
            return m
        cdef Morsel shim = cxx_to_morsel(m)
        cdef object res = _filter_morsel_c_native(self._compiled_predicate, shim)
        if res is None:
            res = shim.filter_mask(execute_bytecode(self._compiled_predicate, shim))
        return morsel_to_cxx(<Morsel>res)

    def _apply_predicates_to_morsel(self, morsel: Morsel):
        """Apply the compiled predicate to a Draken Morsel.

        Routes through the CxxMorsel-native filter (_cxx_apply_predicate): the
        substrate never materializes PyObject columns. The predicate is compiled once
        at execute() time; this just applies it.
        """
        if self._compiled_predicate is None:
            return morsel, morsel.num_rows, morsel.num_rows

        cdef int64_t rows_before_filter = morsel.num_rows
        cdef Morsel filtered = cxx_to_morsel(
            self._cxx_apply_predicate(morsel_to_cxx(morsel)))
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
        """Apply per-emit accounting to scan_readings (typed, native).

        The Python `telemetry`/`readings` object mirror was REMOVED from this
        per-morsel hot path: those fields are `object`-typed (forbidden — CLAUDE.md
        §3/§9) and their backing store (`_QueryTelemetry._reading`, a plain unlocked
        `defaultdict`) has no concurrency protection of its own. Under genuine
        multi-threaded concurrent pull (the native worker fan-out), writing to it from
        inside this `_scan_mtx`-held section stalled the whole scan. The rule is
        absolute: blind to this mirror rather than touching Python object state from
        a native concurrent path. `scan_readings` (typed cdef class) remains the
        complete, lossless native record.
        """
        cdef int64_t num_rows = morsel.num_rows
        cdef int64_t num_bytes = morsel.nbytes
        cdef int64_t files_seen = len(self._parquet_files_seen)
        self.scan_readings.record_morsel_yielded(num_rows, num_bytes, files_seen)

    cdef Morsel _commit_morsel(self, Morsel result_morsel, object path):
        """Single seam for ALL per-emitted-morsel shared-state mutation:
        row-group accounting, file-seen tracking, the LIMIT decrement + slice,
        and the telemetry mirror. Returns the (possibly LIMIT-sliced) morsel to
        emit. This is the one site a future per-scan lock guards (Stage 5);
        every yield path must funnel through here.
        """
        cdef int64_t num_rows = result_morsel.num_rows
        self.scan_readings.record_row_group_complete(num_rows)
        self._mark_file_seen(path)
        if not self._records_unlimited:
            if self._records_to_read < num_rows:
                result_morsel = result_morsel.slice(0, self._records_to_read)
                self._records_to_read = 0
            else:
                self._records_to_read -= num_rows
        self._record_morsel_emitted(result_morsel)
        return result_morsel

    cdef shared_ptr[CxxMorsel] _commit_morsel_cxx(self, shared_ptr[CxxMorsel] cxm, object path):
        """S-B.2: the cxm-native twin of `_commit_morsel` for the single-pass path —
        row-group accounting, file-seen, LIMIT decrement + slice (`cxx_slice_c`), all
        on the C++ carrier with NO Python Morsel and NO Python telemetry object touch
        (see `_record_morsel_emitted`'s docstring — the `object`-typed telemetry
        mirror is forbidden inside this `_scan_mtx`-held native section). `nbytes`
        matches Morsel.nbytes' estimate (rows × cols × 8)."""
        cdef int64_t num_rows = cxm.get().num_rows()
        cdef int64_t num_cols = cxm.get().num_columns()
        cdef int64_t files_seen
        self.scan_readings.record_row_group_complete(num_rows)
        self._mark_file_seen(path)
        if not self._records_unlimited:
            if self._records_to_read < num_rows:
                cxm = shared_ptr[CxxMorsel](
                    cxx_slice_c(cxm.get(), 0, <uint32_t>self._records_to_read))
                num_rows = self._records_to_read
                self._records_to_read = 0
            else:
                self._records_to_read -= num_rows
        files_seen = len(self._parquet_files_seen)
        self.scan_readings.record_morsel_yielded(num_rows, num_rows * num_cols * 8, files_seen)
        return cxm

    cdef shared_ptr[CxxMorsel] _single_pass_finish_cxx(self):
        """cxm-native twin of `_single_pass_finish`: emit one empty CxxMorsel if the
        scan produced nothing, else NULL (end-of-stream). Caller holds `_scan_mtx` —
        no Python `readings` dict touch here (see `_record_morsel_emitted`'s
        docstring); `empty_datasets` is no longer tracked from this native path.

        `_sp_claims_pending == 0` is required before deciding "nothing was ever
        produced" — see `_single_pass_next`'s race-fix comment. While another
        worker's claimed row group is still mid-assembly, this returns NULL (this
        caller's own stream just ends) rather than risk a duplicate/premature
        courtesy morsel; whichever caller next observes pending==0 makes the real
        decision, still exactly once (`_empty_guard_done` is the single-fire latch)."""
        cdef shared_ptr[CxxMorsel] out
        if self._sp_claims_pending == 0 and not self._emitted_any and not self._empty_guard_done:
            self._empty_guard_done = True
            self._emitted_any = True
            out = cxx_morsel_from_vectors_sp([], [])
        return out

    cdef shared_ptr[CxxMorsel] _finish_locked_cxx(self):
        cdef shared_ptr[CxxMorsel] out
        self._scan_mtx.lock()
        out = self._single_pass_finish_cxx()
        self._scan_mtx.unlock()
        return out

    cdef inline bint _limit_exhausted(self):
        """True once a finite LIMIT has been fully satisfied — the caller's
        signal to stop pulling. Always False for unlimited scans."""
        return (not self._records_unlimited) and self._records_to_read <= 0

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

    def _apply_topn(self, list pass2_work, dict p1_cache, object sort_identity,
                    int n, bint descending):
        """WP-2: shrink pass-2 work to only the rows that can be in the top-n.

        Keeps every surviving row whose sort key is at-least-as-good as the n-th
        best value (i.e. n rows plus any ties exactly at the boundary). Every row
        dropped here is strictly worse than the true top-n, so the downstream
        HeapSort produces an identical result to the un-pushed plan — ties at the
        boundary can never change which n rows HeapSort finally keeps.

        Returns (new_pass2_work, winners_by_rg):
          - new_pass2_work: [(path, rg_idx, reduced_mask_bytes)] for winning row
            groups only (row groups with no surviving top-n candidate are dropped
            and never decoded in pass 2).
          - winners_by_rg: {(path, rg_idx): [survivor_idx, ...]} (ascending), used
            to gather the matching pass-1 column values for assembly.

        NULLs sort last (matching HeapSort), so they only enter the result when
        fewer than n non-null rows exist; in that case every survivor is kept.
        """
        cdef list candidates = []          # (value, (path, rg_idx), survivor_idx)
        cdef Py_ssize_t nonnull = 0
        cdef Py_ssize_t i
        for key in p1_cache:
            p1_filtered = p1_cache[key][0]
            vals = p1_filtered.column(sort_identity).to_pylist()
            for i in range(len(vals)):
                v = vals[i]
                candidates.append((v, key, i))
                if v is not None:
                    nonnull += 1

        cdef dict winners_by_rg = {}
        if nonnull <= n:
            for (v, key, i) in candidates:
                winners_by_rg.setdefault(key, []).append(i)
        else:
            vals_only = sorted(
                (v for (v, key, i) in candidates if v is not None),
                reverse=descending,
            )
            boundary = vals_only[n - 1]
            for (v, key, i) in candidates:
                if v is None:
                    continue
                if (v >= boundary) if descending else (v <= boundary):
                    winners_by_rg.setdefault(key, []).append(i)

        cdef dict mask_by_rg = {(p, rg): mb for (p, rg, mb) in pass2_work}
        cdef list new_pass2_work = []
        cdef Py_ssize_t pos
        for key in list(winners_by_rg.keys()):
            idxs = sorted(winners_by_rg[key])     # ascending survivor idx == ascending row pos
            winners_by_rg[key] = idxs
            set_positions = _set_bit_positions(mask_by_rg[key])
            reduced = bytearray(len(mask_by_rg[key]))
            for i in idxs:
                pos = set_positions[i]
                reduced[pos >> 3] |= (1 << (pos & 7))
            new_pass2_work.append((key[0], key[1], bytes(reduced)))
        return new_pass2_work, winners_by_rg

    cpdef bint is_concurrent_pull_safe(self) except *:
        """Reentrant for concurrent ``pull_one`` ONLY in single-pass mode (see
        ``next_morsel``'s contract). The two-pass LATMAT state machine (shared
        ``_lm_p1_cache``, unguarded pass-1 barrier) and the empty-manifest
        FALLBACK generator are NOT reentrant. The mode is a runtime decision, so
        resolve it here under the same init lock the pull uses, then report it —
        the parallel strategies branch lockless-vs-serialised pull on this.

        The acquire is done with the thread state released (`with nogil`): under
        free-threaded CPython (3.13t/3.14t), a thread parked in a contended
        `std::mutex::lock()` while still ATTACHED cannot reach a safepoint, so it
        blocks the runtime's stop-the-world (GC/QSBR) indefinitely if the holder
        is itself waiting on that same pause — a real deadlock reproduced with
        `dop=8` concurrent `next_morsel` pullers. Releasing the thread state
        during the wait lets stop-the-world treat this thread as not running
        Python; it re-attaches once the lock is actually acquired."""
        with nogil:
            self._scan_mtx.lock()
        if not self._scan_started:
            self._ensure_scan_started()
        self._scan_mtx.unlock()
        return self._scan_mode == _SCAN_SINGLE

    cdef shared_ptr[CxxMorsel] next_morsel(self) except *:
        """Native source iterator (overrides BasePlanNode). On first call it
        plans the scan; thereafter it returns one morsel per call as the C++
        carrier (`shared_ptr[CxxMorsel]`) — no outer Python generator for the
        single-pass path. Returns a NULL shared_ptr on exhaustion. Two-pass latmat
        and empty manifests still run through the preserved read_morsels generator.

        Stays GIL-requiring during S-B.1 (the morsel is still assembled as a
        Python Morsel by the helpers, then encoded to the carrier here); S-B.2
        converts the scan body to build the CxxMorsel natively with no encode.

        Single-pass pull is reentrant: concurrent callers share the native
        IpcRowGroupSource + `_scan_mtx`. The FALLBACK (latmat) generator is NOT
        reentrant — two-pass scans are pulled serially (the M4 concurrent path is
        single-pass only)."""
        cdef Morsel py
        cdef shared_ptr[CxxMorsel] out
        # Init guard under the lock so concurrent first-callers plan the scan
        # exactly once and observe a fully-built _ipc_source before pulling. The
        # uncontended lock/unlock is negligible beside decode; no atomics needed.
        #
        # The acquire releases the thread state (`with nogil`) — see
        # `is_concurrent_pull_safe` for why: under free-threaded CPython, an
        # ATTACHED thread parked in a contended `std::mutex::lock()` can't reach
        # a safepoint, which deadlocks the runtime's stop-the-world if the lock
        # holder is itself waiting on that pause (reproduced with `dop=8`
        # concurrent pullers racing this same first-call init guard).
        with nogil:
            self._scan_mtx.lock()
        if not self._scan_started:
            self._ensure_scan_started()
        self._scan_mtx.unlock()
        if self._scan_mode == _SCAN_SINGLE:
            # S-B.2: the single-pass path returns the C++ carrier directly — no
            # Python Morsel, no encode.
            return self._single_pass_next()
        elif self._scan_mode == _SCAN_LATMAT:
            py = self._latmat_next()
        else:
            # FALLBACK: drive the empty-manifest generator one step at a time.
            if self._morsel_iter is None:
                self._morsel_iter = iter(self.read_morsels())
            py = <Morsel>next(self._morsel_iter, None)
        if py is not None:
            out = morsel_to_cxx(py)
        return out

    cpdef void close_source(self) except *:
        """Flush decode telemetry and release the source(s) on every exit path.
        The native single-pass and latmat paths own their flush + source close;
        the empty-manifest fallback flushes nothing (it never opened a source)."""
        cdef object src, src1, src2
        if self._scan_mode == _SCAN_SINGLE:
            # Lock so a concurrent late puller can't race the flush/close-once.
            self._scan_mtx.lock()
            if not self._scan_finished:
                self._scan_finished = True
                self._flush_decode_telemetry()
            src = self._ipc_source
            self._ipc_source = None
            self._scan_mtx.unlock()
            if src is not None:
                src.close()
            return
        if self._scan_mode == _SCAN_LATMAT:
            self._scan_mtx.lock()
            if not self._scan_finished:
                self._scan_finished = True
                self._flush_decode_telemetry()
            src1 = self._lm_pass1_src
            src2 = self._lm_pass2_src
            self._lm_pass1_src = None
            self._lm_pass2_src = None
            self._scan_mtx.unlock()
            if src1 is not None:
                src1.close()
            if src2 is not None:
                src2.close()
            return
        BasePlanNode.close_source(self)

    cdef void _flush_decode_telemetry(self) except *:
        """Per-scan decode-time + filter-total telemetry, recorded into the typed
        native `scan_readings` only. Called once from close_source for the
        single-pass path, but still under `_scan_mtx` — no Python `telemetry`/
        `readings` object touch here (see `_record_morsel_emitted`'s docstring); the
        rule is absolute regardless of call frequency. `scan_readings` itself is the
        complete native record; it is no longer mirrored into the Python `readings`
        dict, so the EXPLAIN/reporting surface goes blind to this scan's stats
        rather than the native path touching Python object state."""
        cdef int64_t decode_ns = <int64_t>time.monotonic_ns() - self._decode_start_ns
        self.scan_readings.record_decode_time(decode_ns)
        self.scan_readings.record_filter_totals(
            self._total_rows_before_filter,
            self._total_rows_after_filter,
        )

    cdef void _ensure_scan_started(self) except *:
        """Run the once-per-scan setup (schema resolution, identity maps, predicate
        compile, two-pass eligibility, filesystem resolution) and choose the scan
        mode. For the single-pass mode it opens the inner row-group iterator. The
        hoisted plan is stored on the node so neither next_morsel nor the latmat
        generator recompute it. Idempotent."""
        if self._scan_started:
            return
        self._scan_started = True

        base_schema = self.parameters["schema"]

        # Per-column typed NULL-fill factories, keyed by physical name. Used to
        # materialize a column a given file lacks (schema evolution) so the scan
        # never desyncs the name↔vector pairing. Built once from the union schema.
        self._sp_null_filler_by_name = {
            col.name: _null_filler_for(col.column_type) for col in base_schema.columns
        }
        # Per-column declared string type (VARCHAR/NVARCHAR/VARBINARY), keyed by
        # physical name — same union schema, so the scan tags decoded string
        # columns to match what the schema declared instead of always VARCHAR.
        self._sp_string_type_by_name = {
            col.name: _string_type_for(col.column_type) for col in base_schema.columns
        }

        if self._planner_name_to_identity_cached is None:
            self._planner_name_to_identity_cached = {
                col.schema_column.name: col.schema_column.identity
                for col in (self.columns or [])
            }
        _planner_name_to_identity = self._planner_name_to_identity_cached
        self._sp_planner_identity = _planner_name_to_identity

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
        self._sp_output_identity_order = [
            _planner_name_to_identity[col.schema_column.name]
            for col in (self.columns or [])
            if col.schema_column.name in _planner_name_to_identity
        ]

        # DECIMAL (precision<=18, int64-backed) / DATE / TIMESTAMP coerce sets.
        # Keys are bytes — column names in the data dict are bytes (C++ native).
        from opteryx.types.logical_type import LogicalCategory as _LC
        _decimal_col_map = {}
        for col in base_schema.columns:
            ct = col.column_type
            if ct is None or ct.category != _LC.DECIMAL or ct.logical is None:
                continue
            if ct.logical.precision <= 18:
                _decimal_col_map[col.name.encode('utf-8')] = (
                    ct.logical.precision, ct.logical.scale
                )
        self._sp_decimal_col_map = _decimal_col_map
        self._sp_date_col_set = {
            col.name.encode('utf-8') for col in base_schema.columns
            if col.column_type is not None and col.column_type.category == _LC.DATE
        }
        self._sp_timestamp_col_set = {
            col.name.encode('utf-8') for col in base_schema.columns
            if col.column_type is not None and col.column_type.category == _LC.TIMESTAMP
        }
        # Per-column reinterpret unit (string) for the int64->timestamp retag,
        # sourced from the schema's logical unit; defaults to "us". Keyed by the
        # same bytes names as the set.
        self._sp_timestamp_unit_map = {}
        for col in base_schema.columns:
            ct = col.column_type
            if ct is None or ct.category != _LC.TIMESTAMP:
                continue
            unit_str = "us"
            if ct.logical is not None and ct.logical.unit is not None:
                unit_str = _TS_UNIT_BY_NAME.get(ct.logical.unit.name, "us")
            self._sp_timestamp_unit_map[col.name.encode('utf-8')] = unit_str
        predicate_root = self._compose_predicates(self.predicates or [])

        # Compile the predicate to bytecode once per execute() call.
        if self._compiled_predicate is None and predicate_root is not None:
            self._compiled_predicate = _build_bytecode(_lower_expr(predicate_root))

        # ── Two-pass late-materialization eligibility ─────────────────────────
        _filter_names = filter_column_names
        _projected_names = {col.schema_column.name for col in (self.columns or [])}
        topn_active = (
            self._topn_sort_name is not None
            and self._topn_limit is not None
            and self._topn_sort_name in _projected_names
        )
        _pass1_only_names = set(_filter_names)
        if topn_active:
            _pass1_only_names = _pass1_only_names | {self._topn_sort_name}
        _pass2_names = _projected_names - _pass1_only_names
        two_pass_eligible = (
            config.features.parquet_late_materialization
            and bool(predicate_root)
            and bool(_filter_names)
            and bool(_pass2_names)
        )
        topn_active = topn_active and two_pass_eligible
        import os as _dbg_os
        if _dbg_os.environ.get("OPTERYX_SCAN_DEBUG"):
            import sys as _dbg_sys
            print(f"SCAN-DBG topn_active={topn_active} two_pass={two_pass_eligible} "
                  f"topn_sort={self._topn_sort_name} limit={self._topn_limit} "
                  f"p1={sorted(_pass1_only_names)[:4]} np2={len(_pass2_names)}",
                  file=_dbg_sys.stderr)
        self._sp_topn_active = topn_active
        self._sp_two_pass_eligible = two_pass_eligible
        self._sp_pass1_column_names = []
        self._sp_pass2_column_names = []
        self._sp_pass1_name_to_identity = {}
        self._sp_pass2_name_to_identity = {}
        if two_pass_eligible:
            _p1_cols = [c for c in base_schema.columns if c.name in _pass1_only_names]
            _p2_cols = [c for c in base_schema.columns if c.name in _pass2_names]
            self._sp_pass1_column_names = [c.name for c in _p1_cols]
            self._sp_pass2_column_names = [c.name for c in _p2_cols]
            self._sp_pass1_name_to_identity = {c.name: _planner_name_to_identity.get(c.name, c.identity) for c in _p1_cols}
            self._sp_pass2_name_to_identity = {c.name: _planner_name_to_identity.get(c.name, c.identity) for c in _p2_cols}

        # ── Empty manifest → fallback generator yields a single empty Morsel ──
        # (Returns before the per-scan accounting below, exactly as before.)
        if not self.manifest or self.manifest.get_file_count() == 0:
            self._scan_mode = _SCAN_FALLBACK
            return

        # NOTE: the `self.readings[...] += ...` Python-dict mirror was REMOVED here —
        # this runs under `_scan_mtx`, reachable from `is_concurrent_pull_safe()`
        # called by every worker (see `_record_morsel_emitted`'s docstring); the
        # native-path "object forbidden" rule is absolute regardless of idempotency.

        # Row-group pruning stats from pushed-down predicates.
        self._sp_predicate_stats = extract_predicate_stats(self.predicates or [])

        if self.limit is not None:
            self._records_to_read = <int64_t>self.limit
            self._records_unlimited = False
        else:
            self._records_to_read = 0
            self._records_unlimited = True

        blob_paths = self.manifest.get_file_paths()
        self._sp_blob_paths = blob_paths
        file_sizes = {}
        files = getattr(self.manifest, "files", None)
        if files:
            for file_entry in files:
                size = getattr(file_entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(file_entry.file_path, size)
        self._sp_file_sizes = file_sizes

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
        self._sp_filesystem = filesystem
        self._sp_connector_type = connector_type

        column_names = [col.name for col in read_schema.columns]
        self._sp_column_names = column_names
        self._sp_name_to_identity = {
            col.name: _planner_name_to_identity.get(col.name, col.identity)
            for col in read_schema.columns
        }
        # Identity order is invariant across row groups (positional pairing with
        # row_group.values()); compute once instead of per morsel.
        self._sp_identity_names = [
            self._sp_name_to_identity[col] for col in column_names
        ]
        # Positional logical-type coercion plan (kind, arg) per column, computed
        # once. kind 0=none, 1=decimal(prec,scale), 2=date32, 3=timestamp. Empty
        # of real work for pure numeric scans → coercion is skipped entirely.
        self._sp_coerce_ops = []
        for col in column_names:
            col_b = col.encode('utf-8')
            if col_b in self._sp_decimal_col_map:
                self._sp_coerce_ops.append((1, self._sp_decimal_col_map[col_b]))
            elif col_b in self._sp_date_col_set:
                self._sp_coerce_ops.append((2, None))
            elif col_b in self._sp_timestamp_col_set:
                self._sp_coerce_ops.append((3, self._sp_timestamp_unit_map.get(col_b, "us")))
            else:
                self._sp_coerce_ops.append((0, None))
        self._sp_needs_coerce = any(op[0] != 0 for op in self._sp_coerce_ops)
        self._sp_query_id = getattr(self.properties, "query_id", None)

        self._decode_start_ns = <int64_t>time.monotonic_ns()
        self._total_rows_before_filter = 0
        self._total_rows_after_filter = 0

        if two_pass_eligible:
            # Native two-pass latmat: open the pass-1 (filter columns) source with
            # predicate pushdown; pass-2 is opened after pass-1 drains (the barrier).
            self._scan_mode = _SCAN_LATMAT
            self._lm_pass1_names_bytes = [c.encode('utf-8') for c in self._sp_pass1_column_names]
            self._lm_pass2_names_bytes = [c.encode('utf-8') for c in self._sp_pass2_column_names]
            self._lm_pass1_src = open_ipc_source(
                filesystem,
                blob_paths,
                self._sp_pass1_column_names,
                decode_workers=config.PARQUET_GCS_IO_WORKERS if connector_type in ("GCS", "GS") else config.PARQUET_LOCAL_IO_WORKERS,
                predicates=self._sp_predicate_stats,
                file_sizes=file_sizes or None,
                connector=connector_type,
                query_id=self._sp_query_id,
                footer_bytes_cache=_FOOTER_CACHE,
                null_fillers=[self._sp_null_filler_by_name[c] for c in self._sp_pass1_column_names],
                string_types=[self._sp_string_type_by_name[c] for c in self._sp_pass1_column_names],
            )
            return

        self._scan_mode = _SCAN_SINGLE
        self._ipc_source = open_ipc_source(
            filesystem,
            blob_paths,
            column_names,
            decode_workers=config.PARQUET_GCS_IO_WORKERS if connector_type in ("GCS", "GS") else config.PARQUET_LOCAL_IO_WORKERS,
            predicates=self._sp_predicate_stats,
            file_sizes=file_sizes or None,
            connector=connector_type,
            query_id=self._sp_query_id,
            footer_bytes_cache=_FOOTER_CACHE,
            null_fillers=[self._sp_null_filler_by_name[c] for c in column_names],
            string_types=[self._sp_string_type_by_name[c] for c in column_names],
        )

    cdef void _coerce_vectors(self, list vectors):
        """Reinterpret DATE/TIMESTAMP/DECIMAL columns in place, by position.

        Mirrors _coerce_logical_types but indexes the precomputed _sp_coerce_ops
        plan instead of a name-keyed dict, so the all-direct numeric path never
        builds a dict. The C++ pipeline serialises these as TAG_INT64 (physical);
        the schema-driven logical type is applied here."""
        cdef Py_ssize_t i, n = len(vectors)
        cdef tuple op
        cdef int kind
        cdef object v, v_nb, dec
        for i in range(n):
            op = self._sp_coerce_ops[i]
            kind = op[0]
            if kind == 0:
                continue
            v = vectors[i]
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type != _draken_native_parquet.INT64:
                continue
            if kind == 1:
                dec = op[1]
                vectors[i] = _int64_to_decimal(v_nb, dec[0], dec[1])
            elif kind == 2:
                vectors[i] = _int64_to_date32(v_nb)
            else:  # kind == 3
                vectors[i] = _int64_to_timestamp(v_nb, op[1])

    cdef shared_ptr[CxxMorsel] _single_pass_next(self):
        """Pull and assemble the next single-pass morsel directly from the native
        IpcRowGroupSource as the C++ carrier (`shared_ptr[CxxMorsel]`) — no
        intermediate Python Morsel on the normal projected-column path (S-B.2). The
        cxm is built (`cxx_morsel_from_vectors_sp`), filtered (`_cxx_apply_predicate`),
        and selected (`cxx_select_sp`) on the substrate; only the COUNT(*) filter-only
        read (rare) routes through the Morsel path. Returns a NULL shared_ptr on
        exhaustion (after one empty morsel if nothing was produced).

        Thread-safe: the pull + cxm assembly run on thread-local data with no lock;
        only the per-scan accounting + `_commit_morsel_cxx` run under `_scan_mtx`.

        RACE FIX (found 2026-07-01 via ASan under genuinely concurrent dop>1 pull on a
        tiny — few/one row-group — scan; reproduced as a real SIGSEGV, not a hang):
        claiming a row group (`next_vectors()` returning non-None) and COMMITTING it
        (setting `_emitted_any`) are separated by this method's lock-free assembly
        section — a SECOND worker whose own `next_vectors()` returns None (no more
        items to claim) in that window used to see `_emitted_any` still False and
        wrongly conclude "the whole scan produced nothing", emitting a bogus
        ZERO-COLUMN courtesy morsel via `_single_pass_finish_cxx`. That morsel is NOT
        an EOS sentinel, so `StreamingScanSource`'s trampoline (which only skips
        `MorselState.END_OF_STREAM`) forwarded it to the engine as real data —
        `GroupSumCountSink`/`SumCountSink` then read `in->columns[0]` on an EMPTY
        vector: undefined behaviour, observed as SIGSEGV. `_sp_claims_pending` closes
        the window: a worker only decides "genuinely nothing was ever produced" once
        every claimed-but-not-yet-processed row group (including OTHER workers')
        counts back to zero — see `_single_pass_finish_cxx`. RESIDUAL LIMITATION
        (documented, not hidden): under adversarial concurrent timing where every row
        group's predicate filters to zero surviving rows, the once-per-scan courtesy
        empty morsel (schema visibility for a truly empty result) can now be skipped
        rather than produced — never wrong data, never a crash, just a rarer nicety
        lost in a narrow concurrent corner. Serial (dop=1) callers are unaffected
        (pending always hits zero before the single caller ever re-checks)."""
        cdef tuple pulled
        cdef list vectors
        cdef object path
        cdef shared_ptr[CxxMorsel] result_cxm, emit_cxm
        cdef bint has_identity
        cdef int64_t rows_before_filter, rows_after_filter, surviving_rows
        cdef int64_t bytes_fetched, read_ns, decode_ns
        cdef Morsel count_star
        while True:
            if self._limit_exhausted():
                return self._finish_locked_cxx()
            # Mark a claim attempt as "in flight" BEFORE calling next_vectors() —
            # incrementing only AFTER a successful claim leaves a gap between the
            # claim (under IpcRowGroupSource's OWN separate mutex) and the increment
            # (under this scan's _scan_mtx), which a concurrently-exhausted second
            # worker can slip through. Speculatively incrementing first and
            # decrementing on a None result closes that gap entirely.
            self._scan_mtx.lock()
            self._sp_claims_pending += 1
            self._scan_mtx.unlock()
            pulled = self._ipc_source.next_vectors()
            if pulled is None:
                self._scan_mtx.lock()
                self._sp_claims_pending -= 1
                self._scan_mtx.unlock()
                return self._finish_locked_cxx()
            vectors = pulled[0]
            bytes_fetched = pulled[1]
            read_ns = pulled[2]
            decode_ns = pulled[3]
            path = pulled[4]

            # Phase 2 empty row group (dictionary-membership skip): no assembly,
            # no morsel — just fold in I/O telemetry + the pre-filter row count.
            if vectors is None:
                self._scan_mtx.lock()
                self.bytes_in += bytes_fetched
                self.scan_readings.time_parquet_read_ranges_ns += read_ns
                self.scan_readings.time_parquet_decode_columns_ns += decode_ns
                self._total_rows_before_filter += pulled[6]
                self._sp_claims_pending -= 1
                self._scan_mtx.unlock()
                continue

            # ── Thread-local cxm assembly (no lock) ──────────────────────────
            if self._sp_needs_coerce:
                self._coerce_vectors(vectors)
            has_identity = bool(self._sp_identity_names)
            emit_cxm.reset()
            rows_before_filter = 0
            rows_after_filter = 0
            if has_identity:
                # Positional pairing: vectors order == column_names == identity order.
                result_cxm = cxx_morsel_from_vectors_sp(vectors, self._sp_identity_names)
                rows_before_filter = result_cxm.get().num_rows()
                rows_after_filter = rows_before_filter
                if self._compiled_predicate is not None:
                    result_cxm = self._cxx_apply_predicate(result_cxm)
                    rows_after_filter = result_cxm.get().num_rows()
                if self._sp_output_identity_order:
                    emit_cxm = cxx_select_sp(result_cxm, self._sp_output_identity_order)
                else:
                    # No output columns (e.g. COUNT(*) with a filter-only read) — rare;
                    # the bool-constant column routes through the Morsel encode.
                    surviving_rows = result_cxm.get().num_rows()
                    if surviving_rows > 0:
                        count_star = Morsel.from_vectors(
                            [b'*'],
                            [_draken_native_parquet.vector_from_bool_constant(True, <uint32_t>surviving_rows)],
                        )
                        emit_cxm = morsel_to_cxx(count_star)

            # ── Shared commit (under _scan_mtx; no GIL-releasing call inside) ──
            self._scan_mtx.lock()
            self.bytes_in += bytes_fetched
            self.scan_readings.time_parquet_read_ranges_ns += read_ns
            self.scan_readings.time_parquet_decode_columns_ns += decode_ns
            if has_identity:
                self._total_rows_before_filter += rows_before_filter
                self._total_rows_after_filter += rows_after_filter
            self._sp_claims_pending -= 1
            if emit_cxm.get() != NULL:
                emit_cxm = self._commit_morsel_cxx(emit_cxm, path)
                self._emitted_any = True
                self._scan_mtx.unlock()
                return emit_cxm
            self._scan_mtx.unlock()
            # No emit (no projected identities, or COUNT(*) with 0 survivors) — loop.

    cdef Morsel _finish_locked(self):
        """Locked wrapper around the exhaustion guard so the empty-result bookkeeping
        is consistent under concurrent pull."""
        cdef Morsel out
        self._scan_mtx.lock()
        out = self._single_pass_finish()
        self._scan_mtx.unlock()
        return out

    cdef Morsel _single_pass_finish(self):
        """On exhaustion, emit exactly one empty Morsel if the scan produced
        nothing (parity with the generator's `result_morsel is None` guard),
        otherwise signal end-of-stream with None. Caller holds `_scan_mtx` — no
        Python `readings` dict touch here (see `_record_morsel_emitted`'s
        docstring)."""
        if not self._emitted_any and not self._empty_guard_done:
            self._empty_guard_done = True
            self._emitted_any = True
            return Morsel()
        return None

    cdef void _run_pass1(self) except *:
        """Drain the pass-1 (filter-column) source, evaluate the predicate per row
        group, and collect survivors into the pass-2 work list + p1 cache; then run
        the WP-2 top-N reduction and open the pass-2 (masked, projection-column)
        source. The pass-1→pass-2 barrier: this runs to completion on the first
        _latmat_next() call. Mirrors the former read_morsels pass-1 loop exactly."""
        cdef object pass1_src = self._lm_pass1_src
        cdef tuple pulled
        cdef list vectors
        cdef object path, rg_idx, topn_sort_identity
        cdef dict row_group
        cdef _Pass1Result result
        cdef Py_ssize_t i, n
        cdef list pass2_work = []
        cdef dict p1_cache = {}
        while True:
            pulled = pass1_src.next_vectors()
            if pulled is None:
                break
            vectors = pulled[0]
            self.bytes_in += pulled[1]
            self.scan_readings.time_parquet_read_ranges_ns += pulled[2]
            self.scan_readings.time_parquet_decode_columns_ns += pulled[3]
            path = pulled[4]
            rg_idx = pulled[5]
            # Phase 2 empty row group: dictionary-membership skip → no survivors,
            # no pass-2 work. Count pre-filter rows and record the skip.
            if vectors is None:
                self._total_rows_before_filter += pulled[6]
                self.scan_readings.record_pass1_skipped()
                continue
            n = len(vectors)
            row_group = {self._lm_pass1_names_bytes[i]: vectors[i] for i in range(n)}
            result = _evaluate_pass1_row_group(
                path, rg_idx, row_group, self._compiled_predicate,
                self._sp_pass1_name_to_identity, self._sp_decimal_col_map,
                self._sp_date_col_set, self._sp_timestamp_col_set,
                self._sp_timestamp_unit_map,
                self._sp_pass1_column_names,
            )
            if result.empty:
                continue
            self._total_rows_before_filter += result.rows_before_filter
            if result.survived:
                self._record_pass1_survivor(result, pass2_work, p1_cache)
            else:
                self._record_pass1_skip(result)

        # ── WP-2: top-N reduction (drops row groups with no top-n candidate). ──
        if self._sp_topn_active:
            topn_sort_identity = self._sp_planner_identity[self._topn_sort_name]
            pass2_work, self._lm_topn_winners = self._apply_topn(
                pass2_work, p1_cache, topn_sort_identity,
                int(self._topn_limit), self._topn_descending,
            )

        self._lm_pass2_work = pass2_work
        self._lm_p1_cache = p1_cache
        pass1_src.close()
        self._lm_pass1_src = None
        self._lm_pass2_src = open_pass2_source(
            self._sp_filesystem,
            pass2_work,
            self._sp_pass2_column_names,
            file_sizes=self._sp_file_sizes or None,
            connector=self._sp_connector_type,
            query_id=self._sp_query_id,
            footer_bytes_cache=_FOOTER_CACHE,
            null_fillers=[self._sp_null_filler_by_name[c] for c in self._sp_pass2_column_names],
            string_types=[self._sp_string_type_by_name[c] for c in self._sp_pass2_column_names],
        )
        self._lm_pass1_done = True

    cdef Morsel _latmat_next(self):
        """Native two-pass latmat: run pass-1 to completion on first call, then
        stream pass-2 row groups, combining each with its pass-1 survivors and
        returning one morsel per call. Replaces the read_morsels generator's
        latmat body — no generator frame. Returns None on exhaustion."""
        cdef tuple pulled
        cdef list vectors, combined_identity_names, combined_vectors
        cdef object path, rg_idx, identity, p1_filtered, p1_identity_names
        cdef dict row_group, p1_vectors_by_identity, p2_vectors_by_identity
        cdef Morsel result_morsel
        cdef int64_t rows_after_filter
        cdef Py_ssize_t i, n
        if not self._lm_pass1_done:
            self._run_pass1()
        while True:
            if self._limit_exhausted():
                return self._finish_locked()
            pulled = None if self._lm_pass2_src is None else self._lm_pass2_src.next_vectors()
            if pulled is None:
                return self._finish_locked()
            vectors = pulled[0]
            path = pulled[4]
            rg_idx = pulled[5]
            # Pass-2 runs masked → prefer_dict off → no empty-filtered sentinel.
            # Defensive skip in case that ever changes.
            if vectors is None:
                continue
            # Pass-2 telemetry mirrors the original: bytes only.
            self.scan_readings.record_pass2_decoded(pulled[1])
            self.bytes_in += pulled[1]

            n = len(vectors)
            row_group = {self._lm_pass2_names_bytes[i]: vectors[i] for i in range(n)}
            _coerce_logical_types(
                row_group, self._sp_decimal_col_map,
                self._sp_date_col_set, self._sp_timestamp_col_set,
                self._sp_timestamp_unit_map,
            )

            p1_filtered, p1_identity_names = self._lm_p1_cache.pop((path, rg_idx))
            # WP-2: reduce pass-1 survivors to the same top-n winners pass 2 decoded.
            if self._lm_topn_winners is not None:
                p1_filtered = p1_filtered.take(self._lm_topn_winners[(path, rg_idx)])

            p1_vectors_by_identity = {nm: p1_filtered.column(nm) for nm in p1_identity_names}
            # Positional pairing: pass2_column_names order == row_group.values() order.
            p2_vectors_by_identity = {
                self._sp_pass2_name_to_identity[col]: vec
                for col, vec in zip(self._sp_pass2_column_names, row_group.values())
            }

            if self._sp_output_identity_order:
                combined_identity_names = []
                combined_vectors = []
                for identity in self._sp_output_identity_order:
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

            self._scan_mtx.lock()
            self._total_rows_after_filter += rows_after_filter
            # Already assembled in output_identity_order — no select() needed.
            result_morsel = self._commit_morsel(result_morsel, path)
            self._emitted_any = True
            self._scan_mtx.unlock()
            return result_morsel

    def read_morsels(self):
        """Empty-manifest FALLBACK source generator. Single-pass and two-pass
        latmat scans are served natively by next_morsel; _ensure_scan_started
        routes here only for an empty / zero-file manifest, emitting one empty
        Morsel (mirrors the original guard)."""
        self._ensure_scan_started()
        yield Morsel()

        # Non-empty manifests are served natively (SINGLE or LATMAT); FALLBACK is
        # selected only for an empty manifest, so there is nothing more to yield.
