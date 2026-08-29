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

import heapq as _heapq
import time
from bisect import bisect_left
from copy import deepcopy
from typing import Generator

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io.pass1_predicate_gate import (
    pass1_worker_predicate_admissible as _pass1_worker_predicate_admissible,
)
from opteryx.connectors.parquet_io.pool_reader import open_ipc_source
from opteryx.connectors.parquet_io.pool_reader import open_pass2_source
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.types.logical_type import LogicalCategory
# The single default -> env -> SET resolution point (opteryx/variables.py). Read
# sites call this instead of `config.X` so what SHOW VARIABLES advertises and what
# the engine uses cannot drift.
from opteryx.variables import resolve as _resolve_var

cdef tuple _resolve_http_tuning(variables):
    """Resolve the SET-able http_* variables (default -> env -> SET, via
    `resolve()`) into the 6-tuple CppIOPipeline.__cinit__ expects. Bandwidth is
    stored/SET in Mbps (the human-facing unit) and converted to bytes/s here,
    matching HttpTuning's C++ field.

    The two multiplexing flags are stored as `disable_*` (the state a caller
    normally does NOT want, per variables.py's naming convention) and inverted
    here into the positive sense HttpTuning uses."""
    cdef double _min_bw_mbps = _resolve_var(
        "http_min_bandwidth_mbps", variables, config.HTTP_MIN_BANDWIDTH_MBPS)
    return (
        _resolve_var("http_max_connections_per_host", variables, config.HTTP_MAX_CONNECTIONS_PER_HOST),
        _resolve_var("http_max_retries", variables, config.HTTP_MAX_RETRIES),
        _min_bw_mbps * 1.0e6 / 8.0,
        _resolve_var("http_request_timeout_floor_ms", variables, config.HTTP_REQUEST_TIMEOUT_FLOOR_MS),
        not _resolve_var("disable_http_multiplexing", variables, config.DISABLE_HTTP_MULTIPLEXING),
        _resolve_var("http_pipewait", variables, config.HTTP_PIPEWAIT),
        _resolve_var("disable_http2", variables, config.DISABLE_HTTP2),
    )


cdef tuple _resolve_coalesce_tuning(variables):
    """(waste_ratio, max_bytes) for remote range coalescing — see
    ParquetIOPipeline::set_coalesce_tuning for what each bound is protecting."""
    return (
        _resolve_var("parquet_io_coalesce_waste_ratio", variables,
                     config.PARQUET_IO_COALESCE_WASTE_RATIO),
        _resolve_var("parquet_io_coalesce_max_bytes", variables,
                     config.PARQUET_IO_COALESCE_MAX_BYTES),
    )


cdef int _resolve_in_flight_limit(variables):
    """ABSOLUTE cap on submitted-but-unconsumed row groups; 0 = auto
    (workers + 2). Absolute rather than a delta so "many threads, shallow
    window" is expressible without a negative value."""
    return <int>_resolve_var(
        "parquet_io_in_flight_limit", variables, config.PARQUET_IO_IN_FLIGHT_LIMIT)


# Hoisted out of the per-row-group hot path. Previously these imports happened
# 3× per row group via `from ... import ...` inside the loop body.
import draken.draken_native as _draken_native_parquet
_int64_to_decimal  = _draken_native_parquet.vector_reinterpret_as_decimal
_int64_to_date32   = _draken_native_parquet.vector_reinterpret_as_date32
# Zero-copy retag (moves the just-decoded int64 buffer into a TIMESTAMP64 view).
# Safe here because the decoded column is exclusively owned by the reader and is
# immediately replaced by the retagged result — no other reference survives.
_int64_to_timestamp = _draken_native_parquet.vector_retag_int64_as_timestamp64
# ARRAY<TIMESTAMP> child retag — IN-PLACE (mutates the vector, returns None),
# unlike the scalar retags above which return a new Vector to rebind.
_array_child_to_timestamp = _draken_native_parquet.vector_retag_array_child_as_timestamp64
# IPv4: Parquet stores an address column as a plain uint32 (deliberately — the
# files stay readable by tools that have never heard of the type), so the
# IPv4-ness comes from the catalog and this attaches it. Zero-copy: the physical
# tag is already correct, only the descriptor is missing.
_uint32_to_ipv4 = _draken_native_parquet.vector_retag_uint32_as_ipv4

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
from opteryx.expression.evaluator.evaluation import get_pass1_eval_fn_ptr as _get_pass1_eval_fn_ptr
from opteryx.expression.evaluator.evaluation import Pass1PredResolver as _Pass1PredResolver
from opteryx.expression.evaluator.evaluation import filter_morsel_c_native as _filter_morsel_c_native
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


def scan_footer_bytes_cache():
    """The process-wide footer-envelope byte cache this node reads through. Exposed
    so the plan-time native-scan path shares ONE cache with the trampoline path
    rather than warming a second copy of the same envelopes."""
    return _FOOTER_CACHE


def resolve_scan_filesystem(connector, blob_paths):
    """Resolve (filesystem, connector_type) for a parquet scan's blobs.

    Shared by the execution-time trampoline setup (`_ensure_scan_started`) and the
    plan-time native-scan gate (the compiler), which must agree on BOTH values: the
    filesystem supplies the signed-URL rewrite that decides whether a remote scan is
    even eligible for the native Source, and the connector type picks the IO worker
    budget. Two copies of this would drift into the two paths disagreeing about
    whether a given scan can go native."""
    filesystem = getattr(connector, "filesystem", None)
    if filesystem is not None:
        return filesystem, (
            getattr(connector, "storage_type", None) or connector.__type__
        )
    from opteryx.connectors.io_systems import create_filesystem

    first_path = blob_paths[0] if blob_paths else ""
    protocol = first_path.split("://")[0] if "://" in first_path else ""
    return create_filesystem(protocol), (protocol.upper() if protocol else "FILESYSTEM")


cdef class ScanReadings:
    """Zero-overhead telemetry accumulator for the Parquet scan hot path.

    Replaces per-row-group Python dict writes (self.readings) with direct
    C-level field assignments. flush_into() transfers everything to the Python
    dict once at scan completion so external consumers see the same interface.
    """
    # ── Additive: from the decode path ───────────────────────────────────────
    cdef public int64_t time_parquet_read_ranges_ns
    cdef public int64_t time_parquet_decode_columns_ns

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
    cdef public int64_t empty_datasets
    # ── Merge-on-read deletes ────────────────────────────────────────────────
    # mor_delete_files: files in this scan carrying a delete vector (set once
    # at scan start). mor_rows_deleted: rows the vectors removed from emitted
    # row groups. Reported only when the scan has delete debt — see flush_into.
    cdef public int64_t mor_delete_files
    cdef public int64_t mor_rows_deleted

    # ── Mutation API ─────────────────────────────────────────────────────────
    # All accumulation goes through the methods below rather than direct field
    # writes. This narrows the mutation surface to one class for future thread
    # safety (best-effort under concurrent writers, per the telemetry contract).

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

    cpdef void record_mor_delete_files(self, int64_t file_count):
        self.mor_delete_files = file_count

    cpdef void record_mor_rows_deleted(self, int64_t rows):
        self.mor_rows_deleted += rows

    cpdef void record_filter_totals(self, int64_t rows_before):
        # rows_after (post-relocated-filter survivor count) is intentionally not
        # kept here — it duplicated the downstream ExprFilter operator's own
        # records_in/out and confused rows-in-vs-rows-out readings across scan
        # sources. This scan node tracks only rows fed into it.
        self.parquet_rows_before_filter += rows_before

    cpdef void flush_into(self, object readings):
        readings["time_parquet_read_ranges_ns"]        = self.time_parquet_read_ranges_ns
        readings["time_parquet_decode_columns_ns"]     = self.time_parquet_decode_columns_ns
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
        readings["empty_datasets"]                     = self.empty_datasets
        # Present only when merge-on-read deletes were in play, so the
        # ordinary no-debt scan's telemetry is unchanged — absence means "no
        # delete vectors", exactly like the manifest columns themselves.
        if self.mor_delete_files:
            readings["mor_delete_files"]               = self.mor_delete_files
            readings["mor_rows_deleted"]               = self.mor_rows_deleted


cdef inline void _coerce_logical_types(
    dict row_group,
    dict decimal_col_map,
    set date_col_set,
    set timestamp_col_set,
    dict timestamp_unit_map,
    set ipv4_col_set,
    dict array_ts_unit_map,
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
            # DATE is physical int32 and now decodes at that width (E33 exact-width
            # integers), so accept INT32 as well as INT64.
            if (v_nb.type == _draken_native_parquet.INT64
                    or v_nb.type == _draken_native_parquet.INT32):
                row_group[col_name] = _int64_to_date32(v_nb)
    if timestamp_col_set:
        for col_name in timestamp_col_set:
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type == _draken_native_parquet.INT64:
                row_group[col_name] = _int64_to_timestamp(v_nb, timestamp_unit_map.get(col_name, "us"))
    if array_ts_unit_map:
        # ARRAY<TIMESTAMP>: retag the CHILD in place — no rebind, the parent Vector
        # is unchanged (see vector_retag_array_child_as_timestamp64).
        for col_name, unit_str in array_ts_unit_map.items():
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            if v_nb.type == _draken_native_parquet.ARRAY:
                _array_child_to_timestamp(v_nb, unit_str)
    if ipv4_col_set:
        for col_name in ipv4_col_set:
            v = row_group.get(col_name)
            if v is None:
                continue
            v_nb = (<_DrakenShimVector>v)._nb if isinstance(v, _DrakenShimVector) else v
            # Guarded on UINT32: a catalog that declares IPv4 over a column the
            # file actually stores as something else is a schema/data mismatch,
            # and silently retagging it would reinterpret unrelated bytes as
            # addresses. Leaving it untouched surfaces the real type instead.
            if v_nb.type == _draken_native_parquet.UINT32:
                row_group[col_name] = _uint32_to_ipv4(v_nb)


cdef inline tuple _topn_rank(object v):
    """Order-preserving sort rank for one top-n candidate value.

    A three-tier universal scale, lowest to highest: NULL (tag 0) < any real value
    (tag 1) < NaN (tag 2) — draken's actual ordering (`draken/morsels/sort.hpp`):
    `SortKeyCmp` sorts NULL below every value (NULLs FIRST ascending, LAST
    descending, via the `cmp = va ? 1 : -1` validity check), and `sort_num_key`
    maps NaN to `UINT64_MAX` so it sorts as the single highest key regardless of
    sign (NaN LAST ascending, FIRST descending — "sorts highest" applied through
    whichever direction flip is in effect). The downstream HeapSort applies both
    rules, so `_apply_topn`'s reduction has to see the same ranking or it can
    throw away rows HeapSort would have kept.

    Encoding both as the leading tuple tag means the boundary test in
    `_apply_topn` is one plain tuple comparison with no null/NaN branch, so
    neither rule can be stated inconsistently in two places — and, critically,
    `float('nan') <=/>= x` is never evaluated directly: Python defines EVERY such
    comparison as False, which previously made `_apply_topn` drop NaN rows
    outright (observed: an ASC top-10 over 3 NaNs + real values collapsed to a
    single returned row).

    Tuple comparison stops at the first differing tag, so `(0,)` / `(1, x)` /
    `(2,)` never compare their second element against a different tier — in
    particular `None < None` and any comparison touching a NaN payload are never
    evaluated.
    """
    if v is None:
        return (0,)
    if v != v:   # true only for NaN — every other value in this column is self-equal
        return (2,)
    return (1, v)


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
    set ipv4_col_set,
    dict array_ts_unit_map,
    list pass1_column_names,
    bytes precomputed_mask=None,
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

    _coerce_logical_types(row_group, decimal_col_map, date_col_set, timestamp_col_set,
                          timestamp_unit_map, ipv4_col_set, array_ts_unit_map)

    # Positional pairing: column order in the data dict matches pass1_column_names order.
    # C++ preserves column order; dict keys (bytes) are not used for identity lookup.
    cdef list p1_identity_names = [pass1_name_to_identity[col] for col in pass1_column_names]
    cdef list p1_vectors = list(row_group.values())
    cdef list positions
    if not p1_identity_names:
        result.empty = True
        return result

    # Worker-computed survivor mask (Q24 latmat, pushed pass-1 predicate): the match
    # already ran in parallel on the decode worker — just materialise the survivors.
    # No predicate eval here. mask_bytes is LSB-first over the RG rows.
    if precomputed_mask is not None:
        result.rows_before_filter = len(p1_vectors[0]) if p1_vectors else 0
        positions = _set_bit_positions(precomputed_mask)
        # Drop any trailing padding bits beyond the logical row count.
        while positions and <Py_ssize_t>positions[len(positions) - 1] >= result.rows_before_filter:
            positions.pop()
        if not positions:
            return result
        result.survived = True
        result.p1_filtered = Morsel.from_vectors(p1_identity_names, p1_vectors).take(positions)
        result.p1_identity_names = p1_identity_names
        result.mask_bytes = precomputed_mask
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
    # ── Chunked (non-topn) pass-1/pass-2 pipelining ────────────────────────────
    # topn_active scans need every row group's sort key before the global top-n
    # cutoff is known, so _run_pass1's full drain-then-open-pass2 barrier is a
    # real requirement there. Without topn, each row group's pass-1 survival is
    # already a local decision — draining all of pass1_src before pass 2 can
    # start buys nothing and holds the whole table's filter-column data (e.g. a
    # wide VARCHAR predicate column) in _lm_p1_cache when the predicate has weak
    # selectivity. _run_pass1_chunk flushes pass 1 -> pass 2 in small chunks
    # instead, bounding that buffer to PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER
    # row groups regardless of table size or selectivity.
    cdef list _lm_pending_morsels         # combined morsels ready to return, FIFO
    cdef bint _lm_chunked_pass1_exhausted # pass1_src truly out of row groups (chunked path only)
    cdef int64_t _decode_start_ns
    cdef int64_t _total_rows_before_filter
    cdef bint _scan_finished              # decode-telemetry flushed once
    cdef bint _emitted_any                # any morsel returned (empty-result guard)
    cdef bint _empty_guard_done           # the single empty morsel already returned
    cdef list _sp_row_identity_names      # identities for the synthesized $file/$ordinal
                                           # columns, appended AFTER the read columns.
                                           # Empty list when the scan was not asked for
                                           # row identity — see constants/row_identity.
    cdef dict _sp_file_index              # data-file path -> its index in _sp_blob_paths,
                                           # the value $file carries.
    cdef dict _sp_delete_positions        # path -> sorted tuple of file-global deleted
                                           # row ordinals (merge-on-read delete vectors,
                                           # resolved onto FileEntry at binding). Empty
                                           # dict when the scan has no delete debt.
    cdef dict _sp_rg_offset_cache         # path -> (prefix_offsets, rg_row_counts)
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
    cdef dict _sp_array_ts_unit_map   # ARRAY<TIMESTAMP> cols -> unit, for the child retag
    cdef set _sp_ipv4_col_set
    cdef object _sp_predicate_stats
    cdef list _sp_pass1_column_names
    cdef list _sp_pass2_column_names
    cdef dict _sp_pass1_name_to_identity
    cdef dict _sp_pass2_name_to_identity
    cdef dict _sp_null_filler_by_name     # schema-evolution typed NULL-fill, by physical column name
    cdef dict _sp_string_type_by_name     # declared DrakenType (VARCHAR/NVARCHAR/VARBINARY), by physical column name
    cdef bint _sp_topn_active
    cdef bint _sp_two_pass_eligible
    # Snapshot of _lm_pass1_src.pruned_row_group_count taken by _run_pass1 /
    # _run_pass1_chunk right before they close and null _lm_pass1_src — both
    # retire pass-1's source long before close_source() runs, so by then the
    # live count is already gone; close_source() reads this instead.
    cdef int64_t _sp_pass1_pruned_row_groups
    # Bytecode for the pushed predicate, LOWERED AT PLAN TIME by the compiler
    # (`_compile_scan`) and handed to the scan. The scan never lowers an
    # expression itself: doing so bypassed the plan-time rewrite chain
    # (CASE->IF_THEN_ELSE, BETWEEN->compares, decimal-literal rescale) and
    # silently produced wrong answers for off-scale decimal compares.
    cdef public CompiledBytecode compiled_predicate
    cdef object _pass1_resolver           # Pass1PredResolver, kept alive for the scan
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
    # Column identities proven to be read only through length-answerable
    # operations (set by LengthOnlyColumnStrategy via node properties). The
    # decoder skips long-value byte copies for these.
    cdef public object _length_only_columns

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
        self._lm_pending_morsels = []
        self._lm_chunked_pass1_exhausted = False
        self._decode_start_ns = 0
        self._total_rows_before_filter = 0
        self._scan_finished = False
        self._emitted_any = False
        self._empty_guard_done = False
        self._sp_claims_pending = 0
        self._sp_pass1_pruned_row_groups = 0
        self.compiled_predicate = None  # set at plan time by compiler._compile_scan
        self._pass1_resolver = None
        self._planner_name_to_identity_cached = None  # Cache name-to-identity mapping
        self._filter_column_names_cached = None  # Cache filter column names extraction
        self.scan_readings = ScanReadings()
        # WP-2: physical sort column name, direction, and N. None unless the
        # optimizer matched ORDER BY <physical col> LIMIT n directly over this scan.
        self._topn_sort_name = parameters.get("topn_sort_name")
        self._topn_descending = bool(parameters.get("topn_descending", False))
        self._topn_limit = parameters.get("topn_limit")
        self._length_only_columns = parameters.get("length_only_columns")
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
        # ReaderNode.sensors() sets base["dataset"] from self.dataset, which is
        # never populated on this class (the planner only passes "connector",
        # not "dataset", for Parquet scans) — self.connector.dataset is the
        # only place the real dataset name lives, and is what the old
        # to_mermaid() read directly instead of going through sensors().
        if self.connector is not None:
            base["dataset"] = self.connector.dataset
        base["row_groups_read"] = self.readings.get("row_groups_read", 0)
        # Written by close_source() from IpcRowGroupSource.pruned_row_group_count
        # (pushed-predicate min/max + bloom pruning, counted where open_ipc_source
        # excludes a row group before submission) — 0 whenever there's no pushed
        # predicate, same meaning as the native path's row_groups_pruned.
        base["row_groups_pruned"] = self.readings.get("parquet_row_groups_pruned", 0)
        base["files_read"] = self.readings.get("files_read", 0)
        # No filter/projection column-count sensors here. `parquet_filter_columns_read`
        # had no producer on any path, and `parquet_projection_columns_read` is owned by
        # the NATIVE scan (telemetry harvest sets it from scan facts) — publishing a
        # hard 0 for it here would claim this scan read no projection columns, which is
        # a lie, not a default. Both are plan-time column-set sizes, not runtime
        # measurements; the read column list already lives in the operator's `config`.
        base["parquet_rows_before_filter"] = self.readings.get("parquet_rows_before_filter", 0)
        decode_ns = self.readings.get("time_decoding_blobs", 0)
        if decode_ns > 0 and base["row_groups_read"] > 0:
            base["rowgroups_completed_per_s"] = base["row_groups_read"] / (
                decode_ns / 1_000_000_000
            )
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
            # No `parquet_latmat_skipped_pages`/`decoded_pages`/`page_skip_ratio` here.
            # Unlike row_groups_pruned above, page-level skip/decode counts have no
            # producer anywhere on this scan's path: the trampoline's decode result
            # (MorselRef, rugo/src/parquet/io_pipeline.hpp) carries no page fields at
            # all — that data only exists on the OTHER native scan's ScanRowGroup
            # struct (rugo/src/parquet/parquet_reader.pxi), an architecturally
            # separate path this scan never touches. Populating it would mean adding
            # new C++ page-level instrumentation, not wiring up an existing value —
            # a real feature, not a sensor fix; not done without agreeing the design.
            base["parquet_latmat_skip_ratio"] = (
                self.readings.get("parquet_latmat_skipped_row_groups", 0) / lm_pass1
            )
        return base

    @staticmethod
    def _extract_filter_references(predicates):
        """What the pushed-down predicates read, as (identities, names).

        A predicate operand is resolved to a physical column by IDENTITY where it
        has one, and only by name where it does not. The two can disagree: an
        aliased projection keeps the source column's identity but takes the ALIAS as
        its name, so `SELECT ts AS e1 ...` gives a predicate operand named `e1`
        carrying `ts`'s identity. Reading by name then looked for a physical column
        called `e1`, found none, and decoded nothing — while the compiled predicate,
        which resolves its operands by identity, asked the morsel for `ts` and got
        `KeyError: 'CxxMorsel.column: not found'`. Any filter on a subquery column
        the outer query does not project reproduces it:
        `SELECT flag FROM (SELECT ts AS e1, flag FROM t) AS sub WHERE e1 IS NOT NULL`.

        The caller matches the identities against the scan's own schema, which is
        where the physical name and the planner identity are both known and agree.
        """
        identities = set()
        names = set()
        if not predicates:
            return identities, names
        for predicate in predicates:
            identifiers = get_all_nodes_of_type(predicate, select_nodes=(NodeType.IDENTIFIER,))
            for identifier in identifiers:
                schema_column = getattr(identifier, "schema_column", None)
                identity = getattr(schema_column, "identity", None)
                if identity is not None:
                    identities.add(identity)
                    continue
                name = getattr(schema_column, "name", None) or getattr(identifier, "source_column", None)
                if name:
                    names.add(name)
        return identities, names

    cdef shared_ptr[CxxMorsel] _cxx_apply_predicate(self, shared_ptr[CxxMorsel] m):
        """S-B.2 prereq #2: apply the compiled predicate to a CxxMorsel, returning the
        filtered NATIVE carrier (no PyObject column materialization).

        All-c-native predicates take the one-nogil-span path (filter_morsel_c_native:
        predicate result DV* feeds straight into cxx_mask_c, no Python BoolVector);
        anything else falls back to the VM + filter_mask over a thin Cxx-backed shim.
        The intermediate Morsels are Cxx-backed wrappers (no data copy); morsel_to_cxx
        is a shallow copy. Lets the scan filter without leaving the CxxMorsel substrate.
        """
        if self.compiled_predicate is None:
            return m
        cdef Morsel shim = cxx_to_morsel(m)
        cdef object res = _filter_morsel_c_native(self.compiled_predicate, shim)
        if res is None:
            res = shim.filter_mask(execute_bytecode(self.compiled_predicate, shim))
        return morsel_to_cxx(<Morsel>res)

    def _apply_predicates_to_morsel(self, morsel: Morsel):
        """Apply the compiled predicate to a Draken Morsel.

        Routes through the CxxMorsel-native filter (_cxx_apply_predicate): the
        substrate never materializes PyObject columns. The predicate is compiled once
        at execute() time; this just applies it.
        """
        if self.compiled_predicate is None:
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
        matches Morsel.nbytes' real per-vector footprint (via cxx_morsel_nbytes)."""
        cdef int64_t num_rows = cxm.get().num_rows()
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
        # Real footprint of the (possibly LIMIT-sliced) emitted morsel — string
        # arena included — not rows × cols × 8.
        self.scan_readings.record_morsel_yielded(
            num_rows, <int64_t>cxx_morsel_nbytes(cxm.get()), files_seen)
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
        with nogil:
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
        best (i.e. n rows plus any ties exactly at the boundary). Every row dropped
        here is strictly worse than the true top-n, so the downstream HeapSort
        produces an identical result to the un-pushed plan — ties at the boundary
        can never change which n rows HeapSort finally keeps.

        Returns (new_pass2_work, winners_by_rg):
          - new_pass2_work: [(path, rg_idx, reduced_mask_bytes)] for winning row
            groups only (row groups with no surviving top-n candidate are dropped
            and never decoded in pass 2).
          - winners_by_rg: {(path, rg_idx): [survivor_idx, ...]} (ascending), used
            to gather the matching pass-1 column values for assembly.

        ── NULL ordering ────────────────────────────────────────────────────────
        NULL sorts BELOW every value, so `ORDER BY col` puts NULLs FIRST and
        `ORDER BY col DESC` puts them LAST. That is what the downstream HeapSort
        actually does — draken's `SortKeyCmp` (draken/morsels/sort.hpp) compares
        validity first with `cmp = va ? 1 : -1`, and DESC flips the whole
        comparator, null arm included.

        This used to read "NULLs sort last" in BOTH directions and dropped every
        NULL survivor whenever more than n non-null survivors existed. Ascending,
        those NULLs are the BEST rows — so the reduction was deleting rows that
        belong in the answer and this path returned rows the un-pushed plan does
        not (fixture with 3 NULLs, `ORDER BY k LIMIT 10`: it returned
        `[1003…1012]` where the correct answer is `[NULL, NULL, NULL, 1003…1009]`).

        ── NaN ordering ─────────────────────────────────────────────────────────
        A FLOAT sort key containing NaN had the SAME class of bug, the other
        direction: draken sorts NaN HIGHEST regardless of sign
        (`sort_num_key` -> `UINT64_MAX`), but Python defines every comparison
        against NaN as False, so `v <= boundary` / `v >= boundary` was silently
        False for a NaN survivor no matter where the true boundary sat. Observed:
        an ASC top-10 over 3 NaN keys + several thousand real ones collapsed to a
        SINGLE returned row (every NaN candidate compared "not <= boundary" and
        got dropped, and most real candidates never got a chance to become the
        boundary because the initial `nlargest`/`nsmallest` selection was itself
        corrupted by the same NaN comparisons).

        The rule for BOTH is now expressed ONCE, as a rank, instead of as
        hand-written null/NaN branches: `_topn_rank` maps NULL below every value
        and NaN above every value (never comparing a NaN payload directly), so
        the boundary test is one plain rank comparison. Ties, NULLs, NaN, and the
        fewer-than-n degenerate case all fall out of it — there is no separate
        branch left to get wrong for either. This matches the native twin,
        `LatmatScanSource::reduce_to_topn`, which already had both properties for
        free by reducing with draken's own comparator (`sort_num_key` itself maps
        NaN to `UINT64_MAX`, so the native path never had this bug).
        """
        cdef list candidates = []          # (rank, (path, rg_idx), survivor_idx)
        cdef Py_ssize_t i
        for key in p1_cache:
            p1_filtered = p1_cache[key][0]
            vals = p1_filtered.column(sort_identity).to_pylist()
            for i in range(len(vals)):
                candidates.append((_topn_rank(vals[i]), key, i))

        cdef dict winners_by_rg = {}
        if len(candidates) <= n:
            # Nothing can be cut — every survivor is at-least-as-good as the n-th.
            for (r, key, i) in candidates:
                winners_by_rg.setdefault(key, []).append(i)
        else:
            # The n-th best rank. nsmallest/nlargest is O(m log n) against the old
            # full sort's O(m log m) — m is every pass-1 survivor in the table.
            if descending:
                boundary = _heapq.nlargest(n, (r for (r, key, i) in candidates))[n - 1]
            else:
                boundary = _heapq.nsmallest(n, (r for (r, key, i) in candidates))[n - 1]
            for (r, key, i) in candidates:
                # Keep everything not STRICTLY worse than the boundary — which is
                # the n best plus every row tied with the n-th.
                if (r >= boundary) if descending else (r <= boundary):
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

    def io_diagnostics(self):
        """IO-pipeline diagnostics (GCS/HTTP request count, retries, latency
        histogram, worker_blocked_ns) for this scan's active source(s). MUST be
        read before close_source() drops the source references. Returns {} for the
        empty-manifest fallback (no source opened) or once closed."""
        if self._scan_mode == _SCAN_SINGLE and self._ipc_source is not None:
            return self._ipc_source.diagnostics()
        if self._scan_mode == _SCAN_LATMAT:
            diags = {}
            if self._lm_pass1_src is not None:
                diags = dict(self._lm_pass1_src.diagnostics())
            if self._lm_pass2_src is not None:
                # Sum the two passes' scalar counters; keep pass-2's histogram shape.
                p2 = self._lm_pass2_src.diagnostics()
                for k, v in p2.items():
                    if isinstance(v, (int, float)) and isinstance(diags.get(k), (int, float)):
                        diags[k] += v
                    else:
                        diags[k] = v
            return diags
        return {}

    cpdef void close_source(self) except *:
        """Flush decode telemetry and release the source(s) on every exit path.
        The native single-pass and latmat paths own their flush + source close;
        the empty-manifest fallback flushes nothing (it never opened a source)."""
        cdef object src, src1, src2
        if self._scan_mode == _SCAN_SINGLE:
            # Lock so a concurrent late puller can't race the flush/close-once.
            with nogil:
                self._scan_mtx.lock()
            if not self._scan_finished:
                self._scan_finished = True
                self._flush_decode_telemetry()
                self.scan_readings.flush_into(self.readings)
                if self._ipc_source is not None:
                    self.readings["parquet_row_groups_pruned"] = self._ipc_source.pruned_row_group_count
            src = self._ipc_source
            self._ipc_source = None
            self._scan_mtx.unlock()
            if src is not None:
                src.close()
            return
        if self._scan_mode == _SCAN_LATMAT:
            with nogil:
                self._scan_mtx.lock()
            if not self._scan_finished:
                self._scan_finished = True
                self._flush_decode_telemetry()
                self.scan_readings.flush_into(self.readings)
                # _run_pass1 / _run_pass1_chunk retire (close + null) _lm_pass1_src
                # long before the scan finishes draining pass 2, so it is almost
                # always already None here — fall back to the snapshot they took.
                # Prefer the live source on the rare path where pass 1 never ran
                # (e.g. the scan is closed before its first pull).
                if self._lm_pass1_src is not None:
                    self.readings["parquet_row_groups_pruned"] = self._lm_pass1_src.pruned_row_group_count
                else:
                    self.readings["parquet_row_groups_pruned"] = self._sp_pass1_pruned_row_groups
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
        native `scan_readings` only. Called once from close_source, under
        `_scan_mtx`, before `scan_readings.flush_into(self.readings)` mirrors the
        complete native record into the Python `readings` dict that sensors()
        reads. This is a one-time finalization touch (see `_record_morsel_emitted`'s
        docstring for why the hot loop itself never touches Python object state)."""
        cdef int64_t decode_ns = <int64_t>time.monotonic_ns() - self._decode_start_ns
        self.scan_readings.record_decode_time(decode_ns)
        self.scan_readings.record_filter_totals(self._total_rows_before_filter)

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
            _pred_identities, _pred_names = self._extract_filter_references(self.predicates)
            # Resolve each predicate operand against THIS scan's schema, where the
            # physical name and the planner identity sit on the same column.
            self._filter_column_names_cached = {
                col.name: col.identity
                for col in base_schema.columns
                if col.identity in _pred_identities or col.name in _pred_names
            }
        _filter_name_to_identity = self._filter_column_names_cached
        required_names = set(_planner_name_to_identity.keys()) | set(_filter_name_to_identity.keys())
        # Predicate-only columns are read but NOT emitted, so they never appear in
        # `_planner_name_to_identity` (which is built from the projection). They
        # still have to be LABELLED with the identity the compiled predicate will
        # ask for. A column that is both projected and a predicate input keeps the
        # projection's identity — the two agree there anyway.
        _read_name_to_identity = dict(_filter_name_to_identity)
        _read_name_to_identity.update(_planner_name_to_identity)

        # Select physical columns to read by NAME, not by identity.
        read_schema = deepcopy(base_schema)
        read_schema.columns = [c for c in base_schema.columns if c.name in required_names]
        # Row identity ($file/$ordinal) is SYNTHESIZED, not read: the columns are
        # not in the data file. Resolved here, before the two-pass gate below
        # consults it — a scan emitting an ordinal cannot use the two-pass path.
        from opteryx.constants.row_identity import ROW_IDENTITY_COLUMNS

        _identity_cols = [c for c in read_schema.columns if c.name in ROW_IDENTITY_COLUMNS]
        # Ordering is the contract: $file then $ordinal, matching the order
        # `_row_identity_vectors` builds them and visit_scan appended them.
        _identity_cols.sort(key=lambda c: ROW_IDENTITY_COLUMNS.index(c.name))
        self._sp_row_identity_names = [c.identity for c in _identity_cols]

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
        # IPv4 keys on the DESCRIPTOR, not the category: IPv4's category IS
        # INTEGER (deliberately — that is what makes ordering, grouping and joins
        # work on the raw uint32), so a category test would sweep in every plain
        # integer column and retag them all as addresses.
        from draken.draken_native import LogicalKind as _LogicalKind
        self._sp_ipv4_col_set = {
            col.name.encode('utf-8') for col in base_schema.columns
            if col.column_type is not None
            and col.column_type.logical is not None
            and col.column_type.logical.kind == _LogicalKind.IPV4
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
        # ARRAY<TIMESTAMP> columns need the same retag applied to their CHILD: the
        # list decoder yields an INT64 leaf (physical), and IPC carries no logical
        # type, so without this the elements stay raw micros. Keyed like the scalar
        # map; the unit comes from the ELEMENT's descriptor.
        self._sp_array_ts_unit_map = {}
        for col in base_schema.columns:
            ct = col.column_type
            if ct is None or ct.category != _LC.ARRAY or ct.element is None:
                continue
            if ct.element.category != _LC.TIMESTAMP:
                continue
            unit_str = "us"
            if ct.element.logical is not None and ct.element.logical.unit is not None:
                unit_str = _TS_UNIT_BY_NAME.get(ct.element.logical.unit.name, "us")
            self._sp_array_ts_unit_map[col.name.encode('utf-8')] = unit_str
        # The compiler lowers the pushed predicate at PLAN time and hands it over.
        # A scan carrying predicates with no bytecode would silently emit UNFILTERED
        # rows — fail loud instead.
        cdef bint has_predicates = bool(self.predicates)
        if has_predicates and self.compiled_predicate is None:
            raise RuntimeError(
                "ParquetReadNode: pushed predicates present but no compiled_predicate "
                "was bound at plan time (compiler._compile_scan must lower it)"
            )

        # ── Two-pass late-materialization eligibility ─────────────────────────
        _filter_names = set(_filter_name_to_identity.keys())
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

        # Cheap, file-stats-based selectivity estimate (no I/O beyond what's
        # already in the manifest) -- skip two-pass when the predicate isn't
        # expected to prune enough of the table to be worth the pass-1/pass-2
        # split. A weak predicate still forces pass 1 to decode its filter
        # columns for essentially the whole table before pass 2 can start; for
        # a wide/string filter column that can cost more than just reading
        # everything in one single pass. Estimation failures fail open.
        #
        # self.predicates is a list of separately-pushed conjuncts (implicitly
        # ANDed), not a single Node -- Manifest.estimate_selectivity expects one
        # Node with .node_type/.left/.right etc. Passing the list directly makes
        # getattr(list, 'node_type', None) return None, which silently falls
        # through to the estimator's "unknown -> assume everything matches"
        # default (1.0) regardless of the actual predicate. Estimate each
        # conjunct separately and combine like AND does elsewhere in
        # cost_estimation/selectivity.py (multiply independent selectivities).
        cdef object _selectivity_estimate = None
        cdef object _pred_node
        if has_predicates and self.manifest is not None:
            try:
                _selectivity_estimate = 1.0
                for _pred_node in self.predicates:
                    _selectivity_estimate *= self.manifest.estimate_selectivity(_pred_node)
            except Exception:
                _selectivity_estimate = None

        # ── Merge-on-read delete vectors ──────────────────────────────────────
        # Collected once per scan from the manifest's FileEntry rows. A file
        # that reports delete debt without resolved positions is refused —
        # scanning it would serve the deleted rows back. When ANY file carries
        # deletes the whole scan takes the single-pass path (the latmat gate
        # below): pass-1/pass-2 masks compose with the delete filter at the
        # worker layer, which is machinery the debt does not yet justify —
        # correctness first, the two-pass optimisation can learn deletes later.
        self._sp_delete_positions = {}
        self._sp_rg_offset_cache = {}
        _mf_files = getattr(self.manifest, "files", None) if self.manifest else None
        if _mf_files:
            for _fe in _mf_files:
                _drc = getattr(_fe, "deleted_record_count", 0)
                if not _drc:
                    continue
                _pos = getattr(_fe, "delete_positions", None)
                if _pos is None:
                    raise RuntimeError(
                        f"ParquetReadNode: {_fe.file_path} reports {_drc} deleted rows but "
                        "no delete vector was resolved at binding; refusing to scan and "
                        "serve deleted rows."
                    )
                self._sp_delete_positions[_fe.file_path] = tuple(_pos)
        if self._sp_delete_positions:
            self.scan_readings.record_mor_delete_files(len(self._sp_delete_positions))

        two_pass_eligible = (
            config.features.parquet_late_materialization
            and not self._sp_delete_positions
            # $ordinal is a row's POSITION in its file, and it is only that while
            # row position still equals file ordinal. Pass 2 renumbers rows, so an
            # ordinal produced across the two passes would address a different
            # row — a silent wrong answer that MERGE would then mark deleted.
            and not self._sp_row_identity_names
            and has_predicates
            and bool(_filter_names)
            and bool(_pass2_names)
            and (
                _selectivity_estimate is None
                # Resolved through the full default -> env -> SET chain, so
                # `SET parquet_late_materialization_max_selectivity` takes effect for
                # this query. Falls back to the config constant when this operator was
                # built without a session (EXPLAIN-only, direct-construction tests).
                or _selectivity_estimate <= _resolve_var(
                    "parquet_late_materialization_max_selectivity",
                    getattr(self.properties, "variables", None),
                    config.PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY,
                )
            )
        )
        topn_active = topn_active and two_pass_eligible
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

        filesystem, connector_type = resolve_scan_filesystem(self.connector, blob_paths)
        self._sp_filesystem = filesystem
        self._sp_connector_type = connector_type

        # The identity columns leave the READ list — the reader must never be
        # asked for a column the file does not hold. Their vectors are appended
        # per row group instead, keeping `_sp_identity_names`'s positional
        # pairing intact across both halves.
        _read_cols = [c for c in read_schema.columns if c.name not in ROW_IDENTITY_COLUMNS]
        self._sp_file_index = {p: i for i, p in enumerate(blob_paths)}

        column_names = [col.name for col in _read_cols]
        self._sp_column_names = column_names
        self._sp_name_to_identity = {
            col.name: _read_name_to_identity.get(col.name, col.identity)
            for col in _read_cols
        }
        # Identity order is invariant across row groups (positional pairing with
        # row_group.values()); compute once instead of per morsel.
        self._sp_identity_names = [
            self._sp_name_to_identity[col] for col in column_names
        ] + self._sp_row_identity_names
        # Positional logical-type coercion plan (kind, arg) per column, computed
        # once. kind 0=none, 1=decimal(prec,scale), 2=date32, 3=timestamp,
        # 4=array<timestamp>, 5=ipv4. Empty of real work for pure numeric scans →
        # coercion is skipped entirely.
        #
        # EVERY set `_coerce_logical_types` consults must have an arm here.
        # `_coerce_vectors` is that function's positional twin, and a coercion
        # present in one and absent from the other does not fail — it silently
        # returns the physical type on whichever path is taken. IPV4 was missing
        # here for exactly that reason: it is the only kind whose absence leaves a
        # perfectly well-formed column (a bare UINT32), so nothing downstream
        # could notice. Measured against home.network.netflow 2026-08-19: a file
        # that fails the native scan's footer gate falls back to this path and
        # served addresses as integers all the way to the API.
        self._sp_coerce_ops = []
        for col in column_names:
            col_b = col.encode('utf-8')
            if col_b in self._sp_decimal_col_map:
                self._sp_coerce_ops.append((1, self._sp_decimal_col_map[col_b]))
            elif col_b in self._sp_date_col_set:
                self._sp_coerce_ops.append((2, None))
            elif col_b in self._sp_timestamp_col_set:
                self._sp_coerce_ops.append((3, self._sp_timestamp_unit_map.get(col_b, "us")))
            elif col_b in self._sp_array_ts_unit_map:
                self._sp_coerce_ops.append((4, self._sp_array_ts_unit_map[col_b]))
            elif col_b in self._sp_ipv4_col_set:
                self._sp_coerce_ops.append((5, None))
            else:
                self._sp_coerce_ops.append((0, None))
        self._sp_needs_coerce = any(op[0] != 0 for op in self._sp_coerce_ops)
        self._sp_query_id = getattr(self.properties, "query_id", None)

        self._decode_start_ns = <int64_t>time.monotonic_ns()
        self._total_rows_before_filter = 0

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
                decode_workers=_resolve_var(
                    "parquet_gcs_io_workers", getattr(self.properties, "variables", None),
                    config.PARQUET_GCS_IO_WORKERS,
                ) if connector_type in ("GCS", "GS", "S3") else config.resolve_parquet_local_io_workers(
                    _resolve_var(
                        "parquet_local_io_workers", getattr(self.properties, "variables", None),
                        config.PARQUET_LOCAL_IO_WORKERS,
                    )
                ),
                predicates=self._sp_predicate_stats,
                file_sizes=file_sizes or None,
                connector=connector_type,
                query_id=self._sp_query_id,
                footer_bytes_cache=_FOOTER_CACHE,
                null_fillers=[self._sp_null_filler_by_name[c] for c in self._sp_pass1_column_names],
                string_types=[self._sp_string_type_by_name[c] for c in self._sp_pass1_column_names],
                http_tuning=_resolve_http_tuning(getattr(self.properties, "variables", None)),
                in_flight_limit_override=_resolve_in_flight_limit(getattr(self.properties, "variables", None)),
                coalesce_tuning=_resolve_coalesce_tuning(getattr(self.properties, "variables", None)),
            )
            # Q24 latmat: push the pass-1 predicate to the decode workers so the match
            # runs in parallel there (nogil), not serially on this thread. Only when the
            # predicate is fully c-native (the worker VM requires it); rugo silently
            # falls back to serial for column shapes it can't view (survivor_mask empty).
            _col_type_by_name = {c.name: c.column_type for c in base_schema.columns}
            if (
                self.compiled_predicate is not None
                and self.compiled_predicate.is_all_c_native
                # The resolver needs a plan type for every column it resolves, to
                # stamp on rugo's worker-side view (Pass1PredCtx.col_type). An
                # untyped pass-1 column has no tag to stamp, so decline the push
                # rather than resolve one and find out. This is checked over ALL
                # pass-1 columns, not just the predicate's — the sort key rides
                # along, and requiring it too only ever declines, never mis-pushes.
                and all(_col_type_by_name.get(n) is not None
                        for n in self._sp_pass1_name_to_identity)
            ):
                identity_to_physical = {
                    ident: name for name, ident in self._sp_pass1_name_to_identity.items()
                }
                identity_to_type = {
                    ident: _col_type_by_name[name].physical.value
                    for name, ident in self._sp_pass1_name_to_identity.items()
                }
                self._pass1_resolver = _Pass1PredResolver(
                    self.compiled_predicate, identity_to_physical, identity_to_type
                )
                # ...and only when every predicate column's type fits entirely in the
                # DrakenVector. A column carrying a logical descriptor (DECIMAL scale,
                # TIMESTAMP unit) cannot be reconstructed from a view no matter what
                # tag is stamped on it. Same rule, same helper, as the native
                # LatmatScanSource plan.
                if _pass1_worker_predicate_admissible(
                    [_col_type_by_name.get(n) for n in self._pass1_resolver.col_names]
                ):
                    self._lm_pass1_src.set_pass1_predicate(
                        _get_pass1_eval_fn_ptr(),
                        self._pass1_resolver.ctx_ptr(),
                        self._pass1_resolver.col_names,
                    )
            return

        self._scan_mode = _SCAN_SINGLE
        self._ipc_source = open_ipc_source(
            filesystem,
            blob_paths,
            column_names,
            decode_workers=_resolve_var(
                    "parquet_gcs_io_workers", getattr(self.properties, "variables", None),
                    config.PARQUET_GCS_IO_WORKERS,
                ) if connector_type in ("GCS", "GS", "S3") else config.resolve_parquet_local_io_workers(
                    _resolve_var(
                        "parquet_local_io_workers", getattr(self.properties, "variables", None),
                        config.PARQUET_LOCAL_IO_WORKERS,
                    )
                ),
            predicates=self._sp_predicate_stats,
            file_sizes=file_sizes or None,
            connector=connector_type,
            query_id=self._sp_query_id,
            footer_bytes_cache=_FOOTER_CACHE,
            null_fillers=[self._sp_null_filler_by_name[c] for c in column_names],
            string_types=[self._sp_string_type_by_name[c] for c in column_names],
            limit=self.limit if (not has_predicates and not self._sp_delete_positions) else None,
            http_tuning=_resolve_http_tuning(getattr(self.properties, "variables", None)),
            in_flight_limit_override=_resolve_in_flight_limit(getattr(self.properties, "variables", None)),
            coalesce_tuning=_resolve_coalesce_tuning(getattr(self.properties, "variables", None)),
        )

    cdef void _coerce_vectors(self, list vectors):
        """Reinterpret DATE/TIMESTAMP/DECIMAL columns in place, by position.

        Mirrors _coerce_logical_types but indexes the precomputed _sp_coerce_ops
        plan instead of a name-keyed dict, so the all-direct numeric path never
        builds a dict. The C++ pipeline serialises these as TAG_INT64 (physical);
        the schema-driven logical type is applied here.

        The two must stay in step: see the note on the _sp_coerce_ops build for
        what a coercion present in one and missing from the other actually does."""
        # Bounded by the PLAN, not by the vector list: a scan emitting row
        # identity appends synthesized columns past the read columns, and those
        # have no coercion plan entry (nor need one — they are built at the
        # width the schema declares).
        cdef Py_ssize_t i, n = len(self._sp_coerce_ops)
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
            if kind == 4:
                # ARRAY<TIMESTAMP>: the vector is an ARRAY, not an INT64, so this
                # must be handled BEFORE the INT64 guard below — and it retags the
                # child in place rather than rebinding vectors[i].
                if v_nb.type == _draken_native_parquet.ARRAY:
                    _array_child_to_timestamp(v_nb, op[1])
                continue
            if kind == 5:
                # IPV4 is physically UINT32, so like kind 4 it must be handled
                # BEFORE the INT64 guard below or it would be skipped outright.
                # Guarded on UINT32 for the same reason as _coerce_logical_types:
                # a schema declaring IPv4 over a column the file stores as
                # something else is a mismatch, and retagging it would reinterpret
                # unrelated bytes as addresses.
                if v_nb.type == _draken_native_parquet.UINT32:
                    vectors[i] = _uint32_to_ipv4(v_nb)
                continue
            # DATE is physical int32 and now decodes at that width (E33 exact-width
            # integers), so kind==2 accepts INT32; DECIMAL/TIMESTAMP stay INT64-only.
            if kind == 2:
                if (v_nb.type == _draken_native_parquet.INT64
                        or v_nb.type == _draken_native_parquet.INT32):
                    vectors[i] = _int64_to_date32(v_nb)
                continue
            if v_nb.type != _draken_native_parquet.INT64:
                continue
            if kind == 1:
                dec = op[1]
                vectors[i] = _int64_to_decimal(v_nb, dec[0], dec[1])
            else:  # kind == 3
                vectors[i] = _int64_to_timestamp(v_nb, op[1])

    cdef tuple _rg_row_window(self, str path, int rg_idx, str why):
        """`(file-global first row index, row count)` for one row group.

        The footer's per-row-group counts, prefix-summed and cached per file.
        Keyed by the file's REAL row-group index — upstream predicate pruning
        skips groups but never renumbers them.

        `why` names what wanted the window, so a file with no footer counts
        fails saying which capability it cannot serve.
        """
        cached = self._sp_rg_offset_cache.get(path)
        if cached is None:
            counts = self._ipc_source.rg_row_counts(path)
            if not counts:
                raise RuntimeError(
                    f"ParquetReadNode: no footer row-group counts for {why} "
                    f"file {path}; cannot map row ordinals to row groups."
                )
            offsets = []
            running = 0
            for c in counts:
                offsets.append(running)
                running += c
            cached = (offsets, counts)
            self._sp_rg_offset_cache[path] = cached
        offsets, counts = cached
        return (offsets[rg_idx], counts[rg_idx])

    cdef list _row_identity_vectors(self, str path, int rg_idx):
        """The `$file` and `$ordinal` vectors for one row group, in that order.

        `$ordinal` is the row's FILE-GLOBAL position, so it is built from the
        footer's row-group offset and the row's position within the group —
        valid only while position still equals ordinal, which is why this runs
        before the delete filter and the pushed predicate, and why the two-pass
        path is refused outright for a scan that emits it.

        `$file` is one value for the whole group (a row group belongs to exactly
        one file), so it is built from a repeated sequence rather than measured
        per row.

        Cost note: building the ordinal range measures ~3.3ms per 262K-row row
        group, which is noise under the row cap MERGE ships with. A native iota
        is the obvious replacement if that cap is ever lifted.
        """
        cdef tuple window = self._rg_row_window(path, rg_idx, "row-identity")
        cdef int64_t start = window[0]
        # The group's FULL row count, from the footer — not len(vectors[0]),
        # which is absent when row identity is the only thing projected.
        cdef Py_ssize_t nrows = window[1]
        cdef object file_idx = self._sp_file_index.get(path)
        if file_idx is None:
            raise RuntimeError(
                f"ParquetReadNode: {path} is not in this scan's file list; "
                "cannot address its rows."
            )
        return [
            _vector_from_sequence_typed([file_idx] * nrows, dtype="INTEGER"),
            _vector_from_sequence_typed(
                range(start, start + nrows), dtype="INTEGER"
            ),
        ]

    cdef object _apply_delete_filter(self, list vectors, str path, int rg_idx):
        """Subtract a file's merge-on-read delete vector from one row group.

        `vectors` holds the row group's FULL rows (the single-pass path applies
        any predicate after assembly, and the NULL-fill path fills to the row
        group's logical row count, so row position == row-group-local ordinal).
        The file-global ordinals on the delete vector are mapped to row-group-
        local ones via the footer's per-row-group row counts, keyed by the
        file's REAL row-group index — upstream predicate pruning skips groups
        but never renumbers them.

        Returns ``(vectors, removed)``: the vectors unchanged and 0 when the
        group has no deleted rows, a row-filtered copy and the removed count
        when some are, and ``(None, nrows)`` when EVERY row is deleted (the
        caller skips the group without emitting).
        """
        cdef tuple positions = self._sp_delete_positions.get(path)
        if positions is None:
            return vectors, 0
        cdef tuple window = self._rg_row_window(path, rg_idx, "delete-bearing")
        cdef int64_t start = window[0]
        cdef int64_t nrows = window[1]
        # positions is sorted: slice the file-global window for this group.
        cdef Py_ssize_t lo = bisect_left(positions, start)
        cdef Py_ssize_t hi = bisect_left(positions, start + nrows)
        if lo == hi:
            return vectors, 0
        if hi - lo >= nrows:
            return None, nrows  # whole row group deleted — nothing to emit
        deleted_local = set()
        cdef Py_ssize_t i
        for i in range(lo, hi):
            deleted_local.add(positions[i] - start)
        keep = [i for i in range(nrows) if i not in deleted_local]
        return [v.take(keep) for v in vectors], hi - lo

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
        cdef int64_t rows_before_filter, surviving_rows
        cdef int64_t bytes_fetched, read_ns, decode_ns
        while True:
            if self._limit_exhausted():
                return self._finish_locked_cxx()
            # Mark a claim attempt as "in flight" BEFORE calling next_vectors() —
            # incrementing only AFTER a successful claim leaves a gap between the
            # claim (under IpcRowGroupSource's OWN separate mutex) and the increment
            # (under this scan's _scan_mtx), which a concurrently-exhausted second
            # worker can slip through. Speculatively incrementing first and
            # decrementing on a None result closes that gap entirely.
            with nogil:
                self._scan_mtx.lock()
            self._sp_claims_pending += 1
            self._scan_mtx.unlock()
            pulled = self._ipc_source.next_vectors()
            if pulled is None:
                with nogil:
                    self._scan_mtx.lock()
                self._sp_claims_pending -= 1
                self._scan_mtx.unlock()
                return self._finish_locked_cxx()
            vectors = pulled[0]
            mor_removed = 0
            bytes_fetched = pulled[1]
            read_ns = pulled[2]
            decode_ns = pulled[3]
            path = pulled[4]

            # Phase 2 empty row group (dictionary-membership skip): no assembly,
            # no morsel — just fold in I/O telemetry + the pre-filter row count.
            if vectors is None:
                with nogil:
                    self._scan_mtx.lock()
                self.bytes_in += bytes_fetched
                self.scan_readings.time_parquet_read_ranges_ns += read_ns
                self.scan_readings.time_parquet_decode_columns_ns += decode_ns
                self._total_rows_before_filter += pulled[6]
                self._sp_claims_pending -= 1
                self._scan_mtx.unlock()
                continue

            # ── Row identity (thread-local, before EVERYTHING that drops rows) ──
            # $ordinal is a row's position in its file, so it must be stamped
            # while position still equals ordinal. Appended here, the delete
            # filter below takes it along (it filters every vector positionally)
            # and the pushed predicate does the same through the cxm — so the
            # address stays attached to its row wherever the row survives to.
            if self._sp_row_identity_names:
                vectors = vectors + self._row_identity_vectors(
                    <str>path, <int>pulled[5]
                )

            # ── Merge-on-read delete filter (thread-local, before predicate) ──
            # Deleted ordinals are physical row positions, so they must be
            # subtracted while row position still equals file ordinal — i.e.
            # before the pushed predicate reorders survivorship.
            if self._sp_delete_positions:
                vectors, mor_removed = self._apply_delete_filter(
                    vectors, <str>path, <int>pulled[5]
                )
                if vectors is None:
                    # Every row of this group is deleted — skip it entirely.
                    with nogil:
                        self._scan_mtx.lock()
                    self.bytes_in += bytes_fetched
                    self.scan_readings.time_parquet_read_ranges_ns += read_ns
                    self.scan_readings.time_parquet_decode_columns_ns += decode_ns
                    self.scan_readings.mor_rows_deleted += mor_removed
                    self._sp_claims_pending -= 1
                    self._scan_mtx.unlock()
                    continue

            # ── Thread-local cxm assembly (no lock) ──────────────────────────
            if self._sp_needs_coerce:
                self._coerce_vectors(vectors)
            has_identity = bool(self._sp_identity_names)
            emit_cxm.reset()
            rows_before_filter = 0
            if has_identity:
                # Positional pairing: vectors order == column_names == identity order.
                result_cxm = cxx_morsel_from_vectors_sp(vectors, self._sp_identity_names)
                rows_before_filter = result_cxm.get().num_rows()
                if self.compiled_predicate is not None:
                    result_cxm = self._cxx_apply_predicate(result_cxm)
                if self._sp_output_identity_order:
                    emit_cxm = cxx_select_sp(result_cxm, self._sp_output_identity_order)
                else:
                    # No output columns (e.g. COUNT(*) with a filter-only read, or a
                    # projection of only constants — `SELECT 3.14 FROM t`). Emit a
                    # genuine ZERO-COLUMN morsel that carries the row count in
                    # zero_col_rows (cxx_select with an empty want-list does exactly
                    # this — cxx_morsel_ops.h). A synthetic `b'*'` bool column here
                    # would sit at layout position 0, which the compiler's empty scan
                    # layout does not track: a downstream ExprProject then appends its
                    # computed column at runtime position 1 while the plan expects
                    # position 0, so the final select reads the phantom bool column
                    # (all-True) as the output and shifts every value by one column.
                    # The zero_col_rows contract is the same one UngroupedAggSink's
                    # CountStar already reads, so COUNT(*) is unaffected.
                    surviving_rows = result_cxm.get().num_rows()
                    if surviving_rows > 0:
                        emit_cxm = cxx_select_sp(result_cxm, [])

            # ── Shared commit (under _scan_mtx; no GIL-releasing call inside) ──
            with nogil:
                self._scan_mtx.lock()
            self.bytes_in += bytes_fetched
            self.scan_readings.time_parquet_read_ranges_ns += read_ns
            self.scan_readings.time_parquet_decode_columns_ns += decode_ns
            self.scan_readings.mor_rows_deleted += mor_removed
            if has_identity:
                self._total_rows_before_filter += rows_before_filter
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
        with nogil:
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
            # pulled[6] carries the worker-computed survivor mask (bytes) when the
            # pass-1 predicate was pushed to the decode workers, else None → serial eval.
            result = _evaluate_pass1_row_group(
                path, rg_idx, row_group, self.compiled_predicate,
                self._sp_pass1_name_to_identity, self._sp_decimal_col_map,
                self._sp_date_col_set, self._sp_timestamp_col_set,
                self._sp_timestamp_unit_map,
                self._sp_ipv4_col_set,
                self._sp_array_ts_unit_map,
                self._sp_pass1_column_names,
                <bytes>pulled[6] if pulled[6] is not None else None,
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
        self._sp_pass1_pruned_row_groups = pass1_src.pruned_row_group_count
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
            http_tuning=_resolve_http_tuning(getattr(self.properties, "variables", None)),
            in_flight_limit_override=_resolve_in_flight_limit(getattr(self.properties, "variables", None)),
            coalesce_tuning=_resolve_coalesce_tuning(getattr(self.properties, "variables", None)),
        )
        self._lm_pass1_done = True

    cdef Morsel _combine_pass1_pass2_row_group(self, object path, object rg_idx, list vectors, dict p1_cache):
        """Combine one row group's pass-2 (projection) vectors with its cached
        pass-1 (filter) result into a single output Morsel, in
        _sp_output_identity_order. Pops the row group's entry out of p1_cache —
        each row group's pass-1 result is consumed exactly once, by whichever
        caller (the topn_active whole-scan drain, or a chunk flush) reaches it.
        Shared by _latmat_next's topn_active path and _flush_pass1_chunk so the
        two can't silently diverge."""
        cdef dict row_group, p1_vectors_by_identity, p2_vectors_by_identity
        cdef list combined_identity_names, combined_vectors
        cdef object identity, p1_filtered, p1_identity_names
        cdef Py_ssize_t i, n = len(vectors)

        row_group = {self._lm_pass2_names_bytes[i]: vectors[i] for i in range(n)}
        _coerce_logical_types(
            row_group, self._sp_decimal_col_map,
            self._sp_date_col_set, self._sp_timestamp_col_set,
            self._sp_timestamp_unit_map, self._sp_ipv4_col_set,
            self._sp_array_ts_unit_map,
        )

        p1_filtered, p1_identity_names = p1_cache.pop((path, rg_idx))
        # WP-2: reduce pass-1 survivors to the same top-n winners pass 2 decoded.
        # _lm_topn_winners is always None on the chunked (non-topn) path, so this
        # is a no-op there.
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

        return Morsel.from_vectors(combined_identity_names, combined_vectors)

    cdef void _flush_pass1_chunk(self, list pass2_work, dict p1_cache) except *:
        """Open a pass-2 source scoped to just this chunk's survivors, drain it
        fully, and append each combined morsel to _lm_pending_morsels.

        Unlike the topn_active path (one pass-2 source for the whole scan,
        opened once _run_pass1 fully drains pass1_src), this opens+drains+closes
        a pass-2 source per chunk — trading some CppIOPipeline construction
        overhead (bounded by chunk count, not row-group count) for never holding
        more than one chunk's pass-1 survivor data in memory at once."""
        cdef object pass2_src
        cdef tuple pulled
        cdef list vectors
        cdef object path, rg_idx
        cdef Morsel result_morsel

        if not pass2_work:
            return

        pass2_src = open_pass2_source(
            self._sp_filesystem,
            pass2_work,
            self._sp_pass2_column_names,
            file_sizes=self._sp_file_sizes or None,
            connector=self._sp_connector_type,
            query_id=self._sp_query_id,
            footer_bytes_cache=_FOOTER_CACHE,
            null_fillers=[self._sp_null_filler_by_name[c] for c in self._sp_pass2_column_names],
            string_types=[self._sp_string_type_by_name[c] for c in self._sp_pass2_column_names],
            http_tuning=_resolve_http_tuning(getattr(self.properties, "variables", None)),
            in_flight_limit_override=_resolve_in_flight_limit(getattr(self.properties, "variables", None)),
            coalesce_tuning=_resolve_coalesce_tuning(getattr(self.properties, "variables", None)),
        )
        try:
            while True:
                if self._limit_exhausted():
                    break
                pulled = pass2_src.next_vectors()
                if pulled is None:
                    break
                vectors = pulled[0]
                path = pulled[4]
                rg_idx = pulled[5]
                if vectors is None:
                    continue
                self.scan_readings.record_pass2_decoded(pulled[1])
                self.bytes_in += pulled[1]

                result_morsel = self._combine_pass1_pass2_row_group(path, rg_idx, vectors, p1_cache)

                with nogil:
                    self._scan_mtx.lock()
                result_morsel = self._commit_morsel(result_morsel, path)
                self._emitted_any = True
                self._scan_mtx.unlock()
                self._lm_pending_morsels.append(result_morsel)
        finally:
            pass2_src.close()

    cdef void _run_pass1_chunk(self) except *:
        """Chunked (non-topn) pass-1 driver: pull row groups from pass1_src,
        accumulating survivors, until PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER
        consecutive fully-passing row groups accumulate (the predicate isn't
        pruning anything — no reason to keep buffering before pass 2 can start)
        or pass1_src is exhausted — then flush that chunk through pass 2
        immediately via _flush_pass1_chunk. One call produces at most one
        chunk's worth of pending morsels; _latmat_next calls this again only
        once _lm_pending_morsels has been fully drained back to the caller, so
        the previous chunk's data is already gone (consumed by HeapSort et al.)
        before the next chunk's pass-1 work even starts."""
        cdef object pass1_src = self._lm_pass1_src
        cdef tuple pulled
        cdef list vectors
        cdef object path, rg_idx
        cdef dict row_group
        cdef _Pass1Result result
        cdef Py_ssize_t i, n
        cdef list pass2_work = []
        cdef dict p1_cache = {}
        cdef Py_ssize_t consecutive_full_pass = 0
        cdef Py_ssize_t abandon_after = _resolve_var(
            "parquet_late_materialization_abandon_after",
            getattr(self.properties, "variables", None),
            config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER,
        )
        cdef bint abandoned = False
        # This chunked path is a secondary safety net: the primary defense against
        # a non-selective predicate is the selectivity-gated two_pass_eligible
        # check in _ensure_scan_started, which keeps queries like this on the
        # cheaper _SCAN_SINGLE path entirely (measured ~2x less peak memory and
        # ~2x faster than any two-pass variant for a ~0-selectivity predicate --
        # see the WP-2 selectivity gate). This path only runs when that estimate
        # turned out to be wrong (predicate looked selective enough on manifest
        # stats but isn't at the row level), so it doesn't need to be tuned for
        # the fully-degenerate case -- abandon_after's existing chunk size is
        # fine, and empirically outperformed a much larger chunk size when tested
        # directly against the degenerate case (smaller chunks -> less pass-1
        # survivor data held in _lm_p1_cache at once; larger chunks did not
        # meaningfully reduce total CppIOPipeline construction cost either).
        cdef Py_ssize_t min_chunk_row_groups = abandon_after

        while True:
            pulled = pass1_src.next_vectors()
            if pulled is None:
                self._lm_chunked_pass1_exhausted = True
                self._sp_pass1_pruned_row_groups = pass1_src.pruned_row_group_count
                pass1_src.close()
                self._lm_pass1_src = None
                break
            vectors = pulled[0]
            self.bytes_in += pulled[1]
            self.scan_readings.time_parquet_read_ranges_ns += pulled[2]
            self.scan_readings.time_parquet_decode_columns_ns += pulled[3]
            path = pulled[4]
            rg_idx = pulled[5]
            if vectors is None:
                self._total_rows_before_filter += pulled[6]
                self.scan_readings.record_pass1_skipped()
                consecutive_full_pass = 0
                continue
            n = len(vectors)
            row_group = {self._lm_pass1_names_bytes[i]: vectors[i] for i in range(n)}
            result = _evaluate_pass1_row_group(
                path, rg_idx, row_group, self.compiled_predicate,
                self._sp_pass1_name_to_identity, self._sp_decimal_col_map,
                self._sp_date_col_set, self._sp_timestamp_col_set,
                self._sp_timestamp_unit_map,
                self._sp_ipv4_col_set,
                self._sp_array_ts_unit_map,
                self._sp_pass1_column_names,
                <bytes>pulled[6] if pulled[6] is not None else None,
            )
            if result.empty:
                continue
            self._total_rows_before_filter += result.rows_before_filter
            if result.survived:
                self._record_pass1_survivor(result, pass2_work, p1_cache)
                if result.p1_filtered.num_rows == result.rows_before_filter:
                    consecutive_full_pass += 1
                else:
                    consecutive_full_pass = 0
                if not abandoned and consecutive_full_pass >= abandon_after:
                    abandoned = True
                    self.readings["parquet_latmat_abandoned_files"] = (
                        self.readings.get("parquet_latmat_abandoned_files", 0) + 1
                    )
                if abandoned and len(pass2_work) >= min_chunk_row_groups:
                    break
            else:
                self._record_pass1_skip(result)
                consecutive_full_pass = 0

        self._flush_pass1_chunk(pass2_work, p1_cache)

    cdef Morsel _latmat_next(self):
        """Native two-pass latmat: stream row groups, combining each with its
        pass-1 survivors and returning one morsel per call. Returns None on
        exhaustion.

        topn_active runs the original whole-scan drain: _run_pass1 fully drains
        pass1_src once (required — the global top-n cutoff needs every row
        group's sort key), then this method streams the resulting single
        pass-2 source.

        Otherwise runs the chunked path: _run_pass1_chunk produces (at most)
        PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER morsels' worth of
        _lm_pending_morsels at a time, refilled only once the previous chunk
        has been fully returned to the caller."""
        cdef tuple pulled
        cdef list vectors
        cdef object path, rg_idx
        cdef Morsel result_morsel

        if not self._sp_topn_active:
            if self._limit_exhausted():
                return self._finish_locked()
            while not self._lm_pending_morsels:
                if self._lm_chunked_pass1_exhausted:
                    return self._finish_locked()
                self._run_pass1_chunk()
                if self._limit_exhausted():
                    break
            if not self._lm_pending_morsels:
                return self._finish_locked()
            return self._lm_pending_morsels.pop(0)

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

            result_morsel = self._combine_pass1_pass2_row_group(path, rg_idx, vectors, self._lm_p1_cache)

            with nogil:
                self._scan_mtx.lock()
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
