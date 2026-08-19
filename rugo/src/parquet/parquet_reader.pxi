









# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough
#
# =============================================================================
# THIS IS RUGO'S STANDALONE READER — NOT THE OPTERYX EXECUTION SCAN.
# =============================================================================
#
# Endpoint: rugo's public `read_parquet()` / `decode_column_from_chunk()` API.
# Its consumers are GENERAL-PURPOSE and Python-facing — `read_parquet` is a
# library entry point, used for catalog/manifest reads and the rugo test suite,
# where the caller wants ordinary Python values out the other end.
#
# BECAUSE OF THAT ENDPOINT, this reader deliberately MATERIALISES dictionary-
# encoded columns into per-row Python lists and re-builds dense/auto-dict
# vectors (see `_int64_list` / `_float64_list` / `_string_list` /
# `_make_typed_*_dictionary_vector`). The dict on disk is FLATTENED to Python
# objects here ON PURPOSE — that is what a standalone Python reader is for. It
# is NOT a bug and NOT the hot path.
#
# The OPTERYX QUERY ENGINE DOES NOT USE THIS FILE TO SCAN DATA. The execution
# scan is the native C++ pipeline in
# `opteryx/connectors/parquet_io/pool_reader.pyx` (`iter_row_groups_ipc`), which
# PRESERVES dictionary shape end-to-end (DK_*_DICT, zero Python objects per row).
# If you are chasing scan performance or §11 dict-shape behaviour, look THERE,
# not here. Do not "fix" this reader to preserve dicts on the assumption it
# feeds the engine — it does not.
# =============================================================================
import datetime
import decimal as _decimal
import os
import struct
import time as _time

_Decimal = _decimal.Decimal

from cpython.bytes cimport PyBytes_FromStringAndSize

# ---------------------------------------------------------------------------
# Telemetry accumulators (reset with reset_telemetry(); read with get_telemetry())
# ---------------------------------------------------------------------------
_TEL = {
    "cpp_decode_s":   0.0,   # time inside C++ ReadParquet()
    "cython_int64_s": 0.0,   # _make_int64_vector / _make_int64_from_int32_vector
    "cython_float_s": 0.0,   # _make_float64_vector
    "cython_str_s":   0.0,   # _make_string_vector / _make_array_vector
    "cython_bool_s":  0.0,   # _make_bool_vector
    "cython_other_s": 0.0,   # anything else
    "calls":          0,
    "row_groups":      0,
    "columns":         0,
    "parquet_dict_columns_decoded": 0,
    "parquet_dict_unique_values": 0,
    "parquet_dict_code_width_bytes": 0,
    "parquet_dict_materialize_fallbacks": 0,
    "parquet_pages_skipped": 0,  # pages skipped via row_mask (no selected rows in page)
    "parquet_pages_decoded": 0,  # pages decompressed/decoded when row_mask was active
}


def reset_telemetry():
    """Zero all telemetry counters."""
    for k in _TEL:
        _TEL[k] = 0


def get_telemetry():
    """Return a copy of the current telemetry dict."""
    return dict(_TEL)

# ---------------------------------------------------------------------------
# C++ phase telemetry (reset_cpp_telemetry / get_cpp_telemetry)
# ---------------------------------------------------------------------------

cdef extern from "telemetry.hpp" namespace "rugo_tel":
    double metadata_s()
    double decompress_s()
    double dict_parse_s()
    double prescan_s()
    double page_parallel_s()
    double rle_s()
    double val_expand_s()
    double mask_filter_s()
    double validity_bmp_s()
    long long calls_count()
    long long ba_chunks_count()
    long long ba_intern_values_count()
    long long ba_drop_no_rederive_count()
    long long ba_drop_cap_count()
    long long ba_drop_rle_dense_count()
    long long ba_emit_dict_count()
    long long ba_emit_dense_count()
    long long ba_drop_cap_entries_sum()
    long long ba_drop_cap_limit_sum()
    long long ba_drop_cap_values_sum()
    long long ba_emit_dict_entries_sum()
    long long ba_emit_dict_rows_sum()
    long long ba_emit_dense_rows_sum()
    void reset() nogil



def reset_cpp_telemetry():
    """Zero all C++ phase telemetry accumulators."""
    reset()


def get_cpp_telemetry():
    """Return a dict with C++ phase timing (seconds) since last reset."""
    return {
        "metadata_s":       metadata_s(),
        "decompress_s":     decompress_s(),
        "dict_parse_s":     dict_parse_s(),
        "prescan_s":        prescan_s(),
        "page_parallel_s":  page_parallel_s(),
        "rle_s":            rle_s(),
        "val_expand_s":     val_expand_s(),
        "mask_filter_s":    mask_filter_s(),
        "validity_bmp_s":   validity_bmp_s(),
        "calls":            calls_count(),
        # byte_array dictionary-shape outcomes (counts, not seconds). Keys are
        # prefixed ba_ so callers that sum "*_s" timing keys skip them.
        "ba_chunks":            ba_chunks_count(),
        "ba_intern_values":     ba_intern_values_count(),
        "ba_drop_no_rederive":  ba_drop_no_rederive_count(),
        "ba_drop_cap":          ba_drop_cap_count(),
        "ba_drop_rle_dense":    ba_drop_rle_dense_count(),
        "ba_emit_dict":         ba_emit_dict_count(),
        "ba_emit_dense":        ba_emit_dense_count(),
        "ba_drop_cap_entries":  ba_drop_cap_entries_sum(),
        "ba_drop_cap_limit":    ba_drop_cap_limit_sum(),
        "ba_drop_cap_values":   ba_drop_cap_values_sum(),
        "ba_emit_dict_entries": ba_emit_dict_entries_sum(),
        "ba_emit_dict_rows":    ba_emit_dict_rows_sum(),
        "ba_emit_dense_rows":   ba_emit_dense_rows_sum(),
    }

cimport parquet_reader
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int32_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.string cimport string
from libcpp.vector cimport vector

# Type widening C wrappers (Tier 2C SIMD acceleration)
cdef extern from "../../src/cpp/disk_io.h" nogil:
    int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len)
    int unmap_memory_c(unsigned char* addr, size_t size)

cdef extern from "type_widening_wrappers.hpp":
    void rugo_widen_int32_to_int64(const int32_t* src, int64_t* dst, size_t count) nogil
    void rugo_widen_float32_to_float64(const float* src, double* dst, size_t count) nogil

# Import Draken vector types and components
# Typed-vector cimports removed as part of E.28 migration (.pxd files deleted):
#   E.28-gap-1: Integer64Vector dense constructor + ptr.data write access
#   E.28-gap-2: Float64Vector dense constructor + ptr.data write access
#   E.28-gap-3: StringVectorBuilder (constructors, append_bytes, append_null, finish)
#   E.28-gap-4: array_vector_from_parts
#   E.28-gap-5: int64_from_dict / int64_from_dict_nullable / int64_from_packed_dict
#   E.28-gap-6: float64_from_dict / float64_from_dict_nullable / float64_from_packed_dict
#   E.28-gap-7: string_from_dict_buffers / make_string_dict_only
#   E.28-gap-8: Integer64Vector.from_constant / Float64Vector.from_constant / StringVector.from_constant
#   E.28-gap-9: bool_vector_from_bits — symbol is compiled inline into each consumer;
#               draken/core/bitmap_ops.cpp must be added to rugo.parquet_reader sources in setup.py
from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel

# E.28 reconstruction: the simple read_parquet path rebuilds Draken vectors from
# decoded column values via the native sequence constructors (null-safe; None =
# null). This is the SERIAL utility path — the parallel execution scan path
# (pool_reader) uses zero-copy buffer construction and is unaffected.
import draken.draken_native as _dn

# Leaf-element DrakenType codes, resolved once from the native enum, used to tell
# the array constructor the child type when a list column carries no inferable
# leaf value (every row null or empty) — see _make_array_vector.
cdef int _DK_EL_VARCHAR = int(_dn.VARCHAR.value)
cdef int _DK_EL_INT64 = int(_dn.INT64.value)
cdef int _DK_EL_UINT64 = int(_dn.UINT64.value)
cdef int _DK_EL_FLOAT64 = int(_dn.FLOAT64.value)
cdef int _DK_EL_BOOL = int(_dn.BOOL.value)


# --- value decoder ---
cdef inline bint _text_is_printable(str text):
    for ch in text:
        code = ord(ch)
        if code < 32 and ch not in ('\t', '\n', '\r'):
            return False
        if code == 127:
            return False
    return True


cdef inline str _safe_decode_utf8(string raw_bytes):
    cdef bytes b = raw_bytes
    return b.decode("utf-8")


def decode_value(
        string physical_type,
        string logical_type,
        string raw,
        bint prefer_text):
    cdef bytes b = raw
    if b is None:
        return None

    cdef str type_str = physical_type.decode("utf-8")
    cdef str logical_str = logical_type.decode("utf-8") if logical_type.size() > 0 else ""
    cdef bint is_string_logical = (
        logical_str in ("varchar", "UTF8", "JSON", "BSON", "ENUM")
        or logical_str.startswith("array<string")
        or logical_str.startswith("array<varchar")
    )
    cdef str candidate

    # E33: an UNSIGNED column stores its magnitude in a signed int32/int64 slot, so
    # a value at or above the signed midpoint has a NEGATIVE bit pattern. Decoding
    # these statistics as signed inverts the column's min/max, and callers that
    # prune row groups by comparing a predicate against them then discard groups
    # that genuinely match — silently dropping rows (a `WHERE u32 > 0` over a
    # column holding 3e9 returned nothing). Match the innermost "uint<width>" the
    # same way decode_column.cpp's IntType detection does, so a LIST leaf
    # ("array<uint32>") is caught too.
    cdef Py_ssize_t _upos = logical_str.rfind("uint")
    cdef bint is_unsigned_logical = (
        _upos != -1
        and _upos + 4 < len(logical_str)
        and logical_str[_upos + 4].isdigit()
    )

    if len(b) == 0:
        if type_str in ("byte_array", "fixed_len_byte_array"):
            if is_string_logical or prefer_text:
                return ""
        return b""

    if type_str == "int32":
        return struct.unpack("<I" if is_unsigned_logical else "<i", b)[0]
    elif type_str == "int64":
        return struct.unpack("<Q" if is_unsigned_logical else "<q", b)[0]
    elif type_str == "float32":
        return struct.unpack("<f", b)[0]
    elif type_str == "float64":
        return struct.unpack("<d", b)[0]
    elif type_str in ("byte_array", "fixed_len_byte_array"):
        if is_string_logical:
            return b.decode("utf-8")
        elif prefer_text and type_str == "byte_array":
            candidate = b.decode("utf-8", errors="replace")
            if _text_is_printable(candidate) and "\ufffd" not in candidate:
                return candidate
        return b
    elif type_str == "int96":
        if len(b) == 12:
            lo, hi = struct.unpack("<qI", b)
            julian_day = hi
            nanos = lo
            days = julian_day - 2440588
            date = datetime.date(1970, 1, 1) + datetime.timedelta(days=days)
            seconds = nanos // 1_000_000_000
            micros = (nanos % 1_000_000_000) // 1000
            return f"{date.isoformat()} {seconds:02d}:{(micros/1e6):.6f}"
        return b.hex()
    elif type_str == "boolean":
        return b[0] != 0
    else:
        return b.hex()


cdef parquet_reader.MetadataParseOptions _build_options(
        bint schema_only,
        bint include_statistics,
        Py_ssize_t max_row_groups):
    cdef parquet_reader.MetadataParseOptions opts = parquet_reader.MetadataParseOptions()
    opts.schema_only = schema_only
    if schema_only:
        opts.include_statistics = False
    else:
        opts.include_statistics = include_statistics
    if max_row_groups >= 0:
        opts.max_row_groups = <long long>max_row_groups
    else:
        opts.max_row_groups = -1
    return opts


cdef class SchemaColumn:
    """Typed schema column record. Replaces the old per-column dict — the
    attribute set is fixed, so it is carried as typed fields, not dict keys."""
    cdef readonly str name
    cdef readonly str physical_type
    cdef readonly str logical_type
    cdef readonly bint nullable
    # draken LogicalKind ordinal recovered from the file's key-value metadata
    # (draken/core/draken_bridge.h: 5 = IPV4); 0 means the file carries no
    # annotation for this column — "don't know", never "no descriptor". Parquet
    # has no logical type for these kinds, so `logical_type` above cannot say it.
    cdef readonly int draken_logical_kind

    def __repr__(self):
        return (
            f"SchemaColumn(name={self.name!r}, physical_type={self.physical_type!r}, "
            f"logical_type={self.logical_type!r}, nullable={self.nullable}, "
            f"draken_logical_kind={self.draken_logical_kind})"
        )


cdef class ParquetMetadata:
    """Typed parquet schema metadata. Replaces the old
    {'num_rows', 'schema_columns'} dict with fixed typed attributes.

    schema_columns is a tuple of SchemaColumn. For column statistics use
    fetch_column_stats(); for data use iter_row_groups_ipc()."""
    cdef readonly long long num_rows
    cdef readonly tuple schema_columns

    def __repr__(self):
        return (
            f"ParquetMetadata(num_rows={self.num_rows}, "
            f"schema_columns={self.schema_columns!r})"
        )


cdef class ScanRowGroup:
    """Typed row-group scan metadata: replaces the old dict of ~40 telemetry
    keys (__path__, __bytes_fetched__, __time_read_ranges_ns__, etc.).

    Populated by parquet scan paths (pool_reader.pyx, reader.py) and consumed
    by the operator (parquet_read.pyx) via merge_row_group_metadata. The dict
    of {col_name: Vector} data columns is still separate; this is pure metadata."""
    cdef readonly str path
    cdef readonly int rg_idx
    cdef readonly str scan_strategy
    cdef readonly long long bytes_fetched
    cdef readonly long long time_read_ranges_ns
    cdef readonly long long time_decode_columns_ns
    cdef readonly long long task_queue_wait_ns
    cdef readonly long long task_total_ns
    cdef readonly long long footer_fetch_ns
    cdef readonly long long scheduler_wait_ns
    cdef readonly long long rowgroup_completion_latency_ns
    cdef readonly long long emit_wait_ns
    cdef readonly long long scheduler_empty_wait_ns
    cdef readonly long long scheduler_empty_wait_events
    cdef readonly long long io_ring_producer_full_wait_ns
    cdef readonly long long io_ring_producer_full_wait_events
    cdef readonly long long io_ring_consumer_empty_wait_ns
    cdef readonly long long io_ring_consumer_empty_wait_events
    cdef readonly long long io_transfer_emit_wait_ns
    cdef readonly long long io_rowgroup_slice_count
    cdef readonly long long io_deserialize_ns
    cdef readonly long long io_serialize_ns
    cdef readonly long long rowgroup_peak_in_flight
    cdef readonly long long ranges_in_flight_peak
    cdef readonly long long active_files_peak
    cdef readonly long long active_rowgroups_peak
    cdef readonly long long rowgroups_in_flight_cap
    cdef readonly long long emit_queue_depth_at_ready
    cdef readonly long long io_ring_slot_bytes
    cdef readonly long long io_ring_slot_count
    cdef readonly long long io_ring_total_bytes
    cdef readonly long long io_transfer_ready_backlog_peak
    cdef readonly long long io_transfer_fragment_count_p50
    cdef readonly long long io_transfer_fragment_count_p95
    cdef readonly long long io_transfer_fragment_count_max
    cdef readonly long long io_transfer_payload_bytes_p50
    cdef readonly long long io_transfer_payload_bytes_p95
    cdef readonly long long io_transfer_payload_bytes_max
    cdef readonly long long row_groups_pruned
    cdef readonly long long footer_bytes
    cdef readonly long long range_request_count
    cdef readonly long long range_bytes_requested
    cdef readonly long long pages_decoded
    cdef readonly long long pages_skipped

    def __repr__(self):
        return (
            f"ScanRowGroup(path={self.path!r}, rg_idx={self.rg_idx}, "
            f"scan_strategy={self.scan_strategy!r}, bytes_fetched={self.bytes_fetched})"
        )


cdef SchemaColumn _make_schema_column(parquet_reader.SchemaField& field):
    cdef SchemaColumn col = SchemaColumn.__new__(SchemaColumn)
    col.name = field.name.decode("utf-8")
    col.physical_type = field.physical_type.decode("utf-8")
    col.logical_type = field.logical_type.decode("utf-8")
    col.nullable = field.nullable
    col.draken_logical_kind = field.draken_logical_kind
    return col


cdef ParquetMetadata _make_metadata(parquet_reader.FileStats& fs):
    """Build typed ParquetMetadata from C++ FileStats. Schema only — no row groups."""
    cdef ParquetMetadata meta = ParquetMetadata.__new__(ParquetMetadata)
    meta.num_rows = fs.num_rows
    cdef list cols = []
    cdef size_t i
    for i in range(fs.schema_columns.size()):
        cols.append(_make_schema_column(fs.schema_columns[i]))
    meta.schema_columns = tuple(cols)
    return meta


def _make_scan_row_group(str path, int rg_idx, str scan_strategy,
                         dict telemetry):
    """Build typed ScanRowGroup from telemetry dict. Extracts the ~40 __*__ keys
    and populates the typed object, leaving the dict ready for column data."""
    cdef ScanRowGroup rg = ScanRowGroup.__new__(ScanRowGroup)
    rg.path = path
    rg.rg_idx = rg_idx
    rg.scan_strategy = scan_strategy
    rg.bytes_fetched = telemetry.pop("__bytes_fetched__", 0)
    rg.time_read_ranges_ns = telemetry.pop("__time_read_ranges_ns__", 0)
    rg.time_decode_columns_ns = telemetry.pop("__time_decode_columns_ns__", 0)
    rg.task_queue_wait_ns = telemetry.pop("__task_queue_wait_ns__", 0)
    rg.task_total_ns = telemetry.pop("__task_total_ns__", 0)
    rg.footer_fetch_ns = telemetry.pop("__footer_fetch_ns__", 0)
    rg.scheduler_wait_ns = telemetry.pop("__scheduler_wait_ns__", 0)
    rg.rowgroup_completion_latency_ns = telemetry.pop("__rowgroup_completion_latency_ns__", 0)
    rg.emit_wait_ns = telemetry.pop("__emit_wait_ns__", 0)
    rg.scheduler_empty_wait_ns = telemetry.pop("__scheduler_empty_wait_ns__", 0)
    rg.scheduler_empty_wait_events = telemetry.pop("__scheduler_empty_wait_events__", 0)
    rg.io_ring_producer_full_wait_ns = telemetry.pop("__io_ring_producer_full_wait_ns__", 0)
    rg.io_ring_producer_full_wait_events = telemetry.pop("__io_ring_producer_full_wait_events__", 0)
    rg.io_ring_consumer_empty_wait_ns = telemetry.pop("__io_ring_consumer_empty_wait_ns__", 0)
    rg.io_ring_consumer_empty_wait_events = telemetry.pop("__io_ring_consumer_empty_wait_events__", 0)
    rg.io_transfer_emit_wait_ns = telemetry.pop("__io_transfer_emit_wait_ns__", 0)
    rg.io_rowgroup_slice_count = telemetry.pop("__io_rowgroup_slice_count__", 0)
    rg.io_deserialize_ns = telemetry.pop("__io_deserialize_ns__", 0)
    rg.io_serialize_ns = telemetry.pop("__io_serialize_ns__", 0)
    rg.rowgroup_peak_in_flight = telemetry.pop("__rowgroup_peak_in_flight__", 0)
    rg.ranges_in_flight_peak = telemetry.pop("__ranges_in_flight_peak__", 0)
    rg.active_files_peak = telemetry.pop("__active_files_peak__", 0)
    rg.active_rowgroups_peak = telemetry.pop("__active_rowgroups_peak__", 0)
    rg.rowgroups_in_flight_cap = telemetry.pop("__rowgroups_in_flight_cap__", 0)
    rg.emit_queue_depth_at_ready = telemetry.pop("__emit_queue_depth_at_ready__", 0)
    rg.io_ring_slot_bytes = telemetry.pop("__io_ring_slot_bytes__", 0)
    rg.io_ring_slot_count = telemetry.pop("__io_ring_slot_count__", 0)
    rg.io_ring_total_bytes = telemetry.pop("__io_ring_total_bytes__", 0)
    rg.io_transfer_ready_backlog_peak = telemetry.pop("__io_transfer_ready_backlog_peak__", 0)
    rg.io_transfer_fragment_count_p50 = telemetry.pop("__io_transfer_fragment_count_p50__", 0)
    rg.io_transfer_fragment_count_p95 = telemetry.pop("__io_transfer_fragment_count_p95__", 0)
    rg.io_transfer_fragment_count_max = telemetry.pop("__io_transfer_fragment_count_max__", 0)
    rg.io_transfer_payload_bytes_p50 = telemetry.pop("__io_transfer_payload_bytes_p50__", 0)
    rg.io_transfer_payload_bytes_p95 = telemetry.pop("__io_transfer_payload_bytes_p95__", 0)
    rg.io_transfer_payload_bytes_max = telemetry.pop("__io_transfer_payload_bytes_max__", 0)
    rg.row_groups_pruned = telemetry.pop("__row_groups_pruned__", 0)
    rg.footer_bytes = telemetry.pop("__footer_bytes__", 0)
    rg.range_request_count = telemetry.pop("__range_request_count__", 0)
    rg.range_bytes_requested = telemetry.pop("__range_bytes_requested__", 0)
    rg.pages_decoded = telemetry.pop("__pages_decoded__", 0)
    rg.pages_skipped = telemetry.pop("__pages_skipped__", 0)
    # Pop any remaining __*__ keys to leave only column data in the dict
    for key in list(telemetry):
        if key.startswith("__"):
            telemetry.pop(key, None)
    return rg


def read_metadata(str path):
    """Return typed ParquetMetadata for a parquet file (num_rows, schema_columns).

    For column statistics use fetch_column_stats().
    For data use iter_row_groups_ipc().
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef parquet_reader.MetadataParseOptions opts
    opts.schema_only = True
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataC(
        path_bytes, opts
    )
    return _make_metadata(fs)


def read_metadata_from_bytes(bytes data):
    """Return typed ParquetMetadata from an in-memory bytes buffer."""
    cdef parquet_reader.MetadataParseOptions opts
    opts.schema_only = True
    cdef const uint8_t* buf = <const uint8_t*> data
    cdef size_t size = len(data)
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _make_metadata(fs)


def read_metadata_from_memoryview(memoryview mv):
    """Return typed ParquetMetadata from a contiguous memoryview (zero-copy)."""
    if not mv.contiguous:
        raise ValueError("Memoryview must be contiguous")
    cdef parquet_reader.MetadataParseOptions opts
    opts.schema_only = True
    cdef memoryview[uint8_t] mv_bytes = mv.cast('B')
    cdef const uint8_t* buf = &mv_bytes[0]
    cdef size_t size = mv_bytes.nbytes
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _make_metadata(fs)


def read_rowgroup_stats(data):
    """Per-row-group column statistics, for predicate pushdown.

    Args:
        data: bytes, bytearray, or memoryview holding the full parquet file.

    Returns a list with one entry per row group:
        {"num_rows": int,
         "columns": [
             {"name": str, "physical_type": str, "logical_type": str,
              "min": bytes|None, "max": bytes|None, "null_count": int,
              "is_sorted": bool, "sort_descending": bool,
              "sort_nulls_first": bool}, ...]}

    `min`/`max` are the raw parquet statistic bytes (None when absent); decode
    them to typed values with `decode_value(physical_type, logical_type, raw)`.

    `is_sorted` reflects this row group's parquet sorting_columns claim, but
    ONLY when the file's created_by footer field identifies rugo as the
    writer — a file written by any other tool always reads is_sorted=False
    here, regardless of what its footer claims.
    """
    cdef const uint8_t[::1] mem_view
    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")
    cdef size_t size = mem_view.shape[0]

    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        &mem_view[0], size)

    cdef list row_groups = []
    cdef list cols
    cdef size_t rg_i, c_i, n_rg, n_col
    n_rg = fs.row_groups.size()
    for rg_i in range(n_rg):
        cols = []
        n_col = fs.row_groups[rg_i].columns.size()
        for c_i in range(n_col):
            cols.append({
                "name": fs.row_groups[rg_i].columns[c_i].name.decode("utf-8"),
                "physical_type": fs.row_groups[rg_i].columns[c_i].physical_type.decode("utf-8"),
                "logical_type": fs.row_groups[rg_i].columns[c_i].logical_type.decode("utf-8"),
                "min": (<bytes>fs.row_groups[rg_i].columns[c_i].min)
                       if fs.row_groups[rg_i].columns[c_i].has_min else None,
                "max": (<bytes>fs.row_groups[rg_i].columns[c_i].max)
                       if fs.row_groups[rg_i].columns[c_i].has_max else None,
                "null_count": fs.row_groups[rg_i].columns[c_i].null_count,
                "distinct_count": (fs.row_groups[rg_i].columns[c_i].distinct_count
                       if fs.row_groups[rg_i].columns[c_i].distinct_count >= 0 else None),
                "bloom_offset": fs.row_groups[rg_i].columns[c_i].bloom_offset,
                "bloom_length": fs.row_groups[rg_i].columns[c_i].bloom_length,
                # Clustering: True only for files rugo itself wrote (see
                # metadata.cpp's created_by trust gate) — never a foreign tool's
                # claim, and never a caller-asserted hint verified after the fact.
                "is_sorted": fs.row_groups[rg_i].columns[c_i].is_sorted,
                "sort_descending": fs.row_groups[rg_i].columns[c_i].sort_descending,
                "sort_nulls_first": fs.row_groups[rg_i].columns[c_i].sort_nulls_first,
            })
        row_groups.append({"num_rows": fs.row_groups[rg_i].num_rows, "columns": cols})
    return row_groups


def can_decode(str path):
    """Check if a parquet file can be decoded with our limited decoder.

    Returns True only if:
    - All columns are uncompressed
    - All columns use PLAIN encoding
    - All columns are int32, int64, or string types
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef string cpp_path = path_bytes
    return parquet_reader.CanDecode(cpp_path)

def bloom_filter_maybe_contains(path, bloom_offset, bloom_length, bytes value):
    """Probe a parquet column bloom filter at the given offset.

    Returns False only if the value is DEFINITELY absent; True means it MAY be
    present (bloom filters have no false negatives, but allow false positives).

    `value` is the raw PLAIN-encoded bytes of the candidate — exactly the bytes
    the writer hashed (e.g. 8 little-endian bytes for int64, the UTF-8/raw bytes
    for byte_array). Encoding the candidate to plain bytes is the caller's job
    (the boundary); rugo performs no type coercion.
    """
    if bloom_offset is None:
        raise ValueError("Bloom filter offset is required")

    cdef long long native_offset = <long long>bloom_offset
    if native_offset < 0:
        raise ValueError("Bloom filter offset must be non-negative")

    cdef long long native_length
    if bloom_length is None:
        native_length = -1
    else:
        native_length = <long long>bloom_length
        if native_length <= 0:
            native_length = -1

    cdef bytes path_bytes = os.fspath(path).encode("utf-8")
    cdef parquet_reader.string c_path = path_bytes
    cdef parquet_reader.string c_value = value

    return bool(parquet_reader.TestBloomFilter(c_path, native_offset, native_length, c_value))


def bloom_filter_bytes_maybe_contains(const uint8_t[::1] data, bytes value):
    """In-memory sibling of bloom_filter_maybe_contains: probe a bloom filter
    whose serialized bytes (header + bitset) are already in a buffer, rather than
    reading them from a file. `data` spans exactly the bloom region. `value` is
    the raw PLAIN-encoded candidate bytes (same encoding contract as
    bloom_filter_maybe_contains). Returns False only if DEFINITELY absent.

    This is the exact probe the remote decode-skip runs on the bloom bytes it
    fetches contiguously in front of a column chunk.
    """
    cdef parquet_reader.string c_value = value
    if data.shape[0] == 0:
        return False
    return bool(parquet_reader.TestBloomFilterBytes(&data[0], <size_t>data.shape[0], c_value))


def can_decode_from_memory(data):
    """Check if parquet data in memory can be decoded with our limited decoder.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data

    Returns:
        bool: True if the data can be decoded, False otherwise
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size

    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]
    return bool(parquet_reader.CanDecode(&mem_view[0], size))


# --- Helper functions to build Draken vectors from DecodedColumn ---

cdef inline void _expand_rle_int64_into(int64_t* dst,
                                         parquet_reader.DecodedColumn& decoded_col,
                                         int32_t num_rows):
    """Expand rle_int64_values × rle_run_lengths into dense int64 output."""
    cdef Py_ssize_t off = 0
    cdef Py_ssize_t r, j
    cdef Py_ssize_t cnt
    cdef int64_t val
    for r in range(decoded_col.rle_run_lengths.size()):
        val = decoded_col.rle_int64_values[r]
        cnt = decoded_col.rle_run_lengths[r]
        for j in range(cnt):
            dst[off + j] = val
        off += cnt


cdef inline void _expand_rle_float64_into(double* dst,
                                           parquet_reader.DecodedColumn& decoded_col,
                                           int32_t num_rows):
    """Expand rle_float64_values × rle_run_lengths into dense float64 output."""
    cdef Py_ssize_t off = 0
    cdef Py_ssize_t r, j
    cdef Py_ssize_t cnt
    cdef double val
    for r in range(decoded_col.rle_run_lengths.size()):
        val = decoded_col.rle_float64_values[r]
        cnt = decoded_col.rle_run_lengths[r]
        for j in range(cnt):
            dst[off + j] = val
        off += cnt


# --- E.28 reconstruction helpers ---------------------------------------------
# Materialize a decoded column's values (any shape: plain dense, dictionary via
# indices or packed codes, or RLE) into a Python list with None for nulls, then
# build a Draken vector via the null-safe native sequence constructor. Dense and
# dictionary value buffers hold only NON-NULL values, indexed past nulls via the
# valid_bits bitmap (Arrow-style, 1 = valid), matching the C++ decoder.

cdef inline bint _row_valid(parquet_reader.DecodedColumn& col, Py_ssize_t i) noexcept:
    if col.valid_bits.size() == 0:
        return True
    return ((col.valid_bits[i >> 3] >> (i & 7)) & 1) != 0


cdef inline uint32_t _read_code(vector[uint8_t]& arr, Py_ssize_t i, uint8_t width) noexcept:
    cdef Py_ssize_t off = i * width
    if width == 1:
        return arr[off]
    if width == 2:
        return arr[off] | (<uint32_t>arr[off + 1] << 8)
    return (arr[off] | (<uint32_t>arr[off + 1] << 8)
            | (<uint32_t>arr[off + 2] << 16) | (<uint32_t>arr[off + 3] << 24))


cdef inline bytes _dict_str_at(parquet_reader.DecodedColumn& col, Py_ssize_t idx):
    cdef uint32_t start = col.string_dict_offsets[idx]
    cdef int32_t ln = col.string_dict_lens[idx]
    cdef const uint8_t* base = col.string_dict_arena.data()
    return (<char*>(base + start))[:ln]


cdef inline bytes _dense_str_at(parquet_reader.DecodedColumn& col, Py_ssize_t idx):
    cdef uint32_t start = col.string_offsets[idx]
    cdef int32_t ln = col.string_lens[idx]
    cdef const uint8_t* base = col.string_arena.data()
    return (<char*>(base + start))[:ln]


cdef list _int64_list(parquet_reader.DecodedColumn& col, int32_t num_rows,
                      bint from_int32):
    # FLATTEN-TO-PYTHON BY DESIGN (see module banner). This is rugo's standalone
    # reader endpoint: even when the column is dict-encoded on disk, we expand it
    # into a per-row Python list (`out[i] = dict_values[code]`) for a Python
    # consumer. This is NOT the opteryx scan — that path (pool_reader) keeps the
    # dict shape and never builds Python objects per row.
    cdef list out = [None] * num_rows
    cdef Py_ssize_t i, vi = 0, off = 0, r, j, cnt
    cdef bint has_v = col.valid_bits.size() > 0
    cdef uint8_t cw
    # Unsigned columns store the raw magnitude in signed int32/int64 slots. Emit
    # NON-NEGATIVE Python ints (reinterpret the bits) so the caller can build a
    # DRAKEN_UINT64 vector — a plain `<int64_t>` cast would sign-extend a uint32
    # (4e9 -> -294967296) or hand back a negative int64 for a uint64 > 2**63.
    # Mirrors the array leaf path (`_array_leaf_values`).
    cdef bint uns = col.is_unsigned
    if _decoded_has_dictionary(col):
        if not col.dict_codes_array.empty():
            cw = col.code_width if col.code_width in (1, 2, 4) else 1
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                if from_int32:
                    if uns:
                        out[i] = <uint64_t><uint32_t>col.dict_int32_values[_read_code(col.dict_codes_array, i, cw)]
                    else:
                        out[i] = <int64_t>col.dict_int32_values[_read_code(col.dict_codes_array, i, cw)]
                else:
                    if uns:
                        out[i] = <uint64_t>col.dict_int64_values[_read_code(col.dict_codes_array, i, cw)]
                    else:
                        out[i] = col.dict_int64_values[_read_code(col.dict_codes_array, i, cw)]
        else:
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                if from_int32:
                    if uns:
                        out[i] = <uint64_t><uint32_t>col.dict_int32_values[col.dict_indices[vi]]
                    else:
                        out[i] = <int64_t>col.dict_int32_values[col.dict_indices[vi]]
                else:
                    if uns:
                        out[i] = <uint64_t>col.dict_int64_values[col.dict_indices[vi]]
                    else:
                        out[i] = col.dict_int64_values[col.dict_indices[vi]]
                vi += 1
        return out
    if col.rle_run_lengths.size() > 0:
        # rle_int64_values holds resolved values for both int64 and int32 (rle_path)
        for r in range(col.rle_run_lengths.size()):
            cnt = col.rle_run_lengths[r]
            for j in range(cnt):
                if uns:
                    out[off + j] = <uint64_t>col.rle_int64_values[r]
                else:
                    out[off + j] = col.rle_int64_values[r]
            off += cnt
        return out
    for i in range(num_rows):
        if has_v and not _row_valid(col, i):
            continue
        if from_int32:
            if uns:
                out[i] = <uint64_t><uint32_t>col.int32_values[vi]
            else:
                out[i] = <int64_t>col.int32_values[vi]
        else:
            if uns:
                out[i] = <uint64_t>col.int64_values[vi]
            else:
                out[i] = col.int64_values[vi]
        vi += 1
    return out


# _DRAKEN_LK_IPV4 is defined once for the whole extension in rugo_native.pyx
# (the parquet reader and writer are two .pxi in ONE translation unit).
cdef int _DK_UINT32 = int(_dn.UINT32.value)


cdef Vector _attach_draken_logical(Vector vec, int kind, object col_name):
    """Attach the file's draken logical descriptor to a freshly built vector.

    Zero-copy: the retag MOVES the vector's buffers and attaches the descriptor,
    leaving the physical type tag untouched (IPv4 IS uint32). Safe here because
    `vec` was built one statement ago and this is its sole reference.

    An annotation whose kind cannot apply to the column as decoded is a file
    that contradicts itself — rugo only ever writes the IPV4 entry over a
    UINT32 column. Reinterpreting some other column's bytes as addresses, or
    dropping the annotation and returning a plausible wrong type, are both
    silent; this fails instead.
    """
    if kind == _DRAKEN_LK_IPV4:
        if int(vec._nb.type.value) != _DK_UINT32:
            raise ValueError(
                "rugo parquet reader: column %r is annotated IPV4 in the file's "
                "key-value metadata but decoded as %r, not UINT32"
                % (col_name, vec._nb.type)
            )
        return Vector(_dn.vector_retag_uint32_as_ipv4(vec._nb))
    raise NotImplementedError(
        "rugo parquet reader: column %r carries draken logical kind %d, which "
        "this reader cannot reconstruct" % (col_name, kind)
    )


cdef inline Vector _make_int_vector(parquet_reader.DecodedColumn& col,
                                    int32_t num_rows, bint from_int32):
    """Build an int Vector from the flattened list at the column's DECLARED width
    and signedness, so a write/read round trip returns the type it started with.

    Width comes from the IntType annotation (`int_bit_width`); an unannotated
    column has no annotation to read, so its width is exactly what the physical
    type says — int32 on the wire is a 32-bit column, int64 is a 64-bit one.
    `_int64_list` has already reinterpreted unsigned bits to non-negative Python
    ints, which the unsigned builders require."""
    cdef list vals = _int64_list(col, num_rows, from_int32)
    cdef int32_t w = col.int_bit_width
    if col.is_unsigned:
        if w == 8:
            return Vector(_dn.vector_uint8_from_sequence(vals))
        if w == 16:
            return Vector(_dn.vector_uint16_from_sequence(vals))
        if w == 32:
            return Vector(_dn.vector_uint32_from_sequence(vals))
        return Vector(_dn.vector_uint64_from_sequence(vals))
    if w == 8:
        return Vector(_dn.vector_int8_from_sequence(vals))
    if w == 16:
        return Vector(_dn.vector_int16_from_sequence(vals))
    if w == 32 or (w == 0 and from_int32):
        return Vector(_dn.vector_int32_from_sequence(vals))
    return Vector(_dn.vector_from_sequence(vals))


# int128 → Python int. `DecodedColumn::int128_values` is a std::vector<__int128>
# (compact: non-null values only), which Cython cannot represent — this tiny C++
# shim reads each 16-byte little-endian signed value into a Python int, emitting
# None for null rows. Defined inline (not in decode.hpp) so rugo's pure-C++ core
# stays free of Python.h.
cdef extern from *:
    """
    #include <Python.h>
    #include <cstdint>
    static inline PyObject* _rugo_int128_to_pylong(__int128 v) {
        return _PyLong_FromByteArray(
            reinterpret_cast<const unsigned char*>(&v), 16,
            /*little_endian=*/1, /*is_signed=*/1);
    }
    static inline uint32_t _rugo_read_code(const std::vector<uint8_t>& arr,
                                           size_t i, uint8_t width) {
        size_t off = (size_t)i * width;
        if (width == 1) return arr[off];
        if (width == 2) return arr[off] | ((uint32_t)arr[off + 1] << 8);
        return arr[off] | ((uint32_t)arr[off + 1] << 8)
             | ((uint32_t)arr[off + 2] << 16) | ((uint32_t)arr[off + 3] << 24);
    }
    static PyObject* rugo_int128_unscaled_pylist(const DecodedColumn& col, int32_t num_rows) {
        // Two shapes:
        //   dict  — dict_int128_values holds the value table; per-row codes live in
        //           dict_codes_array (nullable, packed by row) or dict_indices
        //           (non-nullable, compact valid-only). Mirrors _int64_list exactly.
        //   plain — int128_values is the compact (non-null) dense payload.
        PyObject* out = PyList_New(num_rows);
        if (!out) return NULL;
        const bool has_v = !col.valid_bits.empty();
        if (!col.dict_int128_values.empty()) {
            const size_t dict_sz = col.dict_int128_values.size();
            const bool use_codes = !col.dict_codes_array.empty();
            const uint8_t cw = (col.code_width == 1 || col.code_width == 2 ||
                                col.code_width == 4) ? col.code_width : 1;
            size_t vi = 0;
            for (int32_t i = 0; i < num_rows; ++i) {
                bool valid = true;
                if (has_v) valid = ((col.valid_bits[i >> 3] >> (i & 7)) & 1) != 0;
                if (!valid) { Py_INCREF(Py_None); PyList_SET_ITEM(out, i, Py_None); continue; }
                uint32_t code = use_codes
                    ? _rugo_read_code(col.dict_codes_array, (size_t)i, cw)
                    : col.dict_indices[vi++];
                if ((size_t)code >= dict_sz) {  // fail safe on a corrupt code
                    Py_DECREF(out);
                    PyErr_SetString(PyExc_ValueError,
                        "int128 dict code out of range");
                    return NULL;
                }
                PyObject* num = _rugo_int128_to_pylong(col.dict_int128_values[code]);
                if (!num) { Py_DECREF(out); return NULL; }
                PyList_SET_ITEM(out, i, num);
            }
            return out;
        }
        size_t vi = 0;
        for (int32_t i = 0; i < num_rows; ++i) {
            bool valid = true;
            if (has_v) valid = ((col.valid_bits[i >> 3] >> (i & 7)) & 1) != 0;
            if (!valid) { Py_INCREF(Py_None); PyList_SET_ITEM(out, i, Py_None); continue; }
            PyObject* num = _rugo_int128_to_pylong(col.int128_values[vi++]);
            if (!num) { Py_DECREF(out); return NULL; }
            PyList_SET_ITEM(out, i, num);
        }
        return out;
    }
    """
    object rugo_int128_unscaled_pylist(parquet_reader.DecodedColumn& col, int32_t num_rows)


cdef Vector _make_decimal_vector(parquet_reader.DecodedColumn& col, int32_t num_rows):
    """Build a DECIMAL Vector from a decoded parquet DECIMAL column.

    Parquet stores decimals as INT32/INT64/FIXED_LEN_BYTE_ARRAY; rugo's decoder
    tiers by byte width into `int64_values` (width <= 8, `col.type` int64/int32)
    or `int128_values` (width 9..16, `col.type` int128). Either way the value is
    the SIGNED UNSCALED integer; we scale it by 10**-S into a decimal.Decimal and
    build a native DRAKEN_DECIMAL (P<=18) / DRAKEN_DECIMAL128 vector carrying
    precision+scale — instead of silently dropping the >64-bit tier (old
    behaviour: no int128 branch → returned None) or handing back a bare unscaled
    int with no decimal identity (old <=8 behaviour). Precision/scale come from
    `col.decimal_precision`/`col.decimal_scale` (set in decode_column.cpp), so
    this works on the morsel path where no per-column logical-type string exists.
    """
    cdef int precision = col.decimal_precision
    cdef int scale = col.decimal_scale

    cdef list unscaled
    if col.type == b"int128":
        unscaled = rugo_int128_unscaled_pylist(col, num_rows)
    else:
        unscaled = _int64_list(col, num_rows, col.type == b"int32")

    cdef list decs = [None] * num_rows
    cdef Py_ssize_t i
    cdef object u
    for i in range(num_rows):
        u = unscaled[i]
        if u is not None:
            decs[i] = _Decimal(u).scaleb(-scale)

    if precision <= 18:
        return Vector(_dn.vector_decimal_from_sequence(decs, precision, scale))
    return Vector(_dn.vector_decimal128_from_sequence(decs, precision, scale))


cdef list _float64_list(parquet_reader.DecodedColumn& col, int32_t num_rows,
                        bint from_float32):
    # FLATTEN-TO-PYTHON BY DESIGN — standalone rugo reader endpoint; see
    # `_int64_list` and the module banner. Not the opteryx scan path.
    cdef list out = [None] * num_rows
    cdef Py_ssize_t i, vi = 0, off = 0, r, j, cnt
    cdef bint has_v = col.valid_bits.size() > 0
    cdef uint8_t cw
    if _decoded_has_dictionary(col):
        if not col.dict_codes_array.empty():
            cw = col.code_width if col.code_width in (1, 2, 4) else 1
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                if from_float32:
                    out[i] = <double>col.dict_float32_values[_read_code(col.dict_codes_array, i, cw)]
                else:
                    out[i] = col.dict_float64_values[_read_code(col.dict_codes_array, i, cw)]
        else:
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                if from_float32:
                    out[i] = <double>col.dict_float32_values[col.dict_indices[vi]]
                else:
                    out[i] = col.dict_float64_values[col.dict_indices[vi]]
                vi += 1
        return out
    if col.rle_run_lengths.size() > 0:
        # rle_float64_values holds resolved values for both float64 and float32 (rle_path)
        for r in range(col.rle_run_lengths.size()):
            cnt = col.rle_run_lengths[r]
            for j in range(cnt):
                out[off + j] = col.rle_float64_values[r]
            off += cnt
        return out
    for i in range(num_rows):
        if has_v and not _row_valid(col, i):
            continue
        if from_float32:
            out[i] = <double>col.float32_values[vi]
        else:
            out[i] = col.float64_values[vi]
        vi += 1
    return out


cdef list _string_list(parquet_reader.DecodedColumn& col, int32_t num_rows):
    # FLATTEN-TO-PYTHON BY DESIGN — standalone rugo reader endpoint; see
    # `_int64_list` and the module banner. Not the opteryx scan path.
    cdef list out = [None] * num_rows
    cdef Py_ssize_t i, vi = 0, off = 0, r, j, cnt
    cdef bint has_v = col.valid_bits.size() > 0
    cdef uint8_t cw
    cdef const uint8_t* arena
    cdef uint32_t start
    cdef int32_t ln
    cdef bytes value
    if _decoded_has_dictionary(col):
        if not col.dict_codes_array.empty():
            cw = col.code_width if col.code_width in (1, 2, 4) else 1
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                out[i] = _dict_str_at(col, _read_code(col.dict_codes_array, i, cw))
        else:
            for i in range(num_rows):
                if has_v and not _row_valid(col, i):
                    continue
                out[i] = _dict_str_at(col, col.dict_indices[vi])
                vi += 1
        return out
    if col.rle_run_lengths.size() > 0:
        # Skip-dense RLE output (non-nullable dict byte_array column, mirrors
        # `_int64_list`/`_float64_list`): each run's resolved string bytes live in
        # rle_str_arena at [rle_str_offsets[r], +rle_str_lens[r]), repeated
        # rle_run_lengths[r] times.
        arena = col.rle_str_arena.data()
        for r in range(col.rle_run_lengths.size()):
            cnt = col.rle_run_lengths[r]
            start = col.rle_str_offsets[r]
            ln = col.rle_str_lens[r]
            value = (<char*>(arena + start))[:ln]
            for j in range(cnt):
                out[off + j] = value
            off += cnt
        return out
    for i in range(num_rows):
        if has_v and not _row_valid(col, i):
            continue
        out[i] = _dense_str_at(col, vi)
        vi += 1
    return out


cdef list _bool_list(parquet_reader.DecodedColumn& col, int32_t num_rows):
    cdef list out = [None] * num_rows
    cdef Py_ssize_t i, vi = 0
    cdef bint has_v = col.valid_bits.size() > 0
    for i in range(num_rows):
        if has_v and not _row_valid(col, i):
            continue
        out[i] = col.boolean_values[vi] != 0
        vi += 1
    return out


cdef Vector _make_int64_from_int32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, True)


cdef Vector _make_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, False)


cdef Vector _make_float64_from_float32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, True)))


cdef Vector _make_int32_as_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, True)


cdef Vector _make_float64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, False)))


cdef Vector _make_string_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_from_string_sequence(_string_list(decoded_col, num_rows)))




cdef inline uint8_t _code_width_from_dict_size(Py_ssize_t dict_size):
    if dict_size <= 256:
        return 1
    if dict_size <= 65536:
        return 2
    return 4


cdef inline bint _decoded_has_dictionary(parquet_reader.DecodedColumn& decoded_col):
    cdef bytes col_type = decoded_col.type
    # dict_codes_array path: nullable dict column where C++ scatters codes into a
    # packed array instead of populating dict_indices (mutually exclusive paths).
    if not decoded_col.dict_codes_array.empty():
        if col_type == b"byte_array":
            return decoded_col.string_dict_lens.size() > 0
        if col_type == b"int32":
            return decoded_col.dict_int32_values.size() > 0
        if col_type == b"int64":
            return decoded_col.dict_int64_values.size() > 0
        if col_type == b"float32":
            return decoded_col.dict_float32_values.size() > 0
        if col_type == b"float64":
            return decoded_col.dict_float64_values.size() > 0
        return False
    # dict_indices path: standard dict column (non-nullable or rle).
    if decoded_col.dict_indices.size() == 0:
        return False
    if col_type == b"byte_array":
        return decoded_col.string_dict_lens.size() > 0
    if col_type == b"int32":
        return decoded_col.dict_int32_values.size() > 0
    if col_type == b"int64":
        return decoded_col.dict_int64_values.size() > 0
    if col_type == b"float32":
        return decoded_col.dict_float32_values.size() > 0
    if col_type == b"float64":
        return decoded_col.dict_float64_values.size() > 0
    return False


cdef inline Py_ssize_t _decoded_dict_size(parquet_reader.DecodedColumn& decoded_col):
    cdef bytes col_type = decoded_col.type
    if col_type == b"byte_array":
        return decoded_col.string_dict_lens.size()
    if col_type == b"int32":
        return decoded_col.dict_int32_values.size()
    if col_type == b"int64":
        return decoded_col.dict_int64_values.size()
    if col_type == b"float32":
        return decoded_col.dict_float32_values.size()
    if col_type == b"float64":
        return decoded_col.dict_float64_values.size()
    return 0


cdef inline bint _decoded_all_valid(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t i
    if num_rows <= 0:
        return False
    if decoded_col.valid_bits.size() == 0:
        return True
    for i in range(num_rows):
        if ((decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1) == 0:
            return False
    return True


cdef inline bint _decoded_all_null(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t i
    if num_rows <= 0:
        return False
    if decoded_col.valid_bits.size() == 0:
        return False
    for i in range(num_rows):
        if ((decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1) != 0:
            return False
    return True


cdef inline bint _should_emit_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size
    if not _decoded_has_dictionary(decoded_col):
        return False
    dict_size = _decoded_dict_size(decoded_col)
    if dict_size <= 0:
        return False
    return True


cdef inline bint _should_emit_constant_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    if not _decoded_has_dictionary(decoded_col):
        return False
    if _decoded_dict_size(decoded_col) != 1:
        return False
    return _decoded_all_valid(decoded_col, num_rows) or _decoded_all_null(decoded_col, num_rows)


cdef inline void _record_dictionary_decode(parquet_reader.DecodedColumn& decoded_col):
    cdef Py_ssize_t dict_size = _decoded_dict_size(decoded_col)
    cdef uint8_t code_width

    if dict_size <= 0:
        return

    code_width = decoded_col.code_width if decoded_col.code_width in (1, 2, 4) else _code_width_from_dict_size(dict_size)
    _TEL["parquet_dict_columns_decoded"] += 1
    _TEL["parquet_dict_unique_values"] += dict_size
    _TEL["parquet_dict_code_width_bytes"] += code_width


cdef int _fill_dict_codes_and_validity(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows,
        int32_t* codes,
        uint8_t** validity_out) except -1:
    """Fill pre-allocated codes[num_rows]; malloc and fill validity when nullable.

    Returns 1 (nullable — *validity_out is a malloc'd buffer owned by caller),
            0 (all valid — *validity_out is NULL).
    Caller must free(*validity_out) if non-NULL.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t val_idx = 0
    cdef uint8_t* validity

    validity_out[0] = NULL

    if decoded_col.valid_bits.size() > 0:
        validity = <uint8_t*>malloc(num_rows)
        if validity == NULL:
            raise MemoryError()
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if val_idx >= <Py_ssize_t>decoded_col.dict_indices.size():
                    free(validity)
                    raise ValueError("dictionary index stream shorter than number of valid rows")
                codes[i] = decoded_col.dict_indices[val_idx]
                validity[i] = 1
                val_idx += 1
            else:
                codes[i] = 0
                validity[i] = 0
        validity_out[0] = validity
        return 1
    else:
        if decoded_col.dict_indices.size() != <size_t>num_rows:
            raise ValueError("dictionary index stream length does not match row count")
        for i in range(num_rows):
            codes[i] = decoded_col.dict_indices[i]
        return 0


cdef Vector _make_typed_int64_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, False)


cdef Vector _make_typed_int64_from_int32_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, True)


cdef Vector _make_typed_float64_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, False)))


cdef Vector _make_typed_float64_from_float32_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, True)))


cdef Vector _make_typed_string_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_from_string_sequence(_string_list(decoded_col, num_rows)))


cdef Vector _make_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Vector from a decoded parquet dictionary payload, for rugo's
    STANDALONE reader endpoint (see module banner).

    NOTE: despite the name, this does NOT preserve the on-disk dictionary. It
    routes to the `_make_typed_*_dictionary_vector` makers, which FLATTEN the
    column to a per-row Python list (`_int64_list` / `_string_list`) and rebuild
    a dense/auto-dict vector — because this path serves Python consumers
    (read_parquet / catalog / tests), not the engine. The opteryx execution scan
    keeps dict shape natively in pool_reader; it never calls this.
    """
    cdef bytes col_type = decoded_col.type

    if col_type == b"byte_array":
        return _make_typed_string_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"int32":
        return _make_typed_int64_from_int32_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"int64":
        return _make_typed_int64_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"float32":
        return _make_typed_float64_from_float32_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"float64":
        return _make_typed_float64_dictionary_vector(decoded_col, num_rows)

    raise ValueError(f"unsupported dictionary type for decoded column: {col_type!r}")


cdef Vector _make_typed_constant_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    # A constant is just a dict of size 1; reuse the materializers (dense vector).
    cdef bytes col_type = decoded_col.type
    if col_type == b"int64":
        return _make_int_vector(decoded_col, num_rows, False)
    if col_type == b"int32":
        return _make_int_vector(decoded_col, num_rows, True)
    if col_type == b"float64":
        return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, False)))
    if col_type == b"float32":
        return Vector(_dn.vector_float64_from_sequence(_float64_list(decoded_col, num_rows, True)))
    if col_type == b"byte_array":
        return Vector(_dn.vector_from_string_sequence(_string_list(decoded_col, num_rows)))
    raise ValueError(f"unsupported constant column type: {col_type!r}")


cdef Vector _make_bool_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(_dn.vector_from_bool_sequence(_bool_list(decoded_col, num_rows)))


# --- list / array reconstruction ---------------------------------------------
# A repeated (LIST) column is decoded by C++ into parallel rep_levels/def_levels
# (one entry per leaf position) plus the present leaf values (compact, in order).
# Both the rugo writer and pyarrow emit the standard all-nullable nesting scheme
# for a list nested D deep (D == max_rep_level): max_def_level == 2*D + 1, where
# for list level k (1 = outermost .. D = innermost):
#   * list k is non-null      when def >= 2k - 1
#   * list k has a child entry when def >= 2k
#   * the leaf element exists  when def >= 2D   (null element at 2D, present at 2D+1)
# and repetition level r means list levels 1..r continue the previous record;
# a fresh sub-structure begins at depth r+1. A value is consumed from the leaf
# stream only when def == max_def_level (present element).
#
# Element types: int32/int64 (-> INT64), float32/float64 (-> FLOAT64), bool,
# and byte_array (-> VARCHAR str). Draken's vector_array_from_sequence detects
# the child type from the leaf Python objects (bool before int), so leaves are
# materialized as the matching Python scalar type.


cdef list _array_leaf_values(parquet_reader.DecodedColumn& col):
    """Present leaf values, in stream order, as a Python list.

    Mirrors the per-type/per-encoding accessors used by the scalar
    materializers (plain dense, dict via packed per-level codes, dict via
    compact present-order indices)."""
    cdef bytes col_type = col.type
    cdef Py_ssize_t n_levels = col.def_levels.size()
    cdef int32_t max_def = col.max_def_level
    cdef Py_ssize_t i, vi = 0
    cdef bint has_dict = _decoded_has_dictionary(col)
    cdef bint use_codes = has_dict and (not col.dict_codes_array.empty())
    cdef uint8_t cw = col.code_width if col.code_width in (1, 2, 4) else 1
    cdef list vals = []
    # Unsigned leaf (Parquet INTEGER(width, isSigned=false)): the physical bits
    # are read as signed then reinterpreted so values above INT64_MAX come back
    # as the correct non-negative Python int (draken's UINT64 constructor rejects
    # negatives). Mirrors decode_column.cpp's is_unsigned tagging.
    cdef bint uns = col.is_unsigned
    cdef int64_t iraw

    if col_type == b"int64":
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            if use_codes:
                iraw = col.dict_int64_values[_read_code(col.dict_codes_array, i, cw)]
            elif has_dict:
                iraw = col.dict_int64_values[col.dict_indices[vi]]; vi += 1
            else:
                iraw = col.int64_values[vi]; vi += 1
            if uns:
                vals.append(<uint64_t>iraw)
            else:
                vals.append(iraw)
    elif col_type == b"int32":
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            if use_codes:
                iraw = <int64_t>col.dict_int32_values[_read_code(col.dict_codes_array, i, cw)]
            elif has_dict:
                iraw = <int64_t>col.dict_int32_values[col.dict_indices[vi]]; vi += 1
            else:
                iraw = <int64_t>col.int32_values[vi]; vi += 1
            if uns:
                vals.append(<uint64_t>(<uint32_t>iraw))
            else:
                vals.append(iraw)
    elif col_type == b"byte_array":
        # Draken's array constructor detects a string child by PyUnicode, so leaf
        # values must be `str` (utf-8), matching the scalar string path's logical
        # VARCHAR semantics.
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            if use_codes:
                vals.append(_dict_str_at(col, _read_code(col.dict_codes_array, i, cw)).decode("utf-8"))
            elif has_dict:
                vals.append(_dict_str_at(col, col.dict_indices[vi]).decode("utf-8")); vi += 1
            else:
                vals.append(_dense_str_at(col, vi).decode("utf-8")); vi += 1
    elif col_type == b"float64":
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            if use_codes:
                vals.append(col.dict_float64_values[_read_code(col.dict_codes_array, i, cw)])
            elif has_dict:
                vals.append(col.dict_float64_values[col.dict_indices[vi]]); vi += 1
            else:
                vals.append(col.float64_values[vi]); vi += 1
    elif col_type == b"float32":
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            if use_codes:
                vals.append(<double>col.dict_float32_values[_read_code(col.dict_codes_array, i, cw)])
            elif has_dict:
                vals.append(<double>col.dict_float32_values[col.dict_indices[vi]]); vi += 1
            else:
                vals.append(<double>col.float32_values[vi]); vi += 1
    elif col_type == b"boolean":
        # boolean is never dictionary-encoded by the decoder.
        for i in range(n_levels):
            if col.def_levels[i] != max_def:
                continue
            vals.append(col.boolean_values[vi] != 0); vi += 1
    else:
        raise NotImplementedError(
            "rugo array reader: unsupported list element physical type %r" % col_type
        )
    return vals


cdef Vector _make_array_vector(
        parquet_reader.DecodedColumn& decoded_col):
    cdef int32_t D = decoded_col.max_rep_level
    cdef int32_t max_def = decoded_col.max_def_level
    cdef Py_ssize_t n_levels = decoded_col.def_levels.size()
    cdef Py_ssize_t i, li = 0
    cdef int32_t r, d, k

    if D < 1:
        raise NotImplementedError(
            "rugo array reader: column has max_rep_level=%d but was routed to the "
            "array path" % D
        )
    if max_def != 2 * D + 1:
        raise NotImplementedError(
            "rugo array reader: unsupported list level scheme (max_rep_level=%d, "
            "max_def_level=%d; expected max_def_level=%d for all-nullable nesting)."
            % (D, max_def, 2 * D + 1)
        )

    cdef list leaf_vals = _array_leaf_values(decoded_col)  # raises for float/bool
    cdef list out = []
    # open_lists[k] is the currently-open list at depth k (1..D); index 0 unused.
    cdef list open_lists = [None] * (D + 1)
    cdef list parent
    cdef list new
    cdef bint has_leaf

    for i in range(n_levels):
        r = decoded_col.rep_levels[i]
        d = decoded_col.def_levels[i]
        has_leaf = True
        # Open new sub-lists from depth r+1 down as deep as `d` defines them.
        k = r + 1
        while k <= D:
            parent = out if k == 1 else open_lists[k - 1]
            if d >= 2 * k - 1:               # list k is non-null
                new = []
                parent.append(new)
                open_lists[k] = new
                if d < 2 * k:                # present but EMPTY -> no children
                    has_leaf = False
                    break
            else:                            # list k is null
                parent.append(None)
                has_leaf = False
                break
            k += 1
        if not has_leaf:
            continue
        # The innermost open list receives the leaf element.
        if d == max_def:                     # present element
            open_lists[D].append(leaf_vals[li]); li += 1
        else:                                # d == 2*D -> null element
            open_lists[D].append(None)

    # Leaf element type comes from the schema, not value inference: an all-null
    # or all-empty list column (e.g. manifest min/max_values_display over numeric
    # columns) has no leaf value to infer from. D is the list nesting depth, so
    # the constructor knows to build ARRAY children above the leaf level.
    cdef bytes col_type = decoded_col.type
    cdef int el_type
    if col_type == b"byte_array":
        el_type = _DK_EL_VARCHAR
    elif col_type == b"int64" or col_type == b"int32":
        # Unsigned leaf → UINT64 child so the draken constructor stores the
        # full unsigned range (values were reinterpreted in _array_leaf_values).
        el_type = _DK_EL_UINT64 if decoded_col.is_unsigned else _DK_EL_INT64
    elif col_type == b"float64" or col_type == b"float32":
        el_type = _DK_EL_FLOAT64
    elif col_type == b"boolean":
        el_type = _DK_EL_BOOL
    else:
        raise NotImplementedError(
            "rugo array reader: unsupported list element physical type %r" % col_type
        )

    return Vector(_dn.vector_array_from_sequence(out, el_type, D))


cdef object _morsel_from_row_group(vector[parquet_reader.DecodedColumn]& row_group_columns, list col_names):
    """Build one Morsel from one row group's already-decoded columns.

    Shared by the eager (whole-file list, _decode_from_buffer) and streaming
    (one-row-group-at-a-time generator, stream_parquet/stream_parquet_from_path)
    decode paths below. The only difference between those two callers is how
    many row groups' worth of DecodedColumn buffers are resident in memory
    when this runs — this function itself only ever touches one row group's.
    Caller skips calling this for an empty (pruned, or zero-column-projection)
    row group.
    """
    cdef list vectors = []
    cdef list successful_col_names = []
    cdef int32_t num_rows = 0
    cdef Py_ssize_t col_idx
    cdef parquet_reader.DecodedColumn column
    cdef str col_type
    cdef Vector vec
    cdef double _t0

    # Get the logical row count from the first successful NON-REPEATED column.
    # A repeated (LIST) column's `num_rows` is the leaf-level value/level-pair
    # count (accumulated per page as page_header.num_values), NOT the logical
    # record count — for a list averaging >1 element/row it overshoots. That
    # count is only correct for flat columns, and it is what sizes every
    # scalar/string materializer below (arrays build their own length via
    # _make_array_vector). Picking it off a leading ARRAY column made the flat
    # columns over-read by the element surplus, so the derived filter mask then
    # indexed past the (correctly sized) array column — "take: array index out
    # of range". Skip repeated columns here; if every projected column is
    # repeated, no materializer consumes num_rows so 0 is harmless.
    for col_idx in range(<Py_ssize_t>row_group_columns.size()):
        if (row_group_columns[col_idx].success
                and row_group_columns[col_idx].rep_levels.size() == 0):
            num_rows = row_group_columns[col_idx].num_rows
            if num_rows > 0:
                break

    _TEL["row_groups"] += 1

    for col_idx in range(<Py_ssize_t>row_group_columns.size()):
        column = row_group_columns[col_idx]
        if not column.success:
            continue

        _t0 = _time.perf_counter()

        if column.rep_levels.size() > 0:
            # Repeated (LIST) column — reconstruct nested vector from
            # rep/def levels. Must precede the scalar type branches so a
            # list<int64> / list<float64> etc. is never flattened.
            vec = _make_array_vector(column)
            if column.string_dict_lens.size() > 0:
                _TEL["parquet_dict_materialize_fallbacks"] += 1
            _TEL["cython_str_s"] += _time.perf_counter() - _t0
        elif column.is_decimal:
            # DECIMAL (any tier): real DECIMAL/DECIMAL128 vector, not a bare
            # int — and never drop the int128 tier.
            vec = _make_decimal_vector(column, num_rows)
            _TEL["cython_other_s"] += _time.perf_counter() - _t0
        elif column.type == b"int64":
            if _should_emit_constant_vector(column, num_rows):
                vec = _make_typed_constant_vector(column, num_rows)
            elif _should_emit_dictionary_vector(column, num_rows):
                vec = _make_typed_int64_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_int64_vector(column, num_rows)
            _TEL["cython_int64_s"] += _time.perf_counter() - _t0
        elif column.type == b"int32":
            if _should_emit_constant_vector(column, num_rows):
                vec = _make_typed_constant_vector(column, num_rows)
            elif _should_emit_dictionary_vector(column, num_rows):
                vec = _make_typed_int64_from_int32_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_int64_from_int32_vector(column, num_rows)
            _TEL["cython_int64_s"] += _time.perf_counter() - _t0
        elif column.type == b"byte_array":
            if _should_emit_constant_vector(column, num_rows):
                vec = _make_typed_constant_vector(column, num_rows)
            elif _should_emit_dictionary_vector(column, num_rows):
                vec = _make_typed_string_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_string_vector(column, num_rows)
            _TEL["cython_str_s"] += _time.perf_counter() - _t0
        elif column.type == b"boolean":
            vec = _make_bool_vector(column, num_rows)
            _TEL["cython_bool_s"] += _time.perf_counter() - _t0
        elif column.type == b"float32":
            if _should_emit_constant_vector(column, num_rows):
                vec = _make_typed_constant_vector(column, num_rows)
            elif _should_emit_dictionary_vector(column, num_rows):
                vec = _make_typed_float64_from_float32_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_float64_from_float32_vector(column, num_rows)
            _TEL["cython_float_s"] += _time.perf_counter() - _t0
        elif column.type == b"float64":
            if _should_emit_constant_vector(column, num_rows):
                vec = _make_typed_constant_vector(column, num_rows)
            elif _should_emit_dictionary_vector(column, num_rows):
                vec = _make_typed_float64_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_float64_vector(column, num_rows)
            _TEL["cython_float_s"] += _time.perf_counter() - _t0
        else:
            # A column the C++ decoder accepted but no materializer here can
            # build. Skipping it produced a morsel missing that column — for
            # a single-column file, a zero-column morsel reporting zero rows
            # for a file whose footer says otherwise. There is no honest
            # partial answer: fail with the type we could not build.
            raise NotImplementedError(
                "rugo parquet reader: no vector materializer for column %r "
                "of decoded physical type %r"
                % (col_names[col_idx], column.type.decode("utf-8"))
            )

        # Draken logical descriptor the parquet type system cannot express.
        # The bits are already exactly right — only the label is missing — so
        # this attaches the descriptor and changes nothing else. A column with
        # no annotation (every file written before the writer emitted one)
        # falls straight through: absent means "don't know", never "not IPV4".
        if column.draken_logical_kind != 0:
            vec = _attach_draken_logical(vec, column.draken_logical_kind,
                                         col_names[col_idx])

        _TEL["columns"] += 1
        vectors.append(vec)
        successful_col_names.append(col_names[col_idx])

    return Morsel.from_vectors(successful_col_names, vectors)


cdef _decode_from_buffer(const uint8_t* buf, size_t size, column_names, row_group_mask):
    """Shared decode core: takes a raw C pointer and decodes into Morsels."""
    cdef vector[string] cpp_column_names
    cdef vector[uint8_t] cpp_mask
    cdef parquet_reader.DecodedTable result
    cdef parquet_reader.FileStats fs

    cdef double _t0, _t1

    cdef uint8_t _mbit
    if row_group_mask is not None:
        for m in row_group_mask:
            _mbit = 1 if m else 0
            cpp_mask.push_back(_mbit)

    _t0 = _time.perf_counter()
    if column_names is None and row_group_mask is None:
        with nogil:
            result = parquet_reader.ReadParquet(buf, size)
    else:
        if column_names is None:
            # Mask given without explicit projection: decode all columns.
            fs = parquet_reader.ReadParquetMetadataFromBuffer(buf, size)
            if fs.row_groups.size() > 0:
                for col in fs.row_groups[0].columns:
                    cpp_column_names.push_back(col.name)
        else:
            for name in column_names:
                cpp_column_names.push_back(str(name).encode("utf-8"))
        if row_group_mask is None:
            with nogil:
                result = parquet_reader.ReadParquet(buf, size, cpp_column_names)
        else:
            with nogil:
                result = parquet_reader.ReadParquet(
                    buf, size, cpp_column_names, cpp_mask)
    _t1 = _time.perf_counter()
    _TEL["cpp_decode_s"] += _t1 - _t0
    _TEL["calls"] += 1

    if not result.success:
        # Fail loud with the specific reason (decompression error, corruption,
        # bad footer) the decoder captured. success==false now means a genuine
        # error — an absent column keeps success true and yields an empty morsel.
        if result.error.size() > 0:
            raise RuntimeError("parquet decode failed: " + result.error.decode("utf-8"))
        raise RuntimeError("parquet decode failed: unknown error")

    # Get column names for the Morsel
    cdef list col_names = [name.decode("utf-8") for name in result.column_names]

    if result.row_groups.size() == 0:
        return None

    cdef list all_morsels = []
    cdef Py_ssize_t rg_idx

    for rg_idx in range(<Py_ssize_t>result.row_groups.size()):
        # A row group pruned by row_group_mask is left with no columns — emit
        # no Morsel for it.
        if result.row_groups[rg_idx].size() == 0:
            continue
        all_morsels.append(_morsel_from_row_group(result.row_groups[rg_idx], col_names))

    return all_morsels


def read_parquet(data, column_names=None, row_group_mask=None):
    """Read parquet data from memory with optional column selection.

    RUGO STANDALONE READER ENDPOINT (see module banner). This is the public,
    Python-facing library entry point — used for catalog/manifest reads and
    tests. It FLATTENS dict-encoded columns to Python values by design. It is
    NOT how the opteryx query engine scans data: that is the native pipeline in
    opteryx/connectors/parquet_io/pool_reader.pyx (iter_row_groups_ipc), which
    preserves dictionary shape end-to-end.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data
        column_names: list of column names to read, or None to read all columns
        row_group_mask: optional iterable of truthy/falsy values, one per row
            group; a falsy entry skips decoding that row group entirely.
            None decodes every row group.

    Returns:
        list of Morsels (one per row group). Returns None only when there is no
        data to decode (empty file / all row groups pruned). A genuine decode
        failure (decompression error, corruption, bad footer) raises RuntimeError
        with the specific reason — it never degrades silently to None.
    """
    cdef const uint8_t[::1] mem_view

    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    return _decode_from_buffer(&mem_view[0], <size_t>mem_view.shape[0],
                               column_names, row_group_mask)


# ---------------------------------------------------------------------------
# Codec / encoding string → integer maps (Parquet Thrift enum values)
# Used by decode_column_from_chunk to convert read_metadata dict output back
# to the integer fields expected by the C++ ColumnStats struct.
# ---------------------------------------------------------------------------
_CODEC_INT = {
    'UNCOMPRESSED': 0,
    'SNAPPY':       1,
    'GZIP':         2,
    'LZO':          3,
    'BROTLI':       4,
    # LZ4 is Parquet codec 5 (legacy Hadoop-framed), not 4. It was mapped to 4
    # here, so an LZ4 column was handed to the decoder as BROTLI — which now
    # names the wrong codec in the refusal it raises.
    'LZ4':          5,
    'ZSTD':         6,
    'LZ4_RAW':      7,
}

_ENCODING_INT = {
    'PLAIN':             0,
    'PLAIN_DICTIONARY':  2,
    'RLE':               3,
    'BIT_PACKED':        4,
    'DELTA_BINARY_PACKED': 4,
    'DELTA_LENGTH_BYTE_ARRAY': 6,
    'DELTA_BYTE_ARRAY':  7,
    'RLE_DICTIONARY':    8,
}


def read_parquet_from_path(str path, column_names=None, row_group_mask=None):
    """Read a Parquet file from disk via mmap — no Python bytes materialisation.

    The file is mapped into the process address space and the C++ decoder reads
    directly from the mapped pages.  The mapping is released as soon as decoding
    completes.  All other semantics match read_parquet().
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef const char* c_path = path_bytes
    cdef uint8_t* mapped_ptr = NULL
    cdef size_t mapped_len = 0
    cdef int rc
    cdef const uint8_t[::1] mem_view

    with nogil:
        rc = read_all_mmap(c_path, &mapped_ptr, &mapped_len)
    if rc != 0:
        raise OSError(-rc, f"read_all_mmap failed for {path!r}")

    try:
        # Wrap the mmap'd region as a Cython contiguous typed memoryview.
        # This is a zero-copy view — no Python bytes object is created.
        mem_view = <const uint8_t[:mapped_len:1]>mapped_ptr
        return _decode_from_buffer(&mem_view[0], mapped_len, column_names, row_group_mask)
    finally:
        with nogil:
            unmap_memory_c(mapped_ptr, mapped_len)


def stream_parquet(data, column_names=None, row_group_mask=None):
    """Read parquet data from memory, yielding ONE row group's Morsel at a time.

    Unlike read_parquet() (which decodes and retains every row group into a
    Python list before returning anything — see its docstring's "Returns"),
    this decodes one row group via DecodeRowGroupColumns(), converts it to a
    Morsel, yields it, and only then decodes the next — peak native decode
    memory is bounded by one row group rather than the whole file. Semantics
    otherwise match read_parquet(): column selection, row_group_mask pruning,
    and the same RuntimeError on a genuine per-column decode failure.
    """
    cdef const uint8_t[::1] mem_view
    cdef parquet_reader.FileStats fs
    cdef vector[string] cpp_column_names
    cdef vector[uint8_t] cpp_mask
    cdef vector[parquet_reader.DecodedColumn] row_group_columns
    cdef list col_names
    cdef Py_ssize_t rg_idx
    cdef uint8_t _mbit

    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    fs = parquet_reader.ReadParquetMetadataFromBuffer(&mem_view[0], <size_t>mem_view.shape[0])

    if column_names is None:
        col_names = []
        if fs.row_groups.size() > 0:
            for col in fs.row_groups[0].columns:
                col_names.append(col.name.decode("utf-8"))
    else:
        col_names = [str(name) for name in column_names]
    for name in col_names:
        cpp_column_names.push_back(name.encode("utf-8"))

    if row_group_mask is not None:
        for m in row_group_mask:
            _mbit = 1 if m else 0
            cpp_mask.push_back(_mbit)

    for rg_idx in range(<Py_ssize_t>fs.row_groups.size()):
        if <size_t>rg_idx < cpp_mask.size() and cpp_mask[rg_idx] == 0:
            continue
        with nogil:
            row_group_columns = parquet_reader.DecodeRowGroupColumns(
                &mem_view[0], <size_t>mem_view.shape[0], cpp_column_names,
                fs.row_groups[rg_idx], <int>rg_idx)
        if row_group_columns.size() == 0:
            continue
        yield _morsel_from_row_group(row_group_columns, col_names)


def stream_parquet_from_path(str path, column_names=None, row_group_mask=None):
    """Read a Parquet file from disk via mmap, yielding ONE row group's Morsel
    at a time — see stream_parquet()'s docstring for why. The mmap is held for
    the lifetime of the generator, released in `finally`: an early break out of
    the caller's `for morsel in reader:` loop, or the generator simply being
    garbage-collected before exhaustion, still triggers `finally` (Python runs
    a suspended generator's pending `finally` blocks via GeneratorExit when the
    generator is closed or collected), so the mapping is never leaked.
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef const char* c_path = path_bytes
    cdef uint8_t* mapped_ptr = NULL
    cdef size_t mapped_len = 0
    cdef int rc
    cdef parquet_reader.FileStats fs
    cdef vector[string] cpp_column_names
    cdef vector[uint8_t] cpp_mask
    cdef vector[parquet_reader.DecodedColumn] row_group_columns
    cdef list col_names
    cdef Py_ssize_t rg_idx
    cdef uint8_t _mbit

    with nogil:
        rc = read_all_mmap(c_path, &mapped_ptr, &mapped_len)
    if rc != 0:
        raise OSError(-rc, f"read_all_mmap failed for {path!r}")

    try:
        fs = parquet_reader.ReadParquetMetadataFromBuffer(mapped_ptr, mapped_len)

        if column_names is None:
            col_names = []
            if fs.row_groups.size() > 0:
                for col in fs.row_groups[0].columns:
                    col_names.append(col.name.decode("utf-8"))
        else:
            col_names = [str(name) for name in column_names]
        for name in col_names:
            cpp_column_names.push_back(name.encode("utf-8"))

        if row_group_mask is not None:
            for m in row_group_mask:
                _mbit = 1 if m else 0
                cpp_mask.push_back(_mbit)

        for rg_idx in range(<Py_ssize_t>fs.row_groups.size()):
            if <size_t>rg_idx < cpp_mask.size() and cpp_mask[rg_idx] == 0:
                continue
            with nogil:
                row_group_columns = parquet_reader.DecodeRowGroupColumns(
                    mapped_ptr, mapped_len, cpp_column_names,
                    fs.row_groups[rg_idx], <int>rg_idx)
            if row_group_columns.size() == 0:
                continue
            yield _morsel_from_row_group(row_group_columns, col_names)
    finally:
        with nogil:
            unmap_memory_c(mapped_ptr, mapped_len)


def decode_column_from_chunk_to_python(chunk_bytes, col_stats):
    """Decode a single column from an isolated range-read buffer, returning a Python list.

    For compatibility: returns a Python list instead of a Draken vector.
    Prefer decode_column_from_chunk() which returns Draken vectors directly.

    Args:
        chunk_bytes: bytes / bytearray / memoryview — the raw column chunk.
        col_stats:   dict — one column entry from read_metadata()['row_groups'][rg]['columns'][i].

    Returns a Python list of decoded values, or None on failure.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef parquet_reader.ColumnStats cpp_col

    if isinstance(chunk_bytes, (bytes, bytearray)):
        mem_view = memoryview(chunk_bytes).cast('B')
    elif isinstance(chunk_bytes, memoryview):
        mem_view = chunk_bytes.cast('B')
    else:
        raise TypeError("chunk_bytes must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # -----------------------------------------------------------------------
    # Compute base_offset: the earliest byte of this column chunk in the file.
    # All offsets stored in col_stats are absolute file positions; we subtract
    # base_offset so they become offsets into chunk_bytes.
    # -----------------------------------------------------------------------
    dict_off = col_stats.get('dictionary_page_offset')
    data_off = col_stats['data_page_offset']

    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off

    # -----------------------------------------------------------------------
    # Populate cpp_col with chunk-relative offsets
    # -----------------------------------------------------------------------
    cpp_col.name = (col_stats.get('name') or '').encode('utf-8')
    cpp_col.physical_type = (col_stats.get('physical_type') or '').encode('utf-8')

    logical = col_stats.get('logical_type') or ''
    cpp_col.logical_type = logical.encode('utf-8')

    cpp_col.num_values             = col_stats.get('num_values') if col_stats.get('num_values') is not None else -1
    cpp_col.total_uncompressed_size = col_stats.get('total_uncompressed_size') if col_stats.get('total_uncompressed_size') is not None else -1
    cpp_col.total_compressed_size   = col_stats.get('total_compressed_size') if col_stats.get('total_compressed_size') is not None else -1

    # Adjust absolute file offsets → chunk-relative
    cpp_col.data_page_offset = (data_off - base_offset) if data_off is not None and data_off >= 0 else -1
    cpp_col.index_page_offset = -1
    cpp_col.dictionary_page_offset = (dict_off - base_offset) if dict_off is not None and dict_off >= 0 else -1

    cpp_col.null_count     = col_stats.get('null_count')     if col_stats.get('null_count')     is not None else -1
    cpp_col.distinct_count = col_stats.get('distinct_count') if col_stats.get('distinct_count') is not None else -1
    cpp_col.bloom_offset   = -1
    cpp_col.bloom_length   = -1

    _tmp = col_stats.get('max_definition_level')
    cpp_col.max_definition_level = _tmp if _tmp is not None else 0
    _tmp = col_stats.get('max_repetition_level')
    cpp_col.max_repetition_level = _tmp if _tmp is not None else 0
    _tmp = col_stats.get('type_length')
    cpp_col.type_length = _tmp if _tmp is not None else 0

    # Convert codec string → int (e.g. 'SNAPPY' → 1). An unmapped name defaulted
    # to 0 (UNCOMPRESSED), which handed compressed bytes to the plain decoder and
    # produced garbage instead of a refusal — the same silent-wrong-answer class
    # the codec guard in decode_column.cpp now rejects.
    codec_str = col_stats.get('compression_codec') or 'UNCOMPRESSED'
    if codec_str not in _CODEC_INT:
        raise ValueError(
            "rugo parquet reader: unrecognised compression codec %r" % codec_str
        )
    cpp_col.codec = _CODEC_INT[codec_str]

    # Convert encoding strings → ints (e.g. ['PLAIN', 'RLE_DICTIONARY'] → [0, 8])
    for enc_str in (col_stats.get('encodings') or []):
        enc_int = _ENCODING_INT.get(enc_str, -1)
        if enc_int >= 0:
            cpp_col.encodings.push_back(enc_int)
    if cpp_col.encodings.empty():
        cpp_col.encodings.push_back(0)  # default: PLAIN

    cdef parquet_reader.DecodedColumn result
    with nogil:
        result = parquet_reader.DecodeColumnFromChunk(&mem_view[0], size, &cpp_col)

    if not result.success:
        return None

    cdef int32_t num_rows = <int32_t>result.num_rows

    if result.type == b"int32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_from_int32_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows).to_pylist()
    elif result.type == b"int64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows).to_pylist()
    elif result.type == b"byte_array":
        if _should_emit_constant_vector(result, num_rows):
            return [
                _safe_decode_utf8(v) if v is not None else None
                for v in _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
            ]
        if _should_emit_dictionary_vector(result, num_rows):
            return [
                _safe_decode_utf8(v) if v is not None else None
                for v in _make_typed_string_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
            ]
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return [
            _safe_decode_utf8(v) if v is not None else None
            for v in _make_string_vector(result, <int32_t>result.num_rows).to_pylist()
        ]
    elif result.type == b"boolean":
        return [bool(val) for val in result.boolean_values]
    elif result.type == b"float32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_from_float32_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows).to_pylist()
    elif result.type == b"float64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_vector(result, num_rows).to_pylist()
    else:
        return None


def decode_column_from_chunk(chunk_bytes, col_stats, row_mask=None):
    """Decode a single column from an isolated range-read buffer (default: returns Draken Vector).

    RUGO STANDALONE READER ENDPOINT (see module banner) — a Python-facing single-
    column decode used by the rugo test suite and range-read callers. Dict-encoded
    columns are FLATTENED to Python values here by design (this serves a Python
    consumer). The opteryx execution scan does NOT use this; it scans via the
    native pool_reader pipeline, which preserves dictionary shape.

    Rather than passing the entire file into memory, the caller:

      1. Reads only the bytes for this column chunk via read_ranges()
         (from base_offset = min(dict_page_offset, data_page_offset) for
          total_compressed_size bytes).
      2. Passes those bytes here along with the column stats dict returned
         by read_metadata() for the matching (row_group, column).

    The function adjusts all absolute file offsets in col_stats to be
    chunk-relative before calling the C++ DecodeColumnFromChunk.

    Args:
        chunk_bytes: bytes / bytearray / memoryview — the raw column chunk.
        col_stats:   dict — one column entry from read_metadata()['row_groups'][rg]['columns'][i].

    Returns a Draken Vector (Integer64Vector, StringVector, Float64Vector, BoolVector, or ArrayVector),
    or None on failure.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef parquet_reader.ColumnStats cpp_col
    cdef parquet_reader.DecodedColumn result
    cdef str col_type
    cdef int32_t num_rows
    cdef dict_off
    cdef data_off
    cdef base_offset
    cdef const uint8_t[::1] mask_view
    cdef const uint8_t* mask_ptr = NULL

    if isinstance(chunk_bytes, (bytes, bytearray)):
        mem_view = memoryview(chunk_bytes).cast('B')
    elif isinstance(chunk_bytes, memoryview):
        mem_view = chunk_bytes.cast('B')
    else:
        raise TypeError("chunk_bytes must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # -----------------------------------------------------------------------
    # Compute base_offset: the earliest byte of this column chunk in the file.
    # All offsets stored in col_stats are absolute file positions; we subtract
    # base_offset so they become offsets into chunk_bytes.
    # -----------------------------------------------------------------------
    dict_off = col_stats.get('dictionary_page_offset')
    data_off = col_stats['data_page_offset']

    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off

    # -----------------------------------------------------------------------
    # Populate cpp_col with chunk-relative offsets
    # -----------------------------------------------------------------------
    cpp_col.name = (col_stats.get('name') or '').encode('utf-8')
    cpp_col.physical_type = (col_stats.get('physical_type') or '').encode('utf-8')

    logical = col_stats.get('logical_type') or ''
    cpp_col.logical_type = logical.encode('utf-8')

    cpp_col.num_values             = col_stats.get('num_values') if col_stats.get('num_values') is not None else -1
    cpp_col.total_uncompressed_size = col_stats.get('total_uncompressed_size') if col_stats.get('total_uncompressed_size') is not None else -1
    cpp_col.total_compressed_size   = col_stats.get('total_compressed_size') if col_stats.get('total_compressed_size') is not None else -1

    # Adjust absolute file offsets → chunk-relative
    cpp_col.data_page_offset = (data_off - base_offset) if data_off is not None and data_off >= 0 else -1
    cpp_col.index_page_offset = -1
    cpp_col.dictionary_page_offset = (dict_off - base_offset) if dict_off is not None and dict_off >= 0 else -1

    cpp_col.null_count     = col_stats.get('null_count')     if col_stats.get('null_count')     is not None else -1
    cpp_col.distinct_count = col_stats.get('distinct_count') if col_stats.get('distinct_count') is not None else -1
    cpp_col.bloom_offset   = -1
    cpp_col.bloom_length   = -1

    _tmp = col_stats.get('max_definition_level')
    cpp_col.max_definition_level = _tmp if _tmp is not None else 0
    _tmp = col_stats.get('max_repetition_level')
    cpp_col.max_repetition_level = _tmp if _tmp is not None else 0
    _tmp = col_stats.get('type_length')
    cpp_col.type_length = _tmp if _tmp is not None else 0

    # Convert codec string → int (e.g. 'SNAPPY' → 1). An unmapped name defaulted
    # to 0 (UNCOMPRESSED), which handed compressed bytes to the plain decoder and
    # produced garbage instead of a refusal — the same silent-wrong-answer class
    # the codec guard in decode_column.cpp now rejects.
    codec_str = col_stats.get('compression_codec') or 'UNCOMPRESSED'
    if codec_str not in _CODEC_INT:
        raise ValueError(
            "rugo parquet reader: unrecognised compression codec %r" % codec_str
        )
    cpp_col.codec = _CODEC_INT[codec_str]

    # Convert encoding strings → ints (e.g. ['PLAIN', 'RLE_DICTIONARY'] → [0, 8])
    for enc_str in (col_stats.get('encodings') or []):
        enc_int = _ENCODING_INT.get(enc_str, -1)
        if enc_int >= 0:
            cpp_col.encodings.push_back(enc_int)
    if cpp_col.encodings.empty():
        cpp_col.encodings.push_back(0)  # default: PLAIN

    if row_mask is not None:
        mask_view = row_mask  # contiguous uint8 buffer (array.array, bytearray, memoryview)
        mask_ptr = &mask_view[0]

    with nogil:
        if mask_ptr != NULL:
            result = parquet_reader.DecodeColumnFromChunk(&mem_view[0], size, &cpp_col,
                                                         mask_ptr)
        else:
            result = parquet_reader.DecodeColumnFromChunk(&mem_view[0], size, &cpp_col)

    if mask_ptr != NULL:
        _TEL["parquet_pages_skipped"] += <int32_t>result.pages_skipped
        _TEL["parquet_pages_decoded"] += <int32_t>result.pages_decoded

    if not result.success:
        return None

    num_rows = <int32_t>result.num_rows

    # Convert C++ DecodedColumn to Draken Vector using the same logic as read_parquet()
    if result.rep_levels.size() > 0:
        # Repeated (LIST) column — reconstruct from rep/def levels. Must precede
        # the scalar type branches so a nested column is never flattened.
        return _make_array_vector(result)

    # DECIMAL (scalar): int64/int32-tier (width<=8) and int128-tier (width 9..16)
    # both flagged is_decimal — materialize a real DECIMAL vector rather than a
    # bare int (or, for int128, dropping the column).
    if result.is_decimal:
        return _make_decimal_vector(result, num_rows)

    if result.type == b"int32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_from_int32_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows)

    elif result.type == b"int64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows)

    elif result.type == b"byte_array":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_string_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_string_vector(result, num_rows)

    elif result.type == b"boolean":
        return _make_bool_vector(result, num_rows)

    elif result.type == b"float32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_from_float32_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows)

    elif result.type == b"float64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_vector(result, num_rows)

    else:
        return None


def decode_column_from_memory(data, str column_name, row_group_stats, int row_group_index):
    """Decode a specific column from memory for a specific row group.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data
        column_name: Name of the column to decode
        row_group_stats: RowGroupStats object containing metadata for the row group
        row_group_index: Index of the row group (for reference/debugging)

    Returns a Python list containing the decoded values.
    Only works for uncompressed, PLAIN-encoded int32, int64, string, boolean, float32, and float64 columns.

    Returns None if the column cannot be decoded.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef parquet_reader.RowGroupStats cpp_row_group
    cdef parquet_reader.ColumnStats cpp_col

    # Convert input data to memory view
    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # Convert column name
    cdef bytes column_bytes = column_name.encode("utf-8")
    cdef string cpp_column = column_bytes

    # Convert the Python row_group_stats to C++ RowGroupStats
    cpp_row_group.num_rows = row_group_stats.num_rows
    cpp_row_group.total_byte_size = row_group_stats.total_byte_size

    # Convert the columns
    for col in row_group_stats.columns:
        cpp_col.name = col.name.encode("utf-8")
        cpp_col.physical_type = col.physical_type.encode("utf-8")
        cpp_col.logical_type = col.logical_type.encode("utf-8") if col.logical_type else b""
        cpp_col.num_values = col.num_values if col.num_values is not None else -1
        cpp_col.total_uncompressed_size = col.total_uncompressed_size if col.total_uncompressed_size is not None else -1
        cpp_col.total_compressed_size = col.total_compressed_size if col.total_compressed_size is not None else -1
        cpp_col.data_page_offset = col.data_page_offset if col.data_page_offset is not None else -1
        cpp_col.index_page_offset = col.index_page_offset if col.index_page_offset is not None else -1
        cpp_col.dictionary_page_offset = col.dictionary_page_offset if col.dictionary_page_offset is not None else -1
        cpp_col.has_min = col.has_min if col.has_min is not None else False
        cpp_col.has_max = col.has_max if col.has_max is not None else False

        # Handle min/max values which can be different types
        if col.min:
            if isinstance(col.min, bytes):
                cpp_col.min = col.min
            elif isinstance(col.min, str):
                cpp_col.min = col.min.encode("utf-8")
            else:
                cpp_col.min = str(col.min).encode("utf-8")
        else:
            cpp_col.min = b""

        if col.max:
            if isinstance(col.max, bytes):
                cpp_col.max = col.max
            elif isinstance(col.max, str):
                cpp_col.max = col.max.encode("utf-8")
            else:
                cpp_col.max = str(col.max).encode("utf-8")
        else:
            cpp_col.max = b""

        cpp_col.null_count = col.null_count if col.null_count is not None else -1
        cpp_col.distinct_count = col.distinct_count if col.distinct_count is not None else -1
        cpp_col.bloom_offset = col.bloom_offset if col.bloom_offset is not None else -1
        cpp_col.bloom_length = col.bloom_length if col.bloom_length is not None else -1
        cpp_col.encodings = col.encodings if col.encodings is not None else []
        cpp_col.codec = col.codec if col.codec is not None else -1
        cpp_col.type_length = col.type_length if getattr(col, 'type_length', None) is not None else 0
        cpp_row_group.columns.push_back(cpp_col)

    cdef parquet_reader.DecodedColumn result
    with nogil:
        result = parquet_reader.DecodeColumnFromMemory(
            &mem_view[0], size, cpp_column, cpp_row_group, row_group_index)

    if not result.success:
        return None

    cdef str col_type = result.type.decode("utf-8")

    if col_type == "int32":
        return list(result.int32_values)
    elif col_type == "int64":
        return list(result.int64_values)
    elif col_type == "byte_array":
        return [
            _safe_decode_utf8(v) if v is not None else None
            for v in _make_string_vector(result, <int32_t>result.num_rows).to_pylist()
        ]
    elif col_type == "boolean":
        return [bool(val) for val in result.boolean_values]
    elif col_type == "float32":
        return list(result.float32_values)
    elif col_type == "float64":
        return list(result.float64_values)
    else:
        return None
