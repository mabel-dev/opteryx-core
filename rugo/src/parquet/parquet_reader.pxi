









# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough
#
# =============================================================================
# THIS IS RUGO'S STANDALONE READER — NOT THE OPTERYX EXECUTION SCAN.
# =============================================================================
#
# Endpoint: rugo's public `read_parquet()` / `decode_column_from_chunk()` API.
# Its consumers are GENERAL-PURPOSE and Python-facing — `read_parquet` is a
# library entry point, used for catalog/manifest reads and the rugo test suite.
#
# THIS READER FLATTENS THE ON-DISK DICTIONARY. A dict-encoded column is expanded
# per row into a DENSE vector (see the `_make_typed_*_dictionary_vector`
# makers); the dict shape does NOT survive this path. That is the endpoint's
# choice and it is deliberate.
#
# WHAT IS *NOT* DELIBERATE — and what an earlier version of this banner claimed
# was: materialising through a per-row PYTHON LIST. Every maker here returns a
# `Vector`; the list was a pure intermediate that no caller ever saw, and the
# `vector_*_from_sequence` constructor immediately parsed it straight back out.
# MEASURED on a 1M-row PLAIN int64 column: 1.08 ms of C++ decode against
# 17.68 ms building and re-reading PyLongs — 94% of the read. The materializers
# are now native (`rugo_int_vector` / `rugo_float_vector` / `rugo_string_vector`
# / `rugo_bool_vector` / `rugo_decimal_vector`), which is what CLAUDE.md §3
# requires of a compiled path in any case. Flattening the dict and allocating a
# Python object per value were never the same decision; do not re-conflate them.
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
import decimal
import os
import struct
import time as _time

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
    long long dict_pages_parsed_unused_count()
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
        "dict_pages_parsed_unused": dict_pages_parsed_unused_count(),
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
#               draken/vectors/_bool_vector_bridge.cpp must be added to rugo.parquet_reader
#               sources in setup.py (it moved out of draken/core/bitmap_ops.cpp so that
#               core/bitmap_ops.h stays free of <Python.h>)
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
cdef int _DK_EL_VARBINARY = int(_dn.VARBINARY.value)
cdef int _DK_EL_INT8 = int(_dn.INT8.value)
cdef int _DK_EL_INT16 = int(_dn.INT16.value)
cdef int _DK_EL_INT32 = int(_dn.INT32.value)
cdef int _DK_EL_INT64 = int(_dn.INT64.value)
cdef int _DK_EL_UINT8 = int(_dn.UINT8.value)
cdef int _DK_EL_UINT16 = int(_dn.UINT16.value)
cdef int _DK_EL_UINT32 = int(_dn.UINT32.value)
cdef int _DK_EL_UINT64 = int(_dn.UINT64.value)
cdef int _DK_EL_FLOAT32 = int(_dn.FLOAT32.value)
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


cdef inline str _logical_str(string logical_type):
    """The column's logical-type annotation as `str`; "" when the file carries none."""
    return logical_type.decode("utf-8") if logical_type.size() > 0 else ""


cdef inline bint _logical_is_string(str logical_str):
    """True when the annotation declares TEXT, false for opaque binary.

    THE single string/binary discriminator for BYTE_ARRAY data — the statistics
    path (`decode_value`), the array leaf path (`_array_leaf_values` /
    `_make_array_vector`) and the scalar path (`_make_string_vector`, which all
    three byte_array shapes route through) all consult it; do not open-code a
    second copy.

    Parquet stores VARCHAR and BINARY identically on the wire, as BYTE_ARRAY;
    only the String/UTF8 annotation tells them apart, and for a LIST column the
    LEAF's annotation arrives here as "array<varchar>" (annotated) versus
    "array<byte_array>" (plain binary). An absent annotation means BINARY, so
    the bytes must be handed back opaque: unannotated BYTE_ARRAY is not required
    to be valid UTF-8 (a truncated string bound carries a 0xff sentinel
    precisely so the prefix sorts above everything sharing it).
    """
    return (
        logical_str in ("varchar", "UTF8", "JSON", "BSON", "ENUM")
        or logical_str.startswith("array<string")
        or logical_str.startswith("array<varchar")
    )


cdef object _decode_decimal_stat(str type_str, str logical_str, bytes b):
    """A DECIMAL statistic's bytes as an exact `decimal.Decimal` at the column's scale.

    `logical_str` is metadata.cpp's "decimal(P,S)" spelling. A form that will not
    parse, or a physical type DECIMAL cannot be stored in, raises: returning the
    raw integer instead is exactly the wrong-bounds defect this exists to stop.
    """
    # Explicit end index and an explicit length check: this module compiles with
    # wraparound=False and boundscheck=False, so a negative slice bound or an
    # out-of-range list index is undefined behaviour, not an exception.
    cdef list parts = logical_str[len("decimal("):len(logical_str) - 1].split(",")
    if not logical_str.endswith(")") or len(parts) != 2 or not parts[1].strip().isdigit():
        raise ValueError(f"Unparseable DECIMAL logical type {logical_str!r}")
    cdef int scale = int(parts[1])

    if type_str == "int32":
        unscaled = struct.unpack("<i", b)[0]
    elif type_str == "int64":
        unscaled = struct.unpack("<q", b)[0]
    elif type_str in ("byte_array", "fixed_len_byte_array"):
        unscaled = int.from_bytes(b, "big", signed=True)
    else:
        raise ValueError(f"DECIMAL statistic stored as unsupported physical type {type_str!r}")
    return decimal.Decimal(unscaled).scaleb(-scale)


def decode_value(
        string physical_type,
        string logical_type,
        string raw,
        bint prefer_text):
    cdef bytes b = raw
    if b is None:
        return None

    cdef str type_str = physical_type.decode("utf-8")
    cdef str logical_str = _logical_str(logical_type)
    cdef bint is_string_logical = _logical_is_string(logical_str)
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

    # A DECIMAL(P,S) column stores the UNSCALED integer: INT32/INT64 little-endian,
    # BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY big-endian two's complement. Handing that
    # integer back as-is made 1.10 a min of 110, and callers that prune by
    # comparing a predicate against these bounds then discarded row groups that
    # genuinely match — `d = 3.30` over [1.10, 7.70] tested 3.30 < 110 and
    # returned nothing (every `=`, `<`, `<=`, IN and BETWEEN did; `>` survived
    # only because 770 happens to exceed the literal). Same failure class as the
    # E33 unsigned case above: the annotation changes what the bytes mean, so it
    # is decided here, once, for every statistics consumer.
    if logical_str.startswith("decimal("):
        return _decode_decimal_stat(type_str, logical_str, b)

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
    # (draken/vectors/_vector_bridge.h: 5 = IPV4); 0 means the file carries no
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
         "file_offset": int|None, "total_compressed_size": int|None,
         "columns": [
             {"name": str, "physical_type": str, "logical_type": str,
              "min": bytes|None, "max": bytes|None, "null_count": int,
              "max_repetition_level": int,
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
                # >0 for a nested (list/map) leaf. null_count is then a count of
                # null LEAF VALUES, which is not a count of null ROWS, so a
                # caller pruning on it must know the difference — see
                # rugo.parquet._row_group_mask.
                "max_repetition_level": fs.row_groups[rg_i].columns[c_i].max_repetition_level,
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
        row_groups.append({
            "num_rows": fs.row_groups[rg_i].num_rows,
            # RowGroup.file_offset / total_compressed_size as the footer states
            # them (None when the writer omitted the optional fields). rugo
            # writes the row group's FIRST byte and the SUM of its chunks; under
            # its grouped layout the pair brackets more than the row group's
            # own bytes, so this is a statement about the footer, not a fetch
            # range — see metadata.hpp.
            "file_offset": (fs.row_groups[rg_i].file_offset
                            if fs.row_groups[rg_i].file_offset >= 0 else None),
            "total_compressed_size": (fs.row_groups[rg_i].total_compressed_size
                                      if fs.row_groups[rg_i].total_compressed_size >= 0 else None),
            "columns": cols,
        })
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

    The in-memory form of the file probe, over bloom bytes a caller already
    holds (the `bloom_length` bytes at the footer's `bloom_offset`).
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
    One native call per column; no Python object is created per row. The width
    dispatch and the unsigned bit reinterpretation live in `materialize_int` —
    see parquet/column_materialize.cpp for the shapes handled and the narrowing check."""
    return Vector(rugo_int_vector(col, num_rows, from_int32))


# Column materialization — native, zero Python objects per row.
#
# DecodedColumn -> owned Draken buffers is pure C++ (parquet/column_materialize.cpp,
# Python-free — the source shapes handled, the narrowing / precision checks, float
# canonicalisation and the measurements behind each builder are documented there).
# Its errors throw, and `except +` maps them: std::invalid_argument -> ValueError,
# std::overflow_error -> OverflowError, std::bad_alloc -> MemoryError. Turning the
# buffers into a Vector is the Python edge (parquet/_parquet_column_wrap.hpp): a NEW
# reference or NULL+exception, taken with _rugo_steal (rugo_native.pyx). The
# draken_vector_own_* symbols the edge calls live in draken_native.so and resolve at
# runtime under RTLD_GLOBAL — rugo/__init__.py imports draken first for that reason.
cdef extern from "parquet/column_materialize.hpp" namespace "rugo::_parquet":
    cppclass MaterializedColumn:
        pass

    MaterializedColumn materialize_decimal(parquet_reader.DecodedColumn& col, int32_t num_rows) except +
    MaterializedColumn materialize_int(parquet_reader.DecodedColumn& col, int32_t num_rows, bint from_int32) except +
    MaterializedColumn materialize_float(parquet_reader.DecodedColumn& col, int32_t num_rows, bint from_float32) except +
    MaterializedColumn materialize_string(parquet_reader.DecodedColumn& col, int32_t num_rows, bint is_text) except +
    MaterializedColumn materialize_bool(parquet_reader.DecodedColumn& col, int32_t num_rows) except +


cdef extern from "parquet/_parquet_column_wrap.hpp" namespace "rugo::_parquet":
    PyObject* wrap_parquet_column(MaterializedColumn& mc) except NULL


# DECIMAL (p<=18, int64) or DECIMAL128 (p>18, int128). The value on the wire already
# IS the unscaled integer at the column's scale, so this is a scatter, not a convert.
cdef inline object rugo_decimal_vector(parquet_reader.DecodedColumn& col, int32_t num_rows):
    cdef MaterializedColumn mc = materialize_decimal(col, num_rows)
    return _rugo_steal(wrap_parquet_column(mc))


cdef Vector _make_decimal_vector(parquet_reader.DecodedColumn& col, int32_t num_rows):
    """Build a DECIMAL Vector from a decoded parquet DECIMAL column.

    One native call per morsel; no Python object is created per row. See
    parquet/column_materialize.cpp for the tiering, the shapes handled, and why
    the old Decimal round trip was an identity.
    """
    return Vector(rugo_decimal_vector(col, num_rows))


# Integer column at its DECLARED width and signedness; narrowing is checked.
cdef inline object rugo_int_vector(parquet_reader.DecodedColumn& col, int32_t num_rows,
                                   bint from_int32):
    cdef MaterializedColumn mc = materialize_int(col, num_rows, from_int32)
    return _rugo_steal(wrap_parquet_column(mc))


# FLOAT32 (parquet `float`) or FLOAT64 (parquet `double`), values canonicalised.
cdef inline object rugo_float_vector(parquet_reader.DecodedColumn& col, int32_t num_rows,
                                     bint from_float32):
    cdef MaterializedColumn mc = materialize_float(col, num_rows, from_float32)
    return _rugo_steal(wrap_parquet_column(mc))


# Dense VARCHAR/VARBINARY. `is_text` is the caller's `_logical_is_string` verdict,
# kept in Cython so the String-annotation discriminator stays in ONE place.
cdef inline object rugo_string_vector(parquet_reader.DecodedColumn& col, int32_t num_rows,
                                      bint is_text):
    cdef MaterializedColumn mc = materialize_string(col, num_rows, is_text)
    return _rugo_steal(wrap_parquet_column(mc))


# Bit-packed BOOL, laid out exactly as make_bool_from_sequence.
cdef inline object rugo_bool_vector(parquet_reader.DecodedColumn& col, int32_t num_rows):
    cdef MaterializedColumn mc = materialize_bool(col, num_rows)
    return _rugo_steal(wrap_parquet_column(mc))








cdef Vector _make_int64_from_int32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, True)


cdef Vector _make_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, False)


cdef Vector _make_float32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    # A parquet `float` column becomes a FLOAT32 vector, not a widened FLOAT64
    # one — it is the CARRIER that was wrong before, not the values: a
    # FLOAT64-tagged vector makes every consumer read the column at 8 bytes.
    return Vector(rugo_float_vector(decoded_col, num_rows, True))


cdef Vector _make_int32_as_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return _make_int_vector(decoded_col, num_rows, True)


cdef Vector _make_float64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(rugo_float_vector(decoded_col, num_rows, False))


cdef Vector _make_string_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """THE byte_array scalar materializer — dense, dictionary and constant alike.

    BYTE_ARRAY carries both VARCHAR and opaque BINARY; parquet stores them
    identically on the wire and only the String annotation separates them, so
    the annotation — via `_logical_is_string`, the one discriminator, shared
    with the statistics path and the array leaf — decides the vector's type
    tag. Storage is byte-identical either way; only the tag differs, and getting
    it wrong makes every value of an unannotated column raise UnicodeDecodeError
    on access while also lying about the type to anything that reads it.
    """
    cdef bint is_text = _logical_is_string(_logical_str(decoded_col.logical_type))
    return Vector(rugo_string_vector(decoded_col, num_rows, is_text))




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
    return Vector(rugo_float_vector(decoded_col, num_rows, False))


cdef Vector _make_typed_float32_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(rugo_float_vector(decoded_col, num_rows, True))


cdef Vector _make_typed_string_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    # Dispatch name only: `rugo_string_vector` resolves the dict shape and the
    # dense shape through the same resolver, so there is ONE byte_array
    # materializer and the text/binary tag cannot drift between the two.
    return _make_string_vector(decoded_col, num_rows)


cdef Vector _make_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Vector from a decoded parquet dictionary payload, for rugo's
    STANDALONE reader endpoint (see module banner).

    NOTE: despite the name, this does NOT preserve the on-disk dictionary. It
    routes to the `_make_typed_*_dictionary_vector` makers, which EXPAND the
    column per row into a dense vector — because this path serves Python
    consumers (read_parquet / catalog / tests), not the engine. The expansion is
    native (no Python object per row); it is the dict SHAPE that is dropped, not
    the cost that is paid. The opteryx execution scan keeps dict shape natively
    in pool_reader; it never calls this.
    """
    cdef bytes col_type = decoded_col.type

    if col_type == b"byte_array":
        return _make_typed_string_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"int32":
        return _make_typed_int64_from_int32_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"int64":
        return _make_typed_int64_dictionary_vector(decoded_col, num_rows)
    elif col_type == b"float32":
        return _make_typed_float32_dictionary_vector(decoded_col, num_rows)
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
        return Vector(rugo_float_vector(decoded_col, num_rows, False))
    if col_type == b"float32":
        return Vector(rugo_float_vector(decoded_col, num_rows, True))
    if col_type == b"byte_array":
        return _make_string_vector(decoded_col, num_rows)
    raise ValueError(f"unsupported constant column type: {col_type!r}")


cdef Vector _make_bool_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    return Vector(rugo_bool_vector(decoded_col, num_rows))


# --- list / array reconstruction ---------------------------------------------
# A repeated (LIST) column is decoded by C++ into parallel rep_levels/def_levels
# (one entry per leaf position) plus the present leaf values (compact, in order).
# The definition levels that mark each nesting depth depend on WHICH schema nodes
# are OPTIONAL, so they are derived from the schema by metadata.cpp's WalkLeaves
# and carried on the column as `list_def_thresholds` (index 0 unused, depth k at
# [k]); see ColumnStats::list_def_thresholds. For a list nested D deep
# (D == max_rep_level), writing T[k] for that threshold:
#   * list k is non-null       when def >= T[k]
#   * list k has a child entry when def >= T[k] + 1   (a REPEATED node is +1 def)
#   * the leaf element exists  when def == max_def_level; a def in
#     [T[D] + 1, max_def_level) is a NULL element
# The all-OPTIONAL case (rugo's writer, and pyarrow's default) gives the familiar
# T[k] == 2k - 1 and max_def_level == 2*D + 1, but that is one shape among several:
# a `required` element (pyarrow writes `list<item: T not null>` happily) lowers
# max_def_level to 2*D and makes the null-element case unreachable, a `required`
# LIST group gives T[k] == 0 (a list that can never be null), and the legacy
# 2-level `repeated <leaf>` encoding gives T[1] == 0 with max_def_level == 1.
# ⛔ Do NOT open-code 2k-1 / 2k here or in ipc_serialize.hpp's serialize_list_column.
# Repetition level r means list levels 1..r continue the previous record;
# a fresh sub-structure begins at depth r+1. A value is consumed from the leaf
# stream only when def == max_def_level (present element).
#
# Element types: int32/int64 and their unsigned/narrow annotations, float32/
# float64, bool, and byte_array (-> VARCHAR str when the leaf carries a String
# annotation, else VARBINARY bytes) — each kept at its DECLARED width, never
# widened (see `_make_array_vector`). Leaf VALUES are materialized as plain
# Python scalars (int / float / bool / str / bytes); the width is carried by
# the `el_type` handed to `vector_array_from_sequence`, not inferred from them.


cdef list _array_leaf_values(parquet_reader.DecodedColumn& col, bint leaf_is_text):
    """Present leaf values, in stream order, as a Python list.

    Mirrors the per-type/per-encoding accessors used by the scalar
    materializers (plain dense, dict via packed per-level codes, dict via
    compact present-order indices).

    `leaf_is_text` comes from `_logical_is_string` and governs the BYTE_ARRAY
    branch ONLY: text leaves are decoded to `str`, binary leaves handed back as
    opaque `bytes`. The caller must derive it from the same predicate it uses to
    pick `el_type`, so the values and the declared child type cannot disagree."""
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
        # Draken's array constructor discriminates the child by the Python type
        # of the leaf: PyUnicode -> VARCHAR, PyBytes -> VARBINARY. So the leaf's
        # String annotation, not its physical type, decides which we emit — an
        # unannotated BYTE_ARRAY leaf is opaque binary and decoding it would both
        # lie about the type and raise on any value that is not valid UTF-8.
        if leaf_is_text:
            for i in range(n_levels):
                if col.def_levels[i] != max_def:
                    continue
                if use_codes:
                    vals.append(_dict_str_at(col, _read_code(col.dict_codes_array, i, cw)).decode("utf-8"))
                elif has_dict:
                    vals.append(_dict_str_at(col, col.dict_indices[vi]).decode("utf-8")); vi += 1
                else:
                    vals.append(_dense_str_at(col, vi).decode("utf-8")); vi += 1
        else:
            for i in range(n_levels):
                if col.def_levels[i] != max_def:
                    continue
                if use_codes:
                    vals.append(_dict_str_at(col, _read_code(col.dict_codes_array, i, cw)))
                elif has_dict:
                    vals.append(_dict_str_at(col, col.dict_indices[vi])); vi += 1
                else:
                    vals.append(_dense_str_at(col, vi)); vi += 1
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
    if decoded_col.list_def_thresholds.size() != <size_t>(D + 1):
        raise RuntimeError(
            "rugo array reader: column is missing its per-depth list level "
            "thresholds (max_rep_level=%d, got %d entries, expected %d) - the "
            "schema walk did not populate them."
            % (D, decoded_col.list_def_thresholds.size(), D + 1)
        )
    # Smallest def at which a leaf slot exists under the innermost list.
    cdef int32_t leaf_slot_def = decoded_col.list_def_thresholds[D] + 1
    if max_def < leaf_slot_def:
        raise RuntimeError(
            "rugo array reader: inconsistent list level scheme (max_rep_level=%d, "
            "max_def_level=%d, innermost list threshold=%d)."
            % (D, max_def, decoded_col.list_def_thresholds[D])
        )

    # ONE derivation of the leaf's text/binary character, shared by the value
    # materialization below and the `el_type` it is tagged with.
    cdef bint leaf_is_text = _logical_is_string(_logical_str(decoded_col.logical_type))
    cdef list leaf_vals = _array_leaf_values(decoded_col, leaf_is_text)
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
            if d >= decoded_col.list_def_thresholds[k]:   # list k is non-null
                new = []
                parent.append(new)
                open_lists[k] = new
                if d < decoded_col.list_def_thresholds[k] + 1:   # present but EMPTY
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
        else:                                # leaf_slot_def <= d < max_def -> null element
            open_lists[D].append(None)

    # Leaf element type comes from the schema, not value inference: an all-null
    # or all-empty list column (e.g. manifest min/max_values_display over numeric
    # columns) has no leaf value to infer from. D is the list nesting depth, so
    # the constructor knows to build ARRAY children above the leaf level.
    cdef bytes col_type = decoded_col.type
    cdef int el_type
    # The element keeps its DECLARED width, exactly like a scalar column. The
    # physical type alone does not carry it: parquet stores every integer
    # narrower than 64 bits on physical int32/int64 plus an INTEGER(bitWidth,
    # isSigned) annotation, which the decoder surfaces as
    # `int_bit_width`/`is_unsigned`. Reading the annotation is what stops a
    # list<int32> coming back as a list<int64> (values identical, type a lie).
    cdef int32_t leaf_bits = decoded_col.int_bit_width
    if col_type == b"byte_array":
        # BYTE_ARRAY carries both VARCHAR and opaque BINARY; only the String
        # annotation separates them (see `_logical_is_string`). This must agree
        # with what `_array_leaf_values` emitted above — it is the same `bint`,
        # so it cannot drift — and it is what types an all-null/all-empty list
        # column, where there is no leaf value for draken to infer from.
        el_type = _DK_EL_VARCHAR if leaf_is_text else _DK_EL_VARBINARY
    elif col_type == b"int32":
        if decoded_col.is_unsigned:
            if leaf_bits == 8:
                el_type = _DK_EL_UINT8
            elif leaf_bits == 16:
                el_type = _DK_EL_UINT16
            else:
                el_type = _DK_EL_UINT32
        elif leaf_bits == 8:
            el_type = _DK_EL_INT8
        elif leaf_bits == 16:
            el_type = _DK_EL_INT16
        else:
            # A bare physical int32 IS a 32-bit signed column — never widen it.
            el_type = _DK_EL_INT32
    elif col_type == b"int64":
        el_type = _DK_EL_UINT64 if decoded_col.is_unsigned else _DK_EL_INT64
    elif col_type == b"float32":
        el_type = _DK_EL_FLOAT32
    elif col_type == b"float64":
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
                vec = _make_typed_float32_dictionary_vector(column, num_rows)
            else:
                if _decoded_has_dictionary(column):
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                vec = _make_float32_vector(column, num_rows)
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
            return _make_typed_float32_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float32_vector(result, num_rows)

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
