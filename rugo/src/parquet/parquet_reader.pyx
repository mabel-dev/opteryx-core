# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough
import datetime
import os
import struct
import time as _time

import opteryx.config as _opteryx_config
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
    double metadata_s
    double decompress_s
    double dict_parse_s
    double prescan_s
    double page_parallel_s
    double rle_s
    double val_expand_s
    double mask_filter_s
    double validity_bmp_s
    long long calls
    void reset() nogil



def reset_cpp_telemetry():
    """Zero all C++ phase telemetry accumulators."""
    reset()


def get_cpp_telemetry():
    """Return a dict with C++ phase timing (seconds) since last reset."""
    return {
        "metadata_s":       metadata_s,
        "decompress_s":     decompress_s,
        "dict_parse_s":     dict_parse_s,
        "prescan_s":        prescan_s,
        "page_parallel_s":  page_parallel_s,
        "rle_s":            rle_s,
        "val_expand_s":     val_expand_s,
        "mask_filter_s":    mask_filter_s,
        "validity_bmp_s":   validity_bmp_s,
        "calls":            calls,
    }

cimport parquet_reader
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int32_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.string cimport string
from libcpp.vector cimport vector

# Type widening C wrappers (Tier 2C SIMD acceleration)
cdef extern from "type_widening_wrappers.hpp":
    void rugo_widen_int32_to_int64(const int32_t* src, int64_t* dst, size_t count) nogil
    void rugo_widen_float32_to_float64(const float* src, double* dst, size_t count) nogil

# Import Draken vector types and components
from draken.vectors.integer64_vector cimport (
    Integer64Vector,
    from_dict as int64_from_dict,
    from_dict_nullable as int64_from_dict_nullable,
    from_packed_dict as int64_from_packed_dict,
)
from draken.vectors.float64_vector cimport (
    Float64Vector,
    from_dict as float64_from_dict,
    from_dict_nullable as float64_from_dict_nullable,
    from_packed_dict as float64_from_packed_dict,
    make_float64_dict_only,
)
from draken.vectors.string_vector cimport (
    StringVector,
    StringVectorBuilder,
    from_dict_buffers as string_from_dict_buffers,
    make_string_dict_only,
)
from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.core.buffers cimport (
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_STRING,
    DrakenVarBuffer,
)
from draken.vectors.array_vector cimport ArrayVector, array_vector_from_parts
from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel


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
    """Safely decode bytes to UTF-8 string, handling invalid sequences."""
    cdef bytes b = raw_bytes
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        # Fall back to latin-1 (which can decode any byte sequence)
        # or use error handling to replace invalid characters
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            # Ultimate fallback: decode as latin-1 which never fails
            return b.decode("latin-1")


cpdef object decode_value(
        string physical_type,
        string logical_type,
        string raw,
        bint prefer_text):
    cdef bytes b = raw
    if b is None:
        return None

    # Decode the C++ string to Python string for comparison
    cdef str type_str = physical_type.decode("utf-8")
    cdef str logical_str = logical_type.decode("utf-8") if logical_type.size() > 0 else ""
    cdef bint is_string_logical = (
        logical_str in ("varchar", "UTF8", "JSON", "BSON", "ENUM")
        or logical_str.startswith("array<string")
        or logical_str.startswith("array<varchar")
    )
    cdef str candidate

    if len(b) == 0:
        if type_str in ("byte_array", "fixed_len_byte_array"):
            if is_string_logical or prefer_text:
                return ""
        return b""   # treat empty binary as bytes for non-string types

    try:
        if type_str == "int32":
            return struct.unpack("<i", b)[0]
        elif type_str == "int64":
            return struct.unpack("<q", b)[0]
        elif type_str == "float32":
            return struct.unpack("<f", b)[0]
        elif type_str == "float64":
            return struct.unpack("<d", b)[0]
        elif type_str in ("byte_array", "fixed_len_byte_array"):
            # If logical type indicates UTF-8 string, decode it
            # Handle "varchar" (new format) and legacy "UTF8" format
            # Also handle array<string> and array<varchar> - the elements are UTF-8 strings
            if is_string_logical:
                try:
                    return b.decode("utf-8")
                except UnicodeDecodeError:
                    # If UTF-8 decoding fails, return as bytes
                    return b
            elif prefer_text and type_str == "byte_array":
                try:
                    candidate = b.decode("utf-8")
                except UnicodeDecodeError:
                    return b
                if _text_is_printable(candidate) and "\ufffd" not in candidate:
                    return candidate
            # Otherwise, return raw bytes (binary data)
            return b
        elif type_str == "int96":
            if len(b) == 12:
                lo, hi = struct.unpack("<qI", b)
                julian_day = hi
                nanos = lo
                # convert Julian day
                days = julian_day - 2440588
                date = datetime.date(1970, 1, 1) + datetime.timedelta(days=days)
                seconds = nanos // 1_000_000_000
                micros = (nanos % 1_000_000_000) // 1000
                return f"{date.isoformat()} {seconds:02d}:{(micros/1e6):.6f}"
            return b.hex()
        elif type_str == "boolean":
            # Parquet encodes boolean as 1 bit, usually in a byte
            return b[0] != 0
        else:
            return b.hex()
    except Exception:
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


cdef list _schema_columns(parquet_reader.FileStats& fs):
    """Build schema_columns list from C++ FileStats. Schema only — no row groups."""
    cdef list out = []
    cdef parquet_reader.SchemaField field
    cdef size_t i
    for i in range(fs.schema_columns.size()):
        field = fs.schema_columns[i]
        out.append({
            "name":          field.name.decode("utf-8"),
            "physical_type": field.physical_type.decode("utf-8"),
            "logical_type":  field.logical_type.decode("utf-8"),
            "nullable":      bool(field.nullable),
        })
    return out


def read_metadata(str path):
    """Return schema metadata for a parquet file: {num_rows, schema_columns}.

    For column statistics use fetch_column_stats().
    For data use iter_row_groups_ipc().
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef parquet_reader.MetadataParseOptions opts
    opts.schema_only = True
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataC(
        path_bytes, opts
    )
    return {"num_rows": fs.num_rows, "schema_columns": _schema_columns(fs)}


def read_metadata_from_bytes(bytes data):
    """Return schema metadata from an in-memory bytes buffer."""
    cdef parquet_reader.MetadataParseOptions opts
    opts.schema_only = True
    cdef const uint8_t* buf = <const uint8_t*> data
    cdef size_t size = len(data)
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return {"num_rows": fs.num_rows, "schema_columns": _schema_columns(fs)}


def read_metadata_from_memoryview(memoryview mv):
    """Return schema metadata from a contiguous memoryview (zero-copy)."""
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
    return {"num_rows": fs.num_rows, "schema_columns": _schema_columns(fs)}


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

def test_bloom_filter(path, bloom_offset, bloom_length, value):
    """Evaluate a parquet column bloom filter at the given offset."""
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

    cdef str path_str = os.fspath(path)
    cdef bytes path_bytes = path_str.encode("utf-8")

    if isinstance(value, (bytes, bytearray, memoryview)):
        value_bytes = bytes(value)
    else:
        value_bytes = str(value).encode("utf-8")

    cdef parquet_reader.string c_path = path_bytes
    cdef parquet_reader.string c_value = value_bytes

    return bool(parquet_reader.TestBloomFilter(c_path, native_offset, native_length, c_value))


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


cdef Integer64Vector _make_int64_from_int32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build an Integer64Vector from a DecodedColumn with int32_t values (upcasting)."""
    cdef Integer64Vector vec = Integer64Vector(num_rows)
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
    # RLE skip-dense path: int32 widened to int64 in C++; expand runs.
    if (decoded_col.rle_run_lengths.size() > 0
            and decoded_col.rle_int64_values.size() > 0
            and decoded_col.rle_total_length == <size_t>num_rows):
        _expand_rle_int64_into(dst, decoded_col, num_rows)
        return vec
    cdef bint dict_mode = (
        decoded_col.dict_indices.size() > 0
        and decoded_col.dict_int32_values.size() > 0
        and decoded_col.int32_values.size() == 0
    )

    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if dict_mode:
                    if val_idx >= decoded_col.dict_indices.size():
                        raise ValueError("dictionary index stream shorter than number of valid rows")
                    code = decoded_col.dict_indices[val_idx]
                    if code < 0 or code >= decoded_col.dict_int32_values.size():
                        raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                    dst[i] = <int64_t>decoded_col.dict_int32_values[code]
                else:
                    dst[i] = <int64_t>decoded_col.int32_values[val_idx]
                val_idx += 1
            else:
                dst[i] = 0
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb
    else:
        for i in range(num_rows):
            if dict_mode:
                code = decoded_col.dict_indices[i]
                if code < 0 or code >= decoded_col.dict_int32_values.size():
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                dst[i] = <int64_t>decoded_col.dict_int32_values[code]
            else:
                dst[i] = <int64_t>decoded_col.int32_values[i]

    return vec


cdef Integer64Vector _make_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build an Integer64Vector from a DecodedColumn with int64_t values."""
    if num_rows == 0:
        return Integer64Vector(0)
    cdef Integer64Vector vec = Integer64Vector(num_rows)
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
    # RLE skip-dense path: expand runs into dense.
    if (decoded_col.rle_run_lengths.size() > 0
            and decoded_col.rle_int64_values.size() > 0
            and decoded_col.rle_total_length == <size_t>num_rows):
        _expand_rle_int64_into(dst, decoded_col, num_rows)
        return vec
    cdef bint dict_mode = (
        decoded_col.dict_indices.size() > 0
        and decoded_col.dict_int64_values.size() > 0
        and decoded_col.int64_values.size() == 0
    )

    # If we have a valid_bits bitmap, scatter values into positions and set null bitmap
    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if dict_mode:
                    if val_idx >= decoded_col.dict_indices.size():
                        raise ValueError("dictionary index stream shorter than number of valid rows")
                    code = decoded_col.dict_indices[val_idx]
                    if code < 0 or code >= decoded_col.dict_int64_values.size():
                        raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                    dst[i] = decoded_col.dict_int64_values[code]
                else:
                    dst[i] = decoded_col.int64_values[val_idx]
                val_idx += 1
            else:
                dst[i] = 0
        # Copy null bitmap
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb
    else:
        if dict_mode:
            for i in range(num_rows):
                code = decoded_col.dict_indices[i]
                if code < 0 or code >= decoded_col.dict_int64_values.size():
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                dst[i] = decoded_col.dict_int64_values[code]
        else:
            # No nulls: bulk copy via memcpy (avoids Cython loop overhead)
            if num_rows > 0:
                memcpy(dst, decoded_col.int64_values.data(), <size_t>num_rows * sizeof(int64_t))

    return vec


cdef Float64Vector _make_float64_from_float32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Float64Vector from float32 values (upcasting), including dict-mode."""
    if num_rows == 0:
        return Float64Vector(0)
    cdef Float64Vector vec = Float64Vector(num_rows)
    cdef double* dst = <double*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
    # RLE skip-dense path: float32 widened to float64 in C++; expand runs.
    if (decoded_col.rle_run_lengths.size() > 0
            and decoded_col.rle_float64_values.size() > 0
            and decoded_col.rle_total_length == <size_t>num_rows):
        _expand_rle_float64_into(dst, decoded_col, num_rows)
        return vec
    cdef bint dict_mode = (
        decoded_col.dict_indices.size() > 0
        and decoded_col.dict_float32_values.size() > 0
        and decoded_col.float32_values.size() == 0
    )

    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if dict_mode:
                    if val_idx >= decoded_col.dict_indices.size():
                        raise ValueError("dictionary index stream shorter than number of valid rows")
                    code = decoded_col.dict_indices[val_idx]
                    if code < 0 or code >= decoded_col.dict_float32_values.size():
                        raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                    dst[i] = <double>decoded_col.dict_float32_values[code]
                else:
                    dst[i] = <double>decoded_col.float32_values[val_idx]
                val_idx += 1
            else:
                dst[i] = 0.0
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb
    else:
        if dict_mode:
            # Dictionary mode: need to gather dict values (not SIMD-friendly, use scalar)
            for i in range(num_rows):
                code = decoded_col.dict_indices[i]
                if code < 0 or code >= decoded_col.dict_float32_values.size():
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                dst[i] = <double>decoded_col.dict_float32_values[code]
        else:
            # Dense case: use SIMD-accelerated type widening (Tier 2C)
            # rugo_widen_float32_to_float64 uses AVX2 instructions when available
            if num_rows > 0:
                rugo_widen_float32_to_float64(decoded_col.float32_values.data(), dst, <size_t>num_rows)
    return vec


cdef Integer64Vector _make_int32_as_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build an Integer64Vector from a DecodedColumn with int32 values (upcasts int32→int64)."""
    if num_rows == 0:
        return Integer64Vector(0)
    cdef Integer64Vector vec = Integer64Vector(num_rows)
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    # RLE skip-dense path: int32 widened to int64 in C++; expand runs.
    if (decoded_col.rle_run_lengths.size() > 0
            and decoded_col.rle_int64_values.size() > 0
            and decoded_col.rle_total_length == <size_t>num_rows):
        _expand_rle_int64_into(dst, decoded_col, num_rows)
        return vec

    if decoded_col.valid_bits.size() > 0:
        # Sparse case: iterate valid_bits and widen selected values
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                dst[i] = <int64_t> decoded_col.int32_values[val_idx]
                val_idx += 1
            else:
                dst[i] = 0
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb
    else:
        # Dense case: use SIMD-accelerated type widening (Tier 2C)
        # rugo_widen_int32_to_int64 uses AVX2 instructions when available
        if num_rows > 0:
            rugo_widen_int32_to_int64(decoded_col.int32_values.data(), dst, <size_t>num_rows)

    return vec


cdef Float64Vector _make_float64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Float64Vector from a DecodedColumn with float64 values."""
    if num_rows == 0:
        return Float64Vector(0)
    cdef Float64Vector vec = Float64Vector(num_rows)
    cdef double* dst = <double*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
    # RLE skip-dense path: expand runs into dense.
    if (decoded_col.rle_run_lengths.size() > 0
            and decoded_col.rle_float64_values.size() > 0
            and decoded_col.rle_total_length == <size_t>num_rows):
        _expand_rle_float64_into(dst, decoded_col, num_rows)
        return vec
    cdef bint dict_mode = (
        decoded_col.dict_indices.size() > 0
        and decoded_col.dict_float64_values.size() > 0
        and decoded_col.float64_values.size() == 0
    )

    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if dict_mode:
                    if val_idx >= decoded_col.dict_indices.size():
                        raise ValueError("dictionary index stream shorter than number of valid rows")
                    code = decoded_col.dict_indices[val_idx]
                    if code < 0 or code >= decoded_col.dict_float64_values.size():
                        raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                    dst[i] = decoded_col.dict_float64_values[code]
                else:
                    dst[i] = decoded_col.float64_values[val_idx]
                val_idx += 1
            else:
                dst[i] = 0.0
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb
    else:
        if dict_mode:
            for i in range(num_rows):
                code = decoded_col.dict_indices[i]
                if code < 0 or code >= decoded_col.dict_float64_values.size():
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                dst[i] = decoded_col.dict_float64_values[code]
        else:
            # No nulls: bulk copy via memcpy (avoids Cython loop overhead)
            if num_rows > 0:
                memcpy(dst, decoded_col.float64_values.data(), <size_t>num_rows * sizeof(double))

    return vec


cdef StringVector _make_string_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a StringVector from a DecodedColumn with string values.

    Dict-mode path uses direct DrakenVarBuffer writes — no per-row function
    calls into StringVectorBuilder:
      1. Pre-materialise the compact dictionary as C ptr+len arrays (D<<N entries).
      2. Count total bytes in one tight loop.
      3. Allocate StringVector with exact capacity.
      4. Expand: fill offsets[] + memcpy string data in one tight loop.
    """
    cdef Py_ssize_t i, d, val_idx
    cdef Py_ssize_t num_values  = decoded_col.string_values.size()
    cdef Py_ssize_t num_indices = decoded_col.dict_indices.size()
    cdef Py_ssize_t num_dict
    cdef Py_ssize_t estimated_bytes
    cdef int32_t dict_idx
    cdef int64_t total_bytes64
    cdef int32_t total_bytes
    cdef int32_t offset, slen
    cdef Py_ssize_t nb_bytes
    cdef const char* sptr
    cdef const char** dict_ptrs
    cdef int32_t* dict_lens
    cdef StringVector vec
    cdef DrakenVarBuffer* buf
    cdef char* dst
    cdef int32_t* offsets
    cdef uint8_t* nb
    cdef StringVectorBuilder builder
    cdef const uint8_t* arena_data
    cdef Py_ssize_t r, j, num_runs, run_cnt, rtotal_bytes
    cdef int32_t roff, rlen
    cdef const uint8_t* rle_arena
    cdef int32_t* roffsets

    # ── RLE skip-dense path ──────────────────────────────────────────────────
    # Non-nullable byte_array dict columns produce rle_str_arena/offsets/lens
    # plus rle_run_lengths instead of dict_indices. Expand into the dense
    # offset+arena layout the StringVector expects.
    # Only enter this path when the run accumulation actually represents every
    # logical row (no row_mask filtering and no PLAIN fallback in the chunk):
    # decode_column.cpp materialises rle_* into dict_indices when transitioning
    # to PLAIN pages, so a non-empty rle_str_lens here is the pure-RLE case.
    num_runs = <Py_ssize_t>decoded_col.rle_run_lengths.size()
    if num_runs > 0 and decoded_col.rle_str_lens.size() > 0 \
            and decoded_col.rle_total_length == <size_t>num_rows:
        rle_arena = decoded_col.rle_str_arena.data()
        rtotal_bytes = 0
        for r in range(num_runs):
            rtotal_bytes += <Py_ssize_t>decoded_col.rle_str_lens[r] * \
                            <Py_ssize_t>decoded_col.rle_run_lengths[r]
        # Route through the builder so finish() populates min/max metadata and
        # the construction invariants (offsets, validity, unified view) live in
        # one place. with_counts guarantees an exact byte budget.
        builder = StringVectorBuilder.with_counts(num_rows, rtotal_bytes)
        for r in range(num_runs):
            roff = <int32_t>decoded_col.rle_str_offsets[r]
            rlen = decoded_col.rle_str_lens[r]
            run_cnt = decoded_col.rle_run_lengths[r]
            for j in range(run_cnt):
                builder.append_bytes(<const char*>rle_arena + roff, rlen)
        return builder.finish()

    # ── Dict mode ─────────────────────────────────────────────────────────────
    # C++ stores: string_dict_arena = flat packed bytes, string_dict_offsets/lens
    # = per-entry index, dict_indices = one index per (non-null) row.
    if num_indices > 0:
        num_dict = <Py_ssize_t>decoded_col.string_dict_lens.size()
        arena_data = decoded_col.string_dict_arena.data()

        # Step 1: pre-materialise dictionary as C pointer+length arrays
        # One pass over D << N entries — very cheap.
        dict_ptrs = <const char**>malloc(num_dict * sizeof(const char*))
        dict_lens = <int32_t*>malloc(num_dict * sizeof(int32_t))
        if dict_ptrs == NULL or dict_lens == NULL:
            free(dict_ptrs)
            free(dict_lens)
            raise MemoryError()
        for d in range(num_dict):
            dict_ptrs[d] = <const char*>(arena_data + decoded_col.string_dict_offsets[d])
            dict_lens[d] = decoded_col.string_dict_lens[d]

        # Step 2: count total expanded bytes via SIMD gather of dict_lens[codes].
        # See _string_materialize.hpp — Pass 1 of the materialization plan.
        total_bytes64 = parquet_reader.sum_dict_lens(
            dict_lens,
            <const int32_t*>decoded_col.dict_indices.data(),
            <size_t>num_indices,
        )
        # StringVector capacity field is int32; guard against overflow.
        if total_bytes64 > <int64_t>2147483647:
            free(dict_ptrs)
            free(dict_lens)
            raise OverflowError("StringVector materialization exceeds 2 GiB")
        total_bytes = <int32_t>total_bytes64

        # Route through the builder so finish() populates min/max metadata
        # and the construction invariants live in one place.
        try:
            builder = StringVectorBuilder.with_counts(num_rows, total_bytes)
            if decoded_col.valid_bits.size() == 0:
                for i in range(num_rows):
                    dict_idx = decoded_col.dict_indices[i]
                    builder.append_bytes(dict_ptrs[dict_idx], dict_lens[dict_idx])
            else:
                val_idx = 0
                for i in range(num_rows):
                    if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                        dict_idx = decoded_col.dict_indices[val_idx]
                        val_idx += 1
                        builder.append_bytes(dict_ptrs[dict_idx], dict_lens[dict_idx])
                    else:
                        builder.append_null()
            return builder.finish()
        finally:
            free(dict_ptrs)
            free(dict_lens)

    # ── Plain mode ─────────────────────────────────────────────────────────────
    estimated_bytes = 0
    for i in range(num_values):
        estimated_bytes += decoded_col.string_values[i].size()
    if estimated_bytes == 0:
        estimated_bytes = 1
    else:
        estimated_bytes = (estimated_bytes * 110) // 100

    builder = StringVectorBuilder(num_rows, estimated_bytes)
    val_idx = 0
    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                builder.append_bytes(
                    <const char*>decoded_col.string_values[val_idx].data(),
                    <Py_ssize_t>decoded_col.string_values[val_idx].size()
                )
                val_idx += 1
            else:
                builder.append_null()
    else:
        for i in range(num_rows):
            builder.append_bytes(
                <const char*>decoded_col.string_values[i].data(),
                <Py_ssize_t>decoded_col.string_values[i].size()
            )
    return builder.finish()


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


cdef Integer64Vector _make_typed_int64_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size = decoded_col.dict_int64_values.size()
    cdef const int64_t[::1] dictionary
    cdef int32_t* codes_buf
    cdef uint8_t* validity_buf
    cdef int32_t[::1] codes
    cdef uint8_t[::1] validity
    cdef Integer64Vector result

    if dict_size == 0:
        raise ValueError("typed int64 dictionary vector requires non-empty dictionary")

    # Fast path: packed codes array built in C++ (nullable dict column, zero materialization)
    if not decoded_col.dict_codes_array.empty():
        _record_dictionary_decode(decoded_col)
        return int64_from_packed_dict(
            decoded_col.dict_codes_array.data(),
            decoded_col.code_width,
            num_rows,
            decoded_col.dict_int64_values.data(),
            dict_size,
            decoded_col.valid_bits.data() if not decoded_col.valid_bits.empty() else NULL,
        )

    codes_buf = <int32_t*>malloc(num_rows * sizeof(int32_t))
    if codes_buf == NULL:
        raise MemoryError()
    validity_buf = NULL
    try:
        _fill_dict_codes_and_validity(decoded_col, num_rows, codes_buf, &validity_buf)
        codes = <int32_t[:num_rows:1]>codes_buf
        dictionary = <const int64_t[:dict_size:1]>decoded_col.dict_int64_values.data()
        _record_dictionary_decode(decoded_col)
        if validity_buf == NULL:
            result = int64_from_dict(codes, dictionary)
        else:
            validity = <uint8_t[:num_rows:1]>validity_buf
            result = int64_from_dict_nullable(codes, dictionary, validity)
    finally:
        free(codes_buf)
        free(validity_buf)
    return result


cdef Integer64Vector _make_typed_int64_from_int32_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size = decoded_col.dict_int32_values.size()
    cdef int64_t* dict_buf
    cdef int32_t* codes_buf
    cdef uint8_t* validity_buf
    cdef int64_t[::1] dictionary
    cdef int32_t[::1] codes
    cdef uint8_t[::1] validity
    cdef Integer64Vector result

    if dict_size == 0:
        raise ValueError("typed int32 dictionary vector requires non-empty dictionary")

    # Widen int32 dict to int64 via SIMD helper (no Python object needed)
    dict_buf = <int64_t*>malloc(dict_size * sizeof(int64_t))
    if dict_buf == NULL:
        raise MemoryError()
    rugo_widen_int32_to_int64(decoded_col.dict_int32_values.data(), dict_buf, dict_size)

    codes_buf = <int32_t*>malloc(num_rows * sizeof(int32_t))
    if codes_buf == NULL:
        free(dict_buf)
        raise MemoryError()
    validity_buf = NULL
    try:
        dictionary = <int64_t[:dict_size:1]>dict_buf

        # Fast path: packed codes array built in C++ (zero materialization)
        if not decoded_col.dict_codes_array.empty():
            _record_dictionary_decode(decoded_col)
            result = int64_from_packed_dict(
                decoded_col.dict_codes_array.data(),
                decoded_col.code_width,
                num_rows,
                dict_buf,
                dict_size,
                decoded_col.valid_bits.data() if not decoded_col.valid_bits.empty() else NULL,
            )
        else:
            _fill_dict_codes_and_validity(decoded_col, num_rows, codes_buf, &validity_buf)
            codes = <int32_t[:num_rows:1]>codes_buf
            _record_dictionary_decode(decoded_col)
            if validity_buf == NULL:
                result = int64_from_dict(codes, dictionary)
            else:
                validity = <uint8_t[:num_rows:1]>validity_buf
                result = int64_from_dict_nullable(codes, dictionary, validity)
    finally:
        free(dict_buf)
        free(codes_buf)
        free(validity_buf)
    return result


cdef Float64Vector _make_typed_float64_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size = decoded_col.dict_float64_values.size()
    cdef const double[::1] dictionary
    cdef int32_t* codes_buf
    cdef uint8_t* validity_buf
    cdef int32_t[::1] codes
    cdef uint8_t[::1] validity
    cdef Float64Vector result

    if dict_size == 0:
        raise ValueError("typed float64 dictionary vector requires non-empty dictionary")

    # Fast path: packed codes array built in C++ (nullable dict column, zero materialization)
    if not decoded_col.dict_codes_array.empty():
        _record_dictionary_decode(decoded_col)
        return float64_from_packed_dict(
            decoded_col.dict_codes_array.data(),
            decoded_col.code_width,
            num_rows,
            decoded_col.dict_float64_values.data(),
            dict_size,
            decoded_col.valid_bits.data() if not decoded_col.valid_bits.empty() else NULL,
        )

    codes_buf = <int32_t*>malloc(num_rows * sizeof(int32_t))
    if codes_buf == NULL:
        raise MemoryError()
    validity_buf = NULL
    try:
        _fill_dict_codes_and_validity(decoded_col, num_rows, codes_buf, &validity_buf)
        codes = <int32_t[:num_rows:1]>codes_buf
        dictionary = <const double[:dict_size:1]>decoded_col.dict_float64_values.data()
        _record_dictionary_decode(decoded_col)
        if validity_buf == NULL:
            result = float64_from_dict(codes, dictionary)
        else:
            validity = <uint8_t[:num_rows:1]>validity_buf
            result = float64_from_dict_nullable(codes, dictionary, validity)
    finally:
        free(codes_buf)
        free(validity_buf)
    return result


cdef Float64Vector _make_typed_float64_from_float32_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size = decoded_col.dict_float32_values.size()
    cdef double* dict_buf
    cdef int32_t* codes_buf
    cdef uint8_t* validity_buf
    cdef double[::1] dictionary
    cdef int32_t[::1] codes
    cdef uint8_t[::1] validity
    cdef Float64Vector result

    if dict_size == 0:
        raise ValueError("typed float32 dictionary vector requires non-empty dictionary")

    # Widen float32 dict to float64 via SIMD helper (no Python object needed)
    dict_buf = <double*>malloc(dict_size * sizeof(double))
    if dict_buf == NULL:
        raise MemoryError()
    rugo_widen_float32_to_float64(decoded_col.dict_float32_values.data(), dict_buf, dict_size)

    codes_buf = <int32_t*>malloc(num_rows * sizeof(int32_t))
    if codes_buf == NULL:
        free(dict_buf)
        raise MemoryError()
    validity_buf = NULL
    try:
        dictionary = <double[:dict_size:1]>dict_buf

        # Fast path: packed codes array built in C++ (zero materialization)
        if not decoded_col.dict_codes_array.empty():
            _record_dictionary_decode(decoded_col)
            result = float64_from_packed_dict(
                decoded_col.dict_codes_array.data(),
                decoded_col.code_width,
                num_rows,
                dict_buf,
                dict_size,
                decoded_col.valid_bits.data() if not decoded_col.valid_bits.empty() else NULL,
            )
        else:
            _fill_dict_codes_and_validity(decoded_col, num_rows, codes_buf, &validity_buf)
            codes = <int32_t[:num_rows:1]>codes_buf
            _record_dictionary_decode(decoded_col)
            if validity_buf == NULL:
                result = float64_from_dict(codes, dictionary)
            else:
                validity = <uint8_t[:num_rows:1]>validity_buf
                result = float64_from_dict_nullable(codes, dictionary, validity)
    finally:
        free(dict_buf)
        free(codes_buf)
        free(validity_buf)
    return result


cdef StringVector _make_typed_string_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size = decoded_col.string_dict_lens.size()
    cdef Py_ssize_t arena_size = decoded_col.string_dict_arena.size()
    cdef int32_t* offsets_buf
    cdef int32_t* lengths_buf
    cdef uint8_t* arena_buf
    cdef int32_t* codes_buf
    cdef uint8_t* validity_buf
    cdef int32_t[::1] dict_offsets
    cdef int32_t[::1] dict_lengths
    cdef uint8_t[::1] arena_bytes
    cdef int32_t[::1] codes
    cdef uint8_t[::1] validity
    cdef StringVector result
    cdef Py_ssize_t i

    if dict_size == 0:
        raise ValueError("typed string dictionary vector requires non-empty dictionary")

    # Fast path: packed codes array built in C++ (nullable dict column, zero materialization)
    if not decoded_col.dict_codes_array.empty():
        _record_dictionary_decode(decoded_col)
        return make_string_dict_only(
            decoded_col.dict_codes_array.data(),
            decoded_col.code_width,
            num_rows,
            decoded_col.string_dict_offsets.data(),
            decoded_col.string_dict_arena.data(),
            dict_size,
            arena_size,
            decoded_col.valid_bits.data() if not decoded_col.valid_bits.empty() else NULL,
        )

    offsets_buf = <int32_t*>malloc(dict_size * sizeof(int32_t))
    if offsets_buf == NULL:
        raise MemoryError()
    lengths_buf = <int32_t*>malloc(dict_size * sizeof(int32_t))
    if lengths_buf == NULL:
        free(offsets_buf)
        raise MemoryError()
    # malloc(0) is implementation-defined; use at least 1 byte for zero-length arena.
    arena_buf = <uint8_t*>malloc(arena_size if arena_size > 0 else 1)
    if arena_buf == NULL:
        free(offsets_buf)
        free(lengths_buf)
        raise MemoryError()
    codes_buf = <int32_t*>malloc(num_rows * sizeof(int32_t))
    if codes_buf == NULL:
        free(offsets_buf)
        free(lengths_buf)
        free(arena_buf)
        raise MemoryError()
    validity_buf = NULL
    try:
        for i in range(dict_size):
            offsets_buf[i] = decoded_col.string_dict_offsets[i]
            lengths_buf[i] = decoded_col.string_dict_lens[i]
        if arena_size > 0:
            memcpy(arena_buf, decoded_col.string_dict_arena.data(), <size_t>arena_size)

        _fill_dict_codes_and_validity(decoded_col, num_rows, codes_buf, &validity_buf)

        dict_offsets = <int32_t[:dict_size:1]>offsets_buf
        dict_lengths = <int32_t[:dict_size:1]>lengths_buf
        arena_bytes  = <uint8_t[:arena_size:1]>arena_buf if arena_size > 0 else <uint8_t[:1:1]>arena_buf
        codes        = <int32_t[:num_rows:1]>codes_buf

        _record_dictionary_decode(decoded_col)
        if validity_buf == NULL:
            result = string_from_dict_buffers(codes, dict_offsets, dict_lengths, arena_bytes)
        else:
            validity = <uint8_t[:num_rows:1]>validity_buf
            result = string_from_dict_buffers(codes, dict_offsets, dict_lengths, arena_bytes, validity)
    finally:
        free(offsets_buf)
        free(lengths_buf)
        free(arena_buf)
        free(codes_buf)
        free(validity_buf)
    return result


cdef Vector _make_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a dictionary-encoded Vector from decoded parquet dictionary payload.

    This is a compatibility shim that delegates to the typed dictionary
    constructors (e.g. :meth:`Integer64Vector.from_dict`). This avoids exposing
    ``DictionaryVector`` as part of the public/parquet path.
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
    cdef bytes col_type = decoded_col.type
    cdef bint all_null = _decoded_all_null(decoded_col, num_rows)
    cdef uint32_t offset
    cdef int32_t length
    cdef bytes value

    _record_dictionary_decode(decoded_col)

    if col_type == b"byte_array":
        if all_null:
            return StringVector.from_constant(None, num_rows, is_null=True)
        offset = decoded_col.string_dict_offsets[0]
        length = decoded_col.string_dict_lens[0]
        value = PyBytes_FromStringAndSize(
            <const char*>decoded_col.string_dict_arena.data() + offset,
            length,
        )
        return StringVector.from_constant(value, num_rows)
    if col_type == b"int32":
        if all_null:
            return Integer64Vector.from_constant(None, num_rows, is_null=True)
        return Integer64Vector.from_constant(<int64_t>decoded_col.dict_int32_values[0], num_rows)
    if col_type == b"int64":
        if all_null:
            return Integer64Vector.from_constant(None, num_rows, is_null=True)
        return Integer64Vector.from_constant(decoded_col.dict_int64_values[0], num_rows)
    if col_type == b"float32":
        if all_null:
            return Float64Vector.from_constant(None, num_rows, is_null=True)
        return Float64Vector.from_constant(<double>decoded_col.dict_float32_values[0], num_rows)
    if col_type == b"float64":
        if all_null:
            return Float64Vector.from_constant(None, num_rows, is_null=True)
        return Float64Vector.from_constant(decoded_col.dict_float64_values[0], num_rows)

    raise ValueError(f"unsupported constant dictionary type for decoded column: {col_type!r}")


cdef BoolVector _make_bool_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a BoolVector from a DecodedColumn with boolean values."""
    cdef uint8_t* value_bits
    cdef uint8_t* valid_bits
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes = (num_rows + 7) >> 3

    # Allocate and pack boolean values into bits
    value_bits = <uint8_t*> malloc(nb_bytes)
    if value_bits == NULL:
        raise MemoryError()

    # Zero out the value_bits
    for i in range(nb_bytes):
        value_bits[i] = 0

    if decoded_col.valid_bits.size() > 0:
        # We have a validity bitmap; values are scattered
        for i in range(num_rows):
            if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                if decoded_col.boolean_values[val_idx]:
                    value_bits[i >> 3] |= (1 << (i & 7))
                val_idx += 1
        # Copy valid_bits to a malloc'd buffer
        valid_bits = <uint8_t*> malloc(nb_bytes)
        if valid_bits == NULL:
            raise MemoryError("Failed to allocate valid_bits")
        memcpy(valid_bits, decoded_col.valid_bits.data(), nb_bytes)
    else:
        # All values are valid
        for i in range(num_rows):
            if decoded_col.boolean_values[i]:
                value_bits[i >> 3] |= (1 << (i & 7))
        valid_bits = NULL

    return bool_vector_from_bits(value_bits, valid_bits, num_rows)


cdef ArrayVector _make_array_vector(
        parquet_reader.DecodedColumn& decoded_col):
    """Build an ArrayVector(StringVector) from a list column's rep/def levels.

    Walks the rep_levels and def_levels vectors (one entry per logical slot)
    to reconstruct Arrow-style list offsets and a list-level null bitmap,
    then calls array_vector_from_parts to produce the ArrayVector.

    Semantics (Parquet 3-level LIST, optional list, optional element):
      rep == 0                : start of a new top-level row
      def == 0 (with rep==0)  : outer list is null (no elements for this row)
      def == 1 (with rep==0)  : outer list is non-null but empty
      def == max_def - 1      : element within list is null
      def == max_def          : element within list is present (string value)

    This generalises cleanly to any depth: only the boundary values
    max_def and max_def-1 need to be known, both stored in decoded_col.
    """
    cdef Py_ssize_t n_levels = decoded_col.rep_levels.size()
    if n_levels == 0:
        return None

    cdef int32_t max_def = decoded_col.max_def_level

    # Count logical rows (rep == 0 entries) and flat child elements
    # in a single pass, so we can allocate exact-size buffers.
    # flat_child_count = entries where def == max_def (real) or def == max_def-1 (null element)
    cdef Py_ssize_t num_rows = 0
    cdef Py_ssize_t flat_child_count = 0
    cdef Py_ssize_t i
    cdef int32_t max_def_m1 = max_def - 1
    for i in range(n_levels):
        if decoded_col.rep_levels[i] == 0:
            num_rows += 1
        if decoded_col.def_levels[i] >= max_def_m1:
            flat_child_count += 1
    if num_rows == 0:
        return None

    # Allocate offsets array (num_rows + 1) and list-level null bitmap.
    cdef int32_t* offsets = <int32_t*> malloc((num_rows + 1) * sizeof(int32_t))
    if offsets == NULL:
        raise MemoryError()
    cdef Py_ssize_t nb_bytes = (num_rows + 7) >> 3
    cdef uint8_t* null_bitmap = <uint8_t*> malloc(nb_bytes)
    if null_bitmap == NULL:
        free(offsets)
        raise MemoryError()
    cdef Py_ssize_t b
    for b in range(nb_bytes):
        null_bitmap[b] = 0

    # Estimate flat child capacity for StringVectorBuilder.
    # dict_codes_array mode: C++ scattered one packed code per slot into dict_codes_array;
    #   dict_indices is empty; arena holds the dictionary strings.
    # dict mode: per-row indices are in dict_indices; the dictionary strings live in
    #   string_dict_arena/offsets/lens (flat arena format, NOT string_values).
    # plain mode: element strings are in string_values directly.
    cdef Py_ssize_t n_values = decoded_col.string_values.size()
    cdef Py_ssize_t estimated_bytes = 0
    cdef bint dict_mode = decoded_col.dict_indices.size() > 0
    cdef bint use_arena = decoded_col.string_dict_arena.size() > 0
    cdef bint use_codes_array = decoded_col.dict_codes_array.size() > 0 and not dict_mode
    cdef uint8_t code_width = decoded_col.code_width
    cdef const uint8_t* arena_data_ptr = NULL
    cdef const uint8_t* codes_arr_ptr = NULL
    cdef int32_t _didx
    cdef int32_t _arena_slen
    cdef const char* _arena_sptr
    if use_codes_array and use_arena:
        codes_arr_ptr = decoded_col.dict_codes_array.data()
        arena_data_ptr = decoded_col.string_dict_arena.data()
        estimated_bytes = max(flat_child_count * 8, 1)
    elif dict_mode and use_arena:
        arena_data_ptr = decoded_col.string_dict_arena.data()
        for i in range(<Py_ssize_t>decoded_col.dict_indices.size()):
            _didx = decoded_col.dict_indices[i]
            estimated_bytes += decoded_col.string_dict_lens[_didx]
    elif dict_mode:
        # Legacy: dict stored in string_values (should not occur with current C++ but kept for safety)
        for i in range(<Py_ssize_t>decoded_col.dict_indices.size()):
            _didx = decoded_col.dict_indices[i]
            estimated_bytes += decoded_col.string_values[_didx].size()
    else:
        for i in range(n_values):
            estimated_bytes += decoded_col.string_values[i].size()
    estimated_bytes = max(estimated_bytes * 110 // 100, 1)

    # Use flat_child_count as the exact row count for the StringVectorBuilder.
    # resizable=True guards against any remaining estimate imprecision.
    cdef StringVectorBuilder builder = StringVectorBuilder(flat_child_count, estimated_bytes, resizable=True)

    cdef Py_ssize_t logical_row = -1
    cdef Py_ssize_t flat_idx = 0
    cdef Py_ssize_t val_idx = 0
    cdef int32_t rep, def_

    for i in range(n_levels):
        rep = decoded_col.rep_levels[i]
        def_ = decoded_col.def_levels[i]

        if rep == 0:  # new top-level row
            logical_row += 1
            offsets[logical_row] = <int32_t> flat_idx
            # def >= 1: outer optional group is present → list is non-null
            if def_ >= 1:
                null_bitmap[logical_row >> 3] |= (1 << (logical_row & 7))
            # def == 0: remains 0 (null list) — no element added regardless

        if def_ == max_def:
            # Present element: consume next string value
            if use_codes_array:
                # i is the global slot index; C++ wrote one code per slot position
                if code_width == 1:
                    _didx = <int32_t>codes_arr_ptr[i]
                elif code_width == 2:
                    _didx = <int32_t>(codes_arr_ptr[2*i] | (codes_arr_ptr[2*i+1] << 8))
                else:
                    _didx = <int32_t>(codes_arr_ptr[4*i] | (codes_arr_ptr[4*i+1] << 8) |
                                      (codes_arr_ptr[4*i+2] << 16) | (codes_arr_ptr[4*i+3] << 24))
                _arena_slen = decoded_col.string_dict_lens[_didx]
                _arena_sptr = <const char*>(arena_data_ptr + decoded_col.string_dict_offsets[_didx])
                builder.append_bytes(_arena_sptr, <Py_ssize_t>_arena_slen)
                # val_idx not used or incremented — codes_arr_ptr is indexed by i
            elif dict_mode and use_arena:
                _didx = decoded_col.dict_indices[val_idx]
                _arena_slen = decoded_col.string_dict_lens[_didx]
                _arena_sptr = <const char*>(arena_data_ptr + decoded_col.string_dict_offsets[_didx])
                builder.append_bytes(_arena_sptr, <Py_ssize_t>_arena_slen)
                val_idx += 1
            elif dict_mode:
                _didx = decoded_col.dict_indices[val_idx]
                builder.append_bytes(
                    <const char*> decoded_col.string_values[_didx].data(),
                    <Py_ssize_t> decoded_col.string_values[_didx].size()
                )
                val_idx += 1
            else:
                builder.append_bytes(
                    <const char*> decoded_col.string_values[val_idx].data(),
                    <Py_ssize_t> decoded_col.string_values[val_idx].size()
                )
                val_idx += 1
            flat_idx += 1
        elif def_ == max_def - 1:
            # Null element within a non-null list
            builder.append_null()
            flat_idx += 1
        # def == 0 (null list) or def == 1 (empty list): no child element

    # Write final end offset.
    if logical_row >= 0:
        offsets[logical_row + 1] = <int32_t> flat_idx

    cdef StringVector flat_child = builder.finish()
    cdef ArrayVector arr_vec = array_vector_from_parts(
        flat_child, offsets, null_bitmap, <Py_ssize_t> num_rows
    )

    # array_vector_from_parts copies both buffers; free our originals.
    free(offsets)
    free(null_bitmap)

    # Tell to_arrow() to cast the binary child to UTF-8 strings.
    arr_vec._child_decode_utf8 = True

    return arr_vec


def read_parquet(data, column_names=None):
    """Read parquet data from memory with optional column selection.

    Designed for serial use; Opteryx achieves parallelism by running
    multiple read_parquet calls concurrently across different files.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data
        column_names: list of column names to read, or None to read all columns

    Returns:
        list of Morsels (one per row group), or None if reading failed.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef vector[string] cpp_column_names

    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    cdef parquet_reader.DecodedTable result

    cdef double _t0, _t1

    _t0 = _time.perf_counter()
    if column_names is None:
        with nogil:
            result = parquet_reader.ReadParquet(&mem_view[0], size)
    else:
        for name in column_names:
            cpp_column_names.push_back(str(name).encode("utf-8"))
        with nogil:
            result = parquet_reader.ReadParquet(&mem_view[0], size, cpp_column_names)
    _t1 = _time.perf_counter()
    _TEL["cpp_decode_s"] += _t1 - _t0
    _TEL["calls"] += 1

    if not result.success:
        return None

    # Get column names for the Morsel
    cdef list col_names = [name.decode("utf-8") for name in result.column_names]

    if result.row_groups.size() == 0:
        return None

    cdef list all_morsels = []
    cdef list vectors = []
    cdef list successful_col_names = []
    cdef int32_t num_rows
    cdef Py_ssize_t col_idx, rg_idx
    cdef parquet_reader.DecodedColumn column
    cdef str col_type
    cdef Vector vec

    for rg_idx in range(<Py_ssize_t>result.row_groups.size()):
        # Get row count from first successful column in this row group
        num_rows = 0
        for col_idx in range(<Py_ssize_t>result.row_groups[rg_idx].size()):
            if result.row_groups[rg_idx][col_idx].success:
                num_rows = result.row_groups[rg_idx][col_idx].num_rows
                if num_rows > 0:
                    break

        vectors = []
        successful_col_names = []

        _TEL["row_groups"] += 1

        for col_idx in range(<Py_ssize_t>result.row_groups[rg_idx].size()):
            column = result.row_groups[rg_idx][col_idx]
            if not column.success:
                continue

            col_type = column.type.decode("utf-8")
            _t0 = _time.perf_counter()

            if col_type == "int64":
                if _should_emit_constant_vector(column, num_rows):
                    vec = _make_typed_constant_vector(column, num_rows)
                elif _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_typed_int64_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_int64_vector(column, num_rows)
                _TEL["cython_int64_s"] += _time.perf_counter() - _t0
            elif col_type == "int32":
                if _should_emit_constant_vector(column, num_rows):
                    vec = _make_typed_constant_vector(column, num_rows)
                elif _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_typed_int64_from_int32_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_int64_from_int32_vector(column, num_rows)
                _TEL["cython_int64_s"] += _time.perf_counter() - _t0
            elif col_type == "byte_array" and column.rep_levels.size() > 0:
                vec = _make_array_vector(column)
                if column.string_dict_lens.size() > 0:
                    _TEL["parquet_dict_materialize_fallbacks"] += 1
                _TEL["cython_str_s"] += _time.perf_counter() - _t0
            elif col_type == "byte_array":
                if _should_emit_constant_vector(column, num_rows):
                    vec = _make_typed_constant_vector(column, num_rows)
                elif _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_typed_string_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_string_vector(column, num_rows)
                _TEL["cython_str_s"] += _time.perf_counter() - _t0
            elif col_type == "boolean":
                vec = _make_bool_vector(column, num_rows)
                _TEL["cython_bool_s"] += _time.perf_counter() - _t0
            elif col_type == "float32":
                if _should_emit_constant_vector(column, num_rows):
                    vec = _make_typed_constant_vector(column, num_rows)
                elif _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_typed_float64_from_float32_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_float64_from_float32_vector(column, num_rows)
                _TEL["cython_float_s"] += _time.perf_counter() - _t0
            elif col_type == "float64":
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
                _TEL["cython_other_s"] += _time.perf_counter() - _t0
                continue

            _TEL["columns"] += 1
            vectors.append(vec)
            successful_col_names.append(col_names[col_idx])

        all_morsels.append(Morsel.from_vectors(successful_col_names, vectors))

    return all_morsels


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
    'LZ4':          4,
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

    # Convert codec string → int (e.g. 'SNAPPY' → 1)
    codec_str = col_stats.get('compression_codec') or 'UNCOMPRESSED'
    cpp_col.codec = _CODEC_INT.get(codec_str, 0)

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

    cdef str col_type = result.type.decode("utf-8")
    cdef str col_logical = col_stats.get('logical_type') or ''
    cdef int32_t num_rows = <int32_t>result.num_rows

    # Handle uint64: physical_type is "int64" but logical_type is "UINT_64"
    # Treat as int64 (reinterpret the bits for aggregation)
    if col_type == "int64" and col_logical.upper() == "UINT_64":
        col_type = "int64"  # Reinterpret 64-bit unsigned as signed

    if col_type == "int32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_from_int32_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows).to_pylist()
    elif col_type == "int64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows).to_pylist()
    elif col_type == "byte_array":
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
    elif col_type == "boolean":
        return [bool(val) for val in result.boolean_values]
    elif col_type == "float32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, <int32_t>result.num_rows).to_pylist()
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_from_float32_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows).to_pylist()
    elif col_type == "float64":
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

    This is the primary API for the columnar range-read design.  Rather than
    passing the entire file into memory, the caller:

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

    # Convert codec string → int (e.g. 'SNAPPY' → 1)
    codec_str = col_stats.get('compression_codec') or 'UNCOMPRESSED'
    cpp_col.codec = _CODEC_INT.get(codec_str, 0)

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

    col_type = result.type.decode("utf-8")
    col_logical = col_stats.get('logical_type') or ''
    num_rows = <int32_t>result.num_rows

    # Handle uint64: physical_type is "int64" but logical_type is "UINT_64"
    # Treat as int64 (reinterpret the bits for aggregation)
    if col_type == "int64" and col_logical.upper() == "UINT_64":
        col_type = "int64"  # Reinterpret 64-bit unsigned as signed

    # Convert C++ DecodedColumn to Draken Vector using the same logic as read_parquet()
    if col_type == "int32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_from_int32_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows)

    elif col_type == "int64":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_int64_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows)

    elif col_type == "byte_array" and result.rep_levels.size() > 0:
        # LIST / repeated column — mirror the dispatch in read_parquet()
        return _make_array_vector(result)

    elif col_type == "byte_array":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_string_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_string_vector(result, num_rows)

    elif col_type == "boolean":
        return _make_bool_vector(result, num_rows)

    elif col_type == "float32":
        if _should_emit_constant_vector(result, num_rows):
            return _make_typed_constant_vector(result, num_rows)
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_typed_float64_from_float32_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows)

    elif col_type == "float64":
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
