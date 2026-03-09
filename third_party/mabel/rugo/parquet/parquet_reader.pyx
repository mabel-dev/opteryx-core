# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

import datetime
import os
import struct
import time as _time
import opteryx.config as _opteryx_config

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
    double rle_s
    double val_expand_s
    long long calls
    void reset() nogil


def reset_cpp_telemetry():
    """Zero all C++ phase telemetry accumulators."""
    reset()


def get_cpp_telemetry():
    """Return a dict with C++ phase timing (seconds) since last reset."""
    return {
        "metadata_s":    metadata_s,
        "decompress_s":  decompress_s,
        "dict_parse_s":  dict_parse_s,
        "rle_s":         rle_s,
        "val_expand_s":  val_expand_s,
        "calls":         calls,
    }

cimport parquet_reader
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int32_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.string cimport string
from libcpp.vector cimport vector

# Import Draken vector types and components
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from opteryx.draken.core.buffers cimport (
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_STRING,
    DrakenVarBuffer,
)
from opteryx.draken.vectors.array_vector cimport ArrayVector, array_vector_from_parts
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.morsels.morsel cimport Morsel


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


cdef object decode_value(
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
    cdef object candidate

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


cdef object _filestats_to_python(parquet_reader.FileStats fs,
                                 bint include_row_groups):
    cdef dict result = {"num_rows": fs.num_rows}

    cdef list schema_columns = []
    cdef parquet_reader.SchemaField field
    cdef size_t idx
    cdef dict top_level_types = {}
    for idx in range(fs.schema_columns.size()):
        field = fs.schema_columns[idx]
        field_name = field.name.decode("utf-8")
        field_physical = field.physical_type.decode("utf-8")
        field_logical = field.logical_type.decode("utf-8")
        schema_columns.append({
            "name": field_name,
            "physical_type": field_physical,
            "logical_type": field_logical,
            "nullable": bool(field.nullable),
        })
        top_level_types[field_name] = {
            "logical": field_logical,
            "physical": field_physical,
        }
    result["schema_columns"] = schema_columns

    if include_row_groups and fs.row_groups.size() > 0:
        row_groups = []
        for rg in fs.row_groups:
            rg_dict = {
                "num_rows": rg.num_rows,
                "total_byte_size": rg.total_byte_size,
                "columns": []
            }
            for col in rg.columns:
                physical_type_str = col.physical_type.decode("utf-8")
                if col.logical_type.size() > 0:
                    logical_type_str = col.logical_type.decode("utf-8")
                else:
                    logical_type_str = ""

                full_name = col.name.decode("utf-8")
                if "." in full_name:
                    display_name = full_name.split(".", 1)[0]
                else:
                    display_name = full_name

                top_level_info = top_level_types.get(display_name)
                if top_level_info is not None:
                    top_level_type = top_level_info.get("logical", "")
                    prefer_text = top_level_type == "json" or top_level_type.startswith("array<")
                else:
                    top_level_type = ""
                    prefer_text = False

                if full_name != display_name and top_level_info is not None:
                    logical_type_str = top_level_info.get("logical", logical_type_str)

                null_count = col.null_count if col.null_count >= 0 else None
                distinct_count = col.distinct_count if col.distinct_count >= 0 else None
                num_values = col.num_values if col.num_values >= 0 else None
                total_uncompressed_size = col.total_uncompressed_size if col.total_uncompressed_size >= 0 else None
                total_compressed_size = col.total_compressed_size if col.total_compressed_size >= 0 else None
                data_page_offset = col.data_page_offset if col.data_page_offset >= 0 else None
                index_page_offset = col.index_page_offset if col.index_page_offset >= 0 else None
                dictionary_page_offset = col.dictionary_page_offset if col.dictionary_page_offset >= 0 else None
                bloom_offset = col.bloom_offset if col.bloom_offset >= 0 else None
                bloom_length = col.bloom_length if col.bloom_length >= 0 else None

                min_val = decode_value(
                    col.physical_type,
                    col.logical_type,
                    col.min,
                    prefer_text) if col.has_min else None
                max_val = decode_value(
                    col.physical_type,
                    col.logical_type,
                    col.max,
                    prefer_text) if col.has_max else None

                encodings_list = []
                for enc in col.encodings:
                    encodings_list.append(parquet_reader.EncodingToString(enc).decode("utf-8"))

                codec_str = None
                if col.codec >= 0:
                    codec_str = parquet_reader.CompressionCodecToString(col.codec).decode("utf-8")

                kv_metadata = {}
                for item in col.key_value_metadata:
                    kv_metadata[item.first.decode("utf-8")] = item.second.decode("utf-8")

                rg_dict["columns"].append({
                    "name": display_name,
                    "path_in_schema": full_name,
                    "physical_type": physical_type_str,
                    "logical_type": logical_type_str,
                    "max_repetition_level": col.max_repetition_level if col.max_repetition_level >= 0 else None,
                    "max_definition_level": col.max_definition_level if col.max_definition_level >= 0 else None,
                    "min": min_val,
                    "max": max_val,
                    "null_count": null_count,
                    "distinct_count": distinct_count,
                    "num_values": num_values,
                    "total_uncompressed_size": total_uncompressed_size,
                    "total_compressed_size": total_compressed_size,
                    "data_page_offset": data_page_offset,
                    "index_page_offset": index_page_offset,
                    "dictionary_page_offset": dictionary_page_offset,
                    "bloom_offset": bloom_offset,
                    "bloom_length": bloom_length,
                    "encodings": encodings_list,
                    "compression_codec": codec_str,
                    "key_value_metadata": kv_metadata if kv_metadata else None,
                })
            row_groups.append(rg_dict)
        result["row_groups"] = row_groups
    else:
        result["row_groups"] = []

    return result


def read_metadata(str path, *, bint schema_only=False,
                  bint include_statistics=True, Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from a file path."""
    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef bytes path_bytes = path.encode("utf-8")
    cdef const char* c_path = path_bytes
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataC(
        c_path, opts
    )
    return _filestats_to_python(fs, not schema_only)


def read_metadata_from_bytes(bytes data, *, bint schema_only=False,
                             bint include_statistics=True,
                             Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from an in-memory bytes object."""
    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef const uint8_t* buf = <const uint8_t*> data
    cdef size_t size = len(data)
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _filestats_to_python(fs, not schema_only)


def read_metadata_from_memoryview(memoryview mv, *, bint schema_only=False,
                                  bint include_statistics=True,
                                  Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from a Python memoryview (zero-copy)."""
    if not mv.contiguous:
        raise ValueError("Memoryview must be contiguous")

    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef memoryview[uint8_t] mv_bytes = mv.cast('B')  # keep reference alive
    cdef const uint8_t* buf = &mv_bytes[0]
    cdef size_t size = mv_bytes.nbytes

    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _filestats_to_python(fs, not schema_only)


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

cdef Int64Vector _make_int64_from_int32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build an Int64Vector from a DecodedColumn with int32_t values (upcasting)."""
    cdef Int64Vector vec = Int64Vector(num_rows)
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
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


cdef Int64Vector _make_int64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build an Int64Vector from a DecodedColumn with int64_t values."""
    cdef Int64Vector vec = Int64Vector(num_rows)
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
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
            memcpy(dst, decoded_col.int64_values.data(), <size_t>num_rows * sizeof(int64_t))

    return vec


cdef Float64Vector _make_float64_from_float32_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Float64Vector from float32 values (upcasting), including dict-mode."""
    cdef Float64Vector vec = Float64Vector(num_rows)
    cdef double* dst = <double*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
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
        for i in range(num_rows):
            if dict_mode:
                code = decoded_col.dict_indices[i]
                if code < 0 or code >= decoded_col.dict_float32_values.size():
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                dst[i] = <double>decoded_col.dict_float32_values[code]
            else:
                dst[i] = <double>decoded_col.float32_values[i]
    return vec


cdef Float64Vector _make_float64_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a Float64Vector from a DecodedColumn with float64 values."""
    cdef Float64Vector vec = Float64Vector(num_rows)
    cdef double* dst = <double*> vec.ptr.data
    cdef Py_ssize_t i, val_idx = 0
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef int32_t code
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

    # ── Dict mode ─────────────────────────────────────────────────────────────
    # C++ stores: string_dict_arena = flat packed bytes, string_dict_offsets/lens
    # = per-entry index, dict_indices = one index per (non-null) row.
    # We bypass StringVectorBuilder entirely and write directly to the
    # DrakenVarBuffer data/offsets pointers to eliminate per-row call overhead.
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

        # Step 2: count total expanded bytes — tight loop, no Python overhead.
        total_bytes = 0
        for i in range(num_indices):
            total_bytes += dict_lens[decoded_col.dict_indices[i]]
        if total_bytes == 0:
            total_bytes = 1

        # Step 3: allocate StringVector with exact capacity and grab raw ptrs.
        vec = StringVector(num_rows, total_bytes)
        buf = vec.ptr
        dst = <char*>buf.data
        offsets = buf.offsets
        offset = 0

        # Step 4a: non-null path — no null bitmap required, tightest inner loop.
        if decoded_col.valid_bits.size() == 0:
            for i in range(num_rows):
                offsets[i] = offset
                dict_idx = decoded_col.dict_indices[i]
                slen = dict_lens[dict_idx]
                if slen > 0:
                    memcpy(dst + offset, dict_ptrs[dict_idx], slen)
                offset += slen
            offsets[num_rows] = offset

        else:
            # Step 4b: nullable — allocate and fill validity bitmap.
            nb_bytes = (num_rows + 7) >> 3
            nb = <uint8_t*>malloc(nb_bytes)
            if nb == NULL:
                free(dict_ptrs)
                free(dict_lens)
                raise MemoryError()
            memset(nb, 0, nb_bytes)
            val_idx = 0
            for i in range(num_rows):
                offsets[i] = offset
                if (decoded_col.valid_bits[i >> 3] >> (i & 7)) & 1:
                    dict_idx = decoded_col.dict_indices[val_idx]
                    val_idx += 1
                    slen = dict_lens[dict_idx]
                    if slen > 0:
                        memcpy(dst + offset, dict_ptrs[dict_idx], slen)
                    offset += slen
                    nb[i >> 3] |= (1 << (i & 7))
            offsets[num_rows] = offset
            buf.null_bitmap = nb

        free(dict_ptrs)
        free(dict_lens)
        return vec

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


cdef inline double _dictionary_ratio_limit():
    cdef double ratio_limit = 0.5
    cdef object ratio_obj = _opteryx_config.PARQUET_DICT_MAX_CARDINALITY_RATIO
    try:
        ratio_limit = float(ratio_obj)
    except Exception:
        ratio_limit = 0.5
    if ratio_limit < 0.0:
        return 0.0
    return ratio_limit


cdef inline bint _should_emit_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    cdef Py_ssize_t dict_size
    if not _decoded_has_dictionary(decoded_col):
        return False
    dict_size = _decoded_dict_size(decoded_col)
    if dict_size <= 0:
        return False

    # Cardinality fallback is only applied for string dictionaries.
    if decoded_col.type == b"byte_array":
        if num_rows <= 0:
            return False
        if (<double>dict_size / <double>num_rows) > _dictionary_ratio_limit():
            return False
    return True


cdef DictionaryVector _make_dictionary_vector(
        parquet_reader.DecodedColumn& decoded_col,
        int32_t num_rows):
    """Build a DictionaryVector from decoded parquet dictionary payload."""
    cdef bytes col_type = decoded_col.type
    cdef Py_ssize_t dict_size = 0
    cdef Py_ssize_t i
    cdef Py_ssize_t val_idx
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb
    cdef uint8_t code_width
    cdef int32_t code
    cdef Py_ssize_t dict_bytes = 0
    cdef int32_t itemsize = 0
    cdef int dict_value_type = DRAKEN_STRING
    cdef DictionaryVector vec
    cdef uint8_t* code_u8
    cdef uint16_t* code_u16
    cdef uint32_t* code_u32
    cdef uint8_t bit
    cdef uint8_t byte
    cdef int32_t running = 0

    if col_type == b"byte_array":
        dict_size = decoded_col.string_dict_lens.size()
        dict_bytes = decoded_col.string_dict_arena.size()
        dict_value_type = DRAKEN_STRING
    elif col_type == b"int32":
        dict_size = decoded_col.dict_int32_values.size()
        itemsize = sizeof(int32_t)
        dict_bytes = dict_size * itemsize
        dict_value_type = DRAKEN_INT32
    elif col_type == b"int64":
        dict_size = decoded_col.dict_int64_values.size()
        itemsize = sizeof(int64_t)
        dict_bytes = dict_size * itemsize
        dict_value_type = DRAKEN_INT64
    elif col_type == b"float32":
        dict_size = decoded_col.dict_float32_values.size()
        itemsize = sizeof(float)
        dict_bytes = dict_size * itemsize
        dict_value_type = DRAKEN_FLOAT32
    elif col_type == b"float64":
        dict_size = decoded_col.dict_float64_values.size()
        itemsize = sizeof(double)
        dict_bytes = dict_size * itemsize
        dict_value_type = DRAKEN_FLOAT64
    else:
        raise ValueError(f"unsupported dictionary type for decoded column: {col_type!r}")

    if dict_size == 0:
        raise ValueError("dictionary vector requires non-empty dictionary")

    code_width = decoded_col.code_width if decoded_col.code_width in (1, 2, 4) else _code_width_from_dict_size(dict_size)
    if dict_bytes == 0:
        dict_bytes = 1

    vec = DictionaryVector(
        <size_t>num_rows,
        <size_t>dict_size,
        <size_t>dict_bytes,
        code_width,
        bool(decoded_col.dict_ordered),
        dict_value_type,
    )

    if dict_value_type == DRAKEN_STRING:
        # Copy dictionary bytes
        if decoded_col.string_dict_arena.size() > 0:
            memcpy(
                vec.ptr.dictionary_values.data,
                decoded_col.string_dict_arena.data(),
                <size_t>decoded_col.string_dict_arena.size(),
            )

        # Convert [offset, len] pairs to Arrow-style cumulative offsets.
        vec.ptr.dictionary_values.offsets[0] = 0
        running = 0
        for i in range(dict_size):
            running += decoded_col.string_dict_lens[i]
            vec.ptr.dictionary_values.offsets[i + 1] = running
    else:
        if dict_value_type == DRAKEN_INT32:
            memcpy(
                vec.ptr.dictionary_values.data,
                decoded_col.dict_int32_values.data(),
                <size_t>dict_bytes,
            )
        elif dict_value_type == DRAKEN_INT64:
            memcpy(
                vec.ptr.dictionary_values.data,
                decoded_col.dict_int64_values.data(),
                <size_t>dict_bytes,
            )
        elif dict_value_type == DRAKEN_FLOAT32:
            memcpy(
                vec.ptr.dictionary_values.data,
                decoded_col.dict_float32_values.data(),
                <size_t>dict_bytes,
            )
        elif dict_value_type == DRAKEN_FLOAT64:
            memcpy(
                vec.ptr.dictionary_values.data,
                decoded_col.dict_float64_values.data(),
                <size_t>dict_bytes,
            )
        for i in range(dict_size + 1):
            vec.ptr.dictionary_values.offsets[i] = <int32_t>(i * itemsize)

    if decoded_col.valid_bits.size() > 0:
        nb_bytes = (num_rows + 7) >> 3
        nb = <uint8_t*> malloc(nb_bytes)
        if nb == NULL:
            raise MemoryError()
        memcpy(nb, decoded_col.valid_bits.data(), nb_bytes)
        vec.ptr.null_bitmap = nb

    val_idx = 0
    code_u8 = <uint8_t*>vec.ptr.codes
    code_u16 = <uint16_t*>vec.ptr.codes
    code_u32 = <uint32_t*>vec.ptr.codes

    if decoded_col.valid_bits.size() > 0:
        for i in range(num_rows):
            byte = decoded_col.valid_bits[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if bit:
                if val_idx >= decoded_col.dict_indices.size():
                    raise ValueError("dictionary index stream shorter than number of valid rows")
                code = decoded_col.dict_indices[val_idx]
                val_idx += 1
                if code < 0 or code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            else:
                code = 0

            if code_width == 1:
                code_u8[i] = <uint8_t>code
            elif code_width == 2:
                code_u16[i] = <uint16_t>code
            else:
                code_u32[i] = <uint32_t>code
    else:
        if decoded_col.dict_indices.size() != num_rows:
            raise ValueError("dictionary index stream length does not match row count")
        for i in range(num_rows):
            code = decoded_col.dict_indices[i]
            if code < 0 or code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            if code_width == 1:
                code_u8[i] = <uint8_t>code
            elif code_width == 2:
                code_u16[i] = <uint16_t>code
            else:
                code_u32[i] = <uint32_t>code

    _TEL["parquet_dict_columns_decoded"] += 1
    _TEL["parquet_dict_unique_values"] += dict_size
    _TEL["parquet_dict_code_width_bytes"] += code_width

    return vec


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
    # If dict mode is active, string_values is the compact dictionary;
    # walk dict_indices to get the true expanded byte budget.
    cdef Py_ssize_t n_values = decoded_col.string_values.size()
    cdef Py_ssize_t estimated_bytes = 0
    cdef bint dict_mode = decoded_col.dict_indices.size() > 0
    cdef int32_t _didx
    if dict_mode:
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
            if dict_mode:
                _didx = decoded_col.dict_indices[val_idx]
                builder.append_bytes(
                    <const char*> decoded_col.string_values[_didx].data(),
                    <Py_ssize_t> decoded_col.string_values[_didx].size()
                )
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
    import pyarrow as _pa
    arr_vec._child_arrow_type = _pa.utf8()

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

    # Convert input data to memory view
    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # Call the appropriate C++ function based on whether columns are specified
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
                if _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_int64_vector(column, num_rows)
                _TEL["cython_int64_s"] += _time.perf_counter() - _t0
            elif col_type == "int32":
                if _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_dictionary_vector(column, num_rows)
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
                if _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_string_vector(column, num_rows)
                _TEL["cython_str_s"] += _time.perf_counter() - _t0
            elif col_type == "boolean":
                vec = _make_bool_vector(column, num_rows)
                _TEL["cython_bool_s"] += _time.perf_counter() - _t0
            elif col_type == "float32":
                if _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_dictionary_vector(column, num_rows)
                else:
                    if _decoded_has_dictionary(column):
                        _TEL["parquet_dict_materialize_fallbacks"] += 1
                    vec = _make_float64_from_float32_vector(column, num_rows)
                _TEL["cython_float_s"] += _time.perf_counter() - _t0
            elif col_type == "float64":
                if _should_emit_dictionary_vector(column, num_rows):
                    vec = _make_dictionary_vector(column, num_rows)
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
    cdef int32_t num_rows = <int32_t>result.num_rows

    if col_type == "int32":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows).to_pylist()
    elif col_type == "int64":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows).to_pylist()
    elif col_type == "byte_array":
        if _should_emit_dictionary_vector(result, num_rows):
            return [
                _safe_decode_utf8(v) if v is not None else None
                for v in _make_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
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
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows).to_pylist()
    elif col_type == "float64":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, <int32_t>result.num_rows).to_pylist()
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_vector(result, num_rows).to_pylist()
    else:
        return None


def decode_column_from_chunk(chunk_bytes, col_stats):
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

    Returns a Draken Vector (Int64Vector, StringVector, Float64Vector, BoolVector, or ArrayVector),
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

    with nogil:
        result = parquet_reader.DecodeColumnFromChunk(&mem_view[0], size, &cpp_col)

    if not result.success:
        return None

    col_type = result.type.decode("utf-8")
    num_rows = <int32_t>result.num_rows

    # Convert C++ DecodedColumn to Draken Vector using the same logic as read_parquet()
    if col_type == "int32":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_from_int32_vector(result, num_rows)
    
    elif col_type == "int64":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_int64_vector(result, num_rows)
    
    elif col_type == "byte_array":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_string_vector(result, num_rows)
    
    elif col_type == "boolean":
        return _make_bool_vector(result, num_rows)
    
    elif col_type == "float32":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, num_rows)
        if _decoded_has_dictionary(result):
            _TEL["parquet_dict_materialize_fallbacks"] += 1
        return _make_float64_from_float32_vector(result, num_rows)
    
    elif col_type == "float64":
        if _should_emit_dictionary_vector(result, num_rows):
            return _make_dictionary_vector(result, num_rows)
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
