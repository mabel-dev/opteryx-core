# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
DRKM v1 serializer/deserializer for Draken Morsels.

This module is intentionally Draken-native in the hot path:
- writes directly from Draken vector buffers
- reads directly into Draken vector buffers
- does not convert to/from Arrow for storage I/O
"""

from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenType
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.buffers cimport DRAKEN_INTERVAL
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.interval_vector cimport IntervalVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.vector cimport Vector
from opteryx.third_party.cyan4973.xxhash cimport hash_bytes

import io
import os
import struct


MAGIC = b"DRKM"
FOOTER_MAGIC = b"DRKF"
FORMAT_VERSION = 1

# Header:
# magic, version, flags, row_count, column_count, block_count, schema_fingerprint, default_codec
HEADER_FMT = "<4sHHQIIQB7x"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# name_len, draken_type, encoding, flags, block_start, block_end
COLUMN_ENTRY_FMT = "<HHBBII"
COLUMN_ENTRY_SIZE = struct.calcsize(COLUMN_ENTRY_FMT)

# block_id, column_id, segment_kind, codec, row_start, row_count, flags, raw_len, comp_len, checksum32
BLOCK_HEADER_FMT = "<IIBBQIBIIIx"
BLOCK_HEADER_SIZE = struct.calcsize(BLOCK_HEADER_FMT)

# block_id, column_id, segment_kind, offset, total_len
BLOCK_DIR_ENTRY_FMT = "<IIB3xQI"
BLOCK_DIR_ENTRY_SIZE = struct.calcsize(BLOCK_DIR_ENTRY_FMT)

# dir_offset, dir_len, footer_checksum, block_count, footer_magic
FOOTER_FMT = "<QIII4s"
FOOTER_SIZE = struct.calcsize(FOOTER_FMT)


CODEC_NONE = 0
CODEC_LZ4 = 1
CODEC_ZSTD = 2

ENCODING_FIXED = 0
ENCODING_VAR = 1

SEG_NULL = 0
SEG_DATA = 1
SEG_OFFSETS = 2
SEG_VALUES = 3

FLAG_HAS_NULLS = 1


class DrakenMorselStorageError(RuntimeError):
    """Base DRKM storage I/O error."""


class DrakenMorselCorruptionError(DrakenMorselStorageError):
    """Raised when DRKM payload validation fails."""


def _codec_name_to_id(codec_name: str) -> int:
    c = codec_name.lower().strip()
    if c == "none":
        return CODEC_NONE
    if c == "lz4":
        return CODEC_LZ4
    if c == "zstd":
        return CODEC_ZSTD
    raise ValueError(f"unsupported codec '{codec_name}'")


def _codec_id_to_name(codec_id: int) -> str:
    if codec_id == CODEC_NONE:
        return "none"
    if codec_id == CODEC_LZ4:
        return "lz4"
    if codec_id == CODEC_ZSTD:
        return "zstd"
    return f"unknown({codec_id})"


def _load_lz4():
    from opteryx.third_party.lz4 import lz4
    return lz4


def _load_zstd():
    from opteryx.third_party.facebook import zstd
    return zstd


def _compress_payload(bytes payload, int codec_id, int zstd_level):
    if codec_id == CODEC_NONE:
        return payload
    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        return lz4.compress_block(payload)
    if codec_id == CODEC_ZSTD:
        zstd = _load_zstd()
        zstd_compress = getattr(zstd, "compress", None)
        if zstd_compress is None:
            raise DrakenMorselStorageError("zstd compression is unavailable in this build")
        try:
            return zstd_compress(payload, level=zstd_level)
        except TypeError:
            return zstd_compress(payload)
    raise DrakenMorselStorageError(f"unsupported codec id {codec_id}")


def _decompress_payload(bytes payload, int codec_id, int expected_len):
    if codec_id == CODEC_NONE:
        return payload
    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        return lz4.decompress_block(payload, expected_len)
    if codec_id == CODEC_ZSTD:
        zstd = _load_zstd()
        return zstd.decompress(payload)
    raise DrakenMorselStorageError(f"unsupported codec id {codec_id}")


cdef inline bytes _bytes_from_pointer(const void* ptr, Py_ssize_t length):
    if length <= 0:
        return b""
    if ptr == NULL:
        raise DrakenMorselStorageError("attempted to read non-empty payload from NULL pointer")
    return PyBytes_FromStringAndSize(<const char*>ptr, length)


cdef inline uint32_t _checksum32(bytes payload):
    return <uint32_t>(hash_bytes(payload) & 0xFFFFFFFF)


def _read_exact(handle, int size):
    payload = handle.read(size)
    if payload is None or len(payload) != size:
        raise DrakenMorselCorruptionError(f"unexpected EOF while reading {size} bytes")
    return payload


cdef inline bint _is_supported_fixed_type(int dtype):
    return (
        dtype == DRAKEN_INT64
        or dtype == DRAKEN_FLOAT64
        or dtype == DRAKEN_BOOL
        or dtype == DRAKEN_DATE32
        or dtype == DRAKEN_TIMESTAMP64
        or dtype == DRAKEN_TIME32
        or dtype == DRAKEN_TIME64
        or dtype == DRAKEN_INTERVAL
    )


cdef DrakenFixedBuffer* _fixed_ptr_from_vector(Vector vec, int dtype):
    if dtype == DRAKEN_INT64:
        return (<Int64Vector>vec).ptr
    if dtype == DRAKEN_FLOAT64:
        return (<Float64Vector>vec).ptr
    if dtype == DRAKEN_BOOL:
        return (<BoolVector>vec).ptr
    if dtype == DRAKEN_DATE32:
        return (<Date32Vector>vec).ptr
    if dtype == DRAKEN_TIMESTAMP64:
        return (<TimestampVector>vec).ptr
    if dtype == DRAKEN_TIME32 or dtype == DRAKEN_TIME64:
        return (<TimeVector>vec).ptr
    if dtype == DRAKEN_INTERVAL:
        return (<IntervalVector>vec).ptr
    return NULL


cdef Vector _build_fixed_vector(
    int dtype,
    Py_ssize_t row_count,
    bytes data_payload,
    bytes null_payload,
):
    cdef Vector out
    cdef DrakenFixedBuffer* ptr
    cdef char* src
    cdef Py_ssize_t data_len
    cdef Py_ssize_t expected_data_len
    cdef char* null_src
    cdef Py_ssize_t null_len
    cdef Py_ssize_t expected_null_len
    cdef uint8_t* bitmap

    if dtype == DRAKEN_INT64:
        out = Int64Vector(<size_t>row_count)
    elif dtype == DRAKEN_FLOAT64:
        out = Float64Vector(<size_t>row_count)
    elif dtype == DRAKEN_BOOL:
        out = BoolVector(<size_t>row_count)
    elif dtype == DRAKEN_DATE32:
        out = Date32Vector(<size_t>row_count)
    elif dtype == DRAKEN_TIMESTAMP64:
        out = TimestampVector(<size_t>row_count)
        (<TimestampVector>out).timestamp_unit = "us"
        (<TimestampVector>out).null_bit_offset = 0
    elif dtype == DRAKEN_TIME32:
        out = TimeVector(<size_t>row_count, False)
    elif dtype == DRAKEN_TIME64:
        out = TimeVector(<size_t>row_count, True)
    elif dtype == DRAKEN_INTERVAL:
        out = IntervalVector(<size_t>row_count)
    else:
        raise DrakenMorselStorageError(f"unsupported fixed dtype {dtype}")

    ptr = _fixed_ptr_from_vector(out, dtype)
    if ptr == NULL:
        raise DrakenMorselStorageError(f"failed to allocate fixed vector for dtype {dtype}")

    if PyBytes_AsStringAndSize(data_payload, &src, &data_len) != 0:
        raise ValueError("invalid fixed payload")

    if dtype == DRAKEN_BOOL:
        expected_data_len = (row_count + 7) >> 3
    else:
        expected_data_len = row_count * <Py_ssize_t>ptr.itemsize

    if data_len != expected_data_len:
        raise DrakenMorselCorruptionError(
            f"fixed payload length mismatch for dtype {dtype}: expected {expected_data_len}, got {data_len}"
        )
    if expected_data_len > 0:
        memcpy(ptr.data, <const void*>src, <size_t>expected_data_len)

    if null_payload and len(null_payload) > 0:
        if PyBytes_AsStringAndSize(null_payload, &null_src, &null_len) != 0:
            raise ValueError("invalid null bitmap payload")
        expected_null_len = (row_count + 7) >> 3
        if null_len != expected_null_len:
            raise DrakenMorselCorruptionError(
                f"null bitmap length mismatch: expected {expected_null_len}, got {null_len}"
            )
        bitmap = <uint8_t*>malloc(<size_t>null_len)
        if bitmap == NULL:
            raise MemoryError()
        memcpy(bitmap, <const void*>null_src, <size_t>null_len)
        ptr.null_bitmap = bitmap
    else:
        ptr.null_bitmap = NULL

    return out


cdef Vector _build_string_vector(
    Py_ssize_t row_count,
    bytes offsets_payload,
    bytes values_payload,
    bytes null_payload,
):
    cdef StringVector out = StringVector(<size_t>row_count, <size_t>len(values_payload))
    cdef DrakenVarBuffer* ptr = out.ptr
    cdef char* offs_src
    cdef Py_ssize_t offs_len
    cdef char* values_src
    cdef Py_ssize_t values_len
    cdef char* null_src
    cdef Py_ssize_t null_len
    cdef uint8_t* bitmap
    cdef int32_t* offsets = ptr.offsets
    cdef Py_ssize_t expected_offsets_len = (row_count + 1) * sizeof(int32_t)

    if PyBytes_AsStringAndSize(offsets_payload, &offs_src, &offs_len) != 0:
        raise ValueError("invalid offsets payload")
    if offs_len != expected_offsets_len:
        raise DrakenMorselCorruptionError(
            f"offset payload length mismatch: expected {expected_offsets_len}, got {offs_len}"
        )

    if PyBytes_AsStringAndSize(values_payload, &values_src, &values_len) != 0:
        raise ValueError("invalid values payload")

    if values_len > 0:
        memcpy(ptr.data, <const void*>values_src, <size_t>values_len)
    if offs_len > 0:
        memcpy(ptr.offsets, <const void*>offs_src, <size_t>offs_len)

    if row_count > 0 and offsets[row_count] != values_len:
        raise DrakenMorselCorruptionError(
            f"offset tail mismatch: expected {values_len}, got {offsets[row_count]}"
        )

    if null_payload and len(null_payload) > 0:
        if PyBytes_AsStringAndSize(null_payload, &null_src, &null_len) != 0:
            raise ValueError("invalid null bitmap payload")
        if null_len != (row_count + 7) >> 3:
            raise DrakenMorselCorruptionError(
                f"null bitmap length mismatch: expected {(row_count + 7) >> 3}, got {null_len}"
            )
        bitmap = <uint8_t*>malloc(<size_t>null_len)
        if bitmap == NULL:
            raise MemoryError()
        memcpy(bitmap, <const void*>null_src, <size_t>null_len)
        ptr.null_bitmap = bitmap
    else:
        ptr.null_bitmap = NULL

    return out


def _open_reader(path_or_handle):
    if hasattr(path_or_handle, "read"):
        return path_or_handle, False, None
    path = os.fspath(path_or_handle)
    return open(path, "rb"), True, path


def _open_writer(path_or_handle):
    if hasattr(path_or_handle, "write"):
        return path_or_handle, False, None
    path = os.fspath(path_or_handle)
    return open(path, "wb"), True, path


cpdef object write_morsel(object path_or_handle, Morsel morsel, dict options=None):
    cdef dict opts = options or {}
    cdef str codec_name = str(opts.get("codec_default", "lz4")).lower()
    cdef int codec_id = _codec_name_to_id(codec_name)
    cdef int zstd_level = int(opts.get("zstd_level", 1))
    cdef bint checksum_enabled = bool(opts.get("checksum_enabled", True))
    cdef int column_count = morsel.num_columns
    cdef Py_ssize_t row_count = morsel.num_rows
    cdef object handle
    cdef bint close_when_done
    cdef object resolved_path
    cdef int i
    cdef int block_cursor = 0
    cdef list column_plans = []
    cdef list block_dir = []
    cdef list column_names = morsel.column_names
    cdef object name_obj
    cdef bytes name_bytes
    cdef int dtype
    cdef int encoding
    cdef int flags
    cdef list segments
    cdef DrakenFixedBuffer* fixed_ptr
    cdef DrakenVarBuffer* var_ptr
    cdef Py_ssize_t data_len
    cdef Py_ssize_t null_len
    cdef Py_ssize_t values_len
    cdef int block_start
    cdef int block_end
    cdef bytes schema_blob = b""
    cdef uint64_t schema_fingerprint = 0
    cdef int block_count = 0
    cdef int col_idx
    cdef int block_id
    cdef int seg_kind
    cdef intptr_t ptr_value
    cdef Py_ssize_t seg_len
    cdef bytes payload
    cdef bytes compressed
    cdef uint32_t checksum
    cdef int64_t block_offset
    cdef bytes header_blob
    cdef bytes dir_blob
    cdef int64_t dir_offset
    cdef uint32_t footer_checksum = 0
    cdef bytearray dir_buffer = bytearray()
    cdef uint64_t raw_written = 0
    cdef uint64_t compressed_written = 0
    cdef uint64_t blocks_written = 0

    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        if not lz4.is_available():
            raise DrakenMorselStorageError("codec_default='lz4' requested but liblz4 is unavailable")

    if codec_id == CODEC_ZSTD:
        zstd = _load_zstd()
        if getattr(zstd, "compress", None) is None:
            raise DrakenMorselStorageError("codec_default='zstd' requested but zstd compression is unavailable")

    for i in range(column_count):
        name_obj = column_names[i]
        if isinstance(name_obj, bytes):
            name_bytes = name_obj
        else:
            name_bytes = str(name_obj).encode("utf-8")

        dtype = <int>morsel.ptr.column_types[i]
        segments = []

        if dtype == DRAKEN_STRING:
            encoding = ENCODING_VAR
            var_ptr = (<StringVector>(<Vector>morsel.ptr.columns[i])).ptr
            null_len = ((row_count + 7) >> 3) if var_ptr.null_bitmap != NULL else 0
            if null_len > 0:
                segments.append((SEG_NULL, <intptr_t>var_ptr.null_bitmap, null_len))
            segments.append((SEG_OFFSETS, <intptr_t>var_ptr.offsets, (row_count + 1) * sizeof(int32_t)))
            values_len = 0
            if row_count > 0:
                values_len = <Py_ssize_t>var_ptr.offsets[row_count]
            segments.append((SEG_VALUES, <intptr_t>var_ptr.data, values_len))
        elif _is_supported_fixed_type(dtype):
            encoding = ENCODING_FIXED
            fixed_ptr = _fixed_ptr_from_vector(<Vector>morsel.ptr.columns[i], dtype)
            if fixed_ptr == NULL:
                raise DrakenMorselStorageError(f"unsupported vector for dtype {dtype}")
            null_len = ((row_count + 7) >> 3) if fixed_ptr.null_bitmap != NULL else 0
            if null_len > 0:
                segments.append((SEG_NULL, <intptr_t>fixed_ptr.null_bitmap, null_len))
            if dtype == DRAKEN_BOOL:
                data_len = (row_count + 7) >> 3
            else:
                data_len = row_count * <Py_ssize_t>fixed_ptr.itemsize
            segments.append((SEG_DATA, <intptr_t>fixed_ptr.data, data_len))
        else:
            raise DrakenMorselStorageError(
                f"dtype {dtype} is not yet supported by DRKM v1 serializer"
            )

        flags = FLAG_HAS_NULLS if segments and segments[0][0] == SEG_NULL else 0
        block_start = block_cursor
        block_cursor += len(segments)
        block_end = block_cursor
        column_plans.append((name_bytes, dtype, encoding, flags, block_start, block_end, segments))

        schema_blob += name_bytes
        schema_blob += struct.pack("<H", dtype)

    block_count = block_cursor
    if schema_blob:
        schema_fingerprint = hash_bytes(schema_blob)

    handle, close_when_done, resolved_path = _open_writer(path_or_handle)
    try:
        handle.write(
            struct.pack(
                HEADER_FMT,
                MAGIC,
                FORMAT_VERSION,
                0,
                row_count,
                column_count,
                block_count,
                schema_fingerprint,
                codec_id,
            )
        )

        for (name_bytes, dtype, encoding, flags, block_start, block_end, segments) in column_plans:
            handle.write(
                struct.pack(
                    COLUMN_ENTRY_FMT,
                    len(name_bytes),
                    dtype,
                    encoding,
                    flags,
                    block_start,
                    block_end,
                )
            )
            handle.write(name_bytes)

        for col_idx, (_, _, _, _, _, _, segments) in enumerate(column_plans):
            for (seg_kind, ptr_value, seg_len) in segments:
                block_id = len(block_dir)
                payload = _bytes_from_pointer(<const void*>ptr_value, seg_len)
                checksum = _checksum32(payload) if checksum_enabled else 0
                compressed = _compress_payload(payload, codec_id, zstd_level)
                block_offset = handle.tell()
                header_blob = struct.pack(
                    BLOCK_HEADER_FMT,
                    block_id,
                    col_idx,
                    seg_kind,
                    codec_id,
                    0,
                    row_count,
                    0,
                    len(payload),
                    len(compressed),
                    checksum,
                )
                handle.write(header_blob)
                handle.write(compressed)
                block_dir.append(
                    (block_id, col_idx, seg_kind, block_offset, len(header_blob) + len(compressed))
                )
                raw_written += <uint64_t>len(payload)
                compressed_written += <uint64_t>len(compressed)
                blocks_written += 1

        dir_offset = handle.tell()
        for (block_id, col_idx, seg_kind, block_offset, total_len) in block_dir:
            dir_buffer.extend(
                struct.pack(
                    BLOCK_DIR_ENTRY_FMT,
                    block_id,
                    col_idx,
                    seg_kind,
                    block_offset,
                    total_len,
                )
            )

        dir_blob = bytes(dir_buffer)
        handle.write(dir_blob)
        footer_checksum = _checksum32(dir_blob) if checksum_enabled else 0
        handle.write(
            struct.pack(
                FOOTER_FMT,
                dir_offset,
                len(dir_blob),
                footer_checksum,
                block_count,
                FOOTER_MAGIC,
            )
        )
    finally:
        if close_when_done:
            handle.close()

    return {
        "path": resolved_path,
        "rows": int(row_count),
        "columns": int(column_count),
        "blocks": int(block_count),
        "bytes_raw_written": int(raw_written),
        "bytes_compressed_written": int(compressed_written),
        "codec_default": _codec_id_to_name(codec_id),
    }


cpdef Morsel read_morsel(object path_or_handle, dict options=None):
    cdef dict opts = options or {}
    cdef bint checksum_enabled = bool(opts.get("checksum_enabled", True))
    cdef object handle
    cdef bint close_when_done
    cdef object resolved_path
    cdef bytes header_bytes
    cdef bytes footer_bytes
    cdef bytes dir_blob
    cdef bytes block_header
    cdef bytes compressed
    cdef bytes payload
    cdef bytes name_bytes
    cdef int64_t file_size
    cdef int64_t dir_offset
    cdef int dir_length
    cdef uint32_t expected_footer_checksum
    cdef uint32_t actual_footer_checksum
    cdef int block_count
    cdef int footer_block_count
    cdef bytes footer_magic
    cdef bytes magic
    cdef int version
    cdef int flags
    cdef uint64_t row_count
    cdef int column_count
    cdef uint64_t schema_fingerprint
    cdef int default_codec
    cdef list columns = []
    cdef int i
    cdef tuple col_meta
    cdef int name_len
    cdef int dtype
    cdef int encoding
    cdef int col_flags
    cdef int block_start
    cdef int block_end
    cdef int block_id
    cdef int col_idx
    cdef int seg_kind
    cdef int codec_id
    cdef uint64_t row_start
    cdef uint32_t chunk_rows
    cdef int block_flags
    cdef int raw_len
    cdef int comp_len
    cdef uint32_t checksum
    cdef int64_t block_offset
    cdef int block_total_len
    cdef list block_entries = []
    cdef list segment_store
    cdef object seg_map
    cdef list vector_names = []
    cdef list vectors = []
    cdef bytes null_payload
    cdef bytes data_payload
    cdef bytes offsets_payload
    cdef bytes values_payload
    cdef Vector vec

    handle, close_when_done, resolved_path = _open_reader(path_or_handle)
    try:
        if not hasattr(handle, "seek"):
            handle = io.BytesIO(handle.read())
            close_when_done = True

        header_bytes = _read_exact(handle, HEADER_SIZE)
        (
            magic,
            version,
            flags,
            row_count,
            column_count,
            block_count,
            schema_fingerprint,
            default_codec,
        ) = struct.unpack(HEADER_FMT, header_bytes)

        if magic != MAGIC:
            raise DrakenMorselCorruptionError(f"invalid DRKM magic: {magic!r}")
        if version != FORMAT_VERSION:
            raise DrakenMorselCorruptionError(
                f"unsupported DRKM version {version}, expected {FORMAT_VERSION}"
            )

        for i in range(column_count):
            col_meta = struct.unpack(COLUMN_ENTRY_FMT, _read_exact(handle, COLUMN_ENTRY_SIZE))
            name_len = col_meta[0]
            dtype = col_meta[1]
            encoding = col_meta[2]
            col_flags = col_meta[3]
            block_start = col_meta[4]
            block_end = col_meta[5]
            name_bytes = _read_exact(handle, name_len)
            columns.append((name_bytes, dtype, encoding, col_flags, block_start, block_end))

        handle.seek(0, os.SEEK_END)
        file_size = handle.tell()
        if file_size < FOOTER_SIZE:
            raise DrakenMorselCorruptionError("file too small to contain DRKM footer")

        handle.seek(file_size - FOOTER_SIZE, os.SEEK_SET)
        footer_bytes = _read_exact(handle, FOOTER_SIZE)
        (dir_offset, dir_length, expected_footer_checksum, footer_block_count, footer_magic) = struct.unpack(
            FOOTER_FMT, footer_bytes
        )
        if footer_magic != FOOTER_MAGIC:
            raise DrakenMorselCorruptionError("invalid DRKM footer magic")
        if footer_block_count != block_count:
            raise DrakenMorselCorruptionError(
                f"block count mismatch: header={block_count}, footer={footer_block_count}"
            )

        handle.seek(dir_offset, os.SEEK_SET)
        dir_blob = _read_exact(handle, dir_length)
        if checksum_enabled and expected_footer_checksum:
            actual_footer_checksum = _checksum32(dir_blob)
            if actual_footer_checksum != expected_footer_checksum:
                raise DrakenMorselCorruptionError(
                    f"block directory checksum mismatch: expected {expected_footer_checksum}, got {actual_footer_checksum}"
                )

        if len(dir_blob) % BLOCK_DIR_ENTRY_SIZE != 0:
            raise DrakenMorselCorruptionError("block directory length is not aligned")

        for i in range(len(dir_blob) // BLOCK_DIR_ENTRY_SIZE):
            block_entries.append(
                struct.unpack(
                    BLOCK_DIR_ENTRY_FMT,
                    dir_blob[i * BLOCK_DIR_ENTRY_SIZE : (i + 1) * BLOCK_DIR_ENTRY_SIZE],
                )
            )

        block_entries.sort()
        segment_store = []
        for i in range(column_count):
            segment_store.append({})

        for (block_id, col_idx, seg_kind, block_offset, block_total_len) in block_entries:
            if col_idx < 0 or col_idx >= column_count:
                raise DrakenMorselCorruptionError(f"invalid column id in block directory: {col_idx}")
            handle.seek(block_offset, os.SEEK_SET)
            block_header = _read_exact(handle, BLOCK_HEADER_SIZE)
            (
                block_id,
                col_idx,
                seg_kind,
                codec_id,
                row_start,
                chunk_rows,
                block_flags,
                raw_len,
                comp_len,
                checksum,
            ) = struct.unpack(BLOCK_HEADER_FMT, block_header)

            compressed = _read_exact(handle, comp_len)
            payload = _decompress_payload(compressed, codec_id, raw_len)
            if len(payload) != raw_len:
                raise DrakenMorselCorruptionError(
                    f"decompressed length mismatch for block {block_id}: expected {raw_len}, got {len(payload)}"
                )
            if checksum_enabled and checksum:
                if _checksum32(payload) != checksum:
                    raise DrakenMorselCorruptionError(
                        f"checksum mismatch for block {block_id}, column {col_idx}"
                    )
            seg_map = segment_store[col_idx]
            if seg_kind in seg_map:
                raise DrakenMorselCorruptionError(
                    f"duplicate segment kind {seg_kind} for column {col_idx}"
                )
            seg_map[seg_kind] = payload

        for i, (name_bytes, dtype, encoding, col_flags, block_start, block_end) in enumerate(columns):
            seg_map = segment_store[i]
            vector_names.append(name_bytes.decode("utf-8"))
            null_payload = seg_map.get(SEG_NULL, b"")

            if encoding == ENCODING_FIXED:
                data_payload = seg_map.get(SEG_DATA, None)
                if data_payload is None:
                    raise DrakenMorselCorruptionError(f"missing fixed data segment for column {i}")
                vec = _build_fixed_vector(dtype, row_count, data_payload, null_payload)
            elif encoding == ENCODING_VAR:
                offsets_payload = seg_map.get(SEG_OFFSETS, None)
                values_payload = seg_map.get(SEG_VALUES, None)
                if offsets_payload is None or values_payload is None:
                    raise DrakenMorselCorruptionError(f"missing var-width segments for column {i}")
                if dtype != DRAKEN_STRING:
                    raise DrakenMorselStorageError(
                        f"unsupported var-width dtype {dtype} in DRKM v1 reader"
                    )
                vec = _build_string_vector(row_count, offsets_payload, values_payload, null_payload)
            else:
                raise DrakenMorselCorruptionError(f"invalid encoding kind {encoding} for column {i}")

            vectors.append(vec)

        return Morsel.from_vectors(vector_names, vectors)
    finally:
        if close_when_done:
            handle.close()
