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

from cpython.buffer cimport PyBUF_READ
from cpython.buffer cimport PyBUF_SIMPLE
from cpython.buffer cimport PyBUF_WRITABLE
from cpython.buffer cimport PyBUF_WRITE
from cpython.buffer cimport Py_buffer
from cpython.buffer cimport PyBuffer_Release
from cpython.buffer cimport PyObject_GetBuffer
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_FromMemory
from array import array as pyarray
from libc.stddef cimport size_t
from libc.stdint cimport int8_t
from libc.stdint cimport int16_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy
from libc.string cimport memset

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenConstantBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.compiled.draken.core.buffers cimport DrakenType
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport ConstAccessor
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DICTIONARY
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT8
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT16
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INTERVAL
from opteryx.compiled.draken.core.buffers cimport DRAKEN_STRING
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport from_packed_dict as float64_from_packed_dict
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport from_packed_dict as int64_from_packed_dict
from opteryx.compiled.draken.vectors.integer_vector cimport IntegerVector
from opteryx.compiled.draken.vectors.interval_vector cimport IntervalVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVectorBuilder
from opteryx.compiled.draken.vectors.string_vector cimport from_dict_buffers as string_from_dict_buffers
from opteryx.compiled.draken.vectors.time_vector cimport TimeVector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64
from opteryx.third_party.cyan4973.xxhash cimport hash_bytes

import io
import os
import struct
import threading


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
ENCODING_DICT = 2
ENCODING_CONST = 3

SEG_NULL = 0
SEG_DATA = 1
SEG_OFFSETS = 2
SEG_VALUES = 3
SEG_CODES = 4
SEG_DICT_NULL = 5
SEG_DICT_OFFSETS = 6
SEG_DICT_VALUES = 7
SEG_CONST_VALUE = 8

FLAG_HAS_NULLS = 1
FLAG_DICT_ORDERED = 1 << 1
FLAG_DICT_HAS_DICT_NULLS = 1 << 2
FLAG_DICT_CODE_WIDTH_SHIFT = 3
FLAG_DICT_VALUE_TYPE_SHIFT = 5
FLAG_CONST_VALUE_TYPE_SHIFT = 3


class DrakenMorselStorageError(RuntimeError):
    """Base DRKM storage I/O error."""


class DrakenMorselCorruptionError(DrakenMorselStorageError):
    """Raised when DRKM payload validation fails."""


_codec_tls = threading.local()


cdef class _PoolWriter:
    """
    Zero-copy writable sink backed by a caller-supplied memoryview.

    All writes go directly into the underlying buffer via memcpy with no
    intermediate allocation.  The Py_buffer view is released only on
    deallocation so the data remains readable after close().
    """
    cdef Py_buffer _view
    cdef char* _ptr
    cdef Py_ssize_t _capacity
    cdef Py_ssize_t _pos
    cdef bint _acquired

    def __cinit__(self, object mv):
        self._acquired = False
        self._pos = 0
        self._ptr = NULL
        self._capacity = 0
        PyObject_GetBuffer(mv, &self._view, PyBUF_WRITABLE)
        self._acquired = True
        self._ptr = <char*>self._view.buf
        self._capacity = self._view.len

    def write(self, object data):
        cdef Py_buffer view
        cdef Py_ssize_t n
        PyObject_GetBuffer(data, &view, PyBUF_SIMPLE)
        n = view.len
        if self._pos + n > self._capacity:
            PyBuffer_Release(&view)
            raise ValueError(
                f"memoryview target too small: buffer capacity {self._capacity} bytes, "
                f"need at least {self._pos + n} bytes"
            )
        memcpy(self._ptr + self._pos, view.buf, n)
        self._pos += n
        PyBuffer_Release(&view)
        return n

    def tell(self):
        return self._pos

    def close(self):
        pass  # data must remain readable after close; buffer released only on dealloc

    def __dealloc__(self):
        if self._acquired:
            PyBuffer_Release(&self._view)
            self._acquired = False

    @property
    def bytes_written(self):
        return self._pos

# Module-level cache for codec modules and function pointers (set on first load, never changes)
_lz4_module = None
_zstd_module = None
_lz4_decompress_into_fn = None
_zstd_decompress_into_fn = None
_lz4_loaded = False
_zstd_loaded = False


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
    global _lz4_module, _lz4_decompress_into_fn, _lz4_loaded
    if _lz4_loaded:
        return _lz4_module
    _lz4_loaded = True

    try:
        from opteryx.third_party.lz4 import lz4 as lz4_mod
        _lz4_decompress_into_fn = getattr(lz4_mod, "decompress_into", None)
        _lz4_module = lz4_mod
    except ImportError:
        _lz4_decompress_into_fn = None
        _lz4_module = None
    return _lz4_module


def _load_zstd():
    global _zstd_module, _zstd_decompress_into_fn, _zstd_loaded
    if _zstd_loaded:
        return _zstd_module
    _zstd_loaded = True

    try:
        from opteryx.third_party.facebook import zstd as zstd_mod
        _zstd_decompress_into_fn = getattr(zstd_mod, "decompress_into", None)
        _zstd_module = zstd_mod
    except ImportError:
        _zstd_decompress_into_fn = None
        _zstd_module = None
    return _zstd_module


def _compress_payload(object payload, int codec_id, int zstd_level):
    if codec_id == CODEC_NONE:
        return payload
    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        if lz4 is None:
            raise DrakenMorselStorageError("codec_default='lz4' requested but liblz4 is unavailable")
        return lz4.compress_block(payload)
    if codec_id == CODEC_ZSTD:
        zstd_compress = getattr(_codec_tls, "zstd_compress", None)
        if zstd_compress is None:
            zstd = _load_zstd()
            zstd_compress = getattr(zstd, "compress", None)
            _codec_tls.zstd_compress = zstd_compress
        if zstd_compress is None:
            raise DrakenMorselStorageError("zstd compression is unavailable in this build")
        try:
            return zstd_compress(payload, level=zstd_level)
        except TypeError:
            try:
                return zstd_compress(payload)
            except TypeError:
                return zstd_compress(bytes(payload))
    raise DrakenMorselStorageError(f"unsupported codec id {codec_id}")


def _decompress_payload(bytes payload, int codec_id, int expected_len):
    if codec_id == CODEC_NONE:
        return payload
    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        if lz4 is None:
            raise DrakenMorselStorageError("lz4 decompression requested but liblz4 is unavailable")
        return lz4.decompress_block(payload, expected_len)
    if codec_id == CODEC_ZSTD:
        zstd = _load_zstd()
        if zstd is None:
            raise DrakenMorselStorageError("zstd decompression requested but zstd is unavailable")
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


cdef inline bint _all_bytes_ff(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    cdef Py_ssize_t i
    cdef Py_ssize_t n = len(payload)
    for i in range(n):
        if ptr[i] != 0xFF:
            return False
    return True


cdef inline uint32_t _checksum32_ptr(const void* ptr, Py_ssize_t length):
    cdef uint8_t zero = 0
    if length <= 0:
        return <uint32_t>(cy_xxhash3_64(<const void*>&zero, 0) & 0xFFFFFFFF)
    if ptr == NULL:
        raise DrakenMorselStorageError("attempted to checksum non-empty payload from NULL pointer")
    return <uint32_t>(cy_xxhash3_64(ptr, <size_t>length) & 0xFFFFFFFF)


cdef inline void _write_ptr_payload(object handle, const void* ptr, Py_ssize_t length):
    cdef object mv
    if length <= 0:
        return
    if ptr == NULL:
        raise DrakenMorselStorageError("attempted to write non-empty payload from NULL pointer")
    mv = PyMemoryView_FromMemory(<char*>ptr, length, PyBUF_READ)
    handle.write(mv)


cdef inline void _decompress_into_ptr(bytes compressed, int codec_id, int expected_len, void* dst):
    cdef object decompress_into_fn
    cdef object dst_view
    cdef bytes payload
    cdef char* src
    cdef Py_ssize_t src_len
    if expected_len <= 0:
        return
    if dst == NULL:
        raise DrakenMorselStorageError("cannot decode into NULL destination pointer")

    if codec_id == CODEC_NONE:
        if PyBytes_AsStringAndSize(compressed, &src, &src_len) != 0:
            raise ValueError("invalid raw segment payload")
        if src_len != expected_len:
            raise DrakenMorselCorruptionError(
                f"raw segment length mismatch: expected {expected_len}, got {src_len}"
            )
        memcpy(dst, <const void*>src, <size_t>expected_len)
        return

    if codec_id == CODEC_LZ4:
        # Load codec module on first use (caches function pointer in module-level variable)
        if not _lz4_loaded:
            _load_lz4()
        # Use pre-cached function pointer (no getattr in hot path)
        if _lz4_decompress_into_fn is not None:
            dst_view = PyMemoryView_FromMemory(<char*>dst, expected_len, PyBUF_WRITE)
            _lz4_decompress_into_fn(compressed, dst_view, expected_len)
            return
        # Fallback: decompress to bytes, then copy
        payload = _decompress_payload(compressed, codec_id, expected_len)
        if len(payload) != expected_len:
            raise DrakenMorselCorruptionError(
                f"decompressed length mismatch: expected {expected_len}, got {len(payload)}"
            )
        if expected_len > 0:
            memcpy(dst, <const void*>PyBytes_AS_STRING(payload), <size_t>expected_len)
        return

    if codec_id == CODEC_ZSTD:
        # Load codec module on first use (caches function pointer in module-level variable)
        if not _zstd_loaded:
            _load_zstd()
        # Use pre-cached function pointer (no getattr in hot path)
        if _zstd_decompress_into_fn is not None:
            dst_view = PyMemoryView_FromMemory(<char*>dst, expected_len, PyBUF_WRITE)
            _zstd_decompress_into_fn(compressed, dst_view, expected_len)
            return
        # Fallback: decompress to bytes, then copy
        payload = _decompress_payload(compressed, codec_id, expected_len)
        if len(payload) != expected_len:
            raise DrakenMorselCorruptionError(
                f"decompressed length mismatch: expected {expected_len}, got {len(payload)}"
            )
        if expected_len > 0:
            memcpy(dst, <const void*>PyBytes_AS_STRING(payload), <size_t>expected_len)
        return

    raise DrakenMorselStorageError(f"unsupported codec id {codec_id}")


def _read_exact(handle, int size):
    payload = handle.read(size)
    if payload is None or len(payload) != size:
        raise DrakenMorselCorruptionError(f"unexpected EOF while reading {size} bytes")
    return payload


cdef inline uint16_t _u16_le_read(const uint8_t* ptr):
    return (<uint16_t>ptr[0]) | ((<uint16_t>ptr[1]) << 8)


cdef inline uint32_t _u32_le_read(const uint8_t* ptr):
    return (
        (<uint32_t>ptr[0])
        | ((<uint32_t>ptr[1]) << 8)
        | ((<uint32_t>ptr[2]) << 16)
        | ((<uint32_t>ptr[3]) << 24)
    )


cdef inline uint64_t _u64_le_read(const uint8_t* ptr):
    return (
        (<uint64_t>ptr[0])
        | ((<uint64_t>ptr[1]) << 8)
        | ((<uint64_t>ptr[2]) << 16)
        | ((<uint64_t>ptr[3]) << 24)
        | ((<uint64_t>ptr[4]) << 32)
        | ((<uint64_t>ptr[5]) << 40)
        | ((<uint64_t>ptr[6]) << 48)
        | ((<uint64_t>ptr[7]) << 56)
    )


cdef inline void _u16_le_write(uint8_t* ptr, uint16_t value):
    ptr[0] = <uint8_t>(value & 0xFF)
    ptr[1] = <uint8_t>((value >> 8) & 0xFF)


cdef inline void _u32_le_write(uint8_t* ptr, uint32_t value):
    ptr[0] = <uint8_t>(value & 0xFF)
    ptr[1] = <uint8_t>((value >> 8) & 0xFF)
    ptr[2] = <uint8_t>((value >> 16) & 0xFF)
    ptr[3] = <uint8_t>((value >> 24) & 0xFF)


cdef inline void _u64_le_write(uint8_t* ptr, uint64_t value):
    ptr[0] = <uint8_t>(value & 0xFF)
    ptr[1] = <uint8_t>((value >> 8) & 0xFF)
    ptr[2] = <uint8_t>((value >> 16) & 0xFF)
    ptr[3] = <uint8_t>((value >> 24) & 0xFF)
    ptr[4] = <uint8_t>((value >> 32) & 0xFF)
    ptr[5] = <uint8_t>((value >> 40) & 0xFF)
    ptr[6] = <uint8_t>((value >> 48) & 0xFF)
    ptr[7] = <uint8_t>((value >> 56) & 0xFF)


cdef inline bytes _encode_header(
    uint64_t row_count,
    uint32_t column_count,
    uint32_t block_count,
    uint64_t schema_fingerprint,
    uint8_t default_codec,
):
    cdef bytes out = PyBytes_FromStringAndSize(NULL, HEADER_SIZE)
    cdef uint8_t* ptr = <uint8_t*>PyBytes_AS_STRING(out)
    memcpy(ptr, <const void*>PyBytes_AS_STRING(MAGIC), 4)
    _u16_le_write(ptr + 4, <uint16_t>FORMAT_VERSION)
    _u16_le_write(ptr + 6, 0)
    _u64_le_write(ptr + 8, row_count)
    _u32_le_write(ptr + 16, column_count)
    _u32_le_write(ptr + 20, block_count)
    _u64_le_write(ptr + 24, schema_fingerprint)
    ptr[32] = default_codec
    memset(ptr + 33, 0, 7)
    return out


cdef inline bytes _encode_column_entry(
    uint16_t name_len,
    uint16_t dtype,
    uint8_t encoding,
    uint8_t flags,
    uint32_t block_start,
    uint32_t block_end,
):
    cdef bytes out = PyBytes_FromStringAndSize(NULL, COLUMN_ENTRY_SIZE)
    cdef uint8_t* ptr = <uint8_t*>PyBytes_AS_STRING(out)
    _u16_le_write(ptr, name_len)
    _u16_le_write(ptr + 2, dtype)
    ptr[4] = encoding
    ptr[5] = flags
    _u32_le_write(ptr + 6, block_start)
    _u32_le_write(ptr + 10, block_end)
    return out


cdef inline bytes _encode_block_header(
    uint32_t block_id,
    uint32_t column_id,
    uint8_t segment_kind,
    uint8_t codec,
    uint64_t row_start,
    uint32_t row_count,
    uint8_t flags,
    uint32_t raw_len,
    uint32_t comp_len,
    uint32_t checksum,
):
    cdef bytes out = PyBytes_FromStringAndSize(NULL, BLOCK_HEADER_SIZE)
    cdef uint8_t* ptr = <uint8_t*>PyBytes_AS_STRING(out)
    _u32_le_write(ptr, block_id)
    _u32_le_write(ptr + 4, column_id)
    ptr[8] = segment_kind
    ptr[9] = codec
    _u64_le_write(ptr + 10, row_start)
    _u32_le_write(ptr + 18, row_count)
    ptr[22] = flags
    _u32_le_write(ptr + 23, raw_len)
    _u32_le_write(ptr + 27, comp_len)
    _u32_le_write(ptr + 31, checksum)
    ptr[35] = 0
    return out


cdef inline bytes _encode_block_dir_entry(
    uint32_t block_id,
    uint32_t column_id,
    uint8_t segment_kind,
    uint64_t offset,
    uint32_t total_len,
):
    cdef bytes out = PyBytes_FromStringAndSize(NULL, BLOCK_DIR_ENTRY_SIZE)
    cdef uint8_t* ptr = <uint8_t*>PyBytes_AS_STRING(out)
    _u32_le_write(ptr, block_id)
    _u32_le_write(ptr + 4, column_id)
    ptr[8] = segment_kind
    ptr[9] = 0
    ptr[10] = 0
    ptr[11] = 0
    _u64_le_write(ptr + 12, offset)
    _u32_le_write(ptr + 20, total_len)
    return out


cdef inline bytes _encode_footer(
    uint64_t dir_offset,
    uint32_t dir_len,
    uint32_t footer_checksum,
    uint32_t block_count,
):
    cdef bytes out = PyBytes_FromStringAndSize(NULL, FOOTER_SIZE)
    cdef uint8_t* ptr = <uint8_t*>PyBytes_AS_STRING(out)
    _u64_le_write(ptr, dir_offset)
    _u32_le_write(ptr + 8, dir_len)
    _u32_le_write(ptr + 12, footer_checksum)
    _u32_le_write(ptr + 16, block_count)
    memcpy(ptr + 20, <const void*>PyBytes_AS_STRING(FOOTER_MAGIC), 4)
    return out


cdef inline tuple _decode_column_entry(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    return (
        <int>_u16_le_read(ptr),
        <int>_u16_le_read(ptr + 2),
        <int>ptr[4],
        <int>ptr[5],
        <int>_u32_le_read(ptr + 6),
        <int>_u32_le_read(ptr + 10),
    )


cdef inline tuple _decode_header(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    return (
        payload[:4],
        <int>_u16_le_read(ptr + 4),
        <int>_u16_le_read(ptr + 6),
        _u64_le_read(ptr + 8),
        <int>_u32_le_read(ptr + 16),
        <int>_u32_le_read(ptr + 20),
        _u64_le_read(ptr + 24),
        <int>ptr[32],
    )


cdef inline tuple _decode_footer(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    return (
        <int64_t>_u64_le_read(ptr),
        <int>_u32_le_read(ptr + 8),
        <uint32_t>_u32_le_read(ptr + 12),
        <int>_u32_le_read(ptr + 16),
        payload[20:24],
    )


cdef inline tuple _decode_block_dir_entry(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    return (
        <int>_u32_le_read(ptr),
        <int>_u32_le_read(ptr + 4),
        <int>ptr[8],
        <int64_t>_u64_le_read(ptr + 12),
        <int>_u32_le_read(ptr + 20),
    )


cdef inline tuple _decode_block_header(bytes payload):
    cdef const uint8_t* ptr = <const uint8_t*>PyBytes_AS_STRING(payload)
    return (
        <int>_u32_le_read(ptr),
        <int>_u32_le_read(ptr + 4),
        <int>ptr[8],
        <int>ptr[9],
        _u64_le_read(ptr + 10),
        <uint32_t>_u32_le_read(ptr + 18),
        <int>ptr[22],
        <int>_u32_le_read(ptr + 23),
        <int>_u32_le_read(ptr + 27),
        <uint32_t>_u32_le_read(ptr + 31),
    )


cdef inline bint _is_supported_fixed_type(int dtype):
    return (
        dtype == DRAKEN_INT8
        or dtype == DRAKEN_INT16
        or dtype == DRAKEN_INT32
        or dtype == DRAKEN_INT64
        or dtype == DRAKEN_FLOAT64
        or dtype == DRAKEN_BOOL
        or dtype == DRAKEN_DATE32
        or dtype == DRAKEN_TIMESTAMP64
        or dtype == DRAKEN_TIME32
        or dtype == DRAKEN_TIME64
        or dtype == DRAKEN_INTERVAL
    )


cdef inline uint8_t _dict_code_width_from_flags(int flags):
    cdef uint8_t encoded = <uint8_t>((flags >> FLAG_DICT_CODE_WIDTH_SHIFT) & 0x3)
    if encoded == 0:
        return 1
    if encoded == 1:
        return 2
    if encoded == 2:
        return 4
    raise DrakenMorselCorruptionError(f"invalid dictionary code-width encoding in flags: {encoded}")


cdef inline int _dict_flags_from_code_width(uint8_t code_width):
    if code_width == 1:
        return 0 << FLAG_DICT_CODE_WIDTH_SHIFT
    if code_width == 2:
        return 1 << FLAG_DICT_CODE_WIDTH_SHIFT
    if code_width == 4:
        return 2 << FLAG_DICT_CODE_WIDTH_SHIFT
    raise DrakenMorselStorageError(f"unsupported dictionary code width {code_width}")


cdef inline int _dict_value_type_from_flags(int flags):
    cdef uint8_t encoded = <uint8_t>((flags >> FLAG_DICT_VALUE_TYPE_SHIFT) & 0x7)
    if encoded == 0:
        return DRAKEN_STRING
    if encoded == 1:
        return DRAKEN_INT8
    if encoded == 2:
        return DRAKEN_INT16
    if encoded == 3:
        return DRAKEN_INT32
    if encoded == 4:
        return DRAKEN_INT64
    if encoded == 5:
        return DRAKEN_FLOAT32
    if encoded == 6:
        return DRAKEN_FLOAT64
    if encoded == 7:
        return DRAKEN_BOOL
    raise DrakenMorselCorruptionError(f"invalid dictionary value-type encoding in flags: {encoded}")


cdef inline int _dict_flags_from_value_type(int dtype):
    if dtype == DRAKEN_STRING:
        return 0 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT8:
        return 1 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT16:
        return 2 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT32:
        return 3 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT64:
        return 4 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_FLOAT32:
        return 5 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_FLOAT64:
        return 6 << FLAG_DICT_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_BOOL:
        return 7 << FLAG_DICT_VALUE_TYPE_SHIFT
    raise DrakenMorselStorageError(f"unsupported dictionary value dtype {dtype}")


cdef inline int _const_value_type_from_flags(int flags):
    cdef uint8_t encoded = <uint8_t>((flags >> FLAG_CONST_VALUE_TYPE_SHIFT) & 0x1F)
    if encoded == 0:
        return DRAKEN_INT64
    if encoded == 1:
        return DRAKEN_FLOAT64
    if encoded == 2:
        return DRAKEN_BOOL
    if encoded == 3:
        return DRAKEN_STRING
    if encoded == 4:
        return DRAKEN_INT8
    if encoded == 5:
        return DRAKEN_INT16
    if encoded == 6:
        return DRAKEN_INT32
    if encoded == 7:
        return DRAKEN_DATE32
    if encoded == 8:
        return DRAKEN_TIME32
    if encoded == 9:
        return DRAKEN_TIME64
    if encoded == 10:
        return DRAKEN_TIMESTAMP64
    raise DrakenMorselCorruptionError(f"invalid constant value-type encoding in flags: {encoded}")


cdef inline int _const_flags_from_value_type(int dtype):
    if dtype == DRAKEN_INT64:
        return 0 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_FLOAT64:
        return 1 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_BOOL:
        return 2 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_STRING:
        return 3 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT8:
        return 4 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT16:
        return 5 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_INT32:
        return 6 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_DATE32:
        return 7 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_TIME32:
        return 8 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_TIME64:
        return 9 << FLAG_CONST_VALUE_TYPE_SHIFT
    if dtype == DRAKEN_TIMESTAMP64:
        return 10 << FLAG_CONST_VALUE_TYPE_SHIFT
    raise DrakenMorselStorageError(f"unsupported constant value dtype {dtype}")


cdef inline Py_ssize_t _const_value_length_for_type(int dtype):
    if dtype == DRAKEN_INT8:
        return sizeof(int8_t)
    if dtype == DRAKEN_INT16:
        return sizeof(int16_t)
    if dtype == DRAKEN_INT32 or dtype == DRAKEN_DATE32 or dtype == DRAKEN_TIME32:
        return sizeof(int32_t)
    if dtype == DRAKEN_INT64 or dtype == DRAKEN_TIME64 or dtype == DRAKEN_TIMESTAMP64:
        return sizeof(int64_t)
    if dtype == DRAKEN_FLOAT64:
        return sizeof(double)
    if dtype == DRAKEN_BOOL:
        return sizeof(uint8_t)
    raise DrakenMorselStorageError(f"unsupported constant value dtype {dtype}")


cdef inline Vector _build_typed_const_vector(
    int dtype,
    int const_value_type,
    object scalar_value,
    Py_ssize_t row_count,
    bint is_null,
):
    cdef Vector vec
    cdef IntegerVector int_vec
    cdef size_t itemsize

    if dtype == DRAKEN_INT64:
        return <Vector>Int64Vector.from_constant(scalar_value, row_count, is_null=is_null)
    if dtype == DRAKEN_FLOAT64:
        return <Vector>Float64Vector.from_constant(scalar_value, row_count, is_null=is_null)
    if dtype == DRAKEN_BOOL:
        return <Vector>BoolVector.from_constant(scalar_value, row_count, is_null=is_null)
    if dtype == DRAKEN_STRING:
        return <Vector>StringVector.from_constant(scalar_value, row_count, is_null=is_null)
    if dtype == DRAKEN_DATE32:
        return <Vector>Date32Vector.from_constant(scalar_value, row_count, is_null=is_null)
    if dtype == DRAKEN_TIMESTAMP64:
        return <Vector>TimestampVector.from_constant(
            scalar_value,
            row_count,
            is_null=is_null,
            timestamp_unit="us",
        )
    if dtype == DRAKEN_TIME32:
        return <Vector>TimeVector.from_constant(
            scalar_value,
            row_count,
            is_null=is_null,
            is_time64=False,
        )
    if dtype == DRAKEN_TIME64:
        return <Vector>TimeVector.from_constant(
            scalar_value,
            row_count,
            is_null=is_null,
            is_time64=True,
        )
    if dtype == DRAKEN_INT8 or dtype == DRAKEN_INT16 or dtype == DRAKEN_INT32:
        int_vec = IntegerVector.from_constant(scalar_value, row_count, is_null=is_null)
        int_vec.ptr.type = <DrakenType>dtype
        if dtype == DRAKEN_INT8:
            itemsize = 1
        elif dtype == DRAKEN_INT16:
            itemsize = 2
        else:
            itemsize = 4
        int_vec.ptr.itemsize = itemsize
        return <Vector>int_vec
    raise DrakenMorselStorageError(
        f"unsupported typed constant restore for dtype {dtype} and constant value type {const_value_type}"
    )


cdef inline uint32_t _dict_read_code(DrakenDictionaryBuffer* ptr, Py_ssize_t row_idx):
    if ptr.code_width == 1:
        return (<uint8_t*>ptr.codes)[row_idx]
    if ptr.code_width == 2:
        return (<uint16_t*>ptr.codes)[row_idx]
    return (<uint32_t*>ptr.codes)[row_idx]


cdef inline uint32_t _dict_read_packed_code(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_idx,
):
    if code_width == 1:
        return (<const uint8_t*>codes)[row_idx]
    if code_width == 2:
        return (<const uint16_t*>codes)[row_idx]
    return (<const uint32_t*>codes)[row_idx]


cdef DrakenFixedBuffer* _fixed_ptr_from_vector(Vector vec, int dtype):
    if dtype == DRAKEN_INT8 or dtype == DRAKEN_INT16 or dtype == DRAKEN_INT32:
        return (<IntegerVector>vec).ptr
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


cdef inline Vector _build_typed_dict_vector(
    int dtype,
    Py_ssize_t row_count,
    const uint8_t* codes,
    uint8_t code_width,
    const uint8_t* row_null_bitmap,
    int dict_value_type,
    const void* dictionary,
    Py_ssize_t dict_size,
    bint ordered,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef object dictionary_obj = None
    cdef int64_t[::1] dictionary_buf_i64
    cdef double[::1] dictionary_buf_f64
    cdef Py_ssize_t i
    cdef const int8_t* src8
    cdef const int16_t* src16
    cdef const int32_t* src32
    cdef const float* srcf

    # Numeric dictionary vectors are stored in their native width (e.g. int32)
    # but are surfaced as int64/float64 in the engine. Convert on-read.
    if dtype == DRAKEN_INT64 and dict_value_type in (
        DRAKEN_INT8,
        DRAKEN_INT16,
        DRAKEN_INT32,
        DRAKEN_INT64,
    ):
        if dict_value_type == DRAKEN_INT64:
            return <Vector>int64_from_packed_dict(
                codes,
                code_width,
                row_count,
                <const int64_t*>dictionary,
                dict_size,
                row_null_bitmap,
                ordered,
                dict_entry_null_bitmap,
            )

        dictionary_obj = pyarray('q', [0]) * dict_size
        dictionary_buf_i64 = dictionary_obj
        if dict_value_type == DRAKEN_INT8:
            src8 = <const int8_t*>dictionary
            for i in range(dict_size):
                dictionary_buf_i64[i] = <int64_t>src8[i]
        elif dict_value_type == DRAKEN_INT16:
            src16 = <const int16_t*>dictionary
            for i in range(dict_size):
                dictionary_buf_i64[i] = <int64_t>src16[i]
        else:
            # DRAKEN_INT32
            src32 = <const int32_t*>dictionary
            for i in range(dict_size):
                dictionary_buf_i64[i] = <int64_t>src32[i]

        return <Vector>int64_from_packed_dict(
            codes,
            code_width,
            row_count,
            &dictionary_buf_i64[0],
            dict_size,
            row_null_bitmap,
            ordered,
            dict_entry_null_bitmap,
        )

    if dtype == DRAKEN_FLOAT64 and dict_value_type in (DRAKEN_FLOAT32, DRAKEN_FLOAT64):
        if dict_value_type == DRAKEN_FLOAT64:
            return <Vector>float64_from_packed_dict(
                codes,
                code_width,
                row_count,
                <const double*>dictionary,
                dict_size,
                row_null_bitmap,
                ordered,
                dict_entry_null_bitmap,
            )

        dictionary_obj = pyarray('d', [0.0]) * dict_size
        dictionary_buf_f64 = dictionary_obj
        srcf = <const float*>dictionary
        for i in range(dict_size):
            dictionary_buf_f64[i] = <double>srcf[i]

        return <Vector>float64_from_packed_dict(
            codes,
            code_width,
            row_count,
            &dictionary_buf_f64[0],
            dict_size,
            row_null_bitmap,
            ordered,
            dict_entry_null_bitmap,
        )

    raise DrakenMorselStorageError(
        f"unsupported typed dictionary restore for dtype {dtype} and dictionary value type {dict_value_type}"
    )


cdef Vector _build_dense_string_dict_vector(
    Py_ssize_t row_count,
    const uint8_t* codes,
    uint8_t code_width,
    bytes dict_offsets_bytes,
    bytes dict_values_bytes,
    bytes null_bytes,
    bytes dict_null_bytes,
):
    cdef const int32_t* dict_offsets
    cdef const uint8_t* arena_bytes
    cdef const uint8_t* row_null_bitmap = NULL
    cdef const uint8_t* dict_entry_null_bitmap = NULL
    cdef Py_ssize_t dict_len
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef int32_t start
    cdef int32_t end
    cdef Py_ssize_t total_bytes = 0
    cdef StringVectorBuilder builder

    dict_len = (len(dict_offsets_bytes) // sizeof(int32_t)) - 1
    if dict_len <= 0:
        raise DrakenMorselCorruptionError("dictionary string payload must contain at least one entry")

    dict_offsets = <const int32_t*>PyBytes_AS_STRING(dict_offsets_bytes)
    arena_bytes = <const uint8_t*>PyBytes_AS_STRING(dict_values_bytes) if len(dict_values_bytes) > 0 else NULL
    if null_bytes is not None and len(null_bytes) > 0:
        row_null_bitmap = <const uint8_t*>PyBytes_AS_STRING(null_bytes)
    if dict_null_bytes is not None and len(dict_null_bytes) > 0:
        dict_entry_null_bitmap = <const uint8_t*>PyBytes_AS_STRING(dict_null_bytes)

    for i in range(row_count):
        if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = _dict_read_packed_code(codes, code_width, i)
        if code >= dict_len:
            raise DrakenMorselCorruptionError(f"dictionary code out of bounds at row {i}: {code}")
        if dict_entry_null_bitmap != NULL and ((dict_entry_null_bitmap[code >> 3] >> (code & 7)) & 1) == 0:
            continue
        start = dict_offsets[code]
        end = dict_offsets[code + 1]
        if end < start:
            raise DrakenMorselCorruptionError("dictionary offsets are invalid")
        total_bytes += end - start

    builder = StringVectorBuilder.with_counts(row_count, total_bytes)
    for i in range(row_count):
        if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
            builder.append_null()
            continue
        code = _dict_read_packed_code(codes, code_width, i)
        if code >= dict_len:
            raise DrakenMorselCorruptionError(f"dictionary code out of bounds at row {i}: {code}")
        if dict_entry_null_bitmap != NULL and ((dict_entry_null_bitmap[code >> 3] >> (code & 7)) & 1) == 0:
            builder.append_null()
            continue
        start = dict_offsets[code]
        end = dict_offsets[code + 1]
        builder.append_bytes(<const char*>(arena_bytes + start), end - start)

    return <Vector>builder.finish()


cdef Vector _allocate_fixed_vector(int dtype, Py_ssize_t row_count):
    cdef Vector out

    if dtype == DRAKEN_INT8 or dtype == DRAKEN_INT16 or dtype == DRAKEN_INT32:
        out = IntegerVector(<DrakenType>dtype, <size_t>row_count)
    elif dtype == DRAKEN_INT64:
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

    return out


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

    out = _allocate_fixed_vector(dtype, row_count)

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


SINK_STREAM = 0
SINK_PATH = 1
SINK_RETURN_BYTES = 2
SINK_BYTEARRAY = 3
SINK_MEMORYVIEW = 4


def _open_reader(path_or_handle):
    """
    Open a reader for the given path_or_handle.

    Accepts:
      - file-like objects (has .read()) -> return as-is
      - bytes / bytearray -> return io.BytesIO(bytes(...))
      - memoryview / buffer-supporting objects -> return a MemoryViewStreamOptimized
        (zero-copy path) so callers can decode from a memoryview without copying.

    Returns:
      (handle, close_when_done, resolved_path)
    """
    if hasattr(path_or_handle, "read"):
        return path_or_handle, False, None

    # Bytes/bytearray: keep previous behaviour (BytesIO)
    if isinstance(path_or_handle, (bytes, bytearray)):
        return io.BytesIO(bytes(path_or_handle)), True, None

    # Prefer zero-copy for memoryview/buffer objects by returning MemoryViewStreamOptimized
    try:
        mv = memoryview(path_or_handle)
        try:
            # Import the optimized memoryview stream and return an instance that
            # presents a file-like interface without creating an intermediate bytes object.
            from opteryx.compiled.structures.memory_view_stream import MemoryViewStreamOptimized

            return MemoryViewStreamOptimized(mv), True, None
        except Exception:
            # Fall back to the previous safe behaviour if the optimized stream
            # is unavailable for any reason (keeps compatibility)
            return io.BytesIO(mv.tobytes()), True, None
    except TypeError:
        pass

    path = os.fspath(path_or_handle)
    return open(path, "rb"), True, path


def _open_writer(path_or_handle):
    if path_or_handle is None:
        return io.BytesIO(), True, None, SINK_RETURN_BYTES, None
    if hasattr(path_or_handle, "write"):
        return path_or_handle, False, None, SINK_STREAM, None
    if isinstance(path_or_handle, bytearray):
        return io.BytesIO(), True, None, SINK_BYTEARRAY, path_or_handle
    if isinstance(path_or_handle, memoryview):
        if path_or_handle.readonly:
            raise TypeError("memoryview target for write_morsel must be writable")
        return _PoolWriter(path_or_handle), True, None, SINK_MEMORYVIEW, None
    try:
        mv = memoryview(path_or_handle)
        if mv.readonly:
            raise TypeError("buffer target for write_morsel must be writable")
        return _PoolWriter(mv), True, None, SINK_MEMORYVIEW, None
    except TypeError:
        pass
    path = os.fspath(path_or_handle)
    return open(path, "wb"), True, path, SINK_PATH, None


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
    cdef int sink_kind
    cdef object sink_target
    cdef int i
    cdef int block_cursor = 0
    cdef list column_plans = []
    cdef list block_dir = []
    cdef list column_names = morsel.column_names
    cdef object name_obj
    cdef bytes name_bytes
    cdef int dtype
    cdef int encoding
    cdef int draken_encoding
    cdef int flags
    cdef list segments
    cdef DrakenFixedBuffer* fixed_ptr
    cdef DrakenVarBuffer* var_ptr
    cdef DrakenDictionaryBuffer* dict_ptr
    cdef DrakenConstantBuffer* const_ptr
    cdef DrakenConstantStringPayload* const_str
    cdef Py_ssize_t data_len
    cdef Py_ssize_t null_len
    cdef Py_ssize_t values_len
    cdef Py_ssize_t dict_len
    cdef Py_ssize_t dict_offsets_len
    cdef Py_ssize_t dict_codes_len
    cdef Py_ssize_t dict_null_len
    cdef Py_ssize_t const_value_len
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
    cdef object segment_payload
    cdef bytes compressed
    cdef uint32_t checksum
    cdef int64_t block_offset
    cdef bytes header_blob
    cdef bytes dir_blob
    cdef int64_t dir_offset
    cdef uint32_t footer_checksum = 0
    cdef bytearray dir_buffer = bytearray()
    cdef bytes serialized_blob = b""
    cdef uint64_t raw_written = 0
    cdef uint64_t compressed_written = 0
    cdef uint64_t blocks_written = 0
    cdef object sink_bytes_view
    cdef ConstAccessor* const_accessor

    if codec_id == CODEC_LZ4:
        lz4 = _load_lz4()
        if lz4 is None or not lz4.is_available():
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
        vec = <Vector>morsel.ptr.columns[i]
        dict_accessor = vec.dict_accessor()
        # Prefer the unified discriminant instead of type-checking for DictionaryVector.
        draken_encoding = vec.encoding
        segments = []

        if draken_encoding == DRAKEN_ENCODING_CONSTANT:
            encoding = ENCODING_CONST
            const_accessor = vec.const_accessor()
            if const_accessor == NULL:
                raise DrakenMorselStorageError("invalid typed constant accessor")

            if const_accessor.value_type == DRAKEN_STRING:
                const_str = <DrakenConstantStringPayload*>const_accessor.value_ptr
                if const_str == NULL:
                    raise DrakenMorselStorageError("invalid typed constant string payload pointer")
                const_value_len = const_str.length
                segments.append((SEG_CONST_VALUE, <intptr_t>const_str.data, const_value_len))
            else:
                const_value_len = _const_value_length_for_type(<int>const_accessor.value_type)
                segments.append((SEG_CONST_VALUE, <intptr_t>const_accessor.value_ptr, const_value_len))

            flags = _const_flags_from_value_type(<int>const_accessor.value_type)
            if const_accessor.is_null != 0:
                flags |= FLAG_HAS_NULLS
        elif dtype == DRAKEN_STRING:
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
            flags = FLAG_HAS_NULLS if null_len > 0 else 0
        elif draken_encoding == DRAKEN_ENCODING_DICTIONARY:
            encoding = ENCODING_DICT

            # Prefer the unified discriminant instead of type-checking for DictionaryVector.
            if dict_accessor == NULL or dict_accessor.dict_values == NULL:
                raise DrakenMorselStorageError("invalid dictionary accessor")

            row_nulls = dict_accessor.row_nulls
            codes_ptr = dict_accessor.codes
            code_width = dict_accessor.code_width
            var_ptr = dict_accessor.dict_values
            dict_ordered = bool(getattr(vec, "ordered", False))

            null_len = ((row_count + 7) >> 3) if row_nulls != NULL else 0
            if null_len > 0:
                segments.append((SEG_NULL, <intptr_t>row_nulls, null_len))

            dict_codes_len = row_count * <Py_ssize_t>code_width
            segments.append((SEG_CODES, <intptr_t>codes_ptr, dict_codes_len))

            dict_len = <Py_ssize_t>var_ptr.length
            dict_offsets_len = (dict_len + 1) * sizeof(int32_t)
            segments.append((SEG_DICT_OFFSETS, <intptr_t>var_ptr.offsets, dict_offsets_len))

            values_len = 0
            if dict_len > 0:
                values_len = <Py_ssize_t>var_ptr.offsets[dict_len]
            segments.append((SEG_DICT_VALUES, <intptr_t>var_ptr.data, values_len))

            dict_null_len = (dict_len + 7) >> 3 if var_ptr.null_bitmap != NULL else 0
            if dict_null_len > 0:
                segments.append((SEG_DICT_NULL, <intptr_t>var_ptr.null_bitmap, dict_null_len))

            flags = 0
            if null_len > 0:
                flags |= FLAG_HAS_NULLS
            if dict_ordered:
                flags |= FLAG_DICT_ORDERED
            if dict_null_len > 0:
                flags |= FLAG_DICT_HAS_DICT_NULLS
            flags |= _dict_flags_from_code_width(code_width)
            flags |= _dict_flags_from_value_type(var_ptr.type)
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
            flags = FLAG_HAS_NULLS if null_len > 0 else 0
        else:
            raise DrakenMorselStorageError(
                f"dtype {dtype} is not yet supported by DRKM v1 serializer"
            )

        block_start = block_cursor
        block_cursor += len(segments)
        block_end = block_cursor
        column_plans.append((name_bytes, dtype, encoding, flags, block_start, block_end, segments))

        schema_blob += name_bytes
        schema_blob += struct.pack("<H", dtype)

    block_count = block_cursor
    if schema_blob:
        schema_fingerprint = hash_bytes(schema_blob)

    handle, close_when_done, resolved_path, sink_kind, sink_target = _open_writer(path_or_handle)
    try:
        handle.write(
            _encode_header(
                <uint64_t>row_count,
                <uint32_t>column_count,
                <uint32_t>block_count,
                schema_fingerprint,
                <uint8_t>codec_id,
            )
        )

        for (name_bytes, dtype, encoding, flags, block_start, block_end, segments) in column_plans:
            handle.write(
                _encode_column_entry(
                    <uint16_t>len(name_bytes),
                    <uint16_t>dtype,
                    <uint8_t>encoding,
                    <uint8_t>flags,
                    <uint32_t>block_start,
                    <uint32_t>block_end,
                )
            )
            handle.write(name_bytes)

        for col_idx, (_, _, _, _, _, _, segments) in enumerate(column_plans):
            for (seg_kind, ptr_value, seg_len) in segments:
                block_id = len(block_dir)
                checksum = _checksum32_ptr(<const void*>ptr_value, seg_len) if checksum_enabled else 0
                block_offset = handle.tell()
                if codec_id == CODEC_NONE:
                    header_blob = _encode_block_header(
                        <uint32_t>block_id,
                        <uint32_t>col_idx,
                        <uint8_t>seg_kind,
                        <uint8_t>codec_id,
                        0,
                        <uint32_t>row_count,
                        0,
                        <uint32_t>seg_len,
                        <uint32_t>seg_len,
                        checksum,
                    )
                    handle.write(header_blob)
                    _write_ptr_payload(handle, <const void*>ptr_value, seg_len)
                    block_dir.append((block_id, col_idx, seg_kind, block_offset, len(header_blob) + seg_len))
                    raw_written += <uint64_t>seg_len
                    compressed_written += <uint64_t>seg_len
                else:
                    segment_payload = (
                        PyMemoryView_FromMemory(<char*>ptr_value, seg_len, PyBUF_READ)
                        if seg_len > 0
                        else b""
                    )
                    compressed = _compress_payload(segment_payload, codec_id, zstd_level)
                    header_blob = _encode_block_header(
                        <uint32_t>block_id,
                        <uint32_t>col_idx,
                        <uint8_t>seg_kind,
                        <uint8_t>codec_id,
                        0,
                        <uint32_t>row_count,
                        0,
                        <uint32_t>seg_len,
                        <uint32_t>len(compressed),
                        checksum,
                    )
                    handle.write(header_blob)
                    handle.write(compressed)
                    block_dir.append(
                        (block_id, col_idx, seg_kind, block_offset, len(header_blob) + len(compressed))
                    )
                    raw_written += <uint64_t>seg_len
                    compressed_written += <uint64_t>len(compressed)
                blocks_written += 1

        dir_offset = handle.tell()
        for (block_id, col_idx, seg_kind, block_offset, total_len) in block_dir:
            dir_buffer.extend(
                _encode_block_dir_entry(
                    <uint32_t>block_id,
                    <uint32_t>col_idx,
                    <uint8_t>seg_kind,
                    <uint64_t>block_offset,
                    <uint32_t>total_len,
                )
            )

        dir_blob = bytes(dir_buffer)
        handle.write(dir_blob)
        footer_checksum = _checksum32(dir_blob) if checksum_enabled else 0
        handle.write(
            _encode_footer(
                <uint64_t>dir_offset,
                <uint32_t>len(dir_blob),
                footer_checksum,
                <uint32_t>block_count,
            )
        )
        if sink_kind == SINK_RETURN_BYTES or sink_kind == SINK_BYTEARRAY:
            serialized_blob = handle.getvalue()
    finally:
        if close_when_done:
            handle.close()

    if sink_kind == SINK_RETURN_BYTES:
        return serialized_blob

    if sink_kind == SINK_BYTEARRAY:
        sink_target[:] = serialized_blob

    return {
        "path": resolved_path,
        "rows": int(row_count),
        "columns": int(column_count),
        "blocks": int(block_count),
        "bytes_raw_written": int(raw_written),
        "bytes_compressed_written": int(compressed_written),
        "bytes_output": int(handle.bytes_written) if sink_kind == SINK_MEMORYVIEW else (int(len(serialized_blob)) if serialized_blob else None),
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
    cdef object seg_info
    cdef list vector_names = []
    cdef list vectors = []
    cdef Vector vec
    cdef DrakenFixedBuffer* fixed_ptr
    cdef DrakenVarBuffer* var_ptr
    cdef DrakenDictionaryBuffer* dict_ptr
    cdef DrakenConstantBuffer* const_ptr
    cdef uint8_t* bitmap
    cdef object offsets_payload
    cdef object values_payload
    cdef object codes_payload
    cdef object dict_offsets_payload
    cdef object dict_values_payload
    cdef object dict_null_payload
    cdef Py_ssize_t expected_len
    cdef Py_ssize_t null_len
    cdef Py_ssize_t dict_len
    cdef Py_ssize_t dict_offsets_len
    cdef Py_ssize_t dict_values_len
    cdef uint8_t code_width
    cdef int dict_value_type
    cdef int const_value_type
    cdef uint32_t code
    cdef Py_ssize_t row_idx
    cdef int64_t payload_offset
    cdef int64_t scalar_i64
    cdef double scalar_f64
    cdef uint8_t scalar_bool
    cdef object scalar_value

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
        ) = _decode_header(header_bytes)

        if magic != MAGIC:
            raise DrakenMorselCorruptionError(f"invalid DRKM magic: {magic!r}")
        if version != FORMAT_VERSION:
            raise DrakenMorselCorruptionError(
                f"unsupported DRKM version {version}, expected {FORMAT_VERSION}"
            )

        for i in range(column_count):
            col_meta = _decode_column_entry(_read_exact(handle, COLUMN_ENTRY_SIZE))
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
        (dir_offset, dir_length, expected_footer_checksum, footer_block_count, footer_magic) = _decode_footer(
            footer_bytes
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
                _decode_block_dir_entry(
                    dir_blob[i * BLOCK_DIR_ENTRY_SIZE : (i + 1) * BLOCK_DIR_ENTRY_SIZE]
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
            ) = _decode_block_header(block_header)
            seg_map = segment_store[col_idx]
            if seg_kind in seg_map:
                raise DrakenMorselCorruptionError(
                    f"duplicate segment kind {seg_kind} for column {col_idx}"
                )
            seg_map[seg_kind] = (
                codec_id,
                raw_len,
                comp_len,
                checksum,
                <int64_t>(block_offset + BLOCK_HEADER_SIZE),
            )

        for i, (name_bytes, dtype, encoding, col_flags, block_start, block_end) in enumerate(columns):
            seg_map = segment_store[i]
            vector_names.append(name_bytes.decode("utf-8"))

            if encoding == ENCODING_FIXED:
                vec = _allocate_fixed_vector(dtype, row_count)
                fixed_ptr = _fixed_ptr_from_vector(vec, dtype)
                if fixed_ptr == NULL:
                    raise DrakenMorselStorageError(f"failed to allocate fixed vector for dtype {dtype}")

                seg_info = seg_map.get(SEG_DATA, None)
                if seg_info is None:
                    raise DrakenMorselCorruptionError(f"missing fixed data segment for column {i}")
                codec_id = seg_info[0]
                raw_len = seg_info[1]
                comp_len = seg_info[2]
                checksum = seg_info[3]
                payload_offset = seg_info[4]

                if dtype == DRAKEN_BOOL:
                    expected_len = (row_count + 7) >> 3
                else:
                    expected_len = row_count * <Py_ssize_t>fixed_ptr.itemsize
                if raw_len != expected_len:
                    raise DrakenMorselCorruptionError(
                        f"fixed payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                    )

                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                _decompress_into_ptr(compressed, codec_id, raw_len, fixed_ptr.data)
                if checksum_enabled and checksum:
                    if _checksum32_ptr(<const void*>fixed_ptr.data, raw_len) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for block data in column {i}"
                        )

                seg_info = seg_map.get(SEG_NULL, None)
                if seg_info is not None:
                    codec_id = seg_info[0]
                    raw_len = seg_info[1]
                    comp_len = seg_info[2]
                    checksum = seg_info[3]
                    payload_offset = seg_info[4]
                    null_len = (row_count + 7) >> 3
                    if raw_len != null_len:
                        raise DrakenMorselCorruptionError(
                            f"null bitmap length mismatch for column {i}: expected {null_len}, got {raw_len}"
                        )
                    if null_len > 0:
                        bitmap = <uint8_t*>malloc(<size_t>null_len)
                        if bitmap == NULL:
                            raise MemoryError()
                        handle.seek(payload_offset, os.SEEK_SET)
                        compressed = _read_exact(handle, comp_len)
                        _decompress_into_ptr(compressed, codec_id, raw_len, bitmap)
                        if checksum_enabled and checksum:
                            if _checksum32_ptr(<const void*>bitmap, raw_len) != checksum:
                                raise DrakenMorselCorruptionError(
                                    f"checksum mismatch for null bitmap in column {i}"
                                )
                        fixed_ptr.null_bitmap = bitmap
                    else:
                        fixed_ptr.null_bitmap = NULL
                else:
                    fixed_ptr.null_bitmap = NULL
            elif encoding == ENCODING_CONST:
                seg_info = seg_map.get(SEG_CONST_VALUE, None)
                if seg_info is None:
                    raise DrakenMorselCorruptionError(
                        f"missing constant value segment for column {i}"
                    )

                const_value_type = _const_value_type_from_flags(col_flags)
                codec_id = seg_info[0]
                raw_len = seg_info[1]
                comp_len = seg_info[2]
                checksum = seg_info[3]
                payload_offset = seg_info[4]

                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)

                if const_value_type == DRAKEN_INT64:
                    expected_len = _const_value_length_for_type(const_value_type)
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"constant int64 payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    scalar_i64 = 0
                    _decompress_into_ptr(compressed, codec_id, raw_len, &scalar_i64)
                    if checksum_enabled and checksum:
                        if _checksum32_ptr(<const void*>&scalar_i64, raw_len) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = scalar_i64
                elif const_value_type == DRAKEN_FLOAT64:
                    expected_len = _const_value_length_for_type(const_value_type)
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"constant float64 payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    scalar_f64 = 0.0
                    _decompress_into_ptr(compressed, codec_id, raw_len, &scalar_f64)
                    if checksum_enabled and checksum:
                        if _checksum32_ptr(<const void*>&scalar_f64, raw_len) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = scalar_f64
                elif const_value_type == DRAKEN_BOOL:
                    expected_len = _const_value_length_for_type(const_value_type)
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"constant bool payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    scalar_bool = 0
                    _decompress_into_ptr(compressed, codec_id, raw_len, &scalar_bool)
                    if checksum_enabled and checksum:
                        if _checksum32_ptr(<const void*>&scalar_bool, raw_len) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = scalar_bool != 0
                elif (
                    const_value_type == DRAKEN_INT8
                    or const_value_type == DRAKEN_INT16
                    or const_value_type == DRAKEN_INT32
                    or const_value_type == DRAKEN_DATE32
                    or const_value_type == DRAKEN_TIME32
                ):
                    expected_len = _const_value_length_for_type(const_value_type)
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"constant 32-bit payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    scalar_i32 = 0
                    _decompress_into_ptr(compressed, codec_id, raw_len, &scalar_i32)
                    if checksum_enabled and checksum:
                        if _checksum32_ptr(<const void*>&scalar_i32, raw_len) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = scalar_i32
                elif const_value_type == DRAKEN_TIME64 or const_value_type == DRAKEN_TIMESTAMP64:
                    expected_len = _const_value_length_for_type(const_value_type)
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"constant 64-bit payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    scalar_i64 = 0
                    _decompress_into_ptr(compressed, codec_id, raw_len, &scalar_i64)
                    if checksum_enabled and checksum:
                        if _checksum32_ptr(<const void*>&scalar_i64, raw_len) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = scalar_i64
                elif const_value_type == DRAKEN_STRING:
                    payload = _decompress_payload(compressed, codec_id, raw_len)
                    if len(payload) != raw_len:
                        raise DrakenMorselCorruptionError(
                            f"constant string payload length mismatch for column {i}: expected {raw_len}, got {len(payload)}"
                        )
                    if checksum_enabled and checksum:
                        if _checksum32(payload) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for constant value in column {i}"
                            )
                    scalar_value = payload
                else:
                    raise DrakenMorselStorageError(
                        f"unsupported constant value dtype {const_value_type} in DRKM reader"
                    )

                vec = _build_typed_const_vector(
                    dtype,
                    const_value_type,
                    scalar_value,
                    row_count,
                    bool(col_flags & FLAG_HAS_NULLS),
                )
            elif encoding == ENCODING_DICT:
                codes_payload = seg_map.get(SEG_CODES, None)
                dict_offsets_payload = seg_map.get(SEG_DICT_OFFSETS, None)
                dict_values_payload = seg_map.get(SEG_DICT_VALUES, None)
                dict_null_payload = seg_map.get(SEG_DICT_NULL, None)
                if codes_payload is None or dict_offsets_payload is None or dict_values_payload is None:
                    raise DrakenMorselCorruptionError(
                        f"missing dictionary segments for column {i}"
                    )

                code_width = _dict_code_width_from_flags(col_flags)
                dict_value_type = _dict_value_type_from_flags(col_flags)
                dict_offsets_len = dict_offsets_payload[1]
                if dict_offsets_len < <Py_ssize_t>sizeof(int32_t) or (dict_offsets_len % sizeof(int32_t)) != 0:
                    raise DrakenMorselCorruptionError(
                        f"invalid dictionary offsets payload length for column {i}: {dict_offsets_len}"
                    )
                dict_len = (dict_offsets_len // sizeof(int32_t)) - 1
                dict_values_len = dict_values_payload[1]
                row_null_payload = seg_map.get(SEG_NULL, None)
                codec_id = codes_payload[0]
                raw_len = codes_payload[1]
                comp_len = codes_payload[2]
                checksum = codes_payload[3]
                payload_offset = codes_payload[4]
                expected_len = row_count * <Py_ssize_t>code_width
                if raw_len != expected_len:
                    raise DrakenMorselCorruptionError(
                        f"dictionary codes length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                    )
                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                codes_bytes = _decompress_payload(compressed, codec_id, raw_len)
                if len(codes_bytes) != raw_len:
                    raise DrakenMorselCorruptionError(
                        f"decompressed dictionary codes length mismatch for column {i}: expected {raw_len}, got {len(codes_bytes)}"
                    )
                if checksum_enabled and checksum:
                    if _checksum32(codes_bytes) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for dictionary codes in column {i}"
                        )

                codec_id = dict_offsets_payload[0]
                raw_len = dict_offsets_payload[1]
                comp_len = dict_offsets_payload[2]
                checksum = dict_offsets_payload[3]
                payload_offset = dict_offsets_payload[4]
                expected_len = (dict_len + 1) * sizeof(int32_t)
                if raw_len != expected_len:
                    raise DrakenMorselCorruptionError(
                        f"dictionary offsets length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                    )
                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                dict_offsets_bytes = _decompress_payload(compressed, codec_id, raw_len)
                if len(dict_offsets_bytes) != raw_len:
                    raise DrakenMorselCorruptionError(
                        f"decompressed dictionary offsets length mismatch for column {i}: expected {raw_len}, got {len(dict_offsets_bytes)}"
                    )
                if checksum_enabled and checksum:
                    if _checksum32(dict_offsets_bytes) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for dictionary offsets in column {i}"
                        )

                codec_id = dict_values_payload[0]
                raw_len = dict_values_payload[1]
                comp_len = dict_values_payload[2]
                checksum = dict_values_payload[3]
                payload_offset = dict_values_payload[4]
                if raw_len != dict_values_len:
                    raise DrakenMorselCorruptionError(
                        f"dictionary values length mismatch for column {i}: expected {dict_values_len}, got {raw_len}"
                    )
                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                dict_values_bytes = _decompress_payload(compressed, codec_id, raw_len)
                if len(dict_values_bytes) != raw_len:
                    raise DrakenMorselCorruptionError(
                        f"decompressed dictionary values length mismatch for column {i}: expected {raw_len}, got {len(dict_values_bytes)}"
                    )
                if checksum_enabled and checksum:
                    if _checksum32(dict_values_bytes) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for dictionary values in column {i}"
                        )

                if dict_null_payload is not None:
                    codec_id = dict_null_payload[0]
                    raw_len = dict_null_payload[1]
                    comp_len = dict_null_payload[2]
                    checksum = dict_null_payload[3]
                    payload_offset = dict_null_payload[4]
                    expected_len = (dict_len + 7) >> 3
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"dictionary null bitmap length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                        )
                    handle.seek(payload_offset, os.SEEK_SET)
                    compressed = _read_exact(handle, comp_len)
                    dict_null_bytes = _decompress_payload(compressed, codec_id, raw_len)
                    if len(dict_null_bytes) != raw_len:
                        raise DrakenMorselCorruptionError(
                            f"decompressed dictionary null length mismatch for column {i}: expected {raw_len}, got {len(dict_null_bytes)}"
                        )
                    if checksum_enabled and checksum:
                        if _checksum32(dict_null_bytes) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for dictionary null bitmap in column {i}"
                            )
                    if dtype != DRAKEN_DICTIONARY and not _all_bytes_ff(dict_null_bytes):
                        raise DrakenMorselStorageError(
                            f"typed dictionary restore does not support dictionary-entry nulls for column {i}"
                        )

                null_bytes = None
                if row_null_payload is not None:
                    codec_id = row_null_payload[0]
                    raw_len = row_null_payload[1]
                    comp_len = row_null_payload[2]
                    checksum = row_null_payload[3]
                    payload_offset = row_null_payload[4]
                    expected_len = (row_count + 7) >> 3
                    if raw_len != expected_len:
                        raise DrakenMorselCorruptionError(
                            f"row null bitmap length mismatch for dictionary column {i}: expected {expected_len}, got {raw_len}"
                        )
                    handle.seek(payload_offset, os.SEEK_SET)
                    compressed = _read_exact(handle, comp_len)
                    null_bytes = _decompress_payload(compressed, codec_id, raw_len)
                    if len(null_bytes) != raw_len:
                        raise DrakenMorselCorruptionError(
                            f"decompressed row null bitmap length mismatch for column {i}: expected {raw_len}, got {len(null_bytes)}"
                        )
                    if checksum_enabled and checksum:
                        if _checksum32(null_bytes) != checksum:
                            raise DrakenMorselCorruptionError(
                                f"checksum mismatch for row null bitmap in dictionary column {i}"
                            )

                if dtype == DRAKEN_DICTIONARY:
                    # Prefer typed dictionary vectors when possible (numeric types).
                    # DictionaryVector has been retired; restore into typed dictionary
                    # vectors where available, or dense strings otherwise.
                    if dict_null_payload is None and dict_value_type in (
                        DRAKEN_INT8,
                        DRAKEN_INT16,
                        DRAKEN_INT32,
                        DRAKEN_INT64,
                    ):
                        vec = _build_typed_dict_vector(
                            DRAKEN_INT64,
                            row_count,
                            <const uint8_t*>PyBytes_AS_STRING(codes_bytes),
                            code_width,
                            <const uint8_t*>NULL if null_bytes is None else <const uint8_t*>PyBytes_AS_STRING(null_bytes),
                            dict_value_type,
                            <const void*>PyBytes_AS_STRING(dict_values_bytes),
                            dict_len,
                            bool(col_flags & FLAG_DICT_ORDERED),
                        )
                    elif dict_null_payload is None and dict_value_type in (
                        DRAKEN_FLOAT32,
                        DRAKEN_FLOAT64,
                    ):
                        vec = _build_typed_dict_vector(
                            DRAKEN_FLOAT64,
                            row_count,
                            <const uint8_t*>PyBytes_AS_STRING(codes_bytes),
                            code_width,
                            <const uint8_t*>NULL if null_bytes is None else <const uint8_t*>PyBytes_AS_STRING(null_bytes),
                            dict_value_type,
                            <const void*>PyBytes_AS_STRING(dict_values_bytes),
                            dict_len,
                            bool(col_flags & FLAG_DICT_ORDERED),
                        )
                    elif dtype == DRAKEN_STRING:
                        vec = _build_dense_string_dict_vector(
                            row_count,
                            <const uint8_t*>PyBytes_AS_STRING(codes_bytes),
                            code_width,
                            dict_offsets_bytes,
                            dict_values_bytes,
                            null_bytes,
                            dict_null_bytes,
                        )
                    else:
                        raise DrakenMorselStorageError(
                            f"unsupported dictionary restore for dtype {dtype} and value type {dict_value_type}"
                        )
                else:
                    vec = _build_typed_dict_vector(
                        dtype,
                        row_count,
                        <const uint8_t*>PyBytes_AS_STRING(codes_bytes),
                        code_width,
                        <const uint8_t*>NULL if null_bytes is None else <const uint8_t*>PyBytes_AS_STRING(null_bytes),
                        dict_value_type,
                        <const void*>PyBytes_AS_STRING(dict_values_bytes),
                        dict_len,
                        bool(col_flags & FLAG_DICT_ORDERED),
                    )
                if dict_len > 0 and _u32_le_read((<const uint8_t*>PyBytes_AS_STRING(dict_offsets_bytes)) + (dict_len * sizeof(int32_t))) != dict_values_len:
                    raise DrakenMorselStorageError(
                        f"dictionary offset tail mismatch in column {i}: expected {dict_values_len}, got {_u32_le_read((<const uint8_t*>PyBytes_AS_STRING(dict_offsets_bytes)) + (dict_len * sizeof(int32_t)))}"
                    )

                if dtype == DRAKEN_DICTIONARY and dict_len > 0:
                    for row_idx in range(row_count):
                        code = _dict_read_code(dict_ptr, row_idx)
                        if code >= <uint32_t>dict_len:
                            raise DrakenMorselCorruptionError(
                                f"dictionary code out of range in column {i}: code={code}, dict_len={dict_len}"
                            )
            elif encoding == ENCODING_VAR:
                if dtype != DRAKEN_STRING:
                    raise DrakenMorselStorageError(
                        f"unsupported var-width dtype {dtype} in DRKM v1 reader"
                    )
                offsets_payload = seg_map.get(SEG_OFFSETS, None)
                values_payload = seg_map.get(SEG_VALUES, None)
                if offsets_payload is None or values_payload is None:
                    raise DrakenMorselCorruptionError(f"missing var-width segments for column {i}")

                vec = StringVector(<size_t>row_count, <size_t>values_payload[1])
                var_ptr = (<StringVector>vec).ptr

                codec_id = offsets_payload[0]
                raw_len = offsets_payload[1]
                comp_len = offsets_payload[2]
                checksum = offsets_payload[3]
                payload_offset = offsets_payload[4]
                expected_len = (row_count + 1) * sizeof(int32_t)
                if raw_len != expected_len:
                    raise DrakenMorselCorruptionError(
                        f"offset payload length mismatch for column {i}: expected {expected_len}, got {raw_len}"
                    )
                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                _decompress_into_ptr(compressed, codec_id, raw_len, var_ptr.offsets)
                if checksum_enabled and checksum:
                    if _checksum32_ptr(<const void*>var_ptr.offsets, raw_len) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for offsets in column {i}"
                        )

                codec_id = values_payload[0]
                raw_len = values_payload[1]
                comp_len = values_payload[2]
                checksum = values_payload[3]
                payload_offset = values_payload[4]
                handle.seek(payload_offset, os.SEEK_SET)
                compressed = _read_exact(handle, comp_len)
                _decompress_into_ptr(compressed, codec_id, raw_len, var_ptr.data)
                if checksum_enabled and checksum:
                    if _checksum32_ptr(<const void*>var_ptr.data, raw_len) != checksum:
                        raise DrakenMorselCorruptionError(
                            f"checksum mismatch for string values in column {i}"
                        )

                seg_info = seg_map.get(SEG_NULL, None)
                if seg_info is not None:
                    codec_id = seg_info[0]
                    raw_len = seg_info[1]
                    comp_len = seg_info[2]
                    checksum = seg_info[3]
                    payload_offset = seg_info[4]
                    null_len = (row_count + 7) >> 3
                    if raw_len != null_len:
                        raise DrakenMorselCorruptionError(
                            f"null bitmap length mismatch for column {i}: expected {null_len}, got {raw_len}"
                        )
                    if null_len > 0:
                        bitmap = <uint8_t*>malloc(<size_t>null_len)
                        if bitmap == NULL:
                            raise MemoryError()
                        handle.seek(payload_offset, os.SEEK_SET)
                        compressed = _read_exact(handle, comp_len)
                        _decompress_into_ptr(compressed, codec_id, raw_len, bitmap)
                        if checksum_enabled and checksum:
                            if _checksum32_ptr(<const void*>bitmap, raw_len) != checksum:
                                raise DrakenMorselCorruptionError(
                                    f"checksum mismatch for null bitmap in column {i}"
                                )
                        var_ptr.null_bitmap = bitmap
                    else:
                        var_ptr.null_bitmap = NULL
                else:
                    var_ptr.null_bitmap = NULL

                if row_count > 0 and var_ptr.offsets[row_count] != values_payload[1]:
                    raise DrakenMorselStorageError(
                        f"offset tail mismatch in column {i}: expected {values_payload[1]}, got {var_ptr.offsets[row_count]}"
                    )
            else:
                raise DrakenMorselCorruptionError(f"invalid encoding kind {encoding} for column {i}")

            vectors.append(vec)

        return Morsel.from_vectors(vector_names, vectors)
    finally:
        if close_when_done:
            handle.close()
