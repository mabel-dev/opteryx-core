# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.buffer cimport PyBUF_CONTIG_RO
from cpython.buffer cimport PyBuffer_Release
from cpython.buffer cimport PyObject_GetBuffer
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.limits cimport INT_MAX


cdef extern from "lz4.h":
    int LZ4_compressBound(int inputSize)
    int LZ4_compress_default(
        const char* src,
        char* dst,
        int srcSize,
        int dstCapacity,
    )
    int LZ4_decompress_safe(
        const char* src,
        char* dst,
        int compressedSize,
        int dstCapacity,
    )


cdef inline int _to_lz4_len(Py_ssize_t value) except -1:
    if value < 0:
        raise ValueError("value must be non-negative")
    if value > INT_MAX:
        raise ValueError("value exceeds LZ4 maximum input size")
    return <int>value


cpdef bint is_available():
    """Vendored compiled LZ4 is always available when this module imports."""
    return True


cpdef int compress_bound(Py_ssize_t size):
    cdef int src_size = _to_lz4_len(size)
    return LZ4_compressBound(src_size)


cpdef bytes compress_block(object src):
    cdef Py_buffer view
    cdef int src_size
    cdef int dst_capacity
    cdef int written
    cdef bytes out
    cdef char* dst_ptr

    if PyObject_GetBuffer(src, &view, PyBUF_CONTIG_RO) != 0:
        raise TypeError("expected a buffer-like object")

    try:
        src_size = _to_lz4_len(view.len)
        if src_size == 0:
            return b""

        dst_capacity = LZ4_compressBound(src_size)
        if dst_capacity <= 0:
            raise RuntimeError("lz4 compressBound failed")

        out = PyBytes_FromStringAndSize(NULL, dst_capacity)
        if out is None:
            raise MemoryError()
        dst_ptr = PyBytes_AS_STRING(out)

        written = LZ4_compress_default(
            <const char*>view.buf,
            dst_ptr,
            src_size,
            dst_capacity,
        )
        if written <= 0:
            raise RuntimeError("lz4 compression failed")
        if written == dst_capacity:
            return out
        return out[:written]
    finally:
        PyBuffer_Release(&view)


cpdef bytes decompress_block(object src, Py_ssize_t uncompressed_size):
    cdef Py_buffer view
    cdef int src_size
    cdef int dst_capacity
    cdef int produced
    cdef bytes out
    cdef char* dst_ptr

    dst_capacity = _to_lz4_len(uncompressed_size)
    if dst_capacity == 0:
        return b""

    if PyObject_GetBuffer(src, &view, PyBUF_CONTIG_RO) != 0:
        raise TypeError("expected a buffer-like object")

    try:
        src_size = _to_lz4_len(view.len)
        out = PyBytes_FromStringAndSize(NULL, dst_capacity)
        if out is None:
            raise MemoryError()
        dst_ptr = PyBytes_AS_STRING(out)

        produced = LZ4_decompress_safe(
            <const char*>view.buf,
            dst_ptr,
            src_size,
            dst_capacity,
        )
        if produced < 0:
            raise RuntimeError("lz4 decompression failed")
        if produced != dst_capacity:
            raise RuntimeError(
                f"lz4 decompressed size mismatch: expected {dst_capacity}, got {produced}"
            )
        return out
    finally:
        PyBuffer_Release(&view)
