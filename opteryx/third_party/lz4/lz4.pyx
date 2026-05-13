# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.buffer cimport PyBUF_CONTIG_RO
from cpython.buffer cimport PyBUF_WRITABLE
from cpython.buffer cimport PyBuffer_Release
from cpython.buffer cimport PyObject_GetBuffer
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.limits cimport INT_MAX
import threading


cdef extern from "lz4.h":
    int LZ4_sizeofState()
    int LZ4_compressBound(int inputSize)
    int LZ4_compress_fast_extState(
        void* state,
        const char* src,
        char* dst,
        int srcSize,
        int dstCapacity,
        int acceleration,
    )
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


cdef object _tls = threading.local()


cdef inline object _get_thread_state_buffer():
    cdef object state
    cdef int state_size = LZ4_sizeofState()
    if state_size <= 0:
        return None
    state = getattr(_tls, "state_buffer", None)
    if state is None or len(state) < state_size:
        state = bytearray(state_size)
        _tls.state_buffer = state
    return state


cpdef bint is_available():
    """Vendored compiled LZ4 is always available when this module imports."""
    return True


cpdef int compress_bound(Py_ssize_t size):
    cdef int src_size = _to_lz4_len(size)
    return LZ4_compressBound(src_size)


cpdef bytes compress_block(object src):
    cdef Py_buffer view
    cdef Py_buffer state_view
    cdef int src_size
    cdef int dst_capacity
    cdef int written
    cdef bytes out
    cdef char* dst_ptr
    cdef object state_buffer = None

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

        state_buffer = _get_thread_state_buffer()
        if state_buffer is not None and PyObject_GetBuffer(state_buffer, &state_view, PyBUF_WRITABLE) == 0:
            try:
                written = LZ4_compress_fast_extState(
                    state_view.buf,
                    <const char*>view.buf,
                    dst_ptr,
                    src_size,
                    dst_capacity,
                    1,
                )
            finally:
                PyBuffer_Release(&state_view)
        else:
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


cpdef int decompress_into(object src, object dst, Py_ssize_t uncompressed_size=-1):
    cdef Py_buffer src_view
    cdef Py_buffer dst_view
    cdef int src_size
    cdef int dst_capacity
    cdef int produced

    if PyObject_GetBuffer(src, &src_view, PyBUF_CONTIG_RO) != 0:
        raise TypeError("expected a readable source buffer")
    if PyObject_GetBuffer(dst, &dst_view, PyBUF_WRITABLE) != 0:
        PyBuffer_Release(&src_view)
        raise TypeError("expected a writable destination buffer")

    try:
        src_size = _to_lz4_len(src_view.len)
        if uncompressed_size < 0:
            dst_capacity = _to_lz4_len(dst_view.len)
        else:
            dst_capacity = _to_lz4_len(uncompressed_size)
            if dst_capacity > dst_view.len:
                raise ValueError("destination buffer is smaller than requested uncompressed size")
        if dst_capacity == 0:
            return 0

        produced = LZ4_decompress_safe(
            <const char*>src_view.buf,
            <char*>dst_view.buf,
            src_size,
            dst_capacity,
        )
        if produced < 0:
            raise RuntimeError("lz4 decompression failed")
        if produced != dst_capacity:
            raise RuntimeError(
                f"lz4 decompressed size mismatch: expected {dst_capacity}, got {produced}"
            )
        return produced
    finally:
        PyBuffer_Release(&dst_view)
        PyBuffer_Release(&src_view)
