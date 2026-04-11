# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

from opteryx.compiled.draken.vectors.string_vector cimport StringVector, DrakenVarBuffer
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t


cdef extern from "simd_string_ops.h":
    void simd_to_upper(char* data, size_t length)


cpdef StringVector vector_uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    """
    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef Py_ssize_t n = in_ptr.length
    cdef int32_t total_bytes = in_ptr.offsets[n]
    cdef Py_ssize_t nb_size

    # Allocate new buffer with same size
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data

    # Copy entire data buffer
    if total_bytes > 0:
        memcpy(out_data, in_data, total_bytes)
        # Apply uppercase transformation to entire buffer using SIMD
        simd_to_upper(out_data, total_bytes)

    # Copy offsets
    memcpy(out_ptr.offsets, in_ptr.offsets, (n + 1) * sizeof(int32_t))

    # Copy null bitmap if present
    if in_ptr.null_bitmap != NULL:
        nb_size = (n + 7) // 8
        out_ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if out_ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(out_ptr.null_bitmap, in_ptr.null_bitmap, nb_size)

    return result
