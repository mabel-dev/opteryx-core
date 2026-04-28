# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

from draken.vectors.string_vector cimport StringVector, DrakenVarBuffer, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport ConstAccessor, DrakenConstantStringPayload, DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_DICTIONARY
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef extern from "simd_string_ops.h":
    void simd_to_lower(char* data, size_t length)


cpdef StringVector vector_lowercase(object input):
    """
    Return a new StringVector with all non-null values lowercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    Handles constant and dictionary encodings.
    """
    if not isinstance(input, StringVector):
        raise TypeError(f"vector_lowercase: expected StringVector, got {type(input)}")

    cdef StringVector vec = <StringVector>input
    cdef DrakenVarBuffer* in_ptr = vec.ptr
    cdef Py_ssize_t n
    cdef int32_t total_bytes
    cdef Py_ssize_t nb_size
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp

    # Dictionary encoding path
    cdef int32_t start, end
    cdef DrakenVarBuffer* dict_ptr

    cdef object result_bytes
    cdef object dict_builder
    cdef object new_dict_sv
    cdef object dict_result_bytes
    cdef char* tmp_dict_buf
    cdef Py_ssize_t i
    cdef object builder
    cdef const uint8_t* const_data_ptr
    cdef int32_t const_data_len
    cdef Py_ssize_t const_row_count
    cdef bint is_constant_vec
    cdef bytes lower_bytes

    # Check for constant encoding FIRST (before accessing dense structure)
    is_constant_vec = _constant_string_value(vec, &const_data_ptr, &const_data_len, &const_row_count)
    if not is_constant_vec:
        n = in_ptr.length
    else:
        n = const_row_count

    # Handle constant encoding
    if is_constant_vec:
        if const_data_ptr == NULL:
            builder = string_vector_module.StringVectorBuilder.with_estimate(n, 0)
            for i in range(n):
                builder.append_null()
            return builder.finish()
        else:
            builder = string_vector_module.StringVectorBuilder.with_estimate(n, const_data_len)
            lower_bytes = PyBytes_FromStringAndSize(<const char*>const_data_ptr, const_data_len).lower()
            for i in range(n):
                builder.append(lower_bytes)
            return builder.finish()

    # Dictionary encoding path
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_ptr = vec._dict_values

        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            start = dict_ptr.offsets[i]
            end = dict_ptr.offsets[i + 1]
            tmp_dict_buf = <char*>malloc(end - start)
            if tmp_dict_buf == NULL:
                raise MemoryError()
            try:
                memcpy(tmp_dict_buf, <char*>dict_ptr.data + start, end - start)
                simd_to_lower(tmp_dict_buf, end - start)
                dict_result_bytes = PyBytes_FromStringAndSize(tmp_dict_buf, end - start)
                dict_builder.append(dict_result_bytes)
            finally:
                free(tmp_dict_buf)

        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    # Dense encoding path
    total_bytes = in_ptr.offsets[n]

    # Allocate new buffer with same size
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data

    # Copy entire data buffer
    if total_bytes > 0:
        memcpy(out_data, in_data, total_bytes)
        # Apply lowercase transformation to entire buffer using SIMD
        simd_to_lower(out_data, total_bytes)

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
