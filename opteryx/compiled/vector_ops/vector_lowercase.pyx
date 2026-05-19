# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data
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
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef int32_t total_bytes
    cdef Py_ssize_t nb_size
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenVarBuffer* vbuf
    cdef int32_t start, end
    cdef char* tmp_dict_buf
    cdef Py_ssize_t i
    cdef object builder, dict_builder, new_dict_sv, dict_result_bytes
    cdef bytes lower_bytes
    cdef DrakenConstantStringPayload* csp
    cdef DrakenGermanArena* lc_gdv
    cdef GermanString* lc_slot
    cdef const uint8_t* lc_sdata
    cdef uint32_t lc_slen

    # Handle constant encoding
    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            builder = string_vector_module.StringVectorBuilder.with_estimate(n, 0)
            for i in range(n):
                builder.append_null()
            return builder.finish()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            builder = string_vector_module.StringVectorBuilder.with_estimate(n, csp.length)
            lower_bytes = PyBytes_FromStringAndSize(<const char*>csp.data, csp.length).lower()
            for i in range(n):
                builder.append(lower_bytes)
            return builder.finish()

    # Dictionary encoding path
    if vec._german_dict_values != NULL:  # dictionary
        lc_gdv = vec._german_dict_values
        dict_size = <Py_ssize_t>lc_gdv.length

        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            lc_slot = &lc_gdv.slots[i]
            lc_slen = gs_length(lc_slot)
            lc_sdata = gs_data(lc_slot, lc_gdv.arena)
            tmp_dict_buf = <char*>malloc(<size_t>lc_slen)
            if tmp_dict_buf == NULL:
                raise MemoryError()
            try:
                memcpy(tmp_dict_buf, lc_sdata, <size_t>lc_slen)
                simd_to_lower(tmp_dict_buf, <size_t>lc_slen)
                dict_result_bytes = PyBytes_FromStringAndSize(tmp_dict_buf, <Py_ssize_t>lc_slen)
                dict_builder.append(dict_result_bytes)
            finally:
                free(tmp_dict_buf)

        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense encoding path
    vbuf = <DrakenVarBuffer*>uv.data
    total_bytes = vbuf.offsets[n]

    # Allocate new buffer with same size
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>vbuf.data
    cdef char* out_data = <char*>out_ptr.data

    # Copy entire data buffer
    if total_bytes > 0:
        memcpy(out_data, in_data, total_bytes)
        # Apply lowercase transformation to entire buffer using SIMD
        simd_to_lower(out_data, total_bytes)

    # Copy offsets
    memcpy(out_ptr.offsets, vbuf.offsets, (n + 1) * sizeof(int32_t))

    # Copy null bitmap if present
    if uv.validity != NULL:
        nb_size = (n + 7) // 8
        out_ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if out_ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(out_ptr.null_bitmap, uv.validity, nb_size)

    return result
