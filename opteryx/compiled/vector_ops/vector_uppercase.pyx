# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t, uint32_t
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef extern from "simd_string_ops.h":
    void simd_to_upper(char* data, size_t length)


cpdef StringVector vector_uppercase(object input):
    """
    Return a new StringVector with all non-null values uppercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    """
    if not isinstance(input, StringVector):
        raise TypeError(f"vector_uppercase: expected StringVector, got {type(input)}")

    cdef StringVector vec = <StringVector>input
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef char* tmp_buf
    cdef StringVectorBuilder builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        if slen > 0:
            tmp_buf = <char*>malloc(<size_t>slen)
            if tmp_buf == NULL:
                raise MemoryError()
            try:
                memcpy(tmp_buf, sdata, <size_t>slen)
                simd_to_upper(tmp_buf, <size_t>slen)
                builder.append_bytes(tmp_buf, <Py_ssize_t>slen)
            finally:
                free(tmp_buf)
        else:
            builder.append_bytes(<const char*>NULL, 0)

    return builder.finish()
