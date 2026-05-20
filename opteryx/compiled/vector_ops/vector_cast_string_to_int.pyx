# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stdlib cimport malloc, free

from draken.vectors.string_vector cimport StringVector
from draken.vectors.integer64_vector cimport Integer64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data


cdef inline int64_t parse_int64(const char* data, int32_t length) except -1:
    cdef int64_t value = 0
    cdef int sign = 1
    cdef int32_t i = 0
    cdef char c

    if length > 0 and data[0] == 45:  # '-'
        sign = -1
        i = 1

    while i < length:
        c = data[i]
        if c < 48 or c > 57:
            raise ValueError("Invalid digit in integer literal")
        value = value * 10 + (c - 48)
        i += 1

    return sign * value


cpdef Integer64Vector vector_cast_bytes_to_int(StringVector vec):
    """Parse each element of a StringVector as a decimal integer."""
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t* result_ptr
    cdef int64_t[::1] result_view
    cdef Integer64Vector result_vector
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    result_ptr = <int64_t*>malloc(n * sizeof(int64_t))
    if result_ptr == NULL:
        raise MemoryError("Failed to allocate memory for result array")

    try:
        result_view = <int64_t[:n]>result_ptr

        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            result_view[i] = parse_int64(<const char*>sdata, <int32_t>slen)

        result_vector = int64_from_sequence(result_view)
        return result_vector
    finally:
        free(result_ptr)


cpdef Integer64Vector vector_cast_ascii_to_int(StringVector vec):
    """Same as vector_cast_bytes_to_int (StringVector is always UTF-8/ASCII)."""
    return vector_cast_bytes_to_int(vec)
