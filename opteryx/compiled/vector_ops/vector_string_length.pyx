# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int64_t, uint8_t, uint32_t
from cpython.array cimport array, clone

from draken.vectors.string_vector cimport StringVector
from draken.vectors.integer64_vector cimport Integer64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length


cpdef Integer64Vector vector_string_length(StringVector vec):
    """Return byte-length of each string in a StringVector."""
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i

    cdef array template = array('q')  # 'q' = signed long long (int64)
    cdef array result_array = clone(template, n, False)
    cdef int64_t[::1] rview = result_array

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            rview[i] = 0
        else:
            rview[i] = <int64_t>str_length(&arena.slots[sel[i]])

    return int64_from_sequence(rview)
