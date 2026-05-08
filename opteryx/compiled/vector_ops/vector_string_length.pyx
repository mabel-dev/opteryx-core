# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t, uint8_t
from cpython.array cimport array, clone

from draken.vectors.string_vector cimport StringVector
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload


cpdef Int64Vector vector_string_length(StringVector vec):
    """Return byte-length of each string in a StringVector."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm
    cdef int64_t[::1] rview
    cdef Py_ssize_t i
    cdef DrakenConstantStringPayload* const_val

    cdef array template = array('q')  # 'q' = signed long long (int64)
    cdef array result_array = clone(template, n, False)
    rview = result_array

    if vec._has_const:
        if not vec._const_is_null and vec._const_value != NULL:
            const_val = vec._const_value
            for i in range(n):
                rview[i] = const_val.length
        else:
            for i in range(n):
                rview[i] = 0
        return int64_from_sequence(rview)

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            rview[i] = 0
        else:
            rview[i] = ptr.offsets[i + 1] - ptr.offsets[i]

    return int64_from_sequence(rview)
