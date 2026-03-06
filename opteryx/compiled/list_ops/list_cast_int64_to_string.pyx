# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, uint8_t

from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module


cdef inline int int64_to_str_buf(int64_t value, char* buf) nogil:
    """Write ASCII digits of value into buf (21 chars) and return length."""
    cdef unsigned long long uval
    cdef int i = 20
    cdef bint neg = value < 0

    if value == 0:
        buf[19] = 48
        return 1

    uval = <unsigned long long>(-value) if neg else <unsigned long long>value
    while uval != 0:
        i -= 1
        buf[i] = 48 + (uval % 10)
        uval //= 10

    if neg:
        i -= 1
        buf[i] = 45

    return 20 - i


cpdef StringVector list_cast_int64_to_bytes(Int64Vector vec):
    """Cast an Int64Vector to a StringVector of UTF-8 decimal bytes."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef int64_t* src = <int64_t*>vec.ptr.data
    cdef uint8_t* null_bm = vec.ptr.null_bitmap
    cdef char buf[21]
    cdef int length
    cdef Py_ssize_t i

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 8)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            length = int64_to_str_buf(src[i], buf)
            builder.append_bytes(buf + (20 - length), length)

    return builder.finish()


cpdef StringVector list_cast_int64_to_ascii(Int64Vector vec):
    """Cast an Int64Vector to a StringVector of ASCII decimal strings (same as bytes)."""
    return list_cast_int64_to_bytes(vec)
