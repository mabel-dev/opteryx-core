# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t, uint8_t

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module


cdef inline int uint64_to_str_buf(uint64_t value, char* buf) nogil:
    """Write ASCII digits of value into buf (20 chars) and return length."""
    cdef int i = 20
    if value == 0:
        buf[19] = 48
        return 1
    while value != 0:
        i -= 1
        buf[i] = 48 + (value % 10)
        value //= 10
    return 20 - i


cpdef StringVector vector_cast_uint64_to_bytes(Int64Vector vec):
    """Cast an Int64Vector (reinterpreted as uint64) to a StringVector of decimal strings."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef uint64_t* src = <uint64_t*>vec.ptr.data
    cdef uint8_t* null_bm = vec.ptr.null_bitmap
    cdef char buf[21]
    cdef int length
    cdef Py_ssize_t i

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 10)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            length = uint64_to_str_buf(src[i], buf)
            builder.append_bytes(buf + (20 - length), length)

    return builder.finish()


cpdef StringVector vector_cast_uint64_to_ascii(Int64Vector vec):
    """Same as vector_cast_uint64_to_bytes."""
    return vector_cast_uint64_to_bytes(vec)
