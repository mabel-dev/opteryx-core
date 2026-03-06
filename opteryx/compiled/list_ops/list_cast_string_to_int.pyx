# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t

import numpy
cimport numpy
numpy.import_array()

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.draken.core.buffers cimport DrakenVarBuffer


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


cpdef Int64Vector list_cast_bytes_to_int(StringVector vec):
    """Parse each element of a StringVector as a decimal integer."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i

    cdef numpy.ndarray[int64_t, ndim=1] result = numpy.zeros(n, dtype=numpy.int64)
    cdef int64_t[::1] result_view = result

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            result_view[i] = 0
        else:
            result_view[i] = parse_int64(
                <const char*>ptr.data + ptr.offsets[i],
                ptr.offsets[i + 1] - ptr.offsets[i]
            )

    return int64_from_sequence(result_view)


cpdef Int64Vector list_cast_ascii_to_int(StringVector vec):
    """Same as list_cast_bytes_to_int (StringVector is always UTF-8/ASCII)."""
    return list_cast_bytes_to_int(vec)
