# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, int32_t, uint8_t

import numpy
cimport numpy
numpy.import_array()

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer


cdef inline int64_t _min3(int64_t x, int64_t y, int64_t z) nogil:
    if x <= y:
        return x if x <= z else z
    return y if y <= z else z


cdef int64_t levenshtein_bytes(
        const uint8_t* s1, int32_t len1,
        const uint8_t* s2, int32_t len2) except -1:
    """Compute Levenshtein distance between two byte strings."""
    if len1 < len2:
        # Swap so s1 is the longer string
        s1, s2 = s2, s1
        len1, len2 = len2, len1

    cdef int32_t len2_1 = len2 + 1
    cdef numpy.ndarray[int64_t, ndim=1] dp_arr = numpy.zeros(
        (len1 + 1) * len2_1, dtype=numpy.int64
    )
    cdef int64_t[::1] dp = dp_arr
    cdef int32_t i, j

    for i in range(len1 + 1):
        for j in range(len2_1):
            if i == 0:
                dp[j] = j
            elif j == 0:
                dp[i * len2_1] = i
            elif s1[i - 1] == s2[j - 1]:
                dp[i * len2_1 + j] = dp[(i - 1) * len2_1 + (j - 1)]
            else:
                dp[i * len2_1 + j] = 1 + _min3(
                    dp[(i - 1) * len2_1 + j],
                    dp[i * len2_1 + (j - 1)],
                    dp[(i - 1) * len2_1 + (j - 1)]
                )

    return dp[len1 * len2_1 + len2]


cpdef Int64Vector vector_levenshtein(StringVector a, StringVector b):
    """
    Compute Levenshtein distance for each row pair in StringVectors a and b.

    Parameters:
        a: StringVector of strings.
        b: StringVector of strings (same length as a).

    Returns:
        Int64Vector: Levenshtein distances; -1 where either input is null.
    """
    cdef DrakenVarBuffer* ap = a.ptr
    cdef DrakenVarBuffer* bp = b.ptr
    cdef Py_ssize_t n = ap.length
    cdef Py_ssize_t i
    cdef int32_t a_start, a_end, b_start, b_end

    cdef numpy.ndarray[int64_t, ndim=1] result = numpy.zeros(n, dtype=numpy.int64)
    cdef int64_t[::1] result_view = result

    for i in range(n):
        if ap.null_bitmap != NULL and not ((ap.null_bitmap[i >> 3] >> (i & 7)) & 1):
            result_view[i] = -1
            continue
        if bp.null_bitmap != NULL and not ((bp.null_bitmap[i >> 3] >> (i & 7)) & 1):
            result_view[i] = -1
            continue

        a_start = ap.offsets[i]
        a_end = ap.offsets[i + 1]
        b_start = bp.offsets[i]
        b_end = bp.offsets[i + 1]

        result_view[i] = levenshtein_bytes(
            <const uint8_t*>ap.data + a_start, a_end - a_start,
            <const uint8_t*>bp.data + b_start, b_end - b_start
        )

    return int64_from_sequence(result_view)
