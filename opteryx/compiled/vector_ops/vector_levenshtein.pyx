# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, int32_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.string_vector cimport StringVector
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenVarBuffer


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
    cdef int64_t total_size = (len1 + 1) * len2_1
    cdef int64_t* dp_data = <int64_t*>malloc(total_size * sizeof(int64_t))
    if dp_data == NULL:
        raise MemoryError()
    memset(dp_data, 0, total_size * sizeof(int64_t))
    cdef int64_t[::1] dp = <int64_t[:total_size]>dp_data
    cdef int32_t i, j

    try:
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
    finally:
        free(dp_data)


cpdef Int64Vector vector_levenshtein(StringVector a, StringVector b):
    """
    Compute Levenshtein distance for each row pair in StringVectors a and b.

    Parameters:
        a: StringVector of strings.
        b: StringVector of strings (same length as a).

    Returns:
        Int64Vector: Levenshtein distances; -1 where either input is null.
    """
    cdef Py_ssize_t n = a.ptr.length
    cdef Py_ssize_t i
    cdef StringRow a_row, b_row

    cdef int64_t* result_data = <int64_t*>malloc(n * sizeof(int64_t))
    if result_data == NULL:
        raise MemoryError()
    memset(result_data, 0, n * sizeof(int64_t))
    cdef int64_t[::1] result_view = <int64_t[:n]>result_data

    try:
        for i in range(n):
            a_row = string_vec_get_at(a, i)
            b_row = string_vec_get_at(b, i)

            if a_row.is_null or b_row.is_null:
                result_view[i] = -1
                continue

            result_view[i] = levenshtein_bytes(
                <const uint8_t*>a_row.data, <int32_t>a_row.length,
                <const uint8_t*>b_row.data, <int32_t>b_row.length,
            )

        return int64_from_sequence(result_view)
    finally:
        free(result_data)
