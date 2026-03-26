# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
POSITION(needle IN haystack) — vectorized 1-based position search.

Returns 1-based byte position of the first occurrence of needle in each row
of haystack; returns 0 when not found or when either side is null.

SQL semantics (SQL-92 E021-11):
    POSITION('foo' IN 'foobar')  → 1
    POSITION('baz' IN 'foobar')  → 0
"""

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.string cimport memchr, memcmp

import numpy
cimport numpy
numpy.import_array()

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer


cdef inline int64_t _find_position(const char *haystack, size_t haystack_len,
                                   const char *needle, size_t needle_len) nogil:
    """
    Return 1-based position of needle in haystack, or 0 if not found.
    Uses Boyer-Moore-Horspool for needles longer than 1 byte.
    """
    cdef size_t i
    cdef unsigned char skip[256]
    cdef size_t tail_index
    cdef char *found

    if needle_len == 0:
        return 1  # empty needle always found at position 1

    if haystack_len < needle_len:
        return 0

    # Single-byte fast path
    if needle_len == 1:
        found = <char *>memchr(haystack, needle[0], haystack_len)
        if found == NULL:
            return 0
        return <int64_t>(found - haystack) + 1

    # BMH skip table
    for i in range(256):
        skip[i] = needle_len
    for i in range(needle_len - 1):
        skip[<unsigned char>needle[i]] = needle_len - i - 1

    cdef unsigned char last_char = <unsigned char>needle[needle_len - 1]
    i = 0
    while i <= haystack_len - needle_len:
        tail_index = i + needle_len - 1
        if <unsigned char>haystack[tail_index] == last_char:
            if memcmp(&haystack[i], needle, needle_len) == 0:
                return <int64_t>i + 1
        i += skip[<unsigned char>haystack[i + needle_len - 1]]

    return 0


cpdef Int64Vector vector_position(StringVector haystack, object needle):
    """
    Vectorized POSITION(needle IN haystack).

    Parameters
    ----------
    haystack : StringVector
        The column of strings to search within.
    needle : StringVector or bytes or str
        The substring to search for. May be a per-row vector or a scalar.

    Returns
    -------
    Int64Vector
        1-based positions; 0 where not found; 0 for null rows.
    """
    cdef DrakenVarBuffer* hay_ptr = haystack.ptr
    cdef Py_ssize_t n = hay_ptr.length
    cdef uint8_t* hay_null = hay_ptr.null_bitmap
    cdef Py_ssize_t i

    cdef bint needle_is_vec = isinstance(needle, StringVector)
    cdef DrakenVarBuffer* ned_ptr = NULL
    cdef bytes needle_scalar = None

    if needle_is_vec:
        ned_ptr = (<StringVector>needle).ptr
    else:
        if isinstance(needle, bytes):
            needle_scalar = needle
        else:
            needle_scalar = str(needle).encode("utf-8")

    cdef numpy.ndarray[int64_t, ndim=1] result = numpy.zeros(n, dtype=numpy.int64)
    cdef int64_t[::1] result_view = result

    cdef int32_t hay_start, hay_end
    cdef int32_t ned_start, ned_end
    cdef const char *hay_data
    cdef const char *ned_data
    cdef size_t hay_len, ned_len
    cdef uint8_t* ned_null

    if needle_is_vec:
        ned_null = ned_ptr.null_bitmap
    else:
        ned_null = NULL

    for i in range(n):
        # null haystack row → 0
        if hay_null != NULL and not ((hay_null[i >> 3] >> (i & 7)) & 1):
            result_view[i] = 0
            continue

        hay_start = hay_ptr.offsets[i]
        hay_end = hay_ptr.offsets[i + 1]
        hay_data = <const char *>hay_ptr.data + hay_start
        hay_len = hay_end - hay_start

        if needle_is_vec:
            # null needle row → 0
            if ned_null != NULL and not ((ned_null[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0
                continue
            ned_start = ned_ptr.offsets[i]
            ned_end = ned_ptr.offsets[i + 1]
            ned_data = <const char *>ned_ptr.data + ned_start
            ned_len = ned_end - ned_start
        else:
            ned_data = <const char *>needle_scalar
            ned_len = len(needle_scalar)

        result_view[i] = _find_position(hay_data, hay_len, ned_data, ned_len)

    return int64_from_sequence(result_view)
