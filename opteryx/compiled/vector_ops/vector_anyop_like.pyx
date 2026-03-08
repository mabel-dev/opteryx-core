# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.string cimport memset

from opteryx.draken.vectors.array_vector cimport ArrayVector
from opteryx.draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector vector_anyop_like(object literal, ArrayVector column):
    """
    Draken 'literal LIKE ANY(row)' — True iff any non-null element matches the SQL LIKE pattern.

    Parameters:
        literal: SQL LIKE pattern (str or bytes).
        column: ArrayVector where each row is a list of string/bytes elements.

    Returns:
        BoolVector: True where ANY element matches the LIKE pattern, False otherwise.
    """
    import re
    from opteryx.utils.sql import sql_like_to_regex

    cdef Py_ssize_t i, n = column.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef object row, elem, pattern_str

    memset(dst, 0, nbytes)

    if literal is None:
        return out

    if isinstance(literal, bytes):
        pattern_str = literal.decode('utf-8')
    else:
        pattern_str = literal

    regex = re.compile(sql_like_to_regex(pattern_str, case_sensitive=True))

    for i in range(n):
        row = column[i]
        if row is None:
            continue
        for elem in row:
            if elem is not None:
                if isinstance(elem, bytes):
                    elem = elem.decode('utf-8')
                if regex.match(elem) is not None:
                    dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                    break

    return out


cpdef BoolVector vector_anyop_ilike(object literal, ArrayVector column):
    """
    Draken 'literal ILIKE ANY(row)' — case-insensitive version of vector_anyop_like.

    Parameters:
        literal: SQL LIKE pattern (str or bytes).
        column: ArrayVector where each row is a list of string/bytes elements.

    Returns:
        BoolVector: True where ANY element matches the ILIKE pattern, False otherwise.
    """
    import re
    from opteryx.utils.sql import sql_like_to_regex

    cdef Py_ssize_t i, n = column.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef object row, elem, pattern_str

    memset(dst, 0, nbytes)

    if literal is None:
        return out

    if isinstance(literal, bytes):
        pattern_str = literal.decode('utf-8')
    else:
        pattern_str = literal

    regex = re.compile(sql_like_to_regex(pattern_str, case_sensitive=True), re.IGNORECASE)

    for i in range(n):
        row = column[i]
        if row is None:
            continue
        for elem in row:
            if elem is not None:
                if isinstance(elem, bytes):
                    elem = elem.decode('utf-8')
                if regex.match(elem) is not None:
                    dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                    break

    return out
