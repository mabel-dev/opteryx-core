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


cpdef BoolVector list_anyop_eq(object literal, ArrayVector column):
    """
    Draken 'literal = ANY(row)' — True iff any non-null element equals literal.

    Parameters:
        literal: scalar value to compare against each element.
        column: ArrayVector where each row is a list of elements.

    Returns:
        BoolVector: True where the ANY condition holds, False otherwise.
    """
    cdef Py_ssize_t i, n = column.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef object row, elem

    memset(dst, 0, nbytes)

    if literal is None:
        return out  # NULL literal: comparison always unknown → all False

    if isinstance(literal, str):
        literal = literal.encode('utf-8')

    for i in range(n):
        row = column[i]
        if row is None:
            continue
        for elem in row:
            if elem is not None and literal == elem:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                break

    return out
