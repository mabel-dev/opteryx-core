# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.string cimport memset

from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector vector_allop_eq(object literal, ArrayVector vec):
    """
    Draken 'literal = ALL(row)' — True iff all non-null elements equal literal. Null/empty row → False.

    Parameters:
        literal: scalar value to compare against each element.
        vec: ArrayVector where each row is a list of elements.

    Returns:
        BoolVector: True where the ALL condition holds, False otherwise.
    """
    cdef Py_ssize_t i, n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef object row, elem
    cdef bint all_match

    memset(dst, 0, nbytes)

    if literal is None:
        return out  # NULL literal: comparison always unknown → all False

    if isinstance(literal, str):
        literal = literal.encode('utf-8')

    for i in range(n):
        row = vec[i]
        if row is None or len(row) == 0:
            continue  # null / empty row -> False
        all_match = True
        for elem in row:
            if elem is None or literal != elem:
                all_match = False
                break
        if all_match:
            dst[i >> 3] |= (<uint8_t>1 << (i & 7))

    return out
