# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.string cimport memset

from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector vector_contains_any(ArrayVector vec, set items):
    """
    For each row in an ArrayVector, test whether any element in that row appears
    in *items*.

    Parameters:
        vec:   ArrayVector — a Draken list-typed column.
        items: set of values to test membership against.

    Returns:
        BoolVector: True at position i iff vec[i] contains at least one item.
        Null rows produce False.
    """
    cdef Py_ssize_t n = vec.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef Py_ssize_t i
    cdef object row
    cdef object elem

    memset(dst, 0, nbytes)

    if n == 0 or not items:
        return out

    for i in range(n):
        row = vec[i]
        if row is None:
            continue
        for elem in row:
            if elem in items:
                dst[i >> 3] |= (1 << (i & 7))
                break

    return out
