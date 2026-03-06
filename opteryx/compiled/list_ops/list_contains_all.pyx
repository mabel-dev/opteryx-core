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


cpdef BoolVector list_contains_all(ArrayVector vec, set items):
    """
    For each row in an ArrayVector, test whether all elements in *items* appear
    in that row.

    Parameters:
        vec:   ArrayVector — a Draken list-typed column.
        items: set of values that must all be present.

    Returns:
        BoolVector: True at position i iff every item in *items* appears in vec[i].
        Null rows produce False.
        If *items* is empty the result is trivially True for every non-null row.
    """
    cdef Py_ssize_t n = vec.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef Py_ssize_t i, nitems
    cdef object row
    cdef object elem
    cdef set found

    memset(dst, 0, nbytes)

    if n == 0:
        return out

    # Empty items set: vacuously True for every non-null row
    if not items:
        for i in range(n):
            if vec[i] is not None:
                dst[i >> 3] |= (1 << (i & 7))
        return out

    nitems = len(items)

    for i in range(n):
        row = vec[i]
        if row is None:
            continue
        found = set()
        for elem in row:
            if elem in items:
                found.add(elem)
                if len(found) == nitems:
                    dst[i >> 3] |= (1 << (i & 7))
                    break

    return out
