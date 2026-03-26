# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector


cpdef list vector_get_element(ArrayVector vec, int key):
    """
    Extract element at index 'key' from each row of an ArrayVector.

    Parameters:
        vec: ArrayVector of lists.
        key: zero-based index to retrieve.

    Returns:
        Python list of extracted elements (None for nulls or out-of-range rows).
    """
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef object row
    cdef list result = [None] * n

    for i in range(n):
        row = vec[i]
        if row is not None and len(row) > key:
            result[i] = row[key]

    return result
