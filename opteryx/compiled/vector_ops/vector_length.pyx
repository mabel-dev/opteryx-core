# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free

from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.compiled.draken.core.buffers cimport DrakenArrayBuffer


cpdef Int64Vector vector_length(ArrayVector vec):
    """
    Compute the length (number of elements) of each row in an ArrayVector.

    Parameters:
        vec: ArrayVector of lists.

    Returns:
        Int64Vector: element counts per row (0 for null rows).
    """
    cdef DrakenArrayBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef int64_t* result_ptr
    cdef int64_t[::1] result_view

    # Allocate result buffer with malloc
    result_ptr = <int64_t*>malloc(n * sizeof(int64_t))
    if result_ptr == NULL:
        raise MemoryError("Failed to allocate memory for result array")

    try:
        result_view = <int64_t[:n]>result_ptr

        for i in range(n):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0
            else:
                result_view[i] = ptr.offsets[i + 1] - ptr.offsets[i]

        return int64_from_sequence(result_view)
    finally:
        free(result_ptr)
