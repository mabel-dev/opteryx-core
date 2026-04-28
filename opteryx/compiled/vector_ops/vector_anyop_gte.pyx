# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.string cimport memset
from libc.stdlib cimport malloc, free

from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector vector_anyop_gte(object literal, ArrayVector vec):
    """
    Draken 'literal >= ANY(row)' — True iff any non-null element is <= literal.

    Parameters:
        literal: scalar value to compare against each element.
        vec: ArrayVector where each row is a list of elements.

    Returns:
        BoolVector: True where the ANY condition holds, False otherwise.
    """
    if isinstance(literal, str):
        literal = literal.encode('utf-8')

    cdef Py_ssize_t i, n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef bint all_valid = True
    cdef object row, elem

    memset(dst, 0, nbytes)

    if nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for i in range(n):
        row = vec[i]
        if row is None:
            all_valid = False
            continue
        if out_null != NULL:
            out_null[i >> 3] |= (<uint8_t>1 << (i & 7))
        for elem in row:
            if elem is not None and literal >= elem:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                break

    if all_valid:
        if out_null != NULL:
            free(out_null)
        out.ptr.null_bitmap = NULL
    else:
        out.ptr.null_bitmap = out_null
    return out
