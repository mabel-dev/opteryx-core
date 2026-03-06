# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cython import Py_ssize_t
from libc.stdint cimport int64_t, uint8_t, int32_t
from libc.string cimport memset

import numpy
cimport numpy
numpy.import_array()

from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.core.buffers cimport DrakenVarBuffer

cdef BoolVector list_in_list_int64_vector(Int64Vector vec, set values):
    cdef Py_ssize_t i, n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef int64_t* data = <int64_t*>vec.ptr.data
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef bint null_in_values = None in values
    cdef bint is_valid

    memset(dst, 0, nbytes)

    if nulls == NULL:
        for i in range(n):
            if data[i] in values:
                dst[i >> 3] |= (1 << (i & 7))
    else:
        for i in range(n):
            is_valid = (nulls[i >> 3] >> (i & 7)) & 1
            if is_valid:
                if data[i] in values:
                    dst[i >> 3] |= (1 << (i & 7))
            elif null_in_values:
                dst[i >> 3] |= (1 << (i & 7))

    return out


cdef BoolVector list_in_list_string_vector(StringVector vec, set values):
    cdef Py_ssize_t i, n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data

    # Normalise string values to bytes for comparison
    cdef set bytes_values = set()
    for val in values:
        if val is None:
            bytes_values.add(None)
        elif isinstance(val, bytes):
            bytes_values.add(val)
        elif isinstance(val, str):
            bytes_values.add(val.encode('utf-8'))
        else:
            bytes_values.add(val)

    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef int32_t start, end
    cdef char* data = <char*>ptr.data
    cdef uint8_t* nulls = ptr.null_bitmap
    cdef bint null_in_values = None in bytes_values
    cdef bint is_valid
    cdef bytes s

    memset(dst, 0, nbytes)

    if nulls == NULL:
        for i in range(n):
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            s = data[start:end]
            if s in bytes_values:
                dst[i >> 3] |= (1 << (i & 7))
    else:
        for i in range(n):
            is_valid = (nulls[i >> 3] >> (i & 7)) & 1
            if is_valid:
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                s = data[start:end]
                if s in bytes_values:
                    dst[i >> 3] |= (1 << (i & 7))
            elif null_in_values:
                dst[i >> 3] |= (1 << (i & 7))

    return out


cdef BoolVector _list_in_list_generic(Vector vec, set values):
    """Generic fallback via to_pylist() for Vector types without a fast path."""
    cdef Py_ssize_t i, n, nbytes
    cdef BoolVector out
    cdef uint8_t* dst

    py_list = vec.to_pylist()
    n = len(py_list)
    nbytes = (n + 7) >> 3
    out = BoolVector(<size_t>n)
    dst = <uint8_t*>out.ptr.data
    memset(dst, 0, nbytes)
    for i in range(n):
        if py_list[i] in values:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cpdef BoolVector list_in_list(object arr, set values):
    """
    Fast membership check for "InList".

    Parameters:
        arr: Draken Vector (Int64Vector, StringVector, or generic Vector).
        values: Set of valid values.

    Returns:
        BoolVector indicating membership.
    """
    if isinstance(arr, Int64Vector):
        return list_in_list_int64_vector(arr, values)
    if isinstance(arr, StringVector):
        return list_in_list_string_vector(arr, values)
    if isinstance(arr, Vector):
        return _list_in_list_generic(arr, values)
    raise TypeError(f"list_in_list requires a Draken Vector, got {type(arr)}")
