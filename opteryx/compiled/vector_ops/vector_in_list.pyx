# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.bytes cimport PyBytes_FromStringAndSize
from cython import Py_ssize_t
from libc.stdint cimport int64_t, uint8_t, int32_t
from libc.string cimport memset

from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer

cdef BoolVector vector_in_list_int64_vector(Int64Vector vec, set values):
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


cdef BoolVector vector_in_list_string_vector(StringVector vec, set values):
    cdef Py_ssize_t i, n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef bint null_in_values
    cdef StringRow row
    cdef bytes s

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

    null_in_values = None in bytes_values

    memset(dst, 0, nbytes)

    for i in range(n):
        row = string_vec_get_at(vec, i)
        if row.is_null:
            if null_in_values:
                dst[i >> 3] |= (1 << (i & 7))
        else:
            s = PyBytes_FromStringAndSize(row.data, row.length)
            if s in bytes_values:
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


cpdef BoolVector vector_in_list(object arr, set values):
    """
    Fast membership check for "InList".

    Parameters:
        arr: Draken Vector (Int64Vector, StringVector, or generic Vector).
        values: Set of valid values.

    Returns:
        BoolVector indicating membership.
    """
    if isinstance(arr, Int64Vector):
        return vector_in_list_int64_vector(arr, values)
    if isinstance(arr, StringVector):
        return vector_in_list_string_vector(arr, values)
    if isinstance(arr, Vector):
        return _list_in_list_generic(arr, values)
    raise TypeError(f"vector_in_list requires a Draken Vector, got {type(arr)}")
