# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Draken-native ABS kernel."""

from libc.math cimport fabs as c_fabs
from libc.stdint cimport uint8_t, int64_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector


cpdef Int64Vector vector_abs_int64(Int64Vector values):
    """ABS(values): element-wise absolute value for Int64Vector."""

    cdef size_t n = <size_t>len(values)
    cdef Int64Vector out_vec = Int64Vector(n)
    cdef int64_t* out_data = <int64_t*>out_vec.ptr.data
    cdef int64_t* in_data = <int64_t*>values.ptr.data
    cdef uint8_t* in_null = <uint8_t*>values.ptr.null_bitmap
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i
    cdef int64_t val

    if in_null != NULL and n > 0:
        out_null = <uint8_t*>malloc((n + 7) >> 3)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, in_null, (n + 7) >> 3)
        out_vec.ptr.null_bitmap = out_null

    for i in range(n):
        if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
            out_data[i] = 0
            continue
        val = in_data[i]
        out_data[i] = val if val >= 0 else -val

    return out_vec


cpdef Float64Vector vector_abs_float64(Float64Vector values):
    """ABS(values): element-wise absolute value for Float64Vector."""

    cdef size_t n = <size_t>len(values)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data
    cdef double* in_data = <double*>values.ptr.data
    cdef uint8_t* in_null = <uint8_t*>values.ptr.null_bitmap
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i

    if in_null != NULL and n > 0:
        out_null = <uint8_t*>malloc((n + 7) >> 3)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, in_null, (n + 7) >> 3)
        out_vec.ptr.null_bitmap = out_null

    for i in range(n):
        if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
            out_data[i] = 0.0
            continue
        out_data[i] = c_fabs(in_data[i])

    return out_vec


cpdef object vector_abs(object values):
    """ABS(values): element-wise absolute value - dispatcher."""
    if isinstance(values, Int64Vector):
        return vector_abs_int64(<Int64Vector>values)
    elif isinstance(values, Float64Vector):
        return vector_abs_float64(<Float64Vector>values)
    else:
        raise TypeError(f"vector_abs: unsupported vector type {type(values)}")
