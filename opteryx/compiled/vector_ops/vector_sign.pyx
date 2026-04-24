# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Draken-native SIGN kernel."""

from libc.stdint cimport uint8_t, int64_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector


cpdef Int64Vector vector_sign_int64(Int64Vector values):
    """SIGN(values): sign for Int64Vector -> Int64Vector."""

    cdef size_t n = <size_t>len(values)
    cdef Int64Vector out_vec = Int64Vector(n)
    cdef int64_t* out_data = <int64_t*>out_vec.ptr.data
    cdef int64_t* in_data = <int64_t*>values.ptr.data
    cdef uint8_t* in_null = <uint8_t*>values.ptr.null_bitmap
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i
    cdef int64_t val

    if n > 0 and in_data == NULL:
        raise ValueError("Int64Vector has NULL data pointer")

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
        out_data[i] = 1 if val > 0 else (-1 if val < 0 else 0)

    return out_vec


cpdef Int64Vector vector_sign_float64(Float64Vector values):
    """SIGN(values): sign for Float64Vector -> Int64Vector."""

    cdef size_t n = <size_t>len(values)
    cdef Int64Vector out_vec = Int64Vector(n)
    cdef int64_t* out_data = <int64_t*>out_vec.ptr.data
    cdef double* in_data = <double*>values.ptr.data
    cdef uint8_t* in_null = <uint8_t*>values.ptr.null_bitmap
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i
    cdef double val

    if n > 0 and in_data == NULL:
        raise ValueError("Float64Vector has NULL data pointer")

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
        out_data[i] = 1 if val > 0 else (-1 if val < 0 else 0)

    return out_vec


cpdef Int64Vector vector_sign(object values):
    """SIGN(values): sign dispatcher, always returns Int64Vector."""
    if isinstance(values, Int64Vector):
        return vector_sign_int64(<Int64Vector>values)
    elif isinstance(values, Float64Vector):
        return vector_sign_float64(<Float64Vector>values)
    else:
        raise TypeError(f"vector_sign: unsupported vector type {type(values)}")
