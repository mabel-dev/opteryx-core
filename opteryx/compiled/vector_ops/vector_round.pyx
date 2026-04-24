# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Draken-native rounding operations.

This module provides fixed-width Draken kernels for SQL ROUND().

These kernels take:
- a fixed-width Draken vector (Float64Vector)
- a constant vector representing the number of digits

And return a fixed-width Draken vector (Float64Vector) with the rounded
results. No type inference is performed in Cython; callers must ensure the
inputs are the correct Draken vector types.
"""

from libc.math cimport round as c_round
from libc.math cimport pow as c_pow
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t, int32_t, int16_t, int8_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.scalar_constructors cimport from_scalar
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT64, DRAKEN_FLOAT32, DRAKEN_INT64, DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8


cdef inline double _round_to_digits(double value, int digits) nogil:
    cdef double scale

    if digits == 0:
        return c_round(value)
    if digits > 0:
        scale = c_pow(10.0, digits)
        return c_round(value * scale) / scale
    # digits < 0
    scale = c_pow(10.0, -digits)
    return c_round(value / scale) * scale


# Fused-type specialization for dict-encoded paths
ctypedef fused _code_t_round:
    uint8_t
    uint16_t
    uint32_t


ctypedef fused _dict_t_round:
    int8_t
    int16_t
    int32_t
    int64_t
    float
    double


cdef inline void _round_dict_no_null(
    double* out_data,
    _code_t_round* codes,
    _dict_t_round* dict_data,
    Py_ssize_t n,
    int digits,
) noexcept nogil:
    cdef Py_ssize_t i
    for i in range(n):
        out_data[i] = _round_to_digits(<double>dict_data[codes[i]], digits)


cdef inline void _round_dict_with_null(
    double* out_data,
    _code_t_round* codes,
    _dict_t_round* dict_data,
    uint8_t* nulls,
    Py_ssize_t n,
    int digits,
) noexcept nogil:
    cdef Py_ssize_t i
    for i in range(n):
        if ((nulls[i >> 3] >> (i & 7)) & 1) == 0:
            out_data[i] = 0.0
            continue
        out_data[i] = _round_to_digits(<double>dict_data[codes[i]], digits)


cdef inline void _dispatch_round_dict(
    double* out_data,
    _code_t_round* codes,
    DrakenVarBuffer* dict_buf,
    int d_val_type,
    uint8_t* nulls,
    Py_ssize_t n,
    int digits,
) noexcept nogil:
    if nulls == NULL:
        if d_val_type == DRAKEN_FLOAT64:
            _round_dict_no_null(out_data, codes, <double*>dict_buf.data, n, digits)
        elif d_val_type == DRAKEN_FLOAT32:
            _round_dict_no_null(out_data, codes, <float*>dict_buf.data, n, digits)
        elif d_val_type == DRAKEN_INT64:
            _round_dict_no_null(out_data, codes, <int64_t*>dict_buf.data, n, digits)
        elif d_val_type == DRAKEN_INT32:
            _round_dict_no_null(out_data, codes, <int32_t*>dict_buf.data, n, digits)
        elif d_val_type == DRAKEN_INT16:
            _round_dict_no_null(out_data, codes, <int16_t*>dict_buf.data, n, digits)
        elif d_val_type == DRAKEN_INT8:
            _round_dict_no_null(out_data, codes, <int8_t*>dict_buf.data, n, digits)
    else:
        if d_val_type == DRAKEN_FLOAT64:
            _round_dict_with_null(out_data, codes, <double*>dict_buf.data, nulls, n, digits)
        elif d_val_type == DRAKEN_FLOAT32:
            _round_dict_with_null(out_data, codes, <float*>dict_buf.data, nulls, n, digits)
        elif d_val_type == DRAKEN_INT64:
            _round_dict_with_null(out_data, codes, <int64_t*>dict_buf.data, nulls, n, digits)
        elif d_val_type == DRAKEN_INT32:
            _round_dict_with_null(out_data, codes, <int32_t*>dict_buf.data, nulls, n, digits)
        elif d_val_type == DRAKEN_INT16:
            _round_dict_with_null(out_data, codes, <int16_t*>dict_buf.data, nulls, n, digits)
        elif d_val_type == DRAKEN_INT8:
            _round_dict_with_null(out_data, codes, <int8_t*>dict_buf.data, nulls, n, digits)


cdef object _constant_scalar_value(object values):
    if len(values) == 0:
        return None
    return values[0]


cpdef Float64Vector vector_round(object values):
    """ROUND(values): round each element to the nearest integer."""
    return vector_round_digits(values, 0)


cpdef Float64Vector vector_round_digits(object values, int digits):
    """ROUND(values, digits): round each element to the specified number of digits."""

    cdef size_t n = <size_t> len(values)
    cdef Float64Vector out_vec = Float64Vector(n)

    cdef double* out_data = <double*> out_vec.ptr.data
    cdef uint8_t* in_null = NULL
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i

    cdef double* in_data = NULL
    cdef int64_t* in_data_i = NULL
    cdef Float64Vector fvals
    cdef Int64Vector ivals
    cdef DictAccessor* d_ptr = NULL
    cdef DrakenVarBuffer* dict_buf
    cdef int d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null = <uint8_t*> d_ptr.row_nulls

        if in_null != NULL and n > 0:
            out_null = <uint8_t*> malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        if d_val_type not in (DRAKEN_FLOAT64, DRAKEN_FLOAT32, DRAKEN_INT64, DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8):
            for i in range(n):
                out_data[i] = 0.0
        elif d_ptr.code_width == 1:
            _dispatch_round_dict[uint8_t](out_data, <uint8_t*>d_ptr.codes, dict_buf, d_val_type, in_null, <Py_ssize_t>n, digits)
        elif d_ptr.code_width == 2:
            _dispatch_round_dict[uint16_t](out_data, <uint16_t*>d_ptr.codes, dict_buf, d_val_type, in_null, <Py_ssize_t>n, digits)
        else:
            _dispatch_round_dict[uint32_t](out_data, <uint32_t*>d_ptr.codes, dict_buf, d_val_type, in_null, <Py_ssize_t>n, digits)

    elif isinstance(values, Int64Vector):
        ivals = <Int64Vector> values
        in_data_i = <int64_t*> ivals.ptr.data
        in_null = <uint8_t*> ivals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*> malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _round_to_digits(<double> in_data_i[i], digits)

    elif isinstance(values, Float64Vector):
        fvals = <Float64Vector> values
        in_data = <double*> fvals.ptr.data
        in_null = <uint8_t*> fvals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*> malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _round_to_digits(in_data[i], digits)

    else:
        raise TypeError(f"vector_round_digits: unsupported vector type {type(values)}")

    return out_vec


cpdef object vector_round_constant(object values, int digits):
    """ROUND(constant, digits): round a constant scalar value to a constant."""
    cdef object val = _constant_scalar_value(values)
    cdef size_t n = <size_t> len(values)

    if val is None:
        return from_scalar(None, n, dtype=DRAKEN_FLOAT64)
    return from_scalar(_round_to_digits(float(val), digits), n, dtype=DRAKEN_FLOAT64)
