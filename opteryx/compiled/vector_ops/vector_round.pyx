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

from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.constant_vector cimport from_scalar, ConstantVector
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.core.buffers cimport DrakenDictionaryBuffer, DrakenVarBuffer
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64, DRAKEN_FLOAT32, DRAKEN_INT64, DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8


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
    cdef DictionaryVector dv
    cdef DrakenDictionaryBuffer* d_ptr
    cdef DrakenVarBuffer* dict_buf
    cdef int d_val_type
    cdef uint32_t code

    if isinstance(values, Int64Vector):
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

    elif isinstance(values, DictionaryVector):
        dv = <DictionaryVector> values
        d_ptr = dv.ptr
        dict_buf = d_ptr.dictionary_values
        d_val_type = dict_buf.type
        in_null = <uint8_t*> d_ptr.null_bitmap

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
            # Decode code index
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            # Decode value from dictionary
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = _round_to_digits((<double*>dict_buf.data)[code], digits)
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = _round_to_digits(<double>((<float*>dict_buf.data)[code]), digits)
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = _round_to_digits(<double>((<int64_t*>dict_buf.data)[code]), digits)
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = _round_to_digits(<double>((<int32_t*>dict_buf.data)[code]), digits)
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = _round_to_digits(<double>((<int16_t*>dict_buf.data)[code]), digits)
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = _round_to_digits(<double>((<int8_t*>dict_buf.data)[code]), digits)
            else:
                out_data[i] = 0.0

    else:
        raise TypeError(f"vector_round_digits: unsupported vector type {type(values)}")

    return out_vec


cpdef ConstantVector vector_round_constant(ConstantVector values, int digits):
    """ROUND(constant, digits): round a constant scalar value to a constant."""
    cdef object val = values.scalar_value()
    cdef size_t n = <size_t> len(values)

    if val is None:
        return from_scalar(None, n)
    return from_scalar(_round_to_digits(float(val), digits), n)
