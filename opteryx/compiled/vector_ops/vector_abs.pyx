# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Draken-native ABS kernel."""

from libc.math cimport fabs as c_fabs
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t, int32_t, int16_t, int8_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from draken.core.buffers cimport DictAccessor
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.scalar_constructors cimport from_scalar
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_FLOAT64, DRAKEN_FLOAT32, DRAKEN_INT64, DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8


cpdef Int64Vector vector_abs_int64(object values):
    """ABS(values): element-wise absolute value for Int64Vector or dict-encoded variant."""

    cdef size_t n = <size_t>len(values)
    cdef Int64Vector out_vec = Int64Vector(n)
    cdef int64_t* out_data = <int64_t*>out_vec.ptr.data
    cdef uint8_t* in_null = NULL
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i
    cdef int64_t val

    cdef int64_t* in_data = NULL
    cdef DictAccessor* d_ptr = NULL
    cdef DrakenVarBuffer* dict_buf = NULL
    cdef int d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null = <uint8_t*>d_ptr.row_nulls

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
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_INT64:
                val = (<int64_t*>dict_buf.data)[code]
                out_data[i] = val if val >= 0 else -val
            elif d_val_type == DRAKEN_INT32:
                val = <int64_t>((<int32_t*>dict_buf.data)[code])
                out_data[i] = val if val >= 0 else -val
            elif d_val_type == DRAKEN_INT16:
                val = <int64_t>((<int16_t*>dict_buf.data)[code])
                out_data[i] = val if val >= 0 else -val
            elif d_val_type == DRAKEN_INT8:
                val = <int64_t>((<int8_t*>dict_buf.data)[code])
                out_data[i] = val if val >= 0 else -val
            else:
                out_data[i] = 0

    else:
        in_data = <int64_t*>(<Int64Vector>values).ptr.data
        in_null = <uint8_t*>(<Int64Vector>values).ptr.null_bitmap

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
            out_data[i] = val if val >= 0 else -val

    return out_vec


cpdef Float64Vector vector_abs_float64(object values):
    """ABS(values): element-wise absolute value for Float64Vector or dict-encoded variant."""

    cdef size_t n = <size_t>len(values)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data
    cdef uint8_t* in_null = NULL
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t i
    cdef double val

    cdef double* in_data = NULL
    cdef DictAccessor* d_ptr = NULL
    cdef DrakenVarBuffer* dict_buf = NULL
    cdef int d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null = <uint8_t*>d_ptr.row_nulls

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
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = c_fabs((<double*>dict_buf.data)[code])
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = c_fabs(<double>((<float*>dict_buf.data)[code]))
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = c_fabs(<double>((<int64_t*>dict_buf.data)[code]))
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = c_fabs(<double>((<int32_t*>dict_buf.data)[code]))
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = c_fabs(<double>((<int16_t*>dict_buf.data)[code]))
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = c_fabs(<double>((<int8_t*>dict_buf.data)[code]))
            else:
                out_data[i] = 0.0

    else:
        in_data = <double*>(<Float64Vector>values).ptr.data
        in_null = <uint8_t*>(<Float64Vector>values).ptr.null_bitmap

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
                out_data[i] = 0.0
                continue
            out_data[i] = c_fabs(in_data[i])

    return out_vec


cpdef object vector_abs_constant(object values):
    """ABS(constant): absolute value of a constant scalar."""
    cdef object val = values[0] if len(values) > 0 else None
    cdef size_t n = <size_t>len(values)

    if val is None:
        return from_scalar(None, n, dtype=DRAKEN_INT64)

    if isinstance(val, float):
        return from_scalar(c_fabs(float(val)), n, dtype=DRAKEN_FLOAT64)
    else:
        val_int = int(val)
        abs_val = val_int if val_int >= 0 else -val_int
        return from_scalar(abs_val, n, dtype=DRAKEN_INT64)


cpdef object vector_abs(object values):
    """ABS(values): element-wise absolute value - dispatcher."""
    if getattr(values, "encoding", None) == 3:
        return vector_abs_constant(values)
    elif isinstance(values, Int64Vector):
        return vector_abs_int64(values)
    elif isinstance(values, Float64Vector):
        return vector_abs_float64(values)
    elif isinstance(values, Vector):
        return vector_abs_int64(values)
    else:
        raise TypeError(f"vector_abs: unsupported vector type {type(values)}")
