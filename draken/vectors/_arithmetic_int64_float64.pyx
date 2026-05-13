# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

"""
INT64 × FLOAT64 arithmetic kernels.
Dense and constant-encoded operand variants.
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t, int32_t
from libc.math cimport isnan
from libc.string cimport memcpy, memset
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.interop.vector_sequence cimport vector_from_sequence

cdef Float64Vector _int64_float64_add_dense(
    Int64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Add int64 and float64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] + right_data[i]

    return result


cdef Float64Vector _int64_float64_subtract_dense(
    Int64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Subtract float64 from int64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] - right_data[i]

    return result


cdef Float64Vector _int64_float64_multiply_dense(
    Int64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Multiply int64 and float64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] * right_data[i]

    return result


cdef Float64Vector _int64_float64_divide_dense(
    Int64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Divide int64 by float64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0.0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>left_data[i] / right_data[i]

    return result


cdef Float64Vector _int64_float64_floordiv_dense(
    Int64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Floor divide int64 by float64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0.0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>left_data[i] // right_data[i]

    return result


cdef Float64Vector _int64_scalar_float64_add_dense(
    int64_t scalar,
    Float64Vector right,
    size_t length,
) except *:
    """Add scalar int64 (constant-encoded) to dense float64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* right_null = right.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if right_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, right_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>scalar + right_data[i]

    return result


cdef Float64Vector _int64_scalar_float64_subtract_dense(
    int64_t scalar,
    Float64Vector right,
    size_t length,
) except *:
    """Subtract dense float64 vector from scalar int64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* right_null = right.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if right_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, right_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>scalar - right_data[i]

    return result


cdef Float64Vector _int64_scalar_float64_multiply_dense(
    int64_t scalar,
    Float64Vector right,
    size_t length,
) except *:
    """Multiply scalar int64 (constant-encoded) by dense float64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* right_null = right.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if right_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, right_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>scalar * right_data[i]

    return result


cdef Float64Vector _int64_scalar_float64_divide_dense(
    int64_t scalar,
    Float64Vector right,
    size_t length,
) except *:
    """Divide scalar int64 (constant-encoded) by dense float64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* right_null = right.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if right_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, right_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            if right_data[i] == 0.0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>scalar / right_data[i]

    return result


cdef Float64Vector _int64_scalar_float64_floordiv_dense(
    int64_t scalar,
    Float64Vector right,
    size_t length,
) except *:
    """Floor divide scalar int64 (constant-encoded) by dense float64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* right_null = right.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if right_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, right_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            if right_data[i] == 0.0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>scalar // right_data[i]

    return result


# ============================================================================
# ADDITIONAL SCALAR-RIGHT KERNELS FOR MIXED TYPES
# ============================================================================

cdef Float64Vector _int64_float64_scalar_subtract_dense(
    Int64Vector left,
    double scalar,
    size_t length,
) except *:
    """Subtract scalar float64 (constant-encoded) from dense int64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] - scalar

    return result


cdef Float64Vector _int64_float64_scalar_multiply_dense(
    Int64Vector left,
    double scalar,
    size_t length,
) except *:
    """Multiply dense int64 vector by scalar float64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] * scalar

    return result


cdef Float64Vector _int64_float64_scalar_divide_dense(
    Int64Vector left,
    double scalar,
    size_t length,
) except *:
    """Divide dense int64 vector by scalar float64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    if scalar == 0.0:
        with nogil:
            for i in range(length):
                result_data[i] = 0.0
    else:
        with nogil:
            for i in range(length):
                result_data[i] = <double>left_data[i] / scalar

    return result


cdef Float64Vector _int64_float64_scalar_floordiv_dense(
    Int64Vector left,
    double scalar,
    size_t length,
) except *:
    """Floor divide dense int64 vector by scalar float64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    if scalar == 0.0:
        with nogil:
            for i in range(length):
                result_data[i] = 0.0
    else:
        with nogil:
            for i in range(length):
                result_data[i] = <double>left_data[i] // scalar

    return result


cdef Float64Vector _int64_float64_scalar_add_dense(
    Int64Vector left,
    double scalar,
    size_t length,
) except *:
    """Add dense int64 vector to scalar float64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    with nogil:
        for i in range(length):
            result_data[i] = <double>left_data[i] + scalar

    return result
