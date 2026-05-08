# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
INT64 × INT64 arithmetic kernels.
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


# ============================================================================
# INT64 × INT64 DENSE KERNELS
# ============================================================================

cdef Int64Vector _int64_int64_add_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Add two dense int64 vectors (no nulls)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] + right_data[i]

    return result


cdef Int64Vector _int64_int64_subtract_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Subtract two dense int64 vectors (no nulls)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] - right_data[i]

    return result


cdef Int64Vector _int64_int64_multiply_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Multiply two dense int64 vectors (no nulls)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] * right_data[i]

    return result


cdef Float64Vector _int64_int64_divide_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Divide two dense int64 vectors (no nulls). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>left_data[i] / <double>right_data[i]

    return result


cdef Int64Vector _int64_int64_floordiv_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Floor divide two dense int64 vectors (no nulls)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0:
                result_data[i] = 0
            else:
                result_data[i] = left_data[i] // right_data[i]

    return result


cdef Int64Vector _int64_int64_modulo_dense(
    Int64Vector left,
    Int64Vector right,
    size_t length,
) except *:
    """Modulo of two dense int64 vectors (no nulls)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0:
                result_data[i] = 0
            else:
                result_data[i] = left_data[i] % right_data[i]

    return result


# ============================================================================
# INT64 × INT64 SCALAR OPERATIONS
# ============================================================================

cdef Int64Vector _int64_scalar_int64_add_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Add scalar (constant-encoded) to dense int64 vector."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = scalar + right_data[i]

    return result


cdef Int64Vector _int64_int64_add_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Add dense int64 vector to scalar (constant-encoded)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = left_data[i] + scalar

    return result


cdef Int64Vector _int64_scalar_int64_subtract_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Subtract dense int64 vector from scalar (constant-encoded)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = scalar - right_data[i]

    return result


cdef Int64Vector _int64_int64_subtract_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Subtract scalar (constant-encoded) from dense int64 vector."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = left_data[i] - scalar

    return result


cdef Int64Vector _int64_scalar_int64_multiply_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Multiply scalar (constant-encoded) by dense int64 vector."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = scalar * right_data[i]

    return result


cdef Int64Vector _int64_int64_multiply_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Multiply dense int64 vector by scalar (constant-encoded)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            result_data[i] = left_data[i] * scalar

    return result


cdef Float64Vector _int64_scalar_int64_divide_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Divide scalar (constant-encoded) by dense int64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
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
            if right_data[i] == 0:
                result_data[i] = 0.0
            else:
                result_data[i] = <double>scalar / <double>right_data[i]

    return result


cdef Float64Vector _int64_int64_divide_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Divide dense int64 vector by scalar (constant-encoded). Result is float64."""
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

    if scalar == 0:
        with nogil:
            for i in range(length):
                result_data[i] = 0.0
    else:
        with nogil:
            for i in range(length):
                result_data[i] = <double>left_data[i] / <double>scalar

    return result


cdef Int64Vector _int64_scalar_int64_floordiv_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Floor divide scalar (constant-encoded) by dense int64 vector."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            if right_data[i] == 0:
                result_data[i] = 0
            else:
                result_data[i] = scalar // right_data[i]

    return result


cdef Int64Vector _int64_int64_floordiv_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Floor divide dense int64 vector by scalar (constant-encoded)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    if scalar == 0:
        with nogil:
            for i in range(length):
                result_data[i] = 0
    else:
        with nogil:
            for i in range(length):
                result_data[i] = left_data[i] // scalar

    return result


cdef Int64Vector _int64_scalar_int64_modulo_dense(
    int64_t scalar,
    Int64Vector right,
    size_t length,
) except *:
    """Modulo scalar (constant-encoded) by dense int64 vector."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
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
            if right_data[i] == 0:
                result_data[i] = 0
            else:
                result_data[i] = scalar % right_data[i]

    return result


cdef Int64Vector _int64_int64_modulo_scalar_dense(
    Int64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Modulo dense int64 vector by scalar (constant-encoded)."""
    cdef Int64Vector result = Int64Vector(length)
    cdef int64_t* left_data = <int64_t*> left.ptr.data
    cdef int64_t* result_data = <int64_t*> result.ptr.data
    cdef uint8_t* left_null = left.ptr.null_bitmap
    cdef uint8_t* result_null = NULL
    cdef size_t i

    if left_null != NULL:
        result_null = <uint8_t*> malloc((length + 7) >> 3)
        if result_null == NULL:
            raise MemoryError()
        memcpy(result_null, left_null, (length + 7) >> 3)
        result.ptr.null_bitmap = result_null

    if scalar == 0:
        with nogil:
            for i in range(length):
                result_data[i] = 0
    else:
        with nogil:
            for i in range(length):
                result_data[i] = left_data[i] % scalar

    return result
