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
FLOAT64 × INT64 arithmetic kernels.
Dense and constant-encoded operand variants.
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t, int32_t
from libc.math cimport isnan
from libc.string cimport memcpy, memset
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.interop.vector_sequence cimport vector_from_sequence

cdef Float64Vector _float64_int64_scalar_add_dense(
    Float64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Add dense float64 vector to scalar int64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
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
            result_data[i] = left_data[i] + <double>scalar

    return result


cdef Float64Vector _float64_int64_scalar_subtract_dense(
    Float64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Subtract scalar int64 (constant-encoded) from dense float64 vector. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
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
            result_data[i] = left_data[i] - <double>scalar

    return result


cdef Float64Vector _float64_int64_scalar_multiply_dense(
    Float64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Multiply dense float64 vector by scalar int64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
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
            result_data[i] = left_data[i] * <double>scalar

    return result


cdef Float64Vector _float64_int64_scalar_divide_dense(
    Float64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Divide dense float64 vector by scalar int64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
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
                result_data[i] = left_data[i] / <double>scalar

    return result


cdef Float64Vector _float64_int64_scalar_floordiv_dense(
    Float64Vector left,
    int64_t scalar,
    size_t length,
) except *:
    """Floor divide dense float64 vector by scalar int64 (constant-encoded). Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
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
                result_data[i] = left_data[i] // <double>scalar

    return result

cdef Float64Vector _float64_scalar_int64_add_dense(
    double scalar,
    Integer64Vector right,
    size_t length,
) except *:
    """Add scalar float64 (constant-encoded) to dense int64 vector. Result is float64."""
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
            result_data[i] = scalar + <double>right_data[i]

    return result


cdef Float64Vector _float64_scalar_int64_subtract_dense(
    double scalar,
    Integer64Vector right,
    size_t length,
) except *:
    """Subtract dense int64 vector from scalar float64 (constant-encoded). Result is float64."""
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
            result_data[i] = scalar - <double>right_data[i]

    return result


cdef Float64Vector _float64_scalar_int64_multiply_dense(
    double scalar,
    Integer64Vector right,
    size_t length,
) except *:
    """Multiply scalar float64 (constant-encoded) by dense int64 vector. Result is float64."""
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
            result_data[i] = scalar * <double>right_data[i]

    return result


cdef Float64Vector _float64_scalar_int64_divide_dense(
    double scalar,
    Integer64Vector right,
    size_t length,
) except *:
    """Divide scalar float64 (constant-encoded) by dense int64 vector. Result is float64."""
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
                result_data[i] = scalar / <double>right_data[i]

    return result


cdef Float64Vector _float64_scalar_int64_floordiv_dense(
    double scalar,
    Integer64Vector right,
    size_t length,
) except *:
    """Floor divide scalar float64 (constant-encoded) by dense int64 vector. Result is float64."""
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
                result_data[i] = scalar // <double>right_data[i]

    return result

cdef Float64Vector _float64_int64_add_dense(
    Float64Vector left,
    Integer64Vector right,
    size_t length,
) except *:
    """Add float64 and int64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] + <double>right_data[i]

    return result


cdef Float64Vector _float64_int64_subtract_dense(
    Float64Vector left,
    Integer64Vector right,
    size_t length,
) except *:
    """Subtract int64 from float64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] - <double>right_data[i]

    return result


cdef Float64Vector _float64_int64_multiply_dense(
    Float64Vector left,
    Integer64Vector right,
    size_t length,
) except *:
    """Multiply float64 and int64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] * <double>right_data[i]

    return result


cdef Float64Vector _float64_int64_divide_dense(
    Float64Vector left,
    Integer64Vector right,
    size_t length,
) except *:
    """Divide float64 by int64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0:
                result_data[i] = 0.0
            else:
                result_data[i] = left_data[i] / <double>right_data[i]

    return result


cdef Float64Vector _float64_int64_floordiv_dense(
    Float64Vector left,
    Integer64Vector right,
    size_t length,
) except *:
    """Floor divide float64 by int64 vectors. Result is float64."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef int64_t* right_data = <int64_t*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0:
                result_data[i] = 0.0
            else:
                result_data[i] = left_data[i] // <double>right_data[i]

    return result
