# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Arithmetic kernels for Draken vectors (Cython implementation).

Provides optimized vector arithmetic operations:
- INT64 + INT64, FLOAT64 + FLOAT64, INT64 + FLOAT64
- Operations: Plus, Minus, Multiply, Divide, Modulo, IntegerDivide
- Handles: dense vectors without nulls only
- Returns None for vectors with nulls (fallback to Python path)

Design:
- Static dispatch (no lambdas, no function pointers)
- Direct buffer access using DrakenFixedBuffer pointers
- Simple dense-path operations
- No complex null handling to avoid memory management issues
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t, int32_t
from libc.math cimport isnan
from libc.string cimport memcpy, memset

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.interop.vector_sequence cimport vector_from_sequence

from opteryx.utils.vector_types import VectorType, is_scalar


# ============================================================================
# INT64 KERNELS: Dense path only (both operands dense, no nulls)
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
# FLOAT64 KERNELS: Dense path only (both operands dense, no nulls)
# ============================================================================

cdef Float64Vector _float64_float64_add_dense(
    Float64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Add two dense float64 vectors (no nulls)."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] + right_data[i]

    return result


cdef Float64Vector _float64_float64_subtract_dense(
    Float64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Subtract two dense float64 vectors (no nulls)."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] - right_data[i]

    return result


cdef Float64Vector _float64_float64_multiply_dense(
    Float64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Multiply two dense float64 vectors (no nulls)."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            result_data[i] = left_data[i] * right_data[i]

    return result


cdef Float64Vector _float64_float64_divide_dense(
    Float64Vector left,
    Float64Vector right,
    size_t length,
) except *:
    """Divide two dense float64 vectors (no nulls)."""
    cdef Float64Vector result = Float64Vector(length)
    cdef double* left_data = <double*> left.ptr.data
    cdef double* right_data = <double*> right.ptr.data
    cdef double* result_data = <double*> result.ptr.data
    cdef size_t i

    with nogil:
        for i in range(length):
            if right_data[i] == 0.0:
                result_data[i] = 0.0
            else:
                result_data[i] = left_data[i] / right_data[i]

    return result


# ============================================================================
# MIXED INT64/FLOAT64 KERNELS
# ============================================================================

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


cdef Float64Vector _float64_int64_add_dense(
    Float64Vector left,
    Int64Vector right,
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


# ============================================================================
# PUBLIC KERNEL FUNCTIONS
# ============================================================================

cdef inline bint _vector_length_check(object left, object right):
    """Check that both operands are vectors of matching length."""
    if len(left) != len(right):
        return False
    return True

def int64_add(left, right):
    """Add two int64 operands. Returns Int64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None  # Length mismatch
    if len(left) == 0:
        # Zero-length vector
        return Int64Vector(0)
    return _int64_int64_add_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def int64_subtract(left, right):
    """Subtract two int64 operands. Returns Int64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_int64_subtract_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def int64_multiply(left, right):
    """Multiply two int64 operands. Returns Int64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_int64_multiply_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def int64_divide(left, right):
    """Divide two int64 operands. Returns Float64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_int64_divide_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def int64_floordiv(left, right):
    """Floor divide two int64 operands. Returns Int64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_int64_floordiv_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def int64_modulo(left, right):
    """Modulo of two int64 operands. Returns Int64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_int64_modulo_dense(<Int64Vector>left, <Int64Vector>right, len(left))


def float64_add(left, right):
    """Add two float64 operands. Returns Float64Vector or None."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _float64_float64_add_dense(<Float64Vector>left, <Float64Vector>right, len(left))


def float64_subtract(left, right):
    """Subtract two float64 operands. Returns Float64Vector or None."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _float64_float64_subtract_dense(<Float64Vector>left, <Float64Vector>right, len(left))


def float64_multiply(left, right):
    """Multiply two float64 operands. Returns Float64Vector or None."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _float64_float64_multiply_dense(<Float64Vector>left, <Float64Vector>right, len(left))


def float64_divide(left, right):
    """Divide two float64 operands. Returns Float64Vector or None."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _float64_float64_divide_dense(<Float64Vector>left, <Float64Vector>right, len(left))


def int64_float64_add(left, right):
    """Add int64 and float64. Returns Float64Vector or None."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _int64_float64_add_dense(<Int64Vector>left, <Float64Vector>right, len(left))


def float64_int64_add(left, right):
    """Add float64 and int64. Returns Float64Vector or None."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    return _float64_int64_add_dense(<Float64Vector>left, <Int64Vector>right, len(left))


# ============================================================================
# KERNEL REGISTRY
# ============================================================================

from opteryx.utils.vector_types import VectorType

ARITHMETIC_KERNELS = {
    # INT64 operations
    (VectorType.INT64, VectorType.INT64, "Plus"): int64_add,
    (VectorType.INT64, VectorType.INT64, "Minus"): int64_subtract,
    (VectorType.INT64, VectorType.INT64, "Multiply"): int64_multiply,
    (VectorType.INT64, VectorType.INT64, "Divide"): int64_divide,
    (VectorType.INT64, VectorType.INT64, "MyIntegerDivide"): int64_floordiv,
    (VectorType.INT64, VectorType.INT64, "Modulo"): int64_modulo,
    # FLOAT64 operations
    (VectorType.FLOAT64, VectorType.FLOAT64, "Plus"): float64_add,
    (VectorType.FLOAT64, VectorType.FLOAT64, "Minus"): float64_subtract,
    (VectorType.FLOAT64, VectorType.FLOAT64, "Multiply"): float64_multiply,
    (VectorType.FLOAT64, VectorType.FLOAT64, "Divide"): float64_divide,
    # Mixed-type INT64/FLOAT64 operations
    (VectorType.INT64, VectorType.FLOAT64, "Plus"): int64_float64_add,
    (VectorType.FLOAT64, VectorType.INT64, "Plus"): float64_int64_add,
}


def get_arithmetic_kernel(left_type, right_type, operator):
    """
    Retrieve arithmetic kernel function for the given operand types and operator.

    Args:
        left_type: VectorType of left operand
        right_type: VectorType of right operand
        operator: Operator name (e.g., "Plus", "Minus", "Multiply", "Divide")

    Returns:
        Kernel function callable or None if no kernel exists for this combination
    """
    return ARITHMETIC_KERNELS.get((left_type, right_type, operator))
