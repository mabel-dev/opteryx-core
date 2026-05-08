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
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.interop.vector_sequence cimport vector_from_sequence

from opteryx.utils.vector_types import VectorType, is_scalar

# Import cdef kernels from type-pair modules
from draken.vectors._arithmetic_int64_int64 cimport (
    _int64_int64_add_dense,
    _int64_int64_subtract_dense,
    _int64_int64_multiply_dense,
    _int64_int64_divide_dense,
    _int64_int64_floordiv_dense,
    _int64_int64_modulo_dense,
    _int64_scalar_int64_add_dense,
    _int64_int64_add_scalar_dense,
    _int64_scalar_int64_subtract_dense,
    _int64_int64_subtract_scalar_dense,
    _int64_scalar_int64_multiply_dense,
    _int64_int64_multiply_scalar_dense,
    _int64_scalar_int64_divide_dense,
    _int64_int64_divide_scalar_dense,
    _int64_scalar_int64_floordiv_dense,
    _int64_int64_floordiv_scalar_dense,
    _int64_scalar_int64_modulo_dense,
    _int64_int64_modulo_scalar_dense,
)

from draken.vectors._arithmetic_float64_float64 cimport (
    _float64_scalar_float64_add_dense,
    _float64_float64_add_scalar_dense,
    _float64_scalar_float64_subtract_dense,
    _float64_float64_subtract_scalar_dense,
    _float64_scalar_float64_multiply_dense,
    _float64_float64_multiply_scalar_dense,
    _float64_scalar_float64_divide_dense,
    _float64_float64_divide_scalar_dense,
    _float64_float64_add_dense,
    _float64_float64_subtract_dense,
    _float64_float64_multiply_dense,
    _float64_float64_divide_dense,
)

from draken.vectors._arithmetic_int64_float64 cimport (
    _int64_float64_add_dense,
    _int64_float64_subtract_dense,
    _int64_float64_multiply_dense,
    _int64_float64_divide_dense,
    _int64_float64_floordiv_dense,
    _int64_scalar_float64_add_dense,
    _int64_scalar_float64_subtract_dense,
    _int64_scalar_float64_multiply_dense,
    _int64_scalar_float64_divide_dense,
    _int64_scalar_float64_floordiv_dense,
    _int64_float64_scalar_subtract_dense,
    _int64_float64_scalar_multiply_dense,
    _int64_float64_scalar_divide_dense,
    _int64_float64_scalar_floordiv_dense,
    _int64_float64_scalar_add_dense,
)

from draken.vectors._arithmetic_float64_int64 cimport (
    _float64_int64_scalar_add_dense,
    _float64_int64_scalar_subtract_dense,
    _float64_int64_scalar_multiply_dense,
    _float64_int64_scalar_divide_dense,
    _float64_int64_scalar_floordiv_dense,
    _float64_scalar_int64_add_dense,
    _float64_scalar_int64_subtract_dense,
    _float64_scalar_int64_multiply_dense,
    _float64_scalar_int64_divide_dense,
    _float64_scalar_int64_floordiv_dense,
    _float64_int64_add_dense,
    _float64_int64_subtract_dense,
    _float64_int64_multiply_dense,
    _float64_int64_divide_dense,
    _float64_int64_floordiv_dense,
)

# ============================================================================
# PUBLIC KERNEL FUNCTIONS
# ============================================================================

cdef inline bint _vector_length_check(object left, object right):
    """Check that both operands are vectors of matching length."""
    if len(left) != len(right):
        return False
    return True

def int64_add(left, right):
    """Add two int64 operands. Returns Int64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Int64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_add_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_add_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_add_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = left_vec._const_value + right_vec._const_value
        return Int64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_subtract(left, right):
    """Subtract two int64 operands. Returns Int64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Int64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_subtract_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_subtract_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_subtract_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = left_vec._const_value - right_vec._const_value
        return Int64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_multiply(left, right):
    """Multiply two int64 operands. Returns Int64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Int64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_multiply_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_multiply_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_multiply_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = left_vec._const_value * right_vec._const_value
        return Int64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_divide(left, right):
    """Divide two int64 operands. Returns Float64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_divide_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_divide_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_divide_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        if right_vec._const_value == 0:
            result_val = 0.0
        else:
            result_val = <double>left_vec._const_value / <double>right_vec._const_value
        from draken.vectors.float64_vector import Float64Vector as FV
        return FV.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_floordiv(left, right):
    """Floor divide two int64 operands. Returns Int64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Int64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_floordiv_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_floordiv_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_floordiv_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        if right_vec._const_value == 0:
            result_val = 0
        else:
            result_val = left_vec._const_value // right_vec._const_value
        return Int64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_modulo(left, right):
    """Modulo of two int64 operands. Returns Int64Vector or None. Handles dense and constant encodings."""
    # Handle Python scalar operands by converting to constant-encoded vectors
    if isinstance(left, int) and not isinstance(left, bool):
        if isinstance(right, Int64Vector):
            left = Int64Vector.from_constant(<int64_t>left, len(right))
        else:
            return None
    if isinstance(right, int) and not isinstance(right, bool):
        if isinstance(left, Int64Vector):
            right = Int64Vector.from_constant(<int64_t>right, len(left))
        else:
            return None

    if not (isinstance(left, Int64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Int64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_int64_modulo_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _int64_scalar_int64_modulo_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _int64_int64_modulo_scalar_dense(left_vec, const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        if right_vec._const_value == 0:
            result_val = 0
        else:
            result_val = left_vec._const_value % right_vec._const_value
        return Int64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def float64_add(left, right):
    """Add two float64 operands. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _float64_float64_add_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        # Note: const_value is int64, need to cast to double
        return _float64_scalar_float64_add_dense(<double>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _float64_float64_add_scalar_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = <double>left_vec._const_value + <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def float64_subtract(left, right):
    """Subtract two float64 operands. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _float64_float64_subtract_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _float64_scalar_float64_subtract_dense(<double>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _float64_float64_subtract_scalar_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = <double>left_vec._const_value - <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def float64_multiply(left, right):
    """Multiply two float64 operands. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    # Detect encodings
    cdef int left_enc = left_vec._encoding
    cdef int right_enc = right_vec._encoding

    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _float64_float64_multiply_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _float64_scalar_float64_multiply_dense(<double>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _float64_float64_multiply_scalar_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = <double>left_vec._const_value * <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def float64_divide(left, right):
    """Divide two float64 operands. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _float64_float64_divide_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left constant, right dense
        const_val = left_vec._const_value
        return _float64_scalar_float64_divide_dense(<double>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left dense, right constant
        const_val = right_vec._const_value
        return _float64_float64_divide_scalar_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        if right_vec._const_value == 0:
            result_val = 0.0
        else:
            result_val = <double>left_vec._const_value / <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_float64_add(left, right):
    """Add int64 and float64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _int64_float64_add_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left int64 constant, right float64 dense
        const_val = left_vec._const_value
        return _int64_scalar_float64_add_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left int64 dense, right float64 constant
        const_val_f64 = right_vec._const_value
        return _float64_int64_scalar_add_dense(left_vec, <int64_t>const_val_f64, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = <double>left_vec._const_value + right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def float64_int64_add(left, right):
    """Add float64 and int64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    # Detect encodings
    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    # Route to handler based on encoding pair
    if left_enc == 0 and right_enc == 0:
        # Both dense
        return _float64_int64_add_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        # Left float64 constant, right int64 dense
        const_val = left_vec._const_value
        return _float64_scalar_int64_add_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        # Left float64 dense, right int64 constant
        const_val_i64 = right_vec._const_value
        return _float64_int64_scalar_add_dense(left_vec, const_val_i64, length)
    elif left_enc == 3 and right_enc == 3:
        # Both constant
        result_val = left_vec._const_value + <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        # Unsupported encoding combination
        return None


def int64_float64_subtract(left, right):
    """Subtract float64 from int64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _int64_float64_subtract_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _int64_scalar_float64_subtract_dense(<int64_t>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val = right_vec._const_value
        return _int64_float64_scalar_subtract_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        result_val = <double>left_vec._const_value - right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def int64_float64_multiply(left, right):
    """Multiply int64 and float64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _int64_float64_multiply_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _int64_scalar_float64_multiply_dense(<int64_t>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val = right_vec._const_value
        return _int64_float64_scalar_multiply_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        result_val = <double>left_vec._const_value * right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def int64_float64_divide(left, right):
    """Divide int64 by float64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _int64_float64_divide_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _int64_scalar_float64_divide_dense(<int64_t>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val = right_vec._const_value
        return _int64_float64_scalar_divide_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        if right_vec._const_value == 0.0:
            result_val = 0.0
        else:
            result_val = <double>left_vec._const_value / right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def int64_float64_floordiv(left, right):
    """Floor divide int64 by float64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Int64Vector) and isinstance(right, Float64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Int64Vector left_vec = <Int64Vector>left
    cdef Float64Vector right_vec = <Float64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _int64_float64_floordiv_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _int64_scalar_float64_floordiv_dense(<int64_t>const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val = right_vec._const_value
        return _int64_float64_scalar_floordiv_dense(left_vec, <double>const_val, length)
    elif left_enc == 3 and right_enc == 3:
        if right_vec._const_value == 0.0:
            result_val = 0.0
        else:
            result_val = <double>left_vec._const_value // right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def float64_int64_subtract(left, right):
    """Subtract int64 from float64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _float64_int64_subtract_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _float64_scalar_int64_subtract_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val_i64 = right_vec._const_value
        return _float64_int64_scalar_subtract_dense(left_vec, const_val_i64, length)
    elif left_enc == 3 and right_enc == 3:
        result_val = left_vec._const_value - <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


# No scalar-left kernel exists for float64 vector / int64 scalar, so we don't need special handling


def float64_int64_multiply(left, right):
    """Multiply float64 and int64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _float64_int64_multiply_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _float64_scalar_int64_multiply_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val_i64 = right_vec._const_value
        return _float64_int64_scalar_multiply_dense(left_vec, const_val_i64, length)
    elif left_enc == 3 and right_enc == 3:
        result_val = left_vec._const_value * <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def float64_int64_divide(left, right):
    """Divide float64 by int64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _float64_int64_divide_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _float64_scalar_int64_divide_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val_i64 = right_vec._const_value
        return _float64_int64_scalar_divide_dense(left_vec, const_val_i64, length)
    elif left_enc == 3 and right_enc == 3:
        if right_vec._const_value == 0:
            result_val = 0.0
        else:
            result_val = left_vec._const_value / <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


def float64_int64_floordiv(left, right):
    """Floor divide float64 by int64. Returns Float64Vector or None. Handles dense and constant encodings."""
    if not (isinstance(left, Float64Vector) and isinstance(right, Int64Vector)):
        return None
    if len(left) != len(right):
        return None
    if len(left) == 0:
        return Float64Vector(0)

    left_enc = getattr(left, 'encoding', 0)
    right_enc = getattr(right, 'encoding', 0)
    cdef Float64Vector left_vec = <Float64Vector>left
    cdef Int64Vector right_vec = <Int64Vector>right
    cdef size_t length = len(left)

    if left_enc == 0 and right_enc == 0:
        return _float64_int64_floordiv_dense(left_vec, right_vec, length)
    elif left_enc == 3 and right_enc == 0:
        const_val = left_vec._const_value
        return _float64_scalar_int64_floordiv_dense(const_val, right_vec, length)
    elif left_enc == 0 and right_enc == 3:
        const_val_i64 = right_vec._const_value
        return _float64_int64_scalar_floordiv_dense(left_vec, const_val_i64, length)
    elif left_enc == 3 and right_enc == 3:
        if right_vec._const_value == 0:
            result_val = 0.0
        else:
            result_val = left_vec._const_value // <double>right_vec._const_value
        return Float64Vector.from_constant(result_val, length)
    else:
        return None


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
    (VectorType.INT64, VectorType.FLOAT64, "Minus"): int64_float64_subtract,
    (VectorType.INT64, VectorType.FLOAT64, "Multiply"): int64_float64_multiply,
    (VectorType.INT64, VectorType.FLOAT64, "Divide"): int64_float64_divide,
    (VectorType.INT64, VectorType.FLOAT64, "MyIntegerDivide"): int64_float64_floordiv,
    (VectorType.FLOAT64, VectorType.INT64, "Plus"): float64_int64_add,
    (VectorType.FLOAT64, VectorType.INT64, "Minus"): float64_int64_subtract,
    (VectorType.FLOAT64, VectorType.INT64, "Multiply"): float64_int64_multiply,
    (VectorType.FLOAT64, VectorType.INT64, "Divide"): float64_int64_divide,
    (VectorType.FLOAT64, VectorType.INT64, "MyIntegerDivide"): float64_int64_floordiv,
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
