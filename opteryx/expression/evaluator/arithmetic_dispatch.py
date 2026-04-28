"""Arithmetic operation dispatch for Draken vectors.

Architecture:
- VectorType-based routing to kernel registry
- Only processes Draken vectors (at least one operand must be Draken)
- Materialize encoded vectors (DICTIONARY_ENCODED, CONSTANT_ENCODED) to base types
- Kernels handle null propagation and type coercion at Python level

Kernels:
- int64_add, int64_subtract, int64_multiply, int64_divide, int64_modulo
- float64_add, float64_subtract, float64_multiply, float64_divide
- mixed-type kernels for int64/float64 combinations
"""

from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


def call_arithmetic_op(op, left, right):
    """
    Execute arithmetic operation with VectorType-based dispatch.

    Routes to native Draken kernels when both operands are Draken vectors
    (or scalar with Draken vector). Materializes encoded vectors to dense form.

    Parameters:
        op: str - Operator ('Plus', 'Minus', 'Multiply', 'Divide', etc.)
        left: Operand (Draken vector, PyArrow array, or scalar)
        right: Operand (Draken vector, PyArrow array, or scalar)

    Returns:
        Result (Draken vector) or None if kernel not found
    """
    left_type_before = get_vector_type(left) if is_draken_vector(left) else None
    right_type_before = get_vector_type(right) if is_draken_vector(right) else None

    # Materialize DICTIONARY_ENCODED and CONSTANT_ENCODED vectors to their base types
    # The kernel registry only has handlers for INT64, FLOAT64, STRING, etc.
    if left_type_before in (VectorType.DICTIONARY_ENCODED, VectorType.CONSTANT_ENCODED):
        left = type(left).from_arrow(left.to_arrow())

    if right_type_before in (VectorType.DICTIONARY_ENCODED, VectorType.CONSTANT_ENCODED):
        right = type(right).from_arrow(right.to_arrow())

    # Only process if at least one operand is a Draken vector
    left_is_draken = is_draken_vector(left)
    right_is_draken = is_draken_vector(right)

    if not (left_is_draken or right_is_draken):
        # Both are scalars — delegate to binary_operations
        return None

    if left_is_draken and is_draken_vector(right):
        # Both are Draken vectors - use kernels
        left_type = get_vector_type(left)
        right_type = get_vector_type(right)
    elif left_is_draken and not right_is_draken:
        # Left is Draken, right is scalar or non-Draken
        left_type = get_vector_type(left)
        right_type = get_vector_type(right) if not is_scalar(right) else left_type
    elif right_is_draken and not left_is_draken:
        # Right is Draken, left is scalar or non-Draken
        right_type = get_vector_type(right)
        left_type = get_vector_type(left) if not is_scalar(left) else right_type
    else:
        # Mixed scalars/non-Draken — delegate
        return None

    # Try to find a kernel for this combination
    from draken.vectors.arithmetic_kernels import get_arithmetic_kernel

    kernel = get_arithmetic_kernel(left_type, right_type, op)
    if kernel is None:
        # No kernel implemented for this combination - fall back to binary_operations
        return None

    try:
        # Call the kernel
        result = kernel(left, right)
        return result
    except (TypeError, ValueError, AttributeError):
        # Kernel failed (e.g., type mismatch) - fall back to binary_operations
        return None
