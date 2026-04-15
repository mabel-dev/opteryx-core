"""Arithmetic operation dispatch for Draken vectors.

Phase 4.5 implementation: Uses native Python arithmetic kernels for Draken vectors.

Architecture:
- VectorType-based routing to kernel registry
- Only processes Draken vectors (at least one operand must be Draken)
- Falls back to PyArrow/numpy for unsupported combinations via binary_operations()
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

    Phase 4.5: Routes to native Draken kernels when both operands are
    Draken vectors (or scalar with Draken vector). Falls back to None
    for PyArrow arrays, which triggers binary_operations() path.

    Parameters:
        op: str - Operator ('Plus', 'Minus', 'Multiply', 'Divide', etc.)
        left: Operand (Draken vector, PyArrow array, or scalar)
        right: Operand (Draken vector, PyArrow array, or scalar)

    Returns:
        Result (Draken vector) or None to trigger fallback to binary_operations()
    """
    # Only process if at least one operand is a Draken vector
    left_is_draken = is_draken_vector(left)
    right_is_draken = is_draken_vector(right)

    if not (left_is_draken or right_is_draken):
        # Both are scalars or PyArrow - delegate to binary_operations
        return None

    if left_is_draken and is_draken_vector(right):
        # Both are Draken vectors - use kernels
        left_type = get_vector_type(left)
        right_type = get_vector_type(right)
    elif left_is_draken and not right_is_draken:
        # Left is Draken, right is scalar or non-Draken (Arrow during transition)
        left_type = get_vector_type(left)
        right_type = get_vector_type(right) if not is_scalar(right) else left_type
    elif right_is_draken and not left_is_draken:
        # Right is Draken, left is scalar or non-Draken (Arrow during transition)
        right_type = get_vector_type(right)
        left_type = get_vector_type(left) if not is_scalar(left) else right_type
    else:
        # Mixed PyArrow/scalar, no Draken - delegate
        return None

    # Try to find a kernel for this combination
    from opteryx.compiled.draken.vectors.arithmetic_kernels import get_arithmetic_kernel

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
