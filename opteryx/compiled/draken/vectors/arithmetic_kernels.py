"""
Arithmetic kernels for Draken vectors (Phase 4.5).

Pure-Python arithmetic functions for Draken vector operations without recompilation.
Handles vector-vector, vector-scalar, and scalar-vector combinations with proper
null propagation.

Design:
- Kernels operate on values from vectors via iteration and indexing
- Null propagation: if either operand is None, result is None
- Results are converted to Draken vectors using vector_from_arrow()
- No NumPy dependency; PyArrow used only for type conversion
"""

from opteryx.utils.vector_types import VectorType, is_scalar


def _compute_result_with_null_propagation(left, right, operator_func):
    """
    Compute binary operation with null propagation.

    Args:
        left: Vector or scalar operand
        right: Vector or scalar operand
        operator_func: Function(left_val, right_val) -> result_val

    Returns:
        List of result values (None where operands were None)
    """
    # Determine length and handle scalar cases
    if is_scalar(left) and is_scalar(right):
        # Both scalars - compute once
        if left is None or right is None:
            return None  # Will be handled by dispatcher
        return operator_func(left, right)

    if is_scalar(left):
        length = len(right)
        right_nulls = right.is_null()
        if left is None:
            return [None] * length
        return [None if right_nulls[i] else operator_func(left, right[i]) for i in range(length)]

    if is_scalar(right):
        length = len(left)
        left_nulls = left.is_null()
        if right is None:
            return [None] * length
        return [None if left_nulls[i] else operator_func(left[i], right) for i in range(length)]

    # Both are vectors
    length = len(left)
    left_nulls = left.is_null()
    right_nulls = right.is_null()
    return [
        None if (left_nulls[i] or right_nulls[i]) else operator_func(left[i], right[i])
        for i in range(length)
    ]


def _make_vector_from_result(result, vector_type):
    """Convert result list to Draken vector."""
    from opteryx.compiled.draken.interop.arrow import vector_from_sequence
    from opteryx.types import OrsoTypes

    # Map VectorType to OrsoTypes for Draken vector construction
    type_map = {
        VectorType.INT64: OrsoTypes.INTEGER,
        VectorType.FLOAT64: OrsoTypes.DOUBLE,
        VectorType.BOOL: OrsoTypes.BOOLEAN,
    }

    if vector_type not in type_map:
        raise ValueError(f"Unsupported result type: {vector_type}")

    # Use Draken's vector_from_sequence to create typed vector without PyArrow
    return vector_from_sequence(result, type_map[vector_type])


# ============================================================================
# INT64 KERNELS
# ============================================================================


def int64_add(left, right):
    """Add two int64 operands. Result is Int64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a + b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.INT64)


def int64_subtract(left, right):
    """Subtract two int64 operands. Result is Int64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a - b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.INT64)


def int64_multiply(left, right):
    """Multiply two int64 operands. Result is Int64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a * b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.INT64)


def int64_divide(left, right):
    """Divide two int64 operands. Result is Float64Vector (true division)."""

    def safe_divide(a, b):
        if b == 0:
            return None
        return a / b

    result = _compute_result_with_null_propagation(left, right, safe_divide)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def int64_floordiv(left, right):
    """Floor division of two int64 operands. Result is Int64Vector."""

    def safe_floordiv(a, b):
        if b == 0:
            return None
        return a // b

    result = _compute_result_with_null_propagation(left, right, safe_floordiv)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.INT64)


def int64_modulo(left, right):
    """Modulo of two int64 operands. Result is Int64Vector."""

    def safe_modulo(a, b):
        if b == 0:
            return None
        return a % b

    result = _compute_result_with_null_propagation(left, right, safe_modulo)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.INT64)


# ============================================================================
# FLOAT64 KERNELS
# ============================================================================


def float64_add(left, right):
    """Add two float64 operands. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a + b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def float64_subtract(left, right):
    """Subtract two float64 operands. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a - b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def float64_multiply(left, right):
    """Multiply two float64 operands. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a * b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def float64_divide(left, right):
    """Divide two float64 operands. Result is Float64Vector."""

    def safe_divide(a, b):
        if b == 0:
            return None
        return a / b

    result = _compute_result_with_null_propagation(left, right, safe_divide)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


# ============================================================================
# MIXED-TYPE KERNELS (INT64 + FLOAT64)
# ============================================================================


def int64_float64_add(left, right):
    """Add int64 and float64. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a + b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def int64_float64_subtract(left, right):
    """Subtract float64 from int64. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a - b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def int64_float64_multiply(left, right):
    """Multiply int64 and float64. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a * b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def int64_float64_divide(left, right):
    """Divide int64 by float64. Result is Float64Vector."""

    def safe_divide(a, b):
        if b == 0:
            return None
        return a / b

    result = _compute_result_with_null_propagation(left, right, safe_divide)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def float64_int64_add(left, right):
    """Add float64 and int64. Result is Float64Vector."""
    return int64_float64_add(left, right)


def float64_int64_subtract(left, right):
    """Subtract int64 from float64. Result is Float64Vector."""
    result = _compute_result_with_null_propagation(left, right, lambda a, b: a - b)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


def float64_int64_multiply(left, right):
    """Multiply float64 and int64. Result is Float64Vector."""
    return int64_float64_multiply(left, right)


def float64_int64_divide(left, right):
    """Divide float64 by int64. Result is Float64Vector."""

    def safe_divide(a, b):
        if b == 0:
            return None
        return a / b

    result = _compute_result_with_null_propagation(left, right, safe_divide)
    if result is None or is_scalar(result):
        return result
    return _make_vector_from_result(result, VectorType.FLOAT64)


# ============================================================================
# KERNEL REGISTRY
# ============================================================================

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
    (VectorType.FLOAT64, VectorType.INT64, "Plus"): float64_int64_add,
    (VectorType.FLOAT64, VectorType.INT64, "Minus"): float64_int64_subtract,
    (VectorType.FLOAT64, VectorType.INT64, "Multiply"): float64_int64_multiply,
    (VectorType.FLOAT64, VectorType.INT64, "Divide"): float64_int64_divide,
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
