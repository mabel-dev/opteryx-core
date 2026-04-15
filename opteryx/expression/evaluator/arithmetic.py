"""Binary arithmetic operations.

Refactored in Phase 4.4 to use VectorType-based dispatch, eliminating
__class__.__name__ anti-patterns and improving maintainability.

Architecture:
- Uses arithmetic_dispatch module for centralized routing
- Delegates to VectorType discriminator (from utils/vector_types.py)
- Maintains backward compatibility while preparing for Phase 4.5 (native Draken)

Operations:
- Binary arithmetic: Plus, Minus, Multiply, Divide, Modulo, MyIntegerDivide
- Bitwise: BitwiseOr, BitwiseAnd, BitwiseXor, ShiftLeft, ShiftRight
- Temporal: Date operations (Plus, Minus with intervals)
- String: Concatenation
"""

import datetime

import pyarrow as _pa

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type

from .arithmetic_dispatch import call_arithmetic_op
from .comparisons import _DATE_TYPES, _INTERVAL_TYPES
from .function_execution import _is_draken_vector, apply_bounded_function
from .temporal_ops import _date_interval_op_draken, _date_minus_date_draken
from .type_coercion import (
    _coerce_date32,
    _coerce_date32_set,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_interval,
    _coerce_str,
    _coerce_str_set,
    _coerce_temporal_scalar_for_arrow,
    _coerce_timestamp,
    _coerce_timestamp_set,
    _constant_scalar_value,
    _dictionary_arrow_type,
    _dictionary_compare_vector,
    _is_constant_vector_like,
    _is_dictionary_encoded_vector,
    _is_null_as_boolvector,
    _is_typed_constant_encoded_vector,
)

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)

_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


def _eval_binary_op_draken(node, morsel):
    """
    Evaluate binary operations in Draken evaluator.

    Refactored in Phase 4.4 to use centralized arithmetic dispatch and
    VectorType-based type discrimination.

    Parameters:
        node: Expression AST node with value (operator), left, right
        morsel: Current data morsel being evaluated

    Returns:
        Result vector (Draken vector, PyArrow array, or scalar)

    Flow:
        1. Evaluate left and right operands
        2. Handle temporal scalar conversions (DATE/TIMESTAMP)
        3. Check for date-specific operations (Minus, Plus)
        4. Delegate to arithmetic_dispatch via call_arithmetic_op()
        5. Convert result to Draken vector if needed
    """
    from .evaluation import _eval_value

    op = node.value
    left = _eval_value(node.left, morsel)
    right = _eval_value(node.right, morsel)

    # ===================================================================
    # TEMPORAL SCALAR HANDLING (preserves existing logic)
    # ===================================================================
    # Convert temporal scalars to vectors (matching length of morsel)
    from opteryx.types import OrsoTypes

    if not hasattr(left, "null_count") and node.left.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arrow_type = (
            _pa.date32() if node.left.schema_column.type == OrsoTypes.DATE else _pa.timestamp("us")
        )
        scalar = _coerce_temporal_scalar_for_arrow(left, node.left.schema_column.type)
        left = vector_from_arrow(_pa.array([scalar] * morsel.num_rows, type=arrow_type))

    if not hasattr(right, "null_count") and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arrow_type = (
            _pa.date32() if node.right.schema_column.type == OrsoTypes.DATE else _pa.timestamp("us")
        )
        scalar = _coerce_temporal_scalar_for_arrow(right, node.right.schema_column.type)
        right = vector_from_arrow(_pa.array([scalar] * morsel.num_rows, type=arrow_type))

    # ===================================================================
    # DATE-SPECIFIC OPERATIONS (refactored to use VectorType discriminator)
    # ===================================================================
    # Phase 4.4: Replace __class__.__name__ checks with VectorType enum
    left_type = get_vector_type(left)
    right_type = get_vector_type(right)

    left_is_date = left_type in (VectorType.DATE32, VectorType.TIMESTAMP)
    right_is_date = right_type in (VectorType.DATE32, VectorType.TIMESTAMP)

    if op == "Minus" and left_is_date and right_is_date:
        return _date_minus_date_draken(left, right)

    if op in ("Plus", "Minus"):
        left_is_interval = left_type == VectorType.INTERVAL
        right_is_interval = right_type == VectorType.INTERVAL

        if left_is_date and right_is_interval:
            return _date_interval_op_draken(left, right, op)
        if left_is_interval and right_is_date:
            return _date_interval_op_draken(right, left, op)

    # ===================================================================
    # GENERAL ARITHMETIC OPERATIONS (Phase 4.4: uses arithmetic_dispatch)
    # ===================================================================
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
    from opteryx.expression.binary_operators import BINARY_OPERATORS, binary_operations

    if op not in BINARY_OPERATORS:
        return None

    # Phase 4.4: Attempt centralized arithmetic dispatch
    # This will use Draken kernels when available (Phase 4.5)
    # For now, falls back to Arrow/numpy via binary_operations()
    result = call_arithmetic_op(op, left, right)

    # If call_arithmetic_op returns None, it means no Draken kernel exists
    # (This occurs during Phase 4.4 transition; Phase 4.5 will populate kernels)
    # Fall back to Arrow/numpy conversion path
    if result is None:
        # Convert to Arrow if needed
        if hasattr(left, "to_arrow"):
            left = left.to_arrow()
        if hasattr(right, "to_arrow"):
            right = right.to_arrow()

        # Use existing binary_operations dispatcher
        result = binary_operations(
            left,
            node.left.schema_column.type,
            op,
            right,
            node.right.schema_column.type,
        )

    # ===================================================================
    # RESULT CONVERSION (Phase 4.4: centralized via arithmetic_dispatch)
    # ===================================================================
    # Convert PyArrow results back to Draken vectors
    if isinstance(result, (_pa.Array, _pa.ChunkedArray)):
        return vector_from_arrow(result)

    # PyArrow scalars → extract Python value
    if isinstance(result, _pa.Scalar):
        return result.as_py()

    # Result is already a Draken vector or Python scalar
    return result
