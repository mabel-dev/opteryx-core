"""Binary arithmetic operations.

Architecture:
- Uses arithmetic_dispatch module for centralized routing
- Delegates to VectorType discriminator (from utils/vector_types.py)

Operations:
- Binary arithmetic: Plus, Minus, Multiply, Divide, Modulo, MyIntegerDivide
- Bitwise: BitwiseOr, BitwiseAnd, BitwiseXor, ShiftLeft, ShiftRight
- Temporal: Date operations (Plus, Minus with intervals)
- String: Concatenation
"""

import datetime

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type

from .arithmetic_dispatch import call_arithmetic_op
from .function_execution import apply_bounded_function, is_draken_vector
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

    if get_vector_type(left) == VectorType.UNKNOWN and node.left.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if node.left.schema_column.type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector

            left = Date32Vector.from_constant(_coerce_date32(left), morsel.num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector

            left = TimestampVector.from_constant(_coerce_timestamp(left), morsel.num_rows)

    if get_vector_type(right) == VectorType.UNKNOWN and node.right.schema_column.type in (
        OrsoTypes.DATE,
        OrsoTypes.TIMESTAMP,
    ):
        if node.right.schema_column.type == OrsoTypes.DATE:
            from draken.vectors.date32_vector import Date32Vector

            right = Date32Vector.from_constant(_coerce_date32(right), morsel.num_rows)
        else:
            from draken.vectors.timestamp_vector import TimestampVector

            right = TimestampVector.from_constant(_coerce_timestamp(right), morsel.num_rows)

    # ===================================================================
    # DATE-SPECIFIC OPERATIONS (refactored to use VectorType discriminator)
    # ===================================================================
    # Use VectorType enum for type discrimination
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
    # StringConcat: handle before Arrow conversion
    # ===================================================================
    if op == "StringConcat":
        from opteryx.compiled.vector_ops import vector_string_concat_binary

        def _to_bytes_or_vec(v):
            if isinstance(v, str):
                return v.encode("utf-8")
            return v  # bytes, None, or StringVector

        return vector_string_concat_binary(_to_bytes_or_vec(left), _to_bytes_or_vec(right))

    # ===================================================================
    # GENERAL ARITHMETIC OPERATIONS
    # ===================================================================
    from opteryx.expression.binary_operators import BINARY_OPERATORS

    if op not in BINARY_OPERATORS:
        return None

    # Attempt centralized arithmetic dispatch

    result = call_arithmetic_op(op, left, right)

    if result is None:
        raise NotImplementedError(
            f"Operator `{op}` has no Draken kernel for {left.__class__.__name__} and "
            f"{right.__class__.__name__}."
        )

    if get_vector_type(result) == VectorType.UNKNOWN and not isinstance(
        result,
        (
            type(None),
            bool,
            int,
            float,
            str,
            bytes,
            datetime.date,
            datetime.datetime,
            datetime.time,
            tuple,
        ),
    ):
        raise TypeError(
            f"Arithmetic op `{op}` returned non-Draken value type {result.__class__.__name__}."
        )

    return result
