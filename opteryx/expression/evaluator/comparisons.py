"""Draken comparison operations.

Explicit comparison dispatch for all native Draken vector types.
ArrowVector has been removed; all paths now use native Draken APIs.

Dispatch strategy:
  1. Check encoding schemes first (CONSTANT_ENCODED, DICTIONARY_ENCODED)
  2. Then dispatch by underlying data type (STRING, INT64, FLOAT64, TIMESTAMP, etc.)

This separation is important because encoding schemes like CONSTANT_ENCODED and
DICTIONARY_ENCODED can wrap any underlying type, so they must be checked before
type-specific comparison logic.
"""

import datetime

from draken.vectors.bool_vector import BoolVector
from opteryx.compiled.vector_ops import vector_contains, vector_in_list, vector_like, vector_rlike
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar

from .string_ops import _string_anyop_like, _string_compare
from .temporal_ops import (
    _date32_compare,
    _int64_temporal_compare,
    _interval_compare,
    _timestamp_compare,
)
from .type_coercion import (
    _coerce_date32,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_str,
    _coerce_timestamp,
    _constant_scalar_value,
    _dictionary_compare_vector,
    _is_constant_vector_like,
)

_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


# ---------------------------------------------------------------------------
# Unified vector-vector comparison dispatch
# ---------------------------------------------------------------------------

_VECTOR_VECTOR_OPS = {
    "Eq": lambda vec, other: vec.equals_vector(other),
    "Lt": lambda vec, other: vec.less_than_vector(other),
    "Gt": lambda vec, other: vec.greater_than_vector(other),
    "LtEq": lambda vec, other: vec.less_than_or_equals_vector(other),
    "GtEq": lambda vec, other: vec.greater_than_or_equals_vector(other),
}


def _call_vector_vector_op(op: str, left_vec, right_vec):
    """Call vector-vector comparison operation with consistent error handling.

    This centralized dispatcher eliminates code duplication across multiple
    _*_compare() functions and ensures consistent operation routing.

    Args:
        op: Operation name (Eq, Lt, Gt, LtEq, GtEq)
        left_vec: Left operand (Draken vector)
        right_vec: Right operand (Draken vector, same type as left)

    Returns:
        BoolVector with comparison results

    Raises:
        NotImplementedError: If operation not supported for this vector type

    Examples:
        >>> from draken.vectors import Int64Vector
        >>> from draken.interop.vector_sequence import vector_from_sequence
        >>> v1 = vector_from_sequence([1, 2, 3])
        >>> v2 = vector_from_sequence([1, 2, 4])
        >>> result = _call_vector_vector_op("Eq", v1, v2)
        >>> result.to_pylist()
        [True, True, False]
    """
    fn = _VECTOR_VECTOR_OPS.get(op)
    if fn is None:
        raise NotImplementedError(f"Vector-vector operation {op!r} not supported")
    return fn(left_vec, right_vec)


# ---------------------------------------------------------------------------
# Scalar-typed compare helpers
# ---------------------------------------------------------------------------


def _int64_compare(op: str, vec, right):

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_int64_set(right)
        if op == "InList":
            return vector_in_list(vec, value_set)
        raise NotImplementedError(f"Int64Vector: set op {op!r} not supported")

    # vector-vector: both sides are Int64Vector or IntegerVector
    right_type = get_vector_type(right)
    if right_type in (VectorType.INT64, VectorType.INTEGER):
        if _is_constant_vector_like(right):
            right = _constant_scalar_value(right)
        else:
            return _call_vector_vector_op(op, vec, right)

    # Int64 vs Float64 — promote the int side to float so vector-vector ops have
    # matching types. Constant float right operands are unwrapped to scalars by
    # _float64_compare's existing path.
    if get_vector_type(right) == VectorType.FLOAT64:
        if _is_constant_vector_like(right):
            return _float64_compare(op, vec, right)
        from draken.interop.vector_sequence import vector_from_sequence

        vec_float = vector_from_sequence([float(x) if x is not None else None for x in vec.to_pylist()])
        return _float64_compare(op, vec_float, right)

    value = _coerce_int64(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    raise NotImplementedError(f"Int64Vector: unsupported op {op!r}")


def _float64_compare(op: str, vec, right):
    from draken.interop.vector_sequence import vector_from_sequence

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_float_set(right)
        if op == "InList":
            return vector_in_list(vec, value_set)
        raise NotImplementedError(f"Float64Vector: set op {op!r} not supported")

    right_type = get_vector_type(right)

    # Float64 vs Int64 vector — extract scalar if constant-encoded, otherwise convert to float
    if right_type in (VectorType.INT64, VectorType.CONSTANT_ENCODED):
        if _is_constant_vector_like(right):
            right = _constant_scalar_value(right)
        else:
            # Convert Int64Vector to Float64Vector for element-wise comparison
            right_float = vector_from_sequence([float(x) for x in right.to_pylist()])
            return _call_vector_vector_op(op, vec, right_float)
    elif right_type == VectorType.FLOAT64:
        if _is_constant_vector_like(right):
            right = _constant_scalar_value(right)
        else:
            return _call_vector_vector_op(op, vec, right)

    value = _coerce_float(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    raise NotImplementedError(f"Float64Vector: unsupported op {op!r}")


def _dict_compare(op: str, vec, right):

    vec = _dictionary_compare_vector(vec)
    if vec is None:
        raise NotImplementedError("Dictionary compare path requires a dictionary-encoded vector.")

    if right is None:
        return BoolVector(len(vec))

    # Unwrap constant vectors to their scalar value
    if _is_constant_vector_like(right):
        right = _constant_scalar_value(right)
        # An empty or null-valued constant vector unwraps to None; comparisons
        # against NULL are UNKNOWN → all-false mask (matches SQL semantics).
        if right is None:
            return BoolVector(len(vec))

    # Column-to-column: right is also a vector with comparison methods
    elif is_draken_vector(right):
        right_vec = _dictionary_compare_vector(right)
        if right_vec is None:
            raise NotImplementedError(
                "dictionary-encoded vector column-to-column comparison requires a "
                "Draken-compatible right-hand vector."
            )
        ops = {
            "Eq": vec.equals_vector,
            "NotEq": vec.not_equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = ops.get(op)
        if fn is None:
            raise NotImplementedError(
                f"dictionary-encoded vector column-to-column: unsupported op {op!r}"
            )
        return fn(right_vec)

    # Temporal scalar: coerce to the integer representation the Draken vector uses
    if isinstance(right, (datetime.datetime, datetime.date)):
        vec_type = get_vector_type(vec)
        if vec_type == VectorType.DATE32:
            int_val = _coerce_date32(right)
        else:
            int_val = _coerce_timestamp(right)

        if op == "Eq":
            return vec.equals(int_val)
        if op == "NotEq":
            return vec.not_equals(int_val)
        if op == "Lt":
            return vec.less_than(int_val)
        if op == "Gt":
            return vec.greater_than(int_val)
        if op == "LtEq":
            return vec.less_than_or_equals(int_val)
        if op == "GtEq":
            return vec.greater_than_or_equals(int_val)
        raise NotImplementedError(
            f"dictionary-encoded vector temporal compare: unsupported op {op!r}"
        )

    # String-keyed dict vectors require bytes, not str, on the Cython boundary.
    def _enc(v):
        return v.encode() if isinstance(v, str) else v

    if op == "InList":
        if isinstance(right, (list, tuple, set, frozenset)):
            right = frozenset(_enc(v) for v in right)
        else:
            right = _enc(right)
        # vector_in_list_int64_vector reads vec.ptr.data which is NULL for
        # dict-encoded vectors; use the vector's own in_list which handles encoding.
        return vec.in_list(right)

    if isinstance(right, (list, tuple, set, frozenset)):
        value_list = [_enc(v) for v in right]
    else:
        value_list = _enc(right)

    if op == "Eq":
        return vec.equals(value_list)
    if op == "NotEq":
        return vec.not_equals(value_list)
    if op == "Lt":
        return vec.less_than(value_list)
    if op == "Gt":
        return vec.greater_than(value_list)
    if op == "LtEq":
        return vec.less_than_or_equals(value_list)
    if op == "GtEq":
        return vec.greater_than_or_equals(value_list)
    if op in ("Like", "ILike", "RLike", "InStr", "IInStr"):
        right = _coerce_str(right)
    if op == "Like":
        return vector_like(vec, right, False)
    if op == "ILike":
        return vector_like(vec, right, True)
    if op == "RLike":
        return vector_rlike(vec, right)
    if op == "InStr":
        return vector_contains(vec, right, False)
    if op == "IInStr":
        return vector_contains(vec, right, True)
    raise NotImplementedError(f"dictionary-encoded vector: unsupported op {op!r}")


def _constant_compare(op: str, vec, right):
    from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values

    if right is None:
        return BoolVector(len(vec))

    # Extract scalar value from CONSTANT_ENCODED right operand if needed
    if is_draken_vector(right) and get_vector_type(right) == VectorType.CONSTANT_ENCODED:
        right = _constant_scalar_value(right)

    # Handle vector-vector comparisons when right is still a vector
    if is_draken_vector(right):
        vec_ops = {
            "Eq": lambda v, r: v.equals_vector(r),
            "Lt": lambda v, r: v.less_than_vector(r),
            "Gt": lambda v, r: v.greater_than_vector(r),
            "LtEq": lambda v, r: v.less_than_or_equals_vector(r),
            "GtEq": lambda v, r: v.greater_than_or_equals_vector(r),
        }
        fn = vec_ops.get(op)
        if fn is not None:
            return fn(vec, right)

    if isinstance(right, (list, tuple, set, frozenset)):
        right = _coerce_in_list_values(right)

    if op == "Eq":
        return vec.equals(right)
    if op == "Lt":
        return vec.less_than(right)
    if op == "Gt":
        return vec.greater_than(right)
    if op == "LtEq":
        return vec.less_than_or_equals(right)
    if op == "GtEq":
        return vec.greater_than_or_equals(right)
    if op == "InList":
        return vector_in_list(vec, right)
    if op in ("Like", "ILike", "RLike", "InStr", "IInStr"):
        if _is_constant_vector_like(right):
            right = _constant_scalar_value(right)
        right = _coerce_str(right)
    if op == "Like":
        return vector_like(vec, right, False)
    if op == "ILike":
        return vector_like(vec, right, True)
    if op == "RLike":
        return vector_rlike(vec, right)
    if op == "InStr":
        return vector_contains(vec, right, False)
    if op == "IInStr":
        return vector_contains(vec, right, True)
    raise NotImplementedError(f"constant-encoded vector: unsupported op {op!r}")


def _decimal_compare(op: str, vec, right):
    """Comparison operations on DecimalVector.

    Handles scalar comparisons, vector-vector comparisons, and set membership.
    Scalar coercion (Decimal/int/float -> unscaled int64) is handled internally
    by the Cython _coerce_scalar method, so the raw Python value is passed
    straight through to the comparison methods.
    """
    from draken.vectors._decimal_vector import DecimalVector

    # Set membership (InList is handled before the scalar/vector branch)
    if op == "InList":
        if isinstance(right, (list, tuple, set, frozenset)):
            return vec.in_list(right)
        raise NotImplementedError(f"DecimalVector InList: expected a set/list, got {type(right)!r}")

    # Vector-vector comparison
    if isinstance(right, DecimalVector):
        if op == "Eq":
            return vec.equals_vector(right)
        if op == "NotEq":
            return vec.not_equals_vector(right)
        if op == "Lt":
            return vec.less_than_vector(right)
        if op == "LtEq":
            return vec.less_than_or_equals_vector(right)
        if op == "Gt":
            return vec.greater_than_vector(right)
        if op == "GtEq":
            return vec.greater_than_or_equals_vector(right)
        raise NotImplementedError(f"DecimalVector vector-vector: unsupported op {op!r}")

    # Decimal vs Float64 — promote the decimal side to float so vector-vector
    # ops have matching types. Mirrors `_int64_compare`'s Float64 path. Constant
    # float right operands fall through to the scalar path below.
    if get_vector_type(right) == VectorType.FLOAT64 and not _is_constant_vector_like(right):
        from draken.interop.vector_sequence import vector_from_sequence

        vec_float = vector_from_sequence(
            [float(x) if x is not None else None for x in vec.to_pylist()]
        )
        return _float64_compare(op, vec_float, right)

    # Unwrap constant-encoded right-hand vectors to their scalar value
    if _is_constant_vector_like(right):
        right = _constant_scalar_value(right)

    if is_scalar(right):
        if op == "Eq":
            return vec.equals(right)
        if op == "NotEq":
            return vec.not_equals(right)
        if op == "Lt":
            return vec.less_than(right)
        if op == "LtEq":
            return vec.less_than_or_equals(right)
        if op == "Gt":
            return vec.greater_than(right)
        if op == "GtEq":
            return vec.greater_than_or_equals(right)

    raise NotImplementedError(
        f"DecimalVector comparison for op={op!r} with right={type(right)!r} not implemented"
    )


# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------


def draken_compare(op: str, left, right, left_schema_type=None, right_schema_type=None):
    from opteryx.types import OrsoTypes

    # Array / set operations — dispatch directly, no flip logic needed
    if op == "AnyOpEq":
        from opteryx.compiled.vector_ops import vector_anyop_eq

        return vector_anyop_eq(literal=left, column=right)
    if op == "AnyOpNotEq":
        from opteryx.compiled.vector_ops import vector_anyop_neq

        return vector_anyop_neq(literal=left, column=right)
    if op == "AnyOpGt":
        from opteryx.compiled.vector_ops import vector_anyop_gt

        return vector_anyop_gt(left, right)
    if op == "AnyOpLt":
        from opteryx.compiled.vector_ops import vector_anyop_lt

        return vector_anyop_lt(left, right)
    if op == "AnyOpGtEq":
        from opteryx.compiled.vector_ops import vector_anyop_gte

        return vector_anyop_gte(left, right)
    if op == "AnyOpLtEq":
        from opteryx.compiled.vector_ops import vector_anyop_lte

        return vector_anyop_lte(left, right)
    if op == "AllOpEq":
        from opteryx.compiled.vector_ops import vector_allop_eq

        return vector_allop_eq(left, right)
    if op == "AllOpNotEq":
        from opteryx.compiled.vector_ops import vector_allop_neq

        return vector_allop_neq(left, right)
    if op == "AtArrow":
        from .json_ops import _json_at_arrow

        return _json_at_arrow(left, right)
    if op == "ArrayContainsAll":
        from .json_ops import _json_array_contains_all

        return _json_array_contains_all(left, right)
    if op == "AnyOpLike":
        from draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False)
        return vector_anyop_like(left, right)
    if op == "AnyOpNotLike":
        from draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False).not_vector()
        return vector_anyop_like(left, right).not_vector()
    if op == "AnyOpILike":
        from draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True)
        return vector_anyop_ilike(left, right)
    if op == "AnyOpNotILike":
        from draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True).not_vector()
        return vector_anyop_ilike(left, right).not_vector()
    if op == "AtQuestion":
        from .json_ops import _json_at_question

        return _json_at_question(left, right)

    # --- Standard comparison operators ---

    negate = op in _NEGATED_OPS
    if negate:
        op = _NEGATED_OPS[op]

    # InList with a pre-built CarcharSetWrapper: hash-based dispatch for all vector types.
    if op == "InList":
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
        if isinstance(right, CarcharSetWrapper):
            result = vector_in_list(left, right)
            return result.not_vector() if negate else result

    # Scalar left with vector right: flip operands and invert directional ops
    # Example: 5 > [1, 2, 3] becomes [1, 2, 3] < 5
    if is_scalar(left) and is_draken_vector(right):
        flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
        op = flip_ops.get(op, op)
        left, right = right, left

    # Vector left with null right: all False
    if right is None and not isinstance(left, (str, int, float, bytes, bool, type(None))):
        return BoolVector(len(left))

    vec_type = get_vector_type(left)

    # --- Dispatch by encoding scheme first ---
    # CONSTANT_ENCODED and DICTIONARY_ENCODED are overlaid on top of underlying types
    if vec_type == VectorType.CONSTANT_ENCODED:
        result = _constant_compare(op, left, right)
    elif vec_type == VectorType.DICTIONARY_ENCODED:
        result = _dict_compare(op, left, right)

    # --- Then dispatch by underlying data type ---
    elif vec_type == VectorType.STRING:
        result = _string_compare(op, left, right)
    elif vec_type in (VectorType.INT64, VectorType.INTEGER):
        if left_schema_type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
            result = _int64_temporal_compare(op, left, right, left_schema_type)
        else:
            result = _int64_compare(op, left, right)
    elif vec_type == VectorType.FLOAT64:
        result = _float64_compare(op, left, right)
    elif vec_type == VectorType.TIMESTAMP:
        result = _timestamp_compare(op, left, right)
    elif vec_type == VectorType.DATE32:
        result = _date32_compare(op, left, right)
    elif vec_type == VectorType.INTERVAL:
        result = _interval_compare(op, left, right)
    elif vec_type == VectorType.BOOL:
        result = _bool_compare(op, left, right)
    elif vec_type == VectorType.DECIMAL:
        result = _decimal_compare(op, left, right)
    else:
        raise NotImplementedError(f"draken_compare: unsupported vector type {vec_type!r}")

    return result.not_vector() if negate else result


def draken_between(col, lower, upper, lower_inclusive: bool, upper_inclusive: bool):
    """Dispatch BETWEEN to the vector type's single-pass between() method."""
    from opteryx.types import OrsoTypes

    from .type_coercion import _coerce_date32, _coerce_float, _coerce_int64, _coerce_timestamp

    vec_type = get_vector_type(col)

    if vec_type in (VectorType.INT64, VectorType.INTEGER):
        return col.between(_coerce_int64(lower), _coerce_int64(upper), lower_inclusive, upper_inclusive)
    if vec_type == VectorType.FLOAT64:
        return col.between(_coerce_float(lower), _coerce_float(upper), lower_inclusive, upper_inclusive)
    if vec_type == VectorType.TIMESTAMP:
        return col.between(_coerce_timestamp(lower), _coerce_timestamp(upper), lower_inclusive, upper_inclusive)
    if vec_type == VectorType.DATE32:
        return col.between(_coerce_date32(lower), _coerce_date32(upper), lower_inclusive, upper_inclusive)
    if vec_type == VectorType.CONSTANT_ENCODED:
        # Call the vector's own between() which has an O(1) const path — avoids
        # expanding the constant into an N-element list just to compare one value.
        from draken.vectors.float64_vector import Float64Vector
        from draken.vectors.int64_vector import Int64Vector
        if isinstance(col, Int64Vector):
            return col.between(_coerce_int64(lower), _coerce_int64(upper),
                               lower_inclusive, upper_inclusive)
        if isinstance(col, Float64Vector):
            return col.between(_coerce_float(lower), _coerce_float(upper),
                               lower_inclusive, upper_inclusive)
        raise NotImplementedError(
            f"draken_between: CONSTANT_ENCODED with unsupported underlying type {type(col).__name__!r}"
        )
    if vec_type == VectorType.DICTIONARY_ENCODED:
        # Dict-encoded vectors don't have a native between(); use two existing
        # comparison methods rather than recursing (which would loop infinitely because
        # _dictionary_compare_vector returns a vector still classified as DICTIONARY_ENCODED).
        lo_op = "GtEq" if lower_inclusive else "Gt"
        hi_op = "LtEq" if upper_inclusive else "Lt"
        return draken_compare(lo_op, col, lower).and_vector(draken_compare(hi_op, col, upper))
    if vec_type == VectorType.DECIMAL:
        # DecimalVector has no between(); use two scalar comparisons.
        lo_op = "GtEq" if lower_inclusive else "Gt"
        hi_op = "LtEq" if upper_inclusive else "Lt"
        return _decimal_compare(lo_op, col, lower).and_vector(_decimal_compare(hi_op, col, upper))
    raise NotImplementedError(f"draken_between: unsupported vector type {vec_type!r}")


def _bool_compare(op: str, left, right):
    """Comparison operations on BoolVector."""
    if op == "Eq":
        return left.equals(bool(right))
    if op == "NotEq":
        return left.not_equals(bool(right))
    if op == "InList":
        bool_set = {bool(v) for v in right if v is not None}
        return vector_in_list(left, bool_set)
    if op == "Lt":
        return left.less_than(bool(right))
    if op == "Gt":
        return left.greater_than(bool(right))
    if op == "LtEq":
        return left.less_than_or_equals(bool(right))
    if op == "GtEq":
        return left.greater_than_or_equals(bool(right))
    raise NotImplementedError(f"BoolVector: unsupported op {op!r}")
