"""Draken comparison operations.

Explicit comparison dispatch for all native Draken vector types.
ArrowVector has been removed; all paths now use native Draken APIs.
NumPy is not imported here.
"""

import datetime

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
    _coerce_date32_set,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_str,
    _coerce_str_set,
    _coerce_temporal_scalar_for_arrow,
    _coerce_timestamp,
    _coerce_timestamp_set,
    _constant_scalar_value,
    _dictionary_compare_vector,
    _is_constant_vector_like,
    _is_null_as_boolvector,
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
        >>> from opteryx.compiled.draken.vectors import Int64Vector
        >>> import pyarrow as pa
        >>> v1 = Int64Vector.from_arrow(pa.array([1, 2, 3]))
        >>> v2 = Int64Vector.from_arrow(pa.array([1, 2, 4]))
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
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

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
        return _call_vector_vector_op(op, vec, right)

    # Int64 vs Float64 — cast int64 side to float64 and re-dispatch
    if get_vector_type(right) == VectorType.FLOAT64:
        import pyarrow as pa

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        float_vec = vector_from_arrow(vec.to_arrow().cast(pa.float64()))
        return _float64_compare(op, float_vec, right)

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
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_float_set(right)
        if op == "InList":
            return vector_in_list(vec, value_set)
        raise NotImplementedError(f"Float64Vector: set op {op!r} not supported")

    if get_vector_type(right) == VectorType.FLOAT64:
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
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    vec = _dictionary_compare_vector(vec)
    if vec is None:
        raise NotImplementedError("Dictionary compare path requires a dictionary-encoded vector.")

    if right is None:
        return BoolVector(len(vec))

    # Unwrap constant vectors to their scalar value
    if _is_constant_vector_like(right):
        right = _constant_scalar_value(right)

    # Column-to-column: right is also a vector with comparison methods
    elif hasattr(right, "to_arrow") and not _is_constant_vector_like(right):
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
        elif vec_type == VectorType.TIMESTAMP:
            int_val = _coerce_timestamp(right)
        else:
            # Fallback: peek at the Arrow type to decide
            import pyarrow as pa

            arr = vec.to_arrow()
            if pa.types.is_dictionary(arr.type):
                arr = arr.dictionary_decode()
            if pa.types.is_date32(arr.type):
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

    value_list = list(right) if isinstance(right, (list, tuple, set, frozenset)) else right

    if op == "Eq":
        return vec.equals(right)
    if op == "NotEq":
        return vec.not_equals(right)
    if op == "Lt":
        return vec.less_than(right)
    if op == "Gt":
        return vec.greater_than(right)
    if op == "LtEq":
        return vec.less_than_or_equals(right)
    if op == "GtEq":
        return vec.greater_than_or_equals(right)
    if op == "InList":
        return vector_in_list(vec, value_list)
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
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values

    if right is None:
        return BoolVector(len(vec))

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
    raise NotImplementedError(f"constant-encoded vector: unsupported op {op!r}")


def _decimal_compare(op: str, vec, right):
    """Comparison operations on DecimalVector.

    Handles scalar comparisons, vector-vector comparisons, and set membership.
    Scalar coercion (Decimal/int/float -> unscaled int64) is handled internally
    by the Cython _coerce_scalar method, so the raw Python value is passed
    straight through to the comparison methods.
    """
    from opteryx.compiled.draken.vectors._decimal_vector import DecimalVector
    from opteryx.utils.vector_types import is_scalar

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
        from opteryx.compiled.vector_ops import vector_contains_any

        items = set(right) if right is not None else set()
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_any(left, items)
    if op == "ArrayContainsAll":
        from opteryx.compiled.vector_ops import vector_contains_all

        items = set(right) if right is not None else set()
        items = {v.encode() if isinstance(v, str) else v for v in items}
        return vector_contains_all(left, items)
    if op == "AnyOpLike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False)
        return vector_anyop_like(right, left)
    if op == "AnyOpNotLike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_like

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=False).not_vector()
        return vector_anyop_like(right, left).not_vector()
    if op == "AnyOpILike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True)
        return vector_anyop_ilike(right, left)
    if op == "AnyOpNotILike":
        from opteryx.compiled.draken.vectors.string_vector import StringVector
        from opteryx.compiled.vector_ops import vector_anyop_ilike

        if isinstance(left, StringVector):
            return _string_anyop_like(left, right, ignore_case=True).not_vector()
        return vector_anyop_ilike(right, left).not_vector()
    if op == "AtQuestion":
        import pyarrow as pa

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
        from opteryx.third_party.tktech import csimdjson as simdjson

        docs = left.to_pylist()
        path = right
        parser = simdjson.Parser()

        if path.startswith("$."):
            result = [None if doc is None else path in parser.parse(doc) for doc in docs]
        else:

            def _pointer(jsonpath: str) -> str:
                return jsonpath[1:].replace(".", "/").replace("[", "/").replace("]", "")

            json_pointer = _pointer(path)

            def _check(doc):
                if doc is None:
                    return None
                try:
                    parser.parse(doc).at_pointer(json_pointer)
                    return True
                except Exception:
                    return False

            result = [_check(doc) for doc in docs]

        return vector_from_arrow(pa.array(result, type=pa.bool_()))

    # --- Standard comparison operators ---

    negate = op in _NEGATED_OPS
    if negate:
        op = _NEGATED_OPS[op]

    # Scalar left with vector right: flip operands and invert directional ops
    # Example: 5 > [1, 2, 3] becomes [1, 2, 3] < 5
    if is_scalar(left) and is_draken_vector(right):
        flip_ops = {"Gt": "Lt", "Lt": "Gt", "GtEq": "LtEq", "LtEq": "GtEq"}
        op = flip_ops.get(op, op)
        left, right = right, left

    # Vector left with null right: all False
    if right is None and not isinstance(left, (str, int, float, bytes, bool, type(None))):
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(left))

    from opteryx.utils.vector_types import VectorType, get_vector_type

    vec_type = get_vector_type(left)

    if vec_type == VectorType.STRING:
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
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        from .type_coercion import _coerce_interval

        result = _interval_compare(op, left, right)
    elif vec_type == VectorType.DICTIONARY_ENCODED:
        result = _dict_compare(op, left, right)
    elif vec_type == VectorType.CONSTANT_ENCODED:
        result = _constant_compare(op, left, right)
    elif vec_type == VectorType.BOOL:
        result = _bool_compare(op, left, right)
    elif vec_type == VectorType.DECIMAL:
        result = _decimal_compare(op, left, right)
    else:
        raise NotImplementedError(f"draken_compare: unsupported vector type {vec_type!r}")

    return result.not_vector() if negate else result


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


_DATE_TYPES = frozenset(("Date32Vector", "TimestampVector"))
_INTERVAL_TYPES = frozenset(("IntervalVector",))
