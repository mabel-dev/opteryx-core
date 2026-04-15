"""Constant vector encoding optimization for filter operations."""

from opteryx.compiled.vector_ops import vector_in_list
from opteryx.expression.operations.fastpath_telemetry import (
    record_constant_fastpath_fallback,
    record_constant_fastpath_hit,
)

_DRAKEN_ENCODING_CONSTANT = 3


def _constant_vector(arr):
    """Extract constant vector if array is constant-encoded."""
    if getattr(arr, "encoding", None) == _DRAKEN_ENCODING_CONSTANT:
        return arr
    return None


def has_constant_candidate(arr):
    """Check if array is a constant-encoded vector candidate."""
    return _constant_vector(arr) is not None


def _typed_constant_scalar(arr):
    """Extract scalar value from constant vector."""
    if len(arr) == 0:
        return None
    return arr[0]


def _normalize_typed_constant_compare_value(scalar, value):
    """Normalize comparison value to match scalar type (e.g., str → bytes)."""
    if isinstance(scalar, bytes):
        if isinstance(value, str):
            return value.encode()
        if isinstance(value, (list, tuple, set)):
            return [item.encode() if isinstance(item, str) else item for item in value]
    return value


def _coerce_in_list_values(value):
    """Convert value to list for IN operations."""
    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


def _typed_constant_fastpath(arr, operator, value):
    """Execute comparison for typed constant vector (fallback from compiled fastpath)."""
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    scalar = _typed_constant_scalar(arr)
    value = _normalize_typed_constant_compare_value(scalar, value)

    if scalar is None:
        if operator == "InList":
            result = None in _coerce_in_list_values(value)
            return BoolVector.from_constant(bool(result), len(arr))
        if operator == "NotInList":
            result = None not in _coerce_in_list_values(value)
            return BoolVector.from_constant(bool(result), len(arr))
        return BoolVector(len(arr))

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    try:
        if operator == "Eq":
            result = scalar == value
        elif operator == "NotEq":
            result = scalar != value
        elif operator == "InList":
            result = scalar in _coerce_in_list_values(value)
        elif operator == "NotInList":
            result = scalar not in _coerce_in_list_values(value)
        elif operator == "Lt":
            result = scalar < value
        elif operator == "Gt":
            result = scalar > value
        elif operator == "LtEq":
            result = scalar <= value
        elif operator == "GtEq":
            result = scalar >= value
        else:
            return None
    except Exception:
        return None

    return BoolVector.from_constant(bool(result), len(arr))


def constant_fastpath(arr, operator, value):
    """
    Fast path for constant-encoded vectors using compiled operations.
    Falls back to typed scalar comparison if compiled path fails.
    """
    vec = _constant_vector(arr)
    if vec is None:
        return None

    scalar = _typed_constant_scalar(vec)
    value = _normalize_typed_constant_compare_value(scalar, value)

    try:
        if operator == "Eq":
            return vec.equals(value)
        if operator == "NotEq":
            return vec.not_equals(value)
        if operator in ("InList", "NotInList"):
            result = vector_in_list(vec, _coerce_in_list_values(value))
            if operator == "NotInList":
                result = result.not_vector()
            return result
        if operator == "Lt":
            return vec.less_than(value)
        if operator == "Gt":
            return vec.greater_than(value)
        if operator == "LtEq":
            return vec.less_than_or_equals(value)
        if operator == "GtEq":
            return vec.greater_than_or_equals(value)
    except Exception:
        return _typed_constant_fastpath(vec, operator, value)

    return _typed_constant_fastpath(vec, operator, value)


_CONSTANT_FASTPATH_OPS = frozenset(
    ("Eq", "NotEq", "InList", "NotInList", "Lt", "Gt", "LtEq", "GtEq")
)


def supports_constant_fastpath(operator: str) -> bool:
    """Check if operator is supported by constant fastpath."""
    return operator in _CONSTANT_FASTPATH_OPS
