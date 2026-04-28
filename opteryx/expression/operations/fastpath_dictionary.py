"""Dictionary vector encoding optimization for filter operations."""

from opteryx.compiled.vector_ops import vector_in_list, vector_like, vector_rlike
from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def _dictionary_arrow_type(arr):
    """Extract dictionary type information from a Draken vector."""
    if not arr.__class__.__module__.startswith("draken.vectors."):
        return None
    return getattr(arr, "dictionary_value_type", None)


def _has_dictionary_fastpath_ops(arr):
    """Check if array supports dictionary fastpath operations."""
    return all(
        hasattr(arr, method)
        for method in (
            "equals",
            "not_equals",
            "in_list",
            "like",
            "rlike",
            "less_than",
            "greater_than",
            "less_than_or_equals",
            "greater_than_or_equals",
        )
    )


def _dictionary_vector(arr):
    """Validate dictionary-encoded Draken vector."""
    if _dictionary_arrow_type(arr) is None:
        return None

    if _has_dictionary_fastpath_ops(arr):
        return arr

    raise TypeError(
        "Dictionary fastpath requires dictionary-capable Draken vector operators."
    )


def has_dictionary_candidate(arr):
    """Check if array is a dictionary-encoded vector candidate."""
    return _dictionary_arrow_type(arr) is not None


def _dictionary_supports_numeric_fastpath(arr):
    """Check if dictionary vector supports numeric comparison fastpaths."""
    vec = _dictionary_vector(arr)
    if vec is None:
        return False

    value_type = _dictionary_arrow_type(vec)
    if value_type is None:
        return False

    return str(value_type).lower() in (
        "integer",
        "double",
        "boolean",
        "date",
        "timestamp",
        "time",
    )


def _normalize_dict_compare_value(value):
    """Normalize comparison value for dictionary vector operations (e.g., str → bytes)."""
    # Handle .item() method on scalar wrapper types
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    # The Draken dictionary vectors expect bytes for string comparisons
    # Try to encode string values to bytes if needed
    if isinstance(value, str):
        try:
            return value.encode()
        except Exception:
            pass

    return value


def dictionary_fastpath(arr, operator, value):
    """
    Fast path for dictionary-encoded vectors using specialized operations.
    Handles comparison, matching, and list operations on dictionary values.
    """
    vec = _dictionary_vector(arr)
    if vec is None:
        return None

    # Normalize value for type compatibility with vector operations
    normalized_value = _normalize_dict_compare_value(value)

    if operator == "Eq":
        return vec.equals(normalized_value)
    if operator == "NotEq":
        return vec.not_equals(normalized_value)
    if operator in ("InList", "NotInList"):
        result = vector_in_list(vec, _coerce_in_list_values(value))
        if operator == "NotInList":
            result = result.not_vector()
        return result
    if operator in ("Like", "NotLike"):
        result = vector_like(vec, normalized_value, False)
        if operator == "NotLike":
            result = result.not_vector()
        return result
    if operator in ("ILike", "NotILike"):
        result = vector_like(vec, normalized_value, True)
        if operator == "NotILike":
            result = result.not_vector()
        return result
    if operator in ("RLike", "NotRLike"):
        result = vector_rlike(vec, normalized_value)
        if operator == "NotRLike":
            result = result.not_vector()
        return result
    if operator == "Lt":
        return vec.less_than(normalized_value)
    if operator == "Gt":
        return vec.greater_than(normalized_value)
    if operator == "LtEq":
        return vec.less_than_or_equals(normalized_value)
    if operator == "GtEq":
        return vec.greater_than_or_equals(normalized_value)

    return None


_DICT_FASTPATH_OPS = frozenset(
    (
        "Eq",
        "NotEq",
        "InList",
        "NotInList",
        "InStr",
        "NotInStr",
        "IInStr",
        "NotIInStr",
        "Like",
        "NotLike",
        "ILike",
        "NotILike",
        "RLike",
        "NotRLike",
    )
)

_DICT_NUMERIC_FASTPATH_OPS = frozenset(("Lt", "Gt", "LtEq", "GtEq"))


def supports_dictionary_fastpath(operator: str) -> bool:
    """Check if operator is supported by dictionary fastpath."""
    return operator in _DICT_FASTPATH_OPS


def supports_dictionary_numeric_fastpath(operator: str) -> bool:
    """Check if operator is a numeric comparison supported by dictionary fastpath."""
    return operator in _DICT_NUMERIC_FASTPATH_OPS
