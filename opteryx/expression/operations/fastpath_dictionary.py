"""Dictionary vector encoding optimization for filter operations."""

import pyarrow
from opteryx.expression.operations.fastpath_constant import _coerce_in_list_values
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def _dictionary_arrow_type(arr):
    """Extract dictionary type information from array."""
    if isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        return arr.type if pyarrow.types.is_dictionary(arr.type) else None

    to_arrow = getattr(arr, "to_arrow", None)
    if to_arrow is None:
        return None

    try:
        arrow_arr = to_arrow()
    except Exception:
        return None

    if isinstance(arrow_arr, (pyarrow.Array, pyarrow.ChunkedArray)) and pyarrow.types.is_dictionary(
        arrow_arr.type
    ):
        return arrow_arr.type

    return None


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
    """Extract or convert to dictionary-encoded vector."""
    if _dictionary_arrow_type(arr) is None:
        return None

    if _has_dictionary_fastpath_ops(arr):
        return arr

    if hasattr(arr, "to_arrow") and not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        arr = arr.to_arrow()

    if isinstance(arr, pyarrow.ChunkedArray):
        if arr.num_chunks != 1:
            raise NotImplementedError(
                "Dictionary motor path does not support multi-chunk dictionary arrays."
            )
        arr = arr.chunk(0)

    if isinstance(arr, pyarrow.DictionaryArray):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        vec = vector_from_arrow(arr)
        if _has_dictionary_fastpath_ops(vec):
            return vec
        raise TypeError(
            "Dictionary fastpath expected a dictionary-capable vector conversion result."
        )

    return None


def has_dictionary_candidate(arr):
    """Check if array is a dictionary-encoded vector candidate."""
    return _dictionary_arrow_type(arr) is not None


def _dictionary_supports_numeric_fastpath(arr):
    """Check if dictionary vector supports numeric comparison fastpaths."""
    vec = _dictionary_vector(arr)
    if vec is None:
        return False

    arrow_type = _dictionary_arrow_type(vec)
    if arrow_type is None:
        return False

    value_type = arrow_type.value_type
    return (
        pyarrow.types.is_integer(value_type)
        or pyarrow.types.is_floating(value_type)
        or pyarrow.types.is_boolean(value_type)
        or pyarrow.types.is_date32(value_type)
        or pyarrow.types.is_timestamp(value_type)
        or pyarrow.types.is_time32(value_type)
        or pyarrow.types.is_time64(value_type)
    )


def _normalize_dict_compare_value(value):
    """Normalize comparison value for dictionary vector operations (e.g., str → bytes)."""
    # Handle hasattr(value, 'item') for numpy/arrow scalars
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
        result = vec.in_list(_coerce_in_list_values(value))
        if operator == "NotInList":
            result = result.not_vector()
        return result
    if operator in ("Like", "NotLike"):
        result = vec.like(normalized_value, False)
        if operator == "NotLike":
            result = result.not_vector()
        return result
    if operator in ("ILike", "NotILike"):
        result = vec.like(normalized_value, True)
        if operator == "NotILike":
            result = result.not_vector()
        return result
    if operator in ("RLike", "NotRLike"):
        result = vec.rlike(normalized_value)
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
