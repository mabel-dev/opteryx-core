"""Dictionary-encoded vector fast path for filter operations.

Recognises Draken dictionary-encoded vectors and routes comparison/match
operators to their native dict-aware kernels, skipping the materialisation
to a flat vector.
"""

from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
)
from opteryx.compiled.nanobind.vector_misc import vector_in_list


# Methods a dictionary-capable vector must implement for the fastpath to kick
# in. Promoted to a module-level tuple so the membership check is built once.
cdef tuple _DICT_FASTPATH_METHODS = (
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


cpdef _dictionary_arrow_type(arr):
    """Return the dictionary value-type for a Draken dict vector, else None."""
    if not type(arr).__module__.startswith("draken.vectors."):
        return None
    return getattr(arr, "dictionary_value_type", None)


cdef bint _has_dictionary_fastpath_ops(arr):
    """True if `arr` carries every method the fastpath dispatches into."""
    cdef str method
    for method in _DICT_FASTPATH_METHODS:
        if getattr(arr, method, None) is None:
            return False
    return True


cdef _dictionary_vector(arr):
    """Return `arr` iff it's a dictionary-encoded vector with the required
    method surface. Raises if the vector is dict-encoded but missing kernels —
    that's a real bug in the vector implementation, not a fallback condition.
    """
    if _dictionary_arrow_type(arr) is None:
        return None
    if _has_dictionary_fastpath_ops(arr):
        return arr
    raise TypeError(
        "Dictionary fastpath requires dictionary-capable Draken vector operators."
    )


cpdef bint has_dictionary_candidate(arr):
    """True if `arr` is a dictionary-encoded vector candidate."""
    return _dictionary_arrow_type(arr) is not None


cdef bint _dictionary_supports_numeric_fastpath(arr):
    """True if the dict vector's value-type can be compared numerically."""
    vec = _dictionary_vector(arr)
    if vec is None:
        return False

    value_type = _dictionary_arrow_type(vec)
    if value_type is None:
        return False

    return str(value_type).lower() in (
        "integer", "double", "boolean", "date", "timestamp", "time",
    )


cpdef _coerce_in_list_values(value):
    """Coerce `value` into the iterable form expected by `vector_in_list`."""
    pylist_fn = getattr(value, "to_pylist", None)
    if pylist_fn is not None:
        value = pylist_fn()
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


cdef _normalize_dict_compare_value(value):
    """Normalise the right-hand value: unwrap numpy-scalars via .item(); encode
    Python str to bytes since dict vectors compare on byte literals.
    """
    item_fn = getattr(value, "item", None)
    if item_fn is not None:
        try:
            value = item_fn()
        except Exception:
            pass

    if isinstance(value, str):
        try:
            return value.encode()
        except Exception:
            pass

    return value


cpdef dictionary_fastpath(arr, str operator, value):
    """Route a comparison / matching operator through the dict-aware kernels.

    Returns None if `arr` isn't dictionary-encoded or the operator isn't
    handled here — the caller falls back to the generic path.
    """
    vec = _dictionary_vector(arr)
    if vec is None:
        return None

    normalized_value = _normalize_dict_compare_value(value)

    if operator == "Eq":
        return vec.equals(normalized_value)
    if operator == "NotEq":
        return vec.not_equals(normalized_value)
    if operator == "InList" or operator == "NotInList":
        return vector_in_list(
            vec,
            _coerce_in_list_values(value),
            operator == "NotInList",
        )
    if operator == "Like" or operator == "NotLike":
        _pat = normalized_value.decode("utf-8") if isinstance(normalized_value, bytes) else normalized_value
        return vector_like(vec, _draken_native.vector_varchar_from_constant(_pat, 1), False, operator == "NotLike")
    if operator == "ILike" or operator == "NotILike":
        _pat = normalized_value.decode("utf-8") if isinstance(normalized_value, bytes) else normalized_value
        return vector_like(vec, _draken_native.vector_varchar_from_constant(_pat, 1), True, operator == "NotILike")
    if operator == "RLike" or operator == "NotRLike":
        _pat = normalized_value.decode("utf-8") if isinstance(normalized_value, bytes) else normalized_value
        return vector_rlike(vec, _draken_native.vector_varchar_from_constant(_pat, 1), operator == "NotRLike")
    if operator == "Lt":
        return vec.less_than(normalized_value)
    if operator == "Gt":
        return vec.greater_than(normalized_value)
    if operator == "LtEq":
        return vec.less_than_or_equals(normalized_value)
    if operator == "GtEq":
        return vec.greater_than_or_equals(normalized_value)

    return None


cdef frozenset _DICT_FASTPATH_OPS = frozenset(
    (
        "Eq", "NotEq", "InList", "NotInList",
        "InStr", "NotInStr", "IInStr", "NotIInStr",
        "Like", "NotLike", "ILike", "NotILike", "RLike", "NotRLike",
    )
)

cdef frozenset _DICT_NUMERIC_FASTPATH_OPS = frozenset(("Lt", "Gt", "LtEq", "GtEq"))


cpdef bint supports_dictionary_fastpath(str operator):
    return operator in _DICT_FASTPATH_OPS


cpdef bint supports_dictionary_numeric_fastpath(str operator):
    return operator in _DICT_NUMERIC_FASTPATH_OPS
