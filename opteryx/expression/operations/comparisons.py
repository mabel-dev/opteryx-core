"""Comparison operations (Eq, NotEq, Lt, Gt, LtEq, GtEq) - Draken-native only.

All inputs must be Draken vectors. No Arrow/numpy conversion or fallbacks.
If you get AttributeError, your input isn't Draken - that's a bug upstream.
"""

from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def equal(arr, value, dict_candidate=False):
    """Equality comparison (Eq). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Eq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Eq`.")
    return arr.equals(value)


def not_equal(arr, value, dict_candidate=False):
    """Inequality comparison (NotEq). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotEq`.")
    return arr.not_equals(value)


def less_than(arr, value, dict_candidate=False):
    """Less than comparison (Lt). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Lt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Lt`.")
    return arr.less_than(value)


def greater_than(arr, value, dict_candidate=False):
    """Greater than comparison (Gt). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Gt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Gt`.")
    return arr.greater_than(value)


def less_than_or_equal(arr, value, dict_candidate=False):
    """Less than or equal comparison (LtEq). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "LtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `LtEq`.")
    return arr.less_than_or_equals(value)


def greater_than_or_equal(arr, value, dict_candidate=False):
    """Greater than or equal comparison (GtEq). Input must be Draken vector."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "GtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `GtEq`.")
    return arr.greater_than_or_equals(value)
