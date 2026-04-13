"""Comparison operations (Eq, NotEq, Lt, Gt, LtEq, GtEq)."""

import pyarrow
from opteryx.compiled.draken.vectors.bool_vector import BoolVector
from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
from opteryx.expression.operations.fastpath_dictionary import has_dictionary_candidate
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit
from pyarrow import compute


def equal(arr, value, dict_candidate=False):
    """Equality comparison (Eq)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Eq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Eq`.")
    return BoolVector.from_arrow(compute.equal(arr, value))


def not_equal(arr, value, dict_candidate=False):
    """Inequality comparison (NotEq)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotEq`.")
    return BoolVector.from_arrow(compute.not_equal(arr, value))


def less_than(arr, value, numeric_dict_candidate=False):
    """Less-than comparison (Lt)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "Lt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Lt`.")
    return BoolVector.from_arrow(compute.less(arr, value))


def greater_than(arr, value, numeric_dict_candidate=False):
    """Greater-than comparison (Gt)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "Gt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Gt`.")
    return BoolVector.from_arrow(compute.greater(arr, value))


def less_than_or_equal(arr, value, numeric_dict_candidate=False):
    """Less-than-or-equal comparison (LtEq)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "LtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `LtEq`.")
    return BoolVector.from_arrow(compute.less_equal(arr, value))


def greater_than_or_equal(arr, value, numeric_dict_candidate=False):
    """Greater-than-or-equal comparison (GtEq)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "GtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `GtEq`.")
    return BoolVector.from_arrow(compute.greater_equal(arr, value))
