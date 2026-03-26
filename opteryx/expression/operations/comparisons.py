"""Comparison operations (Eq, NotEq, Lt, Gt, LtEq, GtEq)."""

import numpy
import pyarrow
from pyarrow import compute

from opteryx.expression.operations.fastpath_dictionary import (
    dictionary_fastpath,
    has_dictionary_candidate,
)
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def equal(arr, value, dict_candidate=False):
    """Equality comparison (Eq)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Eq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Eq`.")
    return compute.equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def not_equal(arr, value, dict_candidate=False):
    """Inequality comparison (NotEq)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotEq`.")
    return compute.not_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def less_than(arr, value, numeric_dict_candidate=False):
    """Less-than comparison (Lt)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "Lt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Lt`.")
    return compute.less(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def greater_than(arr, value, numeric_dict_candidate=False):
    """Greater-than comparison (Gt)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "Gt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Gt`.")
    return compute.greater(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def less_than_or_equal(arr, value, numeric_dict_candidate=False):
    """Less-than-or-equal comparison (LtEq)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "LtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `LtEq`.")
    return compute.less_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def greater_than_or_equal(arr, value, numeric_dict_candidate=False):
    """Greater-than-or-equal comparison (GtEq)."""
    if numeric_dict_candidate:
        fast = dictionary_fastpath(arr, "GtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `GtEq`.")
    return compute.greater_equal(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
