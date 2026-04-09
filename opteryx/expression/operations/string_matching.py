"""String matching operations (Like, RLike, etc.)."""

import numpy
import pyarrow
from pyarrow import compute

from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
from opteryx.expression.operations.fastpath_dictionary import has_dictionary_candidate
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def like(arr, value, dict_candidate=False):
    """SQL LIKE pattern matching (Like)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Like", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Like`.")
    return compute.match_like(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def not_like(arr, value, dict_candidate=False):
    """Negated SQL LIKE pattern matching (NotLike)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotLike`.")
    matches = compute.match_like(arr, value).to_numpy(False).astype(dtype=numpy.bool_)
    return numpy.invert(matches)


def ilike(arr, value, dict_candidate=False):
    """Case-insensitive SQL LIKE pattern matching (ILike)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "ILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `ILike`.")
    return (
        compute.match_like(arr, value, ignore_case=True).to_numpy(False).astype(dtype=numpy.bool_)
    )


def not_ilike(arr, value, dict_candidate=False):
    """Negated case-insensitive SQL LIKE pattern matching (NotILike)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotILike`.")
    matches = compute.match_like(arr, value, ignore_case=True)
    return numpy.invert(matches)


def rlike(arr, value, dict_candidate=False):
    """Regex pattern matching (RLike)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "RLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `RLike`.")
    return compute.match_substring_regex(arr, value).to_numpy(False).astype(dtype=numpy.bool_)


def not_rlike(arr, value, dict_candidate=False):
    """Negated regex pattern matching (NotRLike)."""
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotRLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotRLike`.")
    matches = compute.match_substring_regex(arr, value)
    return numpy.invert(matches)
