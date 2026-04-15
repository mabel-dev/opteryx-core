# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""String matching operations (Like, RLike, etc.).

Uses Draken StringVector native like() and rlike() methods.
No pyarrow dependency.
"""

from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit


def like_match(arr, value, operator):
    """Dispatch Like/NotLike/ILike/NotILike to Draken StringVector native methods.

    Called by filter_operations dispatcher with operator in:
        'Like', 'NotLike', 'ILike', 'NotILike'
    """
    if isinstance(value, (list, tuple)):
        value = value[0] if value else b""

    ignore_case = "ILike" in operator
    negate = operator.startswith("Not")

    result = arr.like(value, ignore_case=ignore_case)
    return result.not_vector() if negate else result


def rlike_match(arr, value, operator):
    """Dispatch RLike/NotRLike to Draken StringVector native methods.

    Called by filter_operations dispatcher with operator in:
        'RLike', 'NotRLike'
    """
    if isinstance(value, (list, tuple)):
        value = value[0] if value else b""

    if isinstance(value, bytes):
        value = value.decode("utf-8")
    elif not isinstance(value, str):
        value = str(value)

    result = arr.rlike(value)
    return result.not_vector() if operator.startswith("Not") else result


# Individual functions retained for any direct callers outside the dispatcher.


def like(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Like", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Like`.")
    return like_match(arr, value, "Like")


def not_like(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotLike`.")
    return like_match(arr, value, "NotLike")


def ilike(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "ILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `ILike`.")
    return like_match(arr, value, "ILike")


def not_ilike(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotILike`.")
    return like_match(arr, value, "NotILike")


def rlike(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "RLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `RLike`.")
    return rlike_match(arr, value, "RLike")


def not_rlike(arr, value, dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotRLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotRLike`.")
    return rlike_match(arr, value, "NotRLike")
