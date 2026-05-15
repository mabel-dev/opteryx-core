"""LIKE / ILIKE / RLIKE pattern-match dispatch.

Calls into the native StringVector.like / rlike kernels; the wrappers handle
list-wrapping of the pattern (some callers pass a length-1 list) and
NotLike-style negation.
"""



cpdef like_match(arr, value, str operator):
    """Dispatch Like / NotLike / ILike / NotILike to StringVector.like."""
    if isinstance(value, (list, tuple)):
        value = value[0] if value else b""

    cdef bint ignore_case = "ILike" in operator
    cdef bint negate = operator.startswith("Not")

    result = arr.like(value, ignore_case=ignore_case)
    return result.not_vector() if negate else result


cpdef rlike_match(arr, value, str operator):
    """Dispatch RLike / NotRLike to StringVector.rlike."""
    if isinstance(value, (list, tuple)):
        value = value[0] if value else b""

    if isinstance(value, bytes):
        value = value.decode("utf-8")
    elif not isinstance(value, str):
        value = str(value)

    cdef bint negate = operator.startswith("Not")
    result = arr.rlike(value)
    return result.not_vector() if negate else result


cpdef like(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Like", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Like`.")
    return like_match(arr, value, "Like")


cpdef not_like(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotLike`.")
    return like_match(arr, value, "NotLike")


cpdef ilike(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "ILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `ILike`.")
    return like_match(arr, value, "ILike")


cpdef not_ilike(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotILike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotILike`.")
    return like_match(arr, value, "NotILike")


cpdef rlike(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "RLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `RLike`.")
    return rlike_match(arr, value, "RLike")


cpdef not_rlike(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotRLike", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotRLike`.")
    return rlike_match(arr, value, "NotRLike")
