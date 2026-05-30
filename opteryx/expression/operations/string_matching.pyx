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


cpdef like(arr, value):
    return like_match(arr, value, "Like")


cpdef not_like(arr, value):
    return like_match(arr, value, "NotLike")


cpdef ilike(arr, value):
    return like_match(arr, value, "ILike")


cpdef not_ilike(arr, value):
    return like_match(arr, value, "NotILike")


cpdef rlike(arr, value):
    return rlike_match(arr, value, "RLike")


cpdef not_rlike(arr, value):
    return rlike_match(arr, value, "NotRLike")
