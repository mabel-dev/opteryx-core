"""Comparison operations (Eq, NotEq, Lt, Gt, LtEq, GtEq).

Draken-native only — `arr` must be a Draken vector. If callers ever pass
something else, the AttributeError from the missing method surfaces; we
don't paper over it.
"""



cpdef equal(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Eq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Eq`.")
    return arr.equals(value)


cpdef not_equal(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "NotEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `NotEq`.")
    return arr.not_equals(value)


cpdef less_than(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Lt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Lt`.")
    return arr.less_than(value)


cpdef greater_than(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "Gt", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `Gt`.")
    return arr.greater_than(value)


cpdef less_than_or_equal(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "LtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `LtEq`.")
    return arr.less_than_or_equals(value)


cpdef greater_than_or_equal(arr, value, bint dict_candidate=False):
    if dict_candidate:
        fast = dictionary_fastpath(arr, "GtEq", value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast
        raise RuntimeError("Dictionary fastpath failed for `GtEq`.")
    return arr.greater_than_or_equals(value)
