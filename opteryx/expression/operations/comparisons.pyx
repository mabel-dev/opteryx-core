"""Comparison operations (Eq, NotEq, Lt, Gt, LtEq, GtEq).

Draken-native only — `arr` must be a Draken vector. If callers ever pass
something else, the AttributeError from the missing method surfaces; we
don't paper over it.
"""



cpdef equal(arr, value):
    return arr.equals(value)


cpdef not_equal(arr, value):
    return arr.not_equals(value)


cpdef less_than(arr, value):
    return arr.less_than(value)


cpdef greater_than(arr, value):
    return arr.greater_than(value)


cpdef less_than_or_equal(arr, value):
    return arr.less_than_or_equals(value)


cpdef greater_than_or_equal(arr, value):
    return arr.greater_than_or_equals(value)
