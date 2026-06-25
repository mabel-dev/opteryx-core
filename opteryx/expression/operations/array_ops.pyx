"""Array operations (AnyOp*, AllOp*, @>, @>>, array contains)."""

import re as _re

from opteryx.compiled import vector_ops
from opteryx.compiled.nanobind.vectors import (
    vector_anyop_eq as _anyop_eq,
    vector_anyop_neq as _anyop_neq,
    vector_anyop_gt as _anyop_gt,
    vector_anyop_lt as _anyop_lt,
    vector_anyop_gte as _anyop_gte,
    vector_anyop_lte as _anyop_lte,
    vector_allop_eq as _allop_eq,
    vector_allop_neq as _allop_neq,
)
from opteryx.compiled.nanobind.vectors import (
    vector_contains_any as _vector_contains_any,
    vector_contains_all as _vector_contains_all,
)

from draken.vectors.bool_vector import BoolVector


cdef int _RE_IGNORECASE = _re.IGNORECASE


cpdef anyop_eq(literal, column):
    return _anyop_eq(literal=literal, column=column)


cpdef anyop_not_eq(literal, column):
    return _anyop_neq(literal=literal, column=column)


cpdef anyop_greater_than(literal, column):
    return _anyop_gt(literal, column)


cpdef anyop_less_than(literal, column):
    return _anyop_lt(literal, column)


cpdef anyop_greater_than_or_equal(literal, column):
    return _anyop_gte(literal, column)


cpdef anyop_less_than_or_equal(literal, column):
    return _anyop_lte(literal, column)


cpdef anyop_like(arr, value, int flags=0):
    return vector_ops.regex_match_any(arr, value, flags=flags)


cpdef anyop_ilike(arr, value):
    return vector_ops.regex_match_any(arr, value, flags=_RE_IGNORECASE)


cpdef anyop_not_like(arr, value, int flags=0):
    return vector_ops.regex_match_any(arr, value, flags=flags, invert=True)


cpdef anyop_not_ilike(arr, value):
    return vector_ops.regex_match_any(arr, value, flags=_RE_IGNORECASE, invert=True)


cpdef allop_eq(literal, column):
    return _allop_eq(literal, column)


cpdef allop_not_eq(literal, column):
    return _allop_neq(literal, column)


cpdef array_contains_any(arr, value):
    """Check if array contains any of the values (@>)."""
    cdef Py_ssize_t n = len(arr)
    if n == 0:
        return BoolVector(0)

    if n == 1:
        elem = arr[0]
        if elem is None:
            return BoolVector.from_constant(False, 1)

        value_set = set(value) if value is not None else set()
        try:
            elem_set = set(elem)
        except TypeError:
            # `elem` isn't iterable — treat the row as a single-element bag.
            elem_set = {elem}

        result = bool(elem_set.intersection(value_set))
        return BoolVector.from_constant(result, 1)

    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    return _vector_contains_any(arr, set(value))


cpdef array_contains_all(arr, value):
    """Check if array contains all of the values (@>>)."""
    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    if len(arr) == 1 and len(value) != 0:
        raise ValueError(
            "Unable to execute @>>, check form matches `column @>> (values)`."
        )

    return _vector_contains_all(arr, set(value))
