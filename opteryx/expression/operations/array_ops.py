"""Array operations (AnyOp*, AllOp*, @>>, array contains)."""

import numpy

from opteryx.compiled import vector_ops


def anyop_eq(literal, column):
    """Check if any element in column equals literal (AnyOpEq)."""
    return vector_ops.vector_anyop_eq(literal=literal, column=column)


def anyop_not_eq(literal, column):
    """Check if any element in column does not equal literal (AnyOpNotEq)."""
    return vector_ops.vector_anyop_neq(literal=literal, column=column)


def anyop_greater_than(literal, column):
    """Check if any element in column is greater than literal (AnyOpGt)."""
    return vector_ops.vector_anyop_gt(literal, column)


def anyop_less_than(literal, column):
    """Check if any element in column is less than literal (AnyOpLt)."""
    return vector_ops.vector_anyop_lt(literal, column)


def anyop_greater_than_or_equal(literal, column):
    """Check if any element in column is greater than or equal to literal (AnyOpGtEq)."""
    return vector_ops.vector_anyop_gte(literal, column)


def anyop_less_than_or_equal(literal, column):
    """Check if any element in column is less than or equal to literal (AnyOpLtEq)."""
    return vector_ops.vector_anyop_lte(literal, column)


def anyop_like(arr, value, flags=0):
    """Check if any element in column matches LIKE pattern (AnyOpLike)."""
    import re

    from opteryx.utils.sql import regex_match_any

    return regex_match_any(arr, value, flags=flags)


def anyop_ilike(arr, value):
    """Check if any element in column matches case-insensitive LIKE pattern (AnyOpILike)."""
    import re

    from opteryx.utils.sql import regex_match_any

    return regex_match_any(arr, value, flags=re.IGNORECASE)


def anyop_not_like(arr, value, flags=0):
    """Check if no element in column matches LIKE pattern (AnyOpNotLike)."""
    import re

    from opteryx.utils.sql import regex_match_any

    return regex_match_any(arr, value, flags=flags, invert=True)


def anyop_not_ilike(arr, value):
    """Check if no element in column matches case-insensitive LIKE pattern (AnyOpNotILike)."""
    import re

    from opteryx.utils.sql import regex_match_any

    return regex_match_any(arr, value, flags=re.IGNORECASE, invert=True)


def allop_eq(literal, column):
    """Check if all elements in column equal literal (AllOpEq)."""
    return vector_ops.vector_allop_eq(literal, column)


def allop_not_eq(literal, column):
    """Check if all elements in column do not equal literal (AllOpNotEq)."""
    return vector_ops.vector_allop_neq(literal, column)


def array_contains_any(arr, value):
    """Check if array contains any of the values (@>)."""
    if len(arr) == 0:
        return numpy.array([], dtype=numpy.bool_)

    if len(arr) == 1:
        elem = arr[0]
        if elem is None:
            return numpy.array([False], dtype=numpy.bool_)

        value_set = set(value) if value is not None else set()
        try:
            elem_set = set(elem)
        except TypeError:
            elem_set = {elem}

        result = bool(elem_set.intersection(value_set))
        return numpy.array([result], dtype=numpy.bool_)

    to_numpy = getattr(arr, "to_numpy", None)
    if to_numpy is not None:
        arr = to_numpy(zero_copy_only=False)

    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    return vector_ops.vector_contains_any(arr, set(value))


def array_contains_all(arr, value):
    """Check if array contains all of the values (@>>)."""
    to_pylist = getattr(value, "to_pylist", None)
    if to_pylist is not None:
        value = to_pylist()

    to_numpy = getattr(arr, "to_numpy", None)
    if to_numpy is not None:
        arr = to_numpy(zero_copy_only=False)

    if len(arr) == 1 and len(value) != 0:
        raise ValueError("Unable to execute @>>, check form matches `column @>> (values)`.")

    return vector_ops.vector_contains_all(arr, set(value))
