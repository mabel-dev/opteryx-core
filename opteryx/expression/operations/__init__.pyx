# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Filter operations dispatcher - Draken-native only.

All array inputs must be Draken vectors. No foreign-array conversion or
fallbacks. Null handling is native to Draken vector operations. If you get
AttributeError, your input isn't Draken — that's a bug upstream.

Consolidation: the leaf modules (comparisons / list_ops / array_ops /
string_matching / special_ops / fastpath_*) are textually included into
this umbrella so the package compiles to a single .so. All cdef/cpdef
helpers from those files live in this module's namespace.
"""

from draken.vectors.bool_vector import BoolVector
from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
)
from opteryx.compiled.nanobind.vectors import vector_in_list

# Leaf includes — the operator wrappers (comparisons / list_ops /
# string_matching) route through the uniform Draken vector path. array_ops is
# independent and follows.
include "comparisons.pyx"
include "list_ops.pyx"
include "string_matching.pyx"
include "array_ops.pyx"


# Operators that should skip null compression during filtering.
_SKIP_COMPRESSION_OPS = frozenset(
    (
        "InList",
        "NotInList",
        "AnyOpEq",
        "AnyOpNotEq",
        "AnyOpGt",
        "AnyOpGtEq",
        "AnyOpLt",
        "AnyOpLtEq",
        "AnyOpLike",
        "AnyOpNotLike",
        "AnyOpILike",
        "AnyOpNotILike",
        "AllOpLike",
        "AllOpNotLike",
        "AllOpILike",
        "AllOpNotILike",
        "AllOpEq",
        "AllOpNotEq",
        "AtArrow",
        "ArrayContainsAll",
    )
)


def _inner_filter_operations(arr, operator, value):
    """Dispatch filter operation to appropriate handler."""
    raw_arr = arr

    # Normalize scalar values for non-array operations
    if not operator.startswith(("AnyOp", "AllOp")):
        try:
            if len(value) == 1:
                value = value[0]
                try:
                    value = value.item()
                except AttributeError:
                    pass
        except TypeError:
            pass

    # InStr — uniform substring match via the Draken vector kernel.
    if operator in ("InStr", "NotInStr", "IInStr", "NotIInStr"):
        ignore_case = operator in ("IInStr", "NotIInStr")
        negate = operator in ("NotInStr", "NotIInStr")
        raw_value = value[0] if getattr(value, "__len__", None) is not None and len(value) == 1 else value
        needle = raw_value if isinstance(raw_value, bytes) else str(raw_value).encode("utf-8")
        result = raw_arr.contains(needle, ignore_case=ignore_case)
        return result.not_vector() if negate else result

    # Dispatch by operator type — symbols are in-scope via the leaf includes.
    if operator in ("Eq", "Equal"):
        return equal(raw_arr, value)
    elif operator in ("NotEq", "NotEqual"):
        return not_equal(raw_arr, value)
    elif operator in ("Lt", "LessThan"):
        return less_than(raw_arr, value)
    elif operator in ("Gt", "GreaterThan"):
        return greater_than(raw_arr, value)
    elif operator in ("LtEq", "LessThanOrEqual"):
        return less_than_or_equal(raw_arr, value)
    elif operator in ("GtEq", "GreaterThanOrEqual"):
        return greater_than_or_equal(raw_arr, value)
    elif operator in ("InList",):
        return in_list(raw_arr, value)
    elif operator in ("NotInList",):
        return not_in_list(raw_arr, value)
    elif operator.startswith("Like"):
        return like_match(raw_arr, value, operator)
    elif operator.startswith("RLike"):
        return rlike_match(raw_arr, value, operator)
    else:
        # AnyOp* / AllOp* / AtArrow are dispatched via
        # evaluator.comparisons.draken_compare before they can reach here.
        raise NotImplementedError(f"Filter operation `{operator}` not implemented.")
