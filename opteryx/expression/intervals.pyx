"""INTERVAL kernel registry and helpers.

Cython migration of the former intervals.py. INTERVAL_KERNELS is consumed
by the binary-operator dispatcher; the helper functions handle interval ⊕
interval and interval ⊕ date/timestamp operations on top of the native
IntervalVector kernels.
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils.vector_types import VectorType, get_vector_type


# Native IntervalVector exposes integer op-codes for compare_vector; mirror
# them as compile-time DEFs so the dispatcher folds to literal ints.
DEF INTERVAL_OP_EQ = 0
DEF INTERVAL_OP_NEQ = 1
DEF INTERVAL_OP_GT = 2
DEF INTERVAL_OP_GTE = 3
DEF INTERVAL_OP_LT = 4
DEF INTERVAL_OP_LTE = 5

# Conversion constants — exposed at module level for callers that historically
# imported them. Module-level Python ints; no need for cdef in the rare
# read path.
MICROSECONDS_PER_SECOND = 1_000_000
MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
NANOSECONDS_PER_MICROSECOND = 1_000


cpdef tuple normalize_interval_value(value):
    """Normalise interval literals to a canonical (months, microseconds) tuple."""
    if not (isinstance(value, tuple) and len(value) == 2):
        raise TypeError(
            f"INTERVAL literal must be a (months, microseconds) tuple, "
            f"got {type(value)!r}."
        )
    return (int(value[0]), int(value[1]))


cpdef _as_interval_vector(values):
    """Verify `values` is an IntervalVector; fail-fast otherwise."""
    if get_vector_type(values) == VectorType.INTERVAL:
        return values
    raise TypeError(
        "Expected IntervalVector for INTERVAL operation, "
        f"got {values.__class__.__name__}."
    )


cpdef _date_plus_interval(left, left_type, right, right_type, str operator):
    """date/timestamp ⊕ interval (operands may be in either order).

    Delegates to the native temporal arithmetic path (temporal_ops), which calls
    the apply_to_temporal draken kernel directly.
    """
    from opteryx.expression.evaluator.temporal_ops import _date_interval_op_draken
    return _date_interval_op_draken(left, right, operator)


cpdef _interval_interval_op(left, left_type, right, right_type, str operator):
    """interval ⊕ interval — addition, subtraction, and the six comparisons."""
    if operator in ("Plus", "Minus"):
        from opteryx.expression.evaluator.temporal_ops import _interval_interval_op_draken
        return _interval_interval_op_draken(left, right, operator)

    left_vector = _as_interval_vector(left)
    right_vector = _as_interval_vector(right)

    cdef int op_code
    if operator == "Eq":
        op_code = INTERVAL_OP_EQ
    elif operator == "NotEq":
        op_code = INTERVAL_OP_NEQ
    elif operator == "Gt":
        op_code = INTERVAL_OP_GT
    elif operator == "GtEq":
        op_code = INTERVAL_OP_GTE
    elif operator == "Lt":
        op_code = INTERVAL_OP_LT
    elif operator == "LtEq":
        op_code = INTERVAL_OP_LTE
    else:
        raise UnsupportedSyntaxError(f"Unsupported INTERVAL operation `{operator}`.")

    try:
        return left_vector.compare_vector(right_vector, op_code, True)
    except ValueError as err:
        # The kernel surfaces precision-mismatch / unsupported-shape errors as
        # ValueError; convert to a user-facing syntax error.
        raise UnsupportedSyntaxError(str(err)) from err


INTERVAL_KERNELS = {
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "Plus"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "Minus"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "Eq"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "NotEq"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "Gt"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "GtEq"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "Lt"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.INTERVAL, "LtEq"): _interval_interval_op,
    (LogicalCategory.INTERVAL, LogicalCategory.TIMESTAMP, "Plus"): _date_plus_interval,
    (LogicalCategory.INTERVAL, LogicalCategory.TIMESTAMP, "Minus"): _date_plus_interval,
    (LogicalCategory.INTERVAL, LogicalCategory.DATE, "Plus"): _date_plus_interval,
    (LogicalCategory.INTERVAL, LogicalCategory.DATE, "Minus"): _date_plus_interval,
    (LogicalCategory.TIMESTAMP, LogicalCategory.INTERVAL, "Plus"): _date_plus_interval,
    (LogicalCategory.TIMESTAMP, LogicalCategory.INTERVAL, "Minus"): _date_plus_interval,
    (LogicalCategory.DATE, LogicalCategory.INTERVAL, "Plus"): _date_plus_interval,
    (LogicalCategory.DATE, LogicalCategory.INTERVAL, "Minus"): _date_plus_interval,
}
