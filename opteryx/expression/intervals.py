# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Callable, Dict, Optional, Tuple

from opteryx.types import OrsoTypes
from opteryx.utils.vector_types import VectorType, get_vector_type

MICROSECONDS_PER_SECOND = 1_000_000
MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
NANOSECONDS_PER_MICROSECOND = 1_000
INTERVAL_OP_EQ = 0
INTERVAL_OP_NEQ = 1
INTERVAL_OP_GT = 2
INTERVAL_OP_GTE = 3
INTERVAL_OP_LT = 4
INTERVAL_OP_LTE = 5


def normalize_interval_value(value) -> Tuple[int, int]:
    """Normalize interval literals to canonical (months, microseconds)."""
    if not (isinstance(value, tuple) and len(value) == 2):
        raise TypeError(
            f"INTERVAL literal must be a (months, microseconds) tuple, got {type(value)!r}."
        )
    return (int(value[0]), int(value[1]))


def _as_interval_vector(values):
    if get_vector_type(values) == VectorType.INTERVAL:
        return values
    raise TypeError(
        "Expected IntervalVector for INTERVAL operation, "
        f"got {values.__class__.__name__}."
    )


def _date_plus_interval(left, left_type, right, right_type, operator):
    signum = 1 if operator == "Plus" else -1
    if left_type == OrsoTypes.INTERVAL:
        left, right = right, left

    interval_vector = _as_interval_vector(right)
    return interval_vector.apply_to_temporal(left, signum)


def _interval_interval_op(left, left_type, right, right_type, operator):
    left_vector = _as_interval_vector(left)
    right_vector = _as_interval_vector(right)

    if operator in ("Plus", "Minus"):
        return (
            left_vector.add_vector(right_vector)
            if operator == "Plus"
            else left_vector.subtract_vector(right_vector)
        )

    compare_ops = {
        "Eq": INTERVAL_OP_EQ,
        "NotEq": INTERVAL_OP_NEQ,
        "Gt": INTERVAL_OP_GT,
        "GtEq": INTERVAL_OP_GTE,
        "Lt": INTERVAL_OP_LT,
        "LtEq": INTERVAL_OP_LTE,
    }

    op_code = compare_ops.get(operator)
    if op_code is None:
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError(f"Unsupported INTERVAL operation `{operator}`.")

    try:
        return left_vector.compare_vector(right_vector, op_code, True)
    except ValueError as err:
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError(str(err)) from err


INTERVAL_KERNELS: Dict[Tuple[OrsoTypes, OrsoTypes, str], Optional[Callable]] = {
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Plus"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Minus"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Eq"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "NotEq"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Gt"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "GtEq"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Lt"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "LtEq"): _interval_interval_op,
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Plus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Minus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Plus"): _date_plus_interval,
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Minus"): _date_plus_interval,
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Plus"): _date_plus_interval,
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Minus"): _date_plus_interval,
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Plus"): _date_plus_interval,
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Minus"): _date_plus_interval,
}
