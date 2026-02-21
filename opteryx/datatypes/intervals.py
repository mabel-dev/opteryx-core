# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

import pyarrow
from orso.types import OrsoTypes

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


def _normalise_interval_value(value) -> Tuple[int, int]:
    """
    Convert different interval representations into the internal (months, microseconds) tuple form.
    """
    if value is None:
        return (0, 0)
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        if len(value) >= 3:
            months, days, nanoseconds = value[:3]
            micros = int(days) * MICROSECONDS_PER_DAY + int(nanoseconds) // NANOSECONDS_PER_MICROSECOND
            return (int(months), micros)
        if len(value) >= 2:
            months, microseconds = value[:2]
            return (int(months), int(microseconds))
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, dict):
        months = value.get("months", 0)
        if "microseconds" in value:
            micros = value.get("microseconds", 0)
        else:
            days = value.get("days", 0)
            nanoseconds = value.get("nanoseconds", 0)
            micros = int(days) * MICROSECONDS_PER_DAY + int(nanoseconds) // NANOSECONDS_PER_MICROSECOND
        if months is None or micros is None:
            return (0, 0)
        return (int(months), int(micros))
    if hasattr(value, "months") and hasattr(value, "nanoseconds"):
        months = int(value.months)
        micros = (
            int(value.days) * MICROSECONDS_PER_DAY
            + int(value.nanoseconds) // NANOSECONDS_PER_MICROSECOND
        )
        return (months, micros)
    return value


def normalize_interval_value(value) -> Tuple[int, int]:
    """
    Public wrapper for interval normalization.
    """
    return _normalise_interval_value(value)


def _coerce_interval_entry(entry) -> Optional[Tuple[int, int]]:
    if entry is None:
        return None
    months, microseconds = _normalise_interval_value(entry)
    if months is None or microseconds is None:
        return None
    return (int(months), int(microseconds))


def _interval_rows_from_values(values) -> List[Optional[Tuple[int, int]]]:
    if isinstance(values, pyarrow.ChunkedArray):
        rows = []
        for chunk in values.chunks:
            rows.extend(_interval_rows_from_values(chunk))
        return rows

    if isinstance(values, pyarrow.Array):
        # Fast-path when Draken can directly decode Arrow interval layouts.
        try:
            from opteryx.draken.interop.arrow import vector_from_arrow

            vector = vector_from_arrow(values)
            if vector.__class__.__name__ == "IntervalVector":
                return [_coerce_interval_entry(value) for value in vector.to_pylist()]
        except Exception:
            pass

        return [_coerce_interval_entry(value) for value in values.to_pylist()]

    if hasattr(values, "to_numpy"):
        values = values.to_numpy(zero_copy_only=False)

    if isinstance(values, tuple):
        return [_coerce_interval_entry(values)]

    if isinstance(values, list):
        return [_coerce_interval_entry(value) for value in values]

    try:
        return [_coerce_interval_entry(value) for value in values]
    except TypeError:
        return [_coerce_interval_entry(values)]


def _as_interval_vector(values):
    from opteryx.draken.interop.arrow import vector_from_arrow

    if values.__class__.__name__ == "IntervalVector":
        return values

    if isinstance(values, pyarrow.Array):
        try:
            vector = vector_from_arrow(values)
            if vector.__class__.__name__ == "IntervalVector":
                return vector
        except Exception:
            pass

    rows = _interval_rows_from_values(values)
    mdn_array = _intervals_to_month_day_nano(rows)
    return vector_from_arrow(mdn_array)


def _intervals_to_month_day_nano(rows: Iterable[Optional[Tuple[int, int]]]) -> pyarrow.Array:
    """
    Convert an iterable of (months, microseconds) tuples into a month-day-nano INTERVAL Arrow array.
    """
    converted = []
    for entry in rows:
        if entry is None or any(component is None for component in entry):
            converted.append(None)
            continue
        months, microseconds = entry
        if microseconds is None:
            converted.append((int(months), 0, 0))
            continue
        days, remainder = divmod(int(microseconds), MICROSECONDS_PER_DAY)
        nanoseconds = remainder * NANOSECONDS_PER_MICROSECOND
        converted.append((int(months), int(days), int(nanoseconds)))
    return pyarrow.array(converted, type=pyarrow.month_day_nano_interval())


def to_arrow_interval(array: pyarrow.Array) -> pyarrow.Array:
    """
    Ensure the provided Arrow array uses the month-day-nano INTERVAL logical type.
    """
    if isinstance(array, pyarrow.ChunkedArray):
        converted = [to_arrow_interval(chunk) for chunk in array.chunks]
        return pyarrow.chunked_array(converted)

    if pyarrow.types.is_interval(array.type) and array.type == pyarrow.month_day_nano_interval():
        return array

    rows = _interval_rows_from_values(array)
    return _intervals_to_month_day_nano(rows)


def _date_plus_interval(left, left_type, right, right_type, operator):
    """
    Adds intervals to dates, utilizing integer arithmetic for performance improvements.
    """
    signum = 1 if operator == "Plus" else -1
    if left_type == OrsoTypes.INTERVAL:
        left, right = right, left

    interval_vector = _as_interval_vector(right)
    return interval_vector.apply_to_temporal(left, signum)


def _interval_interval_op(left, left_type, right, right_type, operator):
    left_vector = _as_interval_vector(left)
    right_vector = _as_interval_vector(right)

    if operator in ("Plus", "Minus"):
        result_vector = (
            left_vector.add_vector(right_vector)
            if operator == "Plus"
            else left_vector.subtract_vector(right_vector)
        )
        return result_vector.to_arrow_binary()

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
        bool_vector = left_vector.compare_vector(right_vector, op_code, True)
    except ValueError as err:
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError(str(err)) from err
    return bool_vector.to_arrow()


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
