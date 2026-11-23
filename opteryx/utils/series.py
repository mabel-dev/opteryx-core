# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from opteryx.exceptions import InvalidFunctionParameterError, SqlError
from opteryx.types.logical_type import LogicalCategory, ColumnType, _NUMERIC_TYPES
from opteryx.utils import dates


def generate_series(*args):
    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].type
    first_arg_cat = first_arg_type.category if isinstance(first_arg_type, ColumnType) else first_arg_type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_cat in _NUMERIC_TYPES:
        if arg_len not in (1, 2, 3):  # pragma: no cover
            raise SqlError(
                "GENERATE_SERIES for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters."
            )
        return numeric_range(*arg_vals)

    # if the params are timestamps, we create time intervals
    if first_arg_cat in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP):
        if arg_len != 3:  # pragma: no cover
            raise SqlError("generate_series for dates needs start, end, and interval parameters")
        return dates.date_range(*arg_vals)

    raise InvalidFunctionParameterError(
        "Unsupported value for GENERATE_SERIES, must be date or numeric series."
    )


def _is_integer_like(v) -> bool:
    """Return True if v is an integer-like value (not bool, not float)."""
    if isinstance(v, bool):
        return False
    if isinstance(v, int):
        return True
    if isinstance(v, float):
        return False
    # Catch integer-like types (e.g. objects with __index__) via duck typing
    return getattr(v, "__index__", None) is not None


def integer_series_extent(start, stop, step) -> tuple:
    """`(start, step, count)` of the integer series `start..stop` by `step`, without
    building it: a `range` answers its length and last value in O(1). This is the one
    place the series' length is decided - the native range source and `numeric_range`
    both take it from here."""
    start, stop, step = int(start), int(stop), int(step)
    values = range(start, stop + step, step)
    count = len(values)
    if count and values[-1] > stop:
        count -= 1
    return start, step, count


def integer_series_arguments(*args) -> tuple:
    """`(start, stop, step)` from GENERATE_SERIES' 1, 2 or 3 numeric arguments."""
    if len(args) == 1:
        return 1, args[0], 1
    if len(args) == 2:
        return args[0], args[1], 1
    if len(args) == 3:
        return args[0], args[1], args[2]
    raise ValueError("Invalid number of arguments. Expected 1, 2, or 3: start, stop [, step].")


def numeric_range(*args) -> list:
    """
    Generate a numeric range of values.

    Args:
        [start, ]stop[, step]: start defaults to 1, step defaults to 1.

    Returns:
        list: List of evenly spaced numeric values.

    Raises:
        ValueError: If the number of arguments is not 1, 2, or 3.

    Examples:
        numeric_range(5)
        numeric_range(1, 5)
        numeric_range(1, 5, 0.5)
    """
    start, stop, step = integer_series_arguments(*args)

    # Use integer arithmetic when all values are integer-like
    if _is_integer_like(start) and _is_integer_like(stop) and _is_integer_like(step):
        start, step, count = integer_series_extent(start, stop, step)
        return list(range(start, start + count * step, step))

    # Float range: iterative addition with tolerance-based boundary check
    start, stop, step = float(start), float(stop), float(step)
    tolerance = step / 2
    result = []
    val = start
    while val <= stop + tolerance:
        result.append(val)
        val += step
    # Remove last value if it doesn't fall on a step boundary or exceeds stop
    if result and (abs(result[-1] - stop) > tolerance or result[-1] > stop):
        result.pop()
    return result
