# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""
Date Utilities
"""

import datetime
from typing import Union

from opteryx.exceptions import InvalidFunctionParameterError

UNIX_EPOCH: datetime.date = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def add_months(start_date: datetime.datetime, number_of_months: int):
    """
    Add months to a date, makes assumptions about how to handle the end of the month.
    """
    new_year, new_month = divmod(start_date.month - 1 + number_of_months, 12)
    new_year += start_date.year
    new_month += 1
    # Ensure the month is valid
    new_month = min(max(1, new_month), 12)
    last_day_of_month = (
        datetime.datetime(new_year, new_month % 12 + 1, 1) - datetime.timedelta(days=1)
    ).day
    new_day = min(start_date.day, last_day_of_month)
    return datetime.datetime(
        new_year,
        new_month,
        new_day,
        start_date.hour,
        start_date.minute,
        start_date.second,
        start_date.microsecond,
    )


def add_interval(
    current_date: datetime.datetime, interval: tuple
) -> Union[datetime.date, datetime.datetime]:
    """
    Add one INTERVAL to a datetime.

    `interval` is the engine's canonical INTERVAL literal value: a
    ``(months, microseconds)`` pair, which is how the logical planner builds
    every INTERVAL and what `_apply_interval_scalar` already applies elsewhere.
    The two components are separate because they are not interconvertible - a
    month is 28 to 31 days - so months are applied by calendar arithmetic
    (`add_months`, which clamps 31st-of-the-month to the target month's last
    day) and microseconds by exact duration. Both may be negative.

    This used to parse a human-readable string ("3d5h19m"). Nothing ever
    produced one: its only caller is `date_range`, whose only caller is
    GENERATE_SERIES, which is handed a planner INTERVAL - so the string
    contract had no live input and the temporal series raised a raw TypeError
    instead of running.
    """
    months, microseconds = interval
    if months:
        current_date = add_months(current_date, int(months))
    if microseconds:
        current_date = current_date + datetime.timedelta(microseconds=int(microseconds))
    return current_date


def date_range(start_date, end_date, interval: tuple):
    """Every timestamp from `start_date` to `end_date` inclusive, one INTERVAL apart.

    `interval` is a ``(months, microseconds)`` pair - see `add_interval`.

    The interval must move the cursor TOWARDS `end_date`, and it must move it at
    all. An interval of zero, or one pointing the wrong way, describes a series
    with no end: the loop below would run until the process died. Both are
    refused up front, by name, rather than being allowed to hang.
    """
    start_date = parse_iso(start_date)
    end_date = parse_iso(end_date)

    if start_date is None or end_date is None:
        raise InvalidFunctionParameterError(
            "GENERATE_SERIES over timestamps needs a start and an end that are "
            "timestamps. Cast them if they are strings: "
            "`GENERATE_SERIES(CAST('2020-01-01' AS TIMESTAMP), "
            "CAST('2020-01-02' AS TIMESTAMP), INTERVAL '1' HOUR)`."
        )

    months, microseconds = interval
    if not months and not microseconds:
        raise InvalidFunctionParameterError(
            "GENERATE_SERIES cannot use an INTERVAL of zero - the series would never "
            "reach its end."
        )

    descending = start_date > end_date
    if descending:
        # A descending series needs a NEGATIVE interval, and vice versa. The step
        # is tested by applying it once rather than by reading the sign off the
        # pair, because the two components can disagree (INTERVAL '1' MONTH minus
        # a day carries a positive month and negative microseconds) and only the
        # calendar knows which wins.
        if add_interval(start_date, interval) >= start_date:
            raise InvalidFunctionParameterError(
                "GENERATE_SERIES was given a start after its end and an INTERVAL that "
                "does not count down. Swap the bounds, or negate the INTERVAL."
            )
    elif add_interval(start_date, interval) <= start_date:
        raise InvalidFunctionParameterError(
            "GENERATE_SERIES was given a start before its end and an INTERVAL that "
            "does not count up. Swap the bounds, or negate the INTERVAL."
        )

    cursor = start_date
    while (cursor >= end_date) if descending else (cursor <= end_date):
        yield cursor
        cursor = add_interval(cursor, interval)


def parse_iso(value):
    # Date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert up to
    # three times faster than dateutil.
    #
    # valid formats (not exhaustive):
    #
    #   YYYY-MM-DD                 <- date
    #   YYYY-MM-DD HH:MM           <- date and time, no seconds
    #   YYYY-MM-DDTHH:MM           <- date and time, T separator
    #   YYYY-MM-DD HH:MM:SS        <- date and time with seconds
    #   YYYY-MM-DD HH:MM:SS.mmmm   <- date and time with milliseconds
    #
    # If the last character is a Z, we ignore it.
    # If we can't parse as a date we return None rather than error

    from opteryx.compiled.functions.timestamp import parse_iso as c_parse_iso

    try:
        input_type = type(value)

        if input_type is str and value.isdigit():
            value = int(value)
            input_type = int

        if input_type in (int, float):
            return datetime.datetime.fromtimestamp(int(value), tz=datetime.timezone.utc).replace(
                tzinfo=None
            )

        if input_type is datetime.datetime:
            return value.replace(microsecond=0)
        if input_type is datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)

        if isinstance(value, str):
            value = value.encode("utf-8")

        return c_parse_iso(value)

    except (ValueError, TypeError):
        return None


def truncate_single(dt: datetime.datetime, unit: str) -> datetime.datetime:
    """
    Floor a datetime to the start of the specified unit.

    Supports units: 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
    Week is ISO week (Monday-based).
    """
    if unit == "second":
        return dt.replace(microsecond=0)
    elif unit == "minute":
        return dt.replace(second=0, microsecond=0)
    elif unit == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif unit == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif unit == "week":
        # ISO week: Monday is day 0. weekday() returns 0=Monday, 6=Sunday
        days_since_monday = dt.weekday()
        floor_date = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return floor_date - datetime.timedelta(days=days_since_monday)
    elif unit == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif unit == "quarter":
        quarter_month = ((dt.month - 1) // 3) * 3 + 1
        return dt.replace(month=quarter_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif unit == "year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unsupported truncation unit: {unit}")


def add_single_unit(dt: datetime.datetime, unit: str, n: int = 1) -> datetime.datetime:
    """
    Add n units to a datetime.

    Supports units: 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
    Week is treated as 7 days.
    Month/quarter/year use add_months to handle edge cases (e.g., Jan 31 + 1 month = Feb 28/29).
    """
    if unit == "second":
        return dt + datetime.timedelta(seconds=n)
    elif unit == "minute":
        return dt + datetime.timedelta(minutes=n)
    elif unit == "hour":
        return dt + datetime.timedelta(hours=n)
    elif unit == "day":
        return dt + datetime.timedelta(days=n)
    elif unit == "week":
        return dt + datetime.timedelta(days=n * 7)
    elif unit == "month":
        return add_months(dt, n)
    elif unit == "quarter":
        return add_months(dt, n * 3)
    elif unit == "year":
        return add_months(dt, n * 12)
    else:
        raise ValueError(f"Unsupported unit: {unit}")
