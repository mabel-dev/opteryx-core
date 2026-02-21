# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""
Date Utilities
"""

import datetime
import re
from typing import Union

import numpy
import pyarrow
from pyarrow import compute

TIMEDELTA_REGEX = (
    r"((?P<years>\d+)\s?(?:ys?|yrs?|years?))?\s*"
    r"((?P<months>\d+)\s?(?:mo|mons?|mths?|months?))?\s*"
    r"((?P<weeks>\d+)\s?(?:w|wks?|weeks?))?\s*"
    r"((?P<days>\d+)\s?(?:d|days?))?\s*"
    r"((?P<hours>\d+)\s?(?:h|hrs?|hours?))?\s*"
    r"((?P<minutes>\d+)\s?(?:m|mins?|minutes?))?\s*"
    r"((?P<seconds>\d+)\s?(?:s|secs?|seconds?))?\s*"
)

TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)
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
    current_date: datetime.datetime, interval: str
) -> Union[datetime.date, datetime.datetime]:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.
    """
    match = TIMEDELTA_PATTERN.match(interval)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        # time delta doesn't include weeks, months or years
        if "weeks" in parts:
            weeks = parts.pop("weeks")
            current_date = current_date + datetime.timedelta(days=weeks * 7)
        if "months" in parts:
            months = parts.pop("months")
            current_date = add_months(current_date, months)
        if "years" in parts:
            # need to avoid 29th Feb problems, so can't just say year - year
            years = parts.pop("years")
            current_date = add_months(current_date, 12 * years)
        if parts:
            return current_date + datetime.timedelta(**parts)
        return current_date
    raise ValueError(f"Unable to interpret interval - {interval}")  # pragma: no cover


def date_range(start_date, end_date, interval: str):
    """Create a series of dates between two dates with a given interval"""
    start_date = parse_iso(start_date)
    end_date = parse_iso(end_date)

    if start_date > end_date:  # pragma: no cover
        raise ValueError("Cannot create an series with the provided start and end dates")

    # if the dates are the same, return that date
    if start_date == end_date:  # pragma: no cover
        yield start_date
        return

    cursor = start_date
    while cursor <= end_date:
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

        if input_type is numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime.datetime)
            input_type = type(value)
            if input_type is int:
                value /= 1000000000

        if input_type in (int, numpy.int64, float, numpy.float64):
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


def date_trunc(truncate_to, date_values) -> numpy.ndarray:
    """
    Truncate an array of datetimes to a specified unit
    """
    if isinstance(date_values, pyarrow.ChunkedArray):
        date_values = date_values.combine_chunks()
    elif not isinstance(date_values, pyarrow.Array):
        date_values = pyarrow.array(date_values)

    if not isinstance(truncate_to, str):
        truncate_to = truncate_to[0]
        if hasattr(truncate_to, "as_py"):
            truncate_to = truncate_to.as_py()
        elif hasattr(truncate_to, "item"):
            truncate_to = truncate_to.item()
        truncate_to = str(truncate_to)

    value_type = date_values.type
    if pyarrow.types.is_date32(value_type) or pyarrow.types.is_date64(value_type):
        date_values = compute.cast(date_values, pyarrow.timestamp("us"))
    elif pyarrow.types.is_integer(value_type):
        non_null_values = compute.drop_null(date_values)
        unit = "us"
        if len(non_null_values):
            absolute_max = int(numpy.max(numpy.abs(non_null_values.to_numpy(zero_copy_only=False))))
            if absolute_max < 10**7:
                date_values = compute.cast(
                    compute.cast(date_values, pyarrow.date32()), pyarrow.timestamp("s")
                )
                unit = None
            elif absolute_max < 10**11:
                unit = "s"
            elif absolute_max < 10**14:
                unit = "ms"
            elif absolute_max < 10**17:
                unit = "us"
            else:
                unit = "ns"
        if unit:
            date_values = compute.cast(date_values, pyarrow.timestamp(unit))
    elif not pyarrow.types.is_timestamp(value_type):
        date_values = compute.cast(date_values, pyarrow.timestamp("us"))

    from opteryx.compiled.list_ops import list_date_trunc

    return list_date_trunc(truncate_to, date_values)
