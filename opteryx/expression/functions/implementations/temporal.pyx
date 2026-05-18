# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Temporal function kernels.

Includes:
- Date/time functions: TRUNC, TIME_BUCKET, DATEDIFF, TIMEDIFF, EXTRACT, DATE_FORMAT
- Current time: CURRENT_TIME, CURRENT_TIMESTAMP, UTC_TIMESTAMP, NOW, CURRENT_DATE, TODAY, YESTERDAY
- Component extraction: YEAR, MONTH, DAY, WEEK, HOUR, MINUTE, SECOND, QUARTER
- Unix epoch conversion: FROM_UNIXTIME, UNIXTIME
"""

import datetime

from opteryx.exceptions import InvalidFunctionParameterError


def date_part(part, arr):
    """
    Extract a part from a date/timestamp (EXTRACT function).

    Inputs (guaranteed by the draken kernel dispatch layer):
      part: a Draken constant vector — the datepart name (bytes scalar)
      arr:  a Draken TimestampVector or Date32Vector

    Compiled kernels only — no Arrow fallback.
    Raises InvalidFunctionParameterError for unsupported dateparts or input types.

    Supported dateparts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter
    Unsupported dateparts: week, isoweek, isoyear, decade, century, epoch, julian, date,
                           millisecond, microsecond, nanosecond (require implementation)
    """
    # part arrives as a Draken constant vector; extract the scalar bytes value.
    part = part[0].lower()

    vector_type = arr.__class__.__name__

    # Reject Integer64Vector — no implicit temporal coercion
    if vector_type == "Integer64Vector":
        raise InvalidFunctionParameterError(
            f"EXTRACT({part.decode().upper()}) cannot operate on INTEGER values. "
            "Provide a TIMESTAMP or DATE column instead. "
            "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
            "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
        )

    # Date32Vector: convert to TimestampVector so the timestamp kernels can be reused.
    if vector_type == "Date32Vector":
        from opteryx.compiled.vector_ops import vector_date32_to_timestamp

        arr = vector_date32_to_timestamp(arr)
        vector_type = "TimestampVector"

    if vector_type == "TimestampVector":
        from opteryx.compiled.vector_ops import (
            vector_datepart_day,
            vector_datepart_dayofweek,
            vector_datepart_dayofyear,
            vector_datepart_hour,
            vector_datepart_minute,
            vector_datepart_month,
            vector_datepart_quarter,
            vector_datepart_second,
            vector_datepart_year,
        )

        if part == b"minute":
            return vector_datepart_minute(arr)
        elif part == b"hour":
            return vector_datepart_hour(arr)
        elif part in (b"second", b"seconds"):
            return vector_datepart_second(arr)
        elif part == b"year":
            return vector_datepart_year(arr)
        elif part == b"month":
            return vector_datepart_month(arr)
        elif part == b"day":
            return vector_datepart_day(arr)
        elif part in (b"dayofweek", b"dow"):
            return vector_datepart_dayofweek(arr)
        elif part in (b"dayofyear", b"doy"):
            return vector_datepart_dayofyear(arr)
        elif part == b"quarter":
            return vector_datepart_quarter(arr)

        raise InvalidFunctionParameterError(
            f"EXTRACT({part.decode().upper()}) is not supported. "
            f"Supported parts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter."
        )

    raise InvalidFunctionParameterError(
        f"EXTRACT({part.decode().upper()}) expects TimestampVector or Date32Vector input, "
        f"got {vector_type}. No fallback available."
    )


def trunc_date(arr, part):
    """TRUNC(Date32Vector, unit) -> TimestampVector."""
    from opteryx.compiled.vector_ops import vector_date_trunc

    return vector_date_trunc(part, arr)


def trunc_timestamp(arr, part):
    """TRUNC(TimestampVector, unit) -> TimestampVector."""
    from opteryx.compiled.vector_ops import vector_timestamp_trunc

    return vector_timestamp_trunc(part, arr)


def date_diff(part, start, end):
    """Calculate the difference between two timestamps.

    All inputs must be TimestampVector or Date32Vector.
    Returns a Draken Integer64Vector.
    """
    from opteryx.compiled.vector_ops import vector_date_diff

    part = str(part[0]).lower()
    if not part.endswith("s"):
        part += "s"

    # Convert inputs to TimestampVector if needed, rejecting INT values
    def _to_timestamp_vector(arr):
        """Ensure input is a Draken TimestampVector."""
        type_name = arr.__class__.__name__
        if type_name == "Integer64Vector":
            raise InvalidFunctionParameterError(
                f"DATEDIFF cannot operate on INTEGER values. "
                "Provide TIMESTAMP or DATE columns instead. "
                "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
                "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
            )
        if type_name == "TimestampVector":
            return arr
        if type_name == "Date32Vector":
            from opteryx.compiled.vector_ops import vector_date32_to_timestamp

            return vector_date32_to_timestamp(arr)
        raise InvalidFunctionParameterError(
            f"DATEDIFF expects TIMESTAMP or DATE input, got {type_name}."
        )

    start_vec = _to_timestamp_vector(start)
    end_vec = _to_timestamp_vector(end)

    # Use Draken vector_date_diff directly - returns Integer64Vector
    return vector_date_diff(start_vec, end_vec, part)


def time_diff(time1, time2):
    return date_diff([b"hours"], time1, time2)


def date_format(dates, pattern):  # [#325]
    """Format dates using strftime pattern.

    Inputs: dates is a Draken TimestampVector or Date32Vector, pattern is a bytes scalar.
    Returns: List of formatted strings.
    """
    from opteryx.compiled.vector_ops import vector_date_format

    pattern = pattern[0]

    return vector_date_format(dates, pattern)


def date_floor(dates, magnitude, units):  # [#325]
    """Floor dates to the nearest unit.

    Inputs: dates (Draken vector), magnitude (scalar), units (str scalar).
    """
    from opteryx.compiled.vector_ops import vector_floor_temporal

    # Extract scalars from constant vectors if needed
    mag_type = magnitude.__class__.__name__
    if mag_type in ("ConstantVector", "Integer64Vector", "Int32Vector"):
        magnitude = magnitude[0]
    magnitude = int(magnitude)

    units_type = units.__class__.__name__
    if units_type in ("ConstantVector", "StringVector"):
        units = units[0]
    if isinstance(units, bytes):
        units = units.decode("utf-8")
    units = str(units)

    return vector_floor_temporal(dates, magnitude, units)


def from_unixtimestamp(values):
    """Convert Unix timestamps to datetime objects.

    Args:
        values: Array-like of Unix timestamps (seconds since epoch).

    Returns:
        List of datetime.datetime objects in UTC.
    """
    return [datetime.datetime.fromtimestamp(i, tz=datetime.timezone.utc) for i in values]


def unixtime(array):
    """
    Convert a Draken vector of timestamps or dates to Unix time (seconds since epoch).

    Returns an Integer64Vector of Unix timestamps.

    Inputs:
      - TimestampVector → Integer64Vector of seconds since epoch
      - Date32Vector → Integer64Vector of seconds since epoch
    """
    from opteryx.compiled.vector_ops import vector_unixtime

    vector_type = array.__class__.__name__

    if vector_type in ("TimestampVector", "Date32Vector"):
        # Use Draken vector_unixtime - returns Integer64Vector
        return vector_unixtime(array)

    raise TypeError(
        f"Unsupported vector type: {vector_type}. Expected TimestampVector or Date32Vector."
    )
