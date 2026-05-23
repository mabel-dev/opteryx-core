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

    Supported dateparts: year, month, day, quarter, dayofyear, dayofweek, hour, minute, second
    """
    from opteryx.compiled.nanobind.vector_temporal_arith import vector_date_part

    raw_part = part[0].lower()
    vector_type = arr.__class__.__name__

    if vector_type == "Integer64Vector":
        part_name = raw_part.decode() if isinstance(raw_part, bytes) else str(raw_part)
        raise InvalidFunctionParameterError(
            f"EXTRACT({part_name.upper()}) cannot operate on INTEGER values. "
            "Provide a TIMESTAMP or DATE column instead. "
            "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
            "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
        )

    if vector_type not in ("TimestampVector", "Date32Vector", "Vector"):
        part_name = raw_part.decode() if isinstance(raw_part, bytes) else str(raw_part)
        raise InvalidFunctionParameterError(
            f"EXTRACT({part_name.upper()}) expects TimestampVector or Date32Vector input, "
            f"got {vector_type}. No fallback available."
        )

    part_str = raw_part.decode("utf-8") if isinstance(raw_part, bytes) else str(raw_part)
    return vector_date_part(arr, part_str)


def trunc_date(arr, part):
    """TRUNC(Date32Vector, unit) -> TIMESTAMP64 in microseconds."""
    from opteryx.compiled.nanobind.vector_temporal_arith import vector_date_trunc

    unit = part[0] if hasattr(part, '__getitem__') else part
    if isinstance(unit, bytes):
        unit = unit.decode("utf-8")
    return vector_date_trunc(arr, str(unit).lower())


def trunc_timestamp(arr, part):
    """TRUNC(TimestampVector, unit) -> TIMESTAMP64 (same unit as input)."""
    from opteryx.compiled.nanobind.vector_temporal_arith import vector_date_trunc

    unit = part[0] if hasattr(part, '__getitem__') else part
    if isinstance(unit, bytes):
        unit = unit.decode("utf-8")
    return vector_date_trunc(arr, str(unit).lower())


def date_diff(part, start, end):
    """Calculate the difference between two timestamps.

    All inputs must be TimestampVector or Date32Vector.
    Returns a Draken INT64 Vector (end - start in the requested unit).
    """
    from opteryx.compiled.nanobind.vector_temporal_arith import vector_date_diff

    part_val = str(part[0]).lower()

    def _to_timestamp_vector(arr):
        type_name = arr.__class__.__name__
        if type_name == "Integer64Vector":
            raise InvalidFunctionParameterError(
                "DATEDIFF cannot operate on INTEGER values. "
                "Provide TIMESTAMP or DATE columns instead. "
                "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
                "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
            )
        if type_name in ("TimestampVector", "Vector"):
            return arr
        if type_name == "Date32Vector":
            from opteryx.compiled.nanobind.vector_temporal_convert import vector_date32_to_timestamp
            return vector_date32_to_timestamp(arr)
        raise InvalidFunctionParameterError(
            f"DATEDIFF expects TIMESTAMP or DATE input, got {type_name}."
        )

    start_vec = _to_timestamp_vector(start)
    end_vec = _to_timestamp_vector(end)
    return vector_date_diff(start_vec, end_vec, part_val)


def time_diff(time1, time2):
    return date_diff([b"hours"], time1, time2)


def date_format(dates, pattern):  # [#325]
    """Format dates using strftime pattern.

    Inputs: dates is a Draken TIMESTAMP64 or DATE32 Vector, pattern is a bytes/str scalar.
    Returns: DRAKEN_VARCHAR Vector.
    """
    from opteryx.compiled.nanobind.vector_temporal_arith import vector_date_format

    fmt = pattern[0]
    if isinstance(fmt, bytes):
        fmt = fmt.decode("utf-8")
    return vector_date_format(dates, fmt)


def date_floor(dates, magnitude, units):  # [#325]
    """Floor dates to the nearest unit.

    Inputs: dates (Draken vector), magnitude (scalar), units (str scalar).
    """
    from opteryx.compiled.nanobind.vector_temporal_convert import vector_floor_temporal

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
    from opteryx.compiled.nanobind.vector_temporal_convert import vector_unixtime

    vector_type = array.__class__.__name__

    if vector_type in ("TimestampVector", "Date32Vector"):
        # Use Draken vector_unixtime - returns Integer64Vector
        return vector_unixtime(array)

    raise TypeError(
        f"Unsupported vector type: {vector_type}. Expected TimestampVector or Date32Vector."
    )
