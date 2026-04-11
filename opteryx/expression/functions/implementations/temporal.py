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

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError


def date_part(part, arr):
    """
    Extract a part from a date/timestamp (EXTRACT function).

    Inputs (guaranteed by the draken kernel dispatch layer):
      part: a Draken constant vector — the datepart name (bytes scalar)
      arr:  a Draken TimestampVector, Int64Vector, or Date32Vector

    Compiled kernels only — no Arrow fallback.
    Raises InvalidFunctionParameterError for unsupported dateparts or input types.

    Supported dateparts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter
    Unsupported dateparts: week, isoweek, isoyear, decade, century, epoch, julian, date,
                           millisecond, microsecond, nanosecond (require implementation)
    """
    # part arrives as a Draken constant vector; extract the scalar bytes value.
    if not isinstance(part, (str, bytes, bytearray)):
        part = part[0]
    if isinstance(part, str):
        part = part.encode("utf-8")
    part = part.lower()  # [#325]

    vector_type = arr.__class__.__name__

    # Date32Vector: cast to TimestampVector so the timestamp kernels can be reused.
    if vector_type == "Date32Vector":
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arr = vector_from_arrow(arr.to_arrow().cast(pyarrow.timestamp("us")))
        vector_type = "TimestampVector"

    if vector_type == "TimestampVector":
        from opteryx.compiled.vector_ops.function_definitions import (
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
            f"EXTRACT({part.decode().upper()}) is not yet supported. "
            f"Supported parts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter. "
            f"To add {part.decode().upper()}, implement vector_datepart_{part.decode()}() in vector_ops/vector_date_part.pyx"
        )

    if vector_type == "Int64Vector":
        from opteryx.compiled.vector_ops.function_definitions import (
            vector_datepart_day_i64,
            vector_datepart_dayofweek_i64,
            vector_datepart_dayofyear_i64,
            vector_datepart_hour_i64,
            vector_datepart_minute_i64,
            vector_datepart_month_i64,
            vector_datepart_quarter_i64,
            vector_datepart_second_i64,
            vector_datepart_year_i64,
        )

        if part == b"minute":
            return vector_datepart_minute_i64(arr)
        elif part == b"hour":
            return vector_datepart_hour_i64(arr)
        elif part in (b"second", b"seconds"):
            return vector_datepart_second_i64(arr)
        elif part == b"year":
            return vector_datepart_year_i64(arr)
        elif part == b"month":
            return vector_datepart_month_i64(arr)
        elif part == b"day":
            return vector_datepart_day_i64(arr)
        elif part in (b"dayofweek", b"dow"):
            return vector_datepart_dayofweek_i64(arr)
        elif part in (b"dayofyear", b"doy"):
            return vector_datepart_dayofyear_i64(arr)
        elif part == b"quarter":
            return vector_datepart_quarter_i64(arr)

        raise InvalidFunctionParameterError(
            f"EXTRACT({part.decode().upper()}) is not yet supported for int64 timestamps. "
            f"Supported parts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter. "
            f"To add {part.decode().upper()}, implement vector_datepart_{part.decode()}_i64() in vector_ops/vector_date_part.pyx"
        )

    raise InvalidFunctionParameterError(
        f"EXTRACT({part.decode().upper()}) expects TimestampVector or Int64Vector input, "
        f"got {vector_type}. No fallback available."
    )


def trunc_temporal(arr, part):
    """
    Truncate a temporal value to the start of the specified unit.

    SQL surface form is TRUNC(value, unit), but the underlying utility uses
    the opposite argument order.
    """
    from opteryx.utils.dates import date_trunc

    return date_trunc(part, arr)


def date_diff(part, start, end):
    """Calculate the difference between two timestamps.

    All inputs are normalised to pyarrow timestamp[us] arrays first so that
    no numpy datetime64 intermediates are needed.
    """
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa
    from opteryx.compiled.vector_ops import vector_date_diff

    arrow_extractors = {
        "months": compute.month_interval_between,
        "quarters": compute.quarters_between,
        "weeks": compute.weeks_between,
        "years": compute.years_between,
    }

    part = str(part[0]).lower()
    if not part.endswith("s"):
        part += "s"

    def _to_timestamp_us(arr):
        """Return a flat pyarrow timestamp[us] Array from any input type."""
        if hasattr(arr, "to_arrow"):
            arr = arr.to_arrow()
        if isinstance(arr, pyarrow.ChunkedArray):
            arr = arr.combine_chunks() if arr.num_chunks > 1 else arr.chunk(0)
        if isinstance(arr, pyarrow.Array):
            if pyarrow.types.is_timestamp(arr.type):
                return (
                    arr
                    if arr.type == pyarrow.timestamp("us")
                    else arr.cast(pyarrow.timestamp("us"))
                )
            if pyarrow.types.is_date32(arr.type):
                return arr.cast(pyarrow.timestamp("us"))
            return arr.cast(pyarrow.timestamp("us"))
        # Python scalars / sequences / numpy arrays
        return pyarrow.array(arr).cast(pyarrow.timestamp("us"))

    start_arr = _to_timestamp_us(start)
    end_arr = _to_timestamp_us(end)

    if part in arrow_extractors:
        diff = arrow_extractors[part](start_arr, end_arr)
        if not hasattr(diff, "__iter__"):
            diff = [diff]
        return [i.as_py() for i in diff]

    start_vec = _vfa(start_arr)
    end_vec = _vfa(end_arr)
    return vector_date_diff(start_vec, end_vec, part).to_arrow()


def time_diff(time1, time2):
    return date_diff(["hours"], time1, time2)


def date_format(dates, pattern):  # [#325]
    pattern = pattern[0]
    return [None if d is None else d.strftime(pattern) for d in dates.tolist()]


def date_floor(dates, magnitude, units):  # [#325]
    if hasattr(magnitude, "as_py"):
        magnitude = magnitude.as_py()
    elif isinstance(magnitude, numpy.ndarray):
        magnitude = magnitude[0]
    if hasattr(units, "as_py"):
        units = units.as_py()
    elif isinstance(units, numpy.ndarray):
        units = units[0]
    return compute.floor_temporal(dates, int(magnitude), units)


def from_unixtimestamp(values):
    return numpy.array(
        [datetime.datetime.fromtimestamp(i, tz=datetime.timezone.utc) for i in values],
        dtype="datetime64[s]",
    )


def unixtime(array):
    """
    Convert a NumPy or Arrow array of timestamps or ISO8601 strings to Unix time
    (seconds since epoch). NaNs or nulls are converted to numpy.nan.
    """
    if hasattr(array, "to_arrow"):
        array = array.to_arrow()

    if isinstance(array, pyarrow.ChunkedArray):
        if array.num_chunks == 0:
            return numpy.array([], dtype=numpy.int64)
        chunks = [unixtime(chunk) for chunk in array.chunks]
        return numpy.concatenate(chunks)

    if isinstance(array, pyarrow.Array):
        if (
            pyarrow.types.is_date32(array.type)
            or pyarrow.types.is_date64(array.type)
            or pyarrow.types.is_timestamp(array.type)
        ):
            return (
                array.cast(pyarrow.timestamp("s"))
                .cast(pyarrow.int64())
                .to_numpy(zero_copy_only=False)
            )
        array = array.to_numpy(zero_copy_only=False)

    if not isinstance(array, numpy.ndarray):
        array = numpy.asarray(array)

    if numpy.issubdtype(array.dtype, numpy.datetime64):
        return array.astype("datetime64[s]").astype(numpy.int64)

    if array.dtype.kind in {"U", "S", "O"}:

        def to_epoch(s):
            if s is None or s != s:
                return numpy.nan
            try:
                dt = numpy.datetime64(s, "s")
                return float(dt.astype(numpy.int64))
            except Exception:
                return numpy.nan

        return numpy.vectorize(to_epoch, otypes=[numpy.float64])(array)

    raise TypeError(f"Unsupported array type: {array.dtype}")
