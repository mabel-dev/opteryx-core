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
from opteryx.exceptions import InvalidInternalStateError


def convert_int64_array_to_pyarrow_datetime(values: numpy.ndarray) -> pyarrow.Array:
    """
    Convert a NumPy int64 array to PyArrow TimestampArray, inferring time unit.
    """
    if isinstance(values, pyarrow.ChunkedArray):
        values = values.to_numpy(zero_copy_only=False)

    if isinstance(values, pyarrow.Array):
        values = values.to_numpy(zero_copy_only=False)

    if not isinstance(values, numpy.ndarray):
        raise InvalidInternalStateError("Expected a NumPy int64 array.")

    if not numpy.issubdtype(values.dtype, numpy.integer):
        raise ValueError("Cannot convert non-integer array to a timestamp.")

    min_value = values.min()
    max_value = values.max()

    RANGES = [
        (1e0, 1e6, "D"),
        (1e9, 1e10, "s"),
        (1e12, 1e13, "ms"),
        (1e15, 1e16, "us"),
        (1e18, 1e19, "ns"),
    ]

    for low, high, unit in RANGES:
        if low <= min_value < high and low <= max_value < high:
            try:
                return pyarrow.array(values.astype(f"datetime64[{unit}]"))
            except Exception as e:
                raise ValueError(f"Failed to cast to datetime64[{unit}]: {e}")

    raise ValueError(
        f"Unable to determine timestamp precision for values in range [{min_value}, {max_value}]"
    )


def date_part(part, arr):
    """
    Extract a part from a date/timestamp (EXTRACT function).

    Accepts Draken vectors (TimestampVector, Int64Vector) or PyArrow arrays.

    Compiled kernels only - NO Arrow compute fallback:
    - Raises InvalidFunctionParameterError if datepart is not supported
    - All extraction is done via compiled Cython kernels for performance
    - PyArrow inputs are converted to Draken vectors automatically

    Supported dateparts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter
    Unsupported dateparts: week, isoweek, isoyear, decade, century, epoch, julian, date,
                           millisecond, microsecond, nanosecond (require implementation)
    """
    # Normalise part to lowercase bytes — draken delivers bytes, arrow delivers str.
    # Encode str to bytes so all comparisons below use a single type.
    part = part[0] if not isinstance(part, (str, bytes, bytearray)) else part
    if isinstance(part, str):
        part = part.encode("utf-8")
    part = part.lower()  # [#325]

    # Handle NumPy arrays that are integer timestamps
    if isinstance(arr, numpy.ndarray):
        if numpy.issubdtype(arr.dtype, numpy.integer):
            # Convert int64 array to timestamp using the helper (detects precision)
            arr = convert_int64_array_to_pyarrow_datetime(arr)
        elif numpy.issubdtype(arr.dtype, numpy.datetime64):
            # Convert numpy datetime64 directly to PyArrow timestamp
            arr = pyarrow.array(arr)

    # Convert PyArrow arrays to Draken vectors
    if isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arr = vector_from_arrow(arr)

    vector_type = arr.__class__.__name__

    if hasattr(arr, "to_arrow") and vector_type not in (
        "TimestampVector",
        "Int64Vector",
        "Date32Vector",
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        arrow_arr = arr.to_arrow()
        if pyarrow.types.is_date32(arrow_arr.type) or pyarrow.types.is_date64(arrow_arr.type):
            arr = vector_from_arrow(arrow_arr.cast(pyarrow.timestamp("us")))
            vector_type = arr.__class__.__name__
        elif pyarrow.types.is_timestamp(arrow_arr.type) or pyarrow.types.is_int64(arrow_arr.type):
            arr = vector_from_arrow(arrow_arr)
            vector_type = arr.__class__.__name__

    if vector_type == "Date32Vector":
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        # Reuse the existing timestamp kernels by interpreting date32 values
        # as midnight UTC timestamps.
        arr = vector_from_arrow(arr.to_arrow().cast(pyarrow.timestamp("us")))
        vector_type = arr.__class__.__name__

    if vector_type == "TimestampVector":
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_day
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_dayofweek
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_dayofyear
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_hour
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_minute
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_month
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_quarter
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_second
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_year

        if part == b"minute":
            return vector_datepart_minute(arr).to_arrow()
        elif part == b"hour":
            return vector_datepart_hour(arr).to_arrow()
        elif part in (b"second", b"seconds"):
            return vector_datepart_second(arr).to_arrow()
        elif part == b"year":
            return vector_datepart_year(arr).to_arrow()
        elif part == b"month":
            return vector_datepart_month(arr).to_arrow()
        elif part == b"day":
            return vector_datepart_day(arr).to_arrow()
        elif part in (b"dayofweek", b"dow"):
            return vector_datepart_dayofweek(arr).to_arrow()
        elif part in (b"dayofyear", b"doy"):
            return vector_datepart_dayofyear(arr).to_arrow()
        elif part == b"quarter":
            return vector_datepart_quarter(arr).to_arrow()

        # Unsupported datepart for TimestampVector - raise error instead of falling back
        raise InvalidFunctionParameterError(
            f"EXTRACT({part.decode().upper()}) is not yet supported. "
            f"Supported parts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter. "
            f"To add {part.decode().upper()}, implement vector_datepart_{part.decode()}() in vector_ops/vector_date_part.pyx"
        )

    if vector_type == "Int64Vector":
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_day_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_dayofweek_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_dayofyear_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_hour_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_minute_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_month_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_quarter_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_second_i64
        from opteryx.compiled.vector_ops.function_definitions import vector_datepart_year_i64

        if part == b"minute":
            return vector_datepart_minute_i64(arr).to_arrow()
        elif part == b"hour":
            return vector_datepart_hour_i64(arr).to_arrow()
        elif part in (b"second", b"seconds"):
            return vector_datepart_second_i64(arr).to_arrow()
        elif part == b"year":
            return vector_datepart_year_i64(arr).to_arrow()
        elif part == b"month":
            return vector_datepart_month_i64(arr).to_arrow()
        elif part == b"day":
            return vector_datepart_day_i64(arr).to_arrow()
        elif part in (b"dayofweek", b"dow"):
            return vector_datepart_dayofweek_i64(arr).to_arrow()
        elif part in (b"dayofyear", b"doy"):
            return vector_datepart_dayofyear_i64(arr).to_arrow()
        elif part == b"quarter":
            return vector_datepart_quarter_i64(arr).to_arrow()

        # Unsupported datepart for Int64Vector - raise error instead of falling back
        raise InvalidFunctionParameterError(
            f"EXTRACT({part.decode().upper()}) is not yet supported for int64 timestamps. "
            f"Supported parts: minute, hour, second, year, month, day, dayofweek, dayofyear, quarter. "
            f"To add {part.decode().upper()}, implement vector_datepart_{part.decode()}_i64() in vector_ops/vector_date_part.pyx"
        )

    # --- NO FALLBACK: Raise error for unsupported input types ---
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
