"""Correctness tests for DATEPART/EXTRACT behavior.

These tests intentionally focus on current correctness across supported input
representations and units, not long-term regression pinning.

As the engine evolves (type system, semantics, optimizer/vector paths), some
cases here are expected to be removed or rewritten to reflect new canonical
behavior.
"""

import datetime
import numbers
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import numpy
import pyarrow as pa
import pyarrow.compute as pc
from draken.vectors.int64_vector import Int64Vector
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
from opteryx.operators.group_state_store import DRAKEN_ENCODING_DICTIONARY

import opteryx
from opteryx.expression.functions.implementations.temporal import date_part

BASE_DT = datetime.datetime(2024, 1, 15, 14, 30, 45)


def _as_pylist(arr):
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks() if arr.num_chunks > 1 else arr.chunk(0)
    return arr.to_pylist()


def _datepart_arrow_expected(part, arr):
    j2000_scalar = pa.array([datetime.datetime(2000, 1, 1, 12, 0, 0)], type=pa.timestamp("us"))
    extractors = {
        "nanosecond": pc.nanosecond,
        "microsecond": pc.microsecond,
        "millisecond": pc.millisecond,
        "second": pc.second,
        "minute": pc.minute,
        "hour": pc.hour,
        "day": pc.day,
        "dayofweek": pc.day_of_week,
        "dow": pc.day_of_week,
        "week": pc.week,
        "isoweek": pc.iso_week,
        "month": pc.month,
        "quarter": pc.quarter,
        "dayofyear": pc.day_of_year,
        "doy": pc.day_of_year,
        "year": pc.year,
        "isoyear": pc.iso_year,
        "decade": lambda x: pc.divide(pc.year(x), 10),
        "century": lambda x: pc.add(pc.divide(pc.year(x), 100), 1),
        "epoch": lambda x: pc.divide(pc.cast(x, "int64"), 1_000_000.0),
        "julian": lambda x: pc.add(
            pc.divide(
                pc.milliseconds_between(pc.cast(x, pa.timestamp("ms")), j2000_scalar),
                86_400_000.0,
            ),
            2_451_545.0,
        ),
        "date": lambda x: pc.cast(x, "date32"),
    }
    return extractors[part](arr)


def test_datepart_timestamp_arrow_all_supported_units():
    """Timestamp Arrow input should work for all currently supported DATEPART units."""
    parts = [
        "nanosecond",
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "dayofweek",
        "week",
        "isoweek",
        "month",
        "quarter",
        "dayofyear",
        "year",
        "isoyear",
        "decade",
        "century",
        "epoch",
        "date",
        "doy",
        "dow",
    ]
    arr = pa.array([BASE_DT], type=pa.timestamp("us"))
    for part in parts:
        actual = date_part(part, arr)
        expected = _datepart_arrow_expected(part, arr)
        assert _as_pylist(actual) == _as_pylist(expected), f"failed for part={part}"


def test_datepart_python_datetime_sequence_input():
    """Python datetime sequences should be accepted and normalized by DATEPART."""
    for part, expected in (("year", [2024]), ("month", [1]), ("day", [15])):
        actual = date_part(part, [BASE_DT])
        assert _as_pylist(actual) == expected, f"failed for part={part}"


def test_datepart_date32_arrow_input():
    """DATE values (date32) should support calendar-oriented extraction units."""
    arr = pa.array([datetime.date(2024, 1, 15)], type=pa.date32())
    for part, expected in (("year", [2024]), ("month", [1]), ("day", [15]), ("quarter", [1])):
        actual = date_part(part, arr)
        assert _as_pylist(actual) == expected, f"failed for part={part}"


def test_datepart_dictionary_timestamp_falls_back_cleanly():
    dictionary = pa.array(
        [BASE_DT, BASE_DT + datetime.timedelta(hours=1)],
        type=pa.timestamp("us"),
    )
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        dictionary,
    )
    actual = date_part("minute", arr)
    assert _as_pylist(actual) == [30, 30, 30, None]


def test_datepart_typed_int64_dictionary_vector_uses_typed_dispatch_cleanly():
    vector = Int64Vector.from_dict(
        [0, 1, 0, 2],
        [1705329045, 1705332645, 1705336245],
        [1, 1, 0, 1],
    )

    minute = date_part("minute", vector)
    year = date_part("year", vector)

    assert _as_pylist(minute) == [30, 30, None, 30]
    assert _as_pylist(year) == [2024, 2024, None, 2024]


def test_datepart_numpy_int64_unix_seconds_input():
    """NumPy int64 Unix timestamps should be converted and extracted correctly."""
    unix_seconds = numpy.array([1705329045], dtype=numpy.int64)  # 2024-01-15 14:30:45 UTC
    for part, expected in (("minute", [30]), ("hour", [14]), ("second", [45])):
        actual = date_part(part, unix_seconds)
        assert _as_pylist(actual) == expected, f"failed for part={part}"


def test_datepart_numpy_int64_multi_precision_inputs():
    """NumPy integer Unix timestamps should work across s/ms/us/ns precisions."""
    test_cases = (
        (numpy.array([1705329045], dtype=numpy.int64), "seconds"),
        (numpy.array([1705329045000], dtype=numpy.int64), "milliseconds"),
        (numpy.array([1705329045000000], dtype=numpy.int64), "microseconds"),
        (numpy.array([1705329045000000000], dtype=numpy.int64), "nanoseconds"),
    )
    for values, label in test_cases:
        for part, expected in (("minute", [30]), ("hour", [14]), ("second", [45])):
            actual = date_part(part, values)
            assert _as_pylist(actual) == expected, f"failed for precision={label}, part={part}"


def test_datepart_numpy_int64_phase2_calendar_units():
    """Phase 2 units should extract correctly from NumPy int64 Unix timestamps."""
    unix_seconds = numpy.array([1705329045], dtype=numpy.int64)  # 2024-01-15 14:30:45 UTC
    expectations = {
        "year": [2024],
        "month": [1],
        "day": [15],
        "dayofweek": [0],  # Monday in Arrow default mode
        "dayofyear": [15],
        "quarter": [1],
        "doy": [15],
        "dow": [0],
    }
    for part, expected in expectations.items():
        actual = date_part(part, unix_seconds)
        assert _as_pylist(actual) == expected, f"failed for part={part}"


def test_datepart_query_level_int64_dictionary_input_from_clickbench():
    """Query-level DATEPART should work on clickbench int64 EventTime input.

    This validates the real execution path (including dictionary/int64 handling)
    used by the engine in practice.
    """
    os.environ["FEATURE_USE_DRAKEN_AGGREGATOR"] = "1"
    session = opteryx.session()
    try:
        for part in ("minute", "hour", "second"):
            result = session.execute_to_arrow(
                f"SELECT EXTRACT({part} FROM EventTime) AS v FROM testdata.clickbench_tiny LIMIT 8"
            )
            values = result.column("v").to_pylist()
            assert len(values) == 8, f"failed for part={part}: wrong row count"
            assert all(v is None or isinstance(v, numbers.Number) for v in values), (
                f"failed for part={part}: unexpected result types"
            )
    finally:
        session.close()


def test_datepart_query_level_phase2_units_from_clickbench():
    """Phase 2 units should run on clickbench EventTime without DATEPART kernel errors."""
    os.environ["FEATURE_USE_DRAKEN_AGGREGATOR"] = "1"
    session = opteryx.session()
    try:
        for part in ("year", "month", "day", "dayofweek", "dayofyear", "quarter"):
            result = session.execute_to_arrow(
                f"SELECT EXTRACT({part} FROM EventTime) AS v FROM testdata.clickbench_tiny LIMIT 16"
            )
            values = result.column("v").to_pylist()
            assert len(values) == 16, f"failed for part={part}: wrong row count"
            assert all(v is None or isinstance(v, numbers.Number) for v in values), (
                f"failed for part={part}: unexpected result types {[type(x) for x in values[:3]]}"
            )
            norm_values = [None if v is None else int(v) for v in values]

            if part == "month":
                assert all(v is None or 1 <= v <= 12 for v in norm_values), "month out of range"
            elif part == "day":
                assert all(v is None or 1 <= v <= 31 for v in norm_values), "day out of range"
            elif part == "dayofweek":
                assert all(v is None or 0 <= v <= 6 for v in norm_values), "dayofweek out of range"
            elif part == "dayofyear":
                assert all(v is None or 1 <= v <= 366 for v in norm_values), (
                    "dayofyear out of range"
                )
            elif part == "quarter":
                assert all(v is None or 1 <= v <= 4 for v in norm_values), "quarter out of range"
    finally:
        session.close()


def test_datepart_int64_dictionary_vector_preserves_dictionary_encoding():
    values = [1705329045, 1705332645, 1705336245]
    vector = Int64Vector.from_dict([0, 1, 0, 2], values, [1, 1, 0, 1])

    minute = vector_datepart_minute_i64(vector)
    hour = vector_datepart_hour_i64(vector)
    second = vector_datepart_second_i64(vector)

    assert minute.encoding == DRAKEN_ENCODING_DICTIONARY
    assert hour.encoding == DRAKEN_ENCODING_DICTIONARY
    assert second.encoding == DRAKEN_ENCODING_DICTIONARY

    assert minute.to_pylist() == [30, 30, None, 30]
    assert hour.to_pylist() == [14, 15, None, 16]
    assert second.to_pylist() == [45, 45, None, 45]


def test_datepart_int64_dictionary_vector_preserves_dictionary_encoding_for_calendar_units():
    values = [1705329045, 1705332645, 1705336245]
    vector = Int64Vector.from_dict([0, 1, 0, 2], values, [1, 1, 0, 1])

    year = vector_datepart_year_i64(vector)
    month = vector_datepart_month_i64(vector)
    day = vector_datepart_day_i64(vector)
    dayofweek = vector_datepart_dayofweek_i64(vector)
    dayofyear = vector_datepart_dayofyear_i64(vector)
    quarter = vector_datepart_quarter_i64(vector)

    assert year.encoding == DRAKEN_ENCODING_DICTIONARY
    assert month.encoding == DRAKEN_ENCODING_DICTIONARY
    assert day.encoding == DRAKEN_ENCODING_DICTIONARY
    assert dayofweek.encoding == DRAKEN_ENCODING_DICTIONARY
    assert dayofyear.encoding == DRAKEN_ENCODING_DICTIONARY
    assert quarter.encoding == DRAKEN_ENCODING_DICTIONARY

    assert year.to_pylist() == [2024, 2024, None, 2024]
    assert month.to_pylist() == [1, 1, None, 1]
    assert day.to_pylist() == [15, 15, None, 15]
    assert dayofweek.to_pylist() == [1, 1, None, 1]
    assert dayofyear.to_pylist() == [15, 15, None, 15]
    assert quarter.to_pylist() == [1, 1, None, 1]


def test_datepart_invalid_int_range_raises_explicit_error():
    """Small non-timestamp integers should fail explicitly rather than degrade silently."""
    bad_values = numpy.array([100, 200, 300], dtype=numpy.int64)
    try:
        date_part("minute", bad_values)
    except pa.ArrowNotImplementedError as err:
        assert "Function 'minute' has no kernel" in str(err)
    else:
        raise AssertionError("Expected ArrowNotImplementedError for invalid integer range")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
