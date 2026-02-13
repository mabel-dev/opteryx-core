import os
import sys

# import opteryx

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import time
import numpy
import pytest

from opteryx.utils import dates

# fmt:off
DATE_TESTS = [
        ("NOT A DATE", None),
        ("2021001011", datetime.datetime(2034,1,16,5,10,11)),  # treated as ms since epoch
        ("2021-02-21", datetime.datetime(2021,2,21)),
        ("2021-02-21T", None),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0,0)),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0,0)),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00", datetime.datetime(2021,1,11,12,0)),
        ("2020-10-01 18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20", datetime.datetime(2020,10,1,18,5,20)),
    #    ("2020-10-01T18:05:20+0100", datetime.datetime(2020,10,1,18,5,20)),
        ("1999-12-31 23:59:59.9", datetime.datetime(1999,12,31,23,59,59,900000)),
        ("1999-12-31 23:59:59.9999", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.9999", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.9999", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.999999", datetime.datetime(1999,12,31,23,59,59,999999)),
        ("1999-12-31T23:59:59.999999", datetime.datetime(1999,12,31,23,59,59,999999)),
        ("1999-12-31T23:59:59.99999999", None),

        (numpy.datetime64('2021-02-21'), datetime.datetime(2021, 2, 21, 0, 0, 0)),  # Numpy datetime64 to datetime
        (numpy.datetime64('2021-02-21T12:00:00'), datetime.datetime(2021, 2, 21, 12, 0)),  # Numpy datetime64 with time to datetime
        (numpy.int64(1585699200), datetime.datetime(2020, 4, 1, 0, 0)),  # Unix timestamp as numpy int64 to datetime
        (1585699200, datetime.datetime(2020, 4, 1, 0, 0)),  # Unix timestamp as int to datetime
        (1585699200.0, datetime.datetime(2020, 4, 1, 0, 0)),  # Unix timestamp as float to datetime
        (datetime.date(2021, 2, 21), datetime.datetime(2021, 2, 21)),  # Python date to datetime
        (numpy.datetime64('2021-02-21T12:00:00'), datetime.datetime(2021, 2, 21, 12, 0)), 
        (numpy.int64(1585699200), datetime.datetime(2020, 4, 1, 0, 0)),  # Unix timestamp as numpy int64 to datetime (repeated to ensure cache performance)
        (datetime.datetime(2021, 2, 21, 12, 0), datetime.datetime(2021, 2, 21, 12, 0)),  # Python datetime to datetime (no conversion)
        (1613918723, datetime.datetime(2021, 2, 21, 14, 45, 23)),  # Unix timestamp (seconds since epoch)
        (1613918723.5678, datetime.datetime(2021, 2, 21, 14, 45, 23)),  # Unix timestamp with fractional seconds
        (numpy.datetime64('2021-02-21'), datetime.datetime(2021, 2, 21)),  # numpy datetime64 with date only
        (numpy.datetime64('2021-02-21T15:32:03'), datetime.datetime(2021, 2, 21, 15, 32, 3)),  # numpy datetime64 with date and time
        (numpy.datetime64('2021-02-21T15:32:03.5678'), datetime.datetime(2021, 2, 21, 15, 32, 3)),  # numpy datetime64 with fractional seconds
        (datetime.datetime(2021, 2, 21, 15, 32, 3), datetime.datetime(2021, 2, 21, 15, 32, 3)),  # datetime object
        (datetime.date(2021, 2, 21), datetime.datetime(2021, 2, 21)),  # date object
        (numpy.datetime64('2021-02-21T00:00:00.000000000'), datetime.datetime(2021, 2, 21, 0, 0)), 

        ("2021/02/21", None),  # Wrong separators
        ("2021-13-01", None),  # Invalid month
        ("2021-02-39", None),  # Very invalid day
        ("2021-02-30", None),  # Contextual Invalid day
        ("2021-02-21T24:00", None),  # Invalid hour
        ("2021-02-21T12:60", None),  # Invalid minute
        ("2021-02-21T12:00:60", None),  # Invalid second
        ("2021-02-21T1200:00", None),  # No separator between date and time
    ]
# fmt:on


@pytest.mark.parametrize("string, expect", DATE_TESTS)
def test_date_parser(string, expect):
    assert dates.parse_iso(string) == expect, f"{string}  {dates.parse_iso(string)}  {expect}"


# Tests for truncate_single helper function
TRUNCATE_SINGLE_TESTS = [
    # (input_datetime, unit, expected_output)
    # Second truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "second", datetime.datetime(2021, 2, 21, 12, 30, 45, 0)),
    # Minute truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "minute", datetime.datetime(2021, 2, 21, 12, 30, 0, 0)),
    # Hour truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "hour", datetime.datetime(2021, 2, 21, 12, 0, 0, 0)),
    # Day truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "day", datetime.datetime(2021, 2, 21, 0, 0, 0, 0)),
    # Week truncation (Monday-based)
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "week", datetime.datetime(2021, 2, 15, 0, 0, 0, 0)),  # 2021-02-21 is Sunday, Monday is 2021-02-15
    (datetime.datetime(2021, 2, 22, 12, 30, 45, 123456), "week", datetime.datetime(2021, 2, 22, 0, 0, 0, 0)),  # 2021-02-22 is Monday
    # Month truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "month", datetime.datetime(2021, 2, 1, 0, 0, 0, 0)),
    # Quarter truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "quarter", datetime.datetime(2021, 1, 1, 0, 0, 0, 0)),
    (datetime.datetime(2021, 5, 21, 12, 30, 45, 123456), "quarter", datetime.datetime(2021, 4, 1, 0, 0, 0, 0)),
    (datetime.datetime(2021, 11, 21, 12, 30, 45, 123456), "quarter", datetime.datetime(2021, 10, 1, 0, 0, 0, 0)),
    # Year truncation
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 123456), "year", datetime.datetime(2021, 1, 1, 0, 0, 0, 0)),
]

@pytest.mark.parametrize("input_dt, unit, expected", TRUNCATE_SINGLE_TESTS)
def test_truncate_single(input_dt, unit, expected):
    assert dates.truncate_single(input_dt, unit) == expected, f"truncate_single({input_dt}, {unit}) = {dates.truncate_single(input_dt, unit)}, expected {expected}"


# Tests for add_single_unit helper function
ADD_SINGLE_UNIT_TESTS = [
    # (input_datetime, unit, n, expected_output)
    # Second addition
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 0), "second", 1, datetime.datetime(2021, 2, 21, 12, 30, 46, 0)),
    (datetime.datetime(2021, 2, 21, 12, 30, 45, 0), "second", 60, datetime.datetime(2021, 2, 21, 12, 31, 45, 0)),
    # Minute addition
    (datetime.datetime(2021, 2, 21, 12, 30, 0, 0), "minute", 1, datetime.datetime(2021, 2, 21, 12, 31, 0, 0)),
    (datetime.datetime(2021, 2, 21, 12, 30, 0, 0), "minute", 60, datetime.datetime(2021, 2, 21, 13, 30, 0, 0)),
    # Hour addition
    (datetime.datetime(2021, 2, 21, 12, 0, 0, 0), "hour", 1, datetime.datetime(2021, 2, 21, 13, 0, 0, 0)),
    (datetime.datetime(2021, 2, 21, 12, 0, 0, 0), "hour", 24, datetime.datetime(2021, 2, 22, 12, 0, 0, 0)),
    # Day addition
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "day", 1, datetime.datetime(2021, 2, 22, 0, 0, 0, 0)),
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "day", 365, datetime.datetime(2022, 2, 21, 0, 0, 0, 0)),
    # Week addition
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "week", 1, datetime.datetime(2021, 2, 28, 0, 0, 0, 0)),
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "week", 4, datetime.datetime(2021, 3, 21, 0, 0, 0, 0)),
    # Month addition
    (datetime.datetime(2021, 1, 31, 0, 0, 0, 0), "month", 1, datetime.datetime(2021, 2, 28, 0, 0, 0, 0)),  # end-of-month handling
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "month", 1, datetime.datetime(2021, 3, 21, 0, 0, 0, 0)),
    (datetime.datetime(2021, 2, 21, 0, 0, 0, 0), "month", 12, datetime.datetime(2022, 2, 21, 0, 0, 0, 0)),
    # Quarter addition
    (datetime.datetime(2021, 1, 15, 0, 0, 0, 0), "quarter", 1, datetime.datetime(2021, 4, 15, 0, 0, 0, 0)),
    (datetime.datetime(2021, 1, 15, 0, 0, 0, 0), "quarter", 4, datetime.datetime(2022, 1, 15, 0, 0, 0, 0)),
    # Year addition
    (datetime.datetime(2020, 2, 29, 0, 0, 0, 0), "year", 1, datetime.datetime(2021, 2, 28, 0, 0, 0, 0)),  # leap year handling
    (datetime.datetime(2021, 6, 15, 0, 0, 0, 0), "year", 1, datetime.datetime(2022, 6, 15, 0, 0, 0, 0)),
]

@pytest.mark.parametrize("input_dt, unit, n, expected", ADD_SINGLE_UNIT_TESTS)
def test_add_single_unit(input_dt, unit, n, expected):
    assert dates.add_single_unit(input_dt, unit, n) == expected, f"add_single_unit({input_dt}, {unit}, {n}) = {dates.add_single_unit(input_dt, unit, n)}, expected {expected}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(DATE_TESTS)} DATE TESTS")
    start = time.perf_counter_ns()

    for i in range(5000):
        for date_string, date_date in DATE_TESTS:
            #print(str(date_string).ljust(33), date_date)
            test_date_parser(date_string, date_date)
    print(f"\n✅ okay - {len(DATE_TESTS)} date tests in {(time.perf_counter_ns() - start) / 1e9} s")
