import os
import sys
import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from datetime import datetime, timezone
from opteryx.utils.dates import date_trunc

DEFAULT_DT = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)


def test_truncate_to_second():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(microsecond=0))
    actual = date_trunc("second", [dt])
    assert actual == expected


def test_truncate_to_minute():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(second=0, microsecond=0))
    actual = date_trunc("minute", [dt])
    assert actual == expected


def test_truncate_to_hour():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(minute=0, second=0, microsecond=0))
    actual = date_trunc("hour", [dt])
    assert actual == expected


def test_truncate_to_day():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(hour=0, minute=0, second=0, microsecond=0))
    actual = date_trunc("day", [dt])
    assert actual == expected, f"{actual}, {expected}"


def test_truncate_to_month():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0))
    actual = date_trunc("month", [dt])
    assert actual == expected


def test_truncate_to_year():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0))
    actual = date_trunc("year", [dt])
    assert actual == expected


def test_truncate_to_week():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(day=9, hour=0, minute=0, second=0, microsecond=0))
    actual = date_trunc("week", [dt])
    assert actual == expected, f"{actual}, {expected}"

    dt = datetime(2012, 7, 9, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(dt.replace(hour=0, minute=0, second=0, microsecond=0))
    actual = date_trunc("week", [dt])
    assert actual == expected


def test_truncate_to_quarter():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    expected = numpy.datetime64(datetime(2012, 7, 1, 0, 0, 0, 0, timezone.utc))
    actual = date_trunc("quarter", [dt])
    assert actual == expected, f"{actual}, {expected}"

    dt = datetime(2012, 1, 15, 10, 30, 45, 123, timezone.utc)
    expected = numpy.datetime64(datetime(2012, 1, 1, 0, 0, 0, 0, timezone.utc))
    actual = date_trunc("quarter", [dt])
    assert actual == expected, f"{actual}, {expected}"

    dt = datetime(2012, 6, 25, 5, 20, 30, 456, timezone.utc)
    expected = numpy.datetime64(datetime(2012, 4, 1, 0, 0, 0, 0, timezone.utc))
    actual = date_trunc("quarter", [dt])
    assert actual == expected, f"{actual}, {expected}"

    dt = datetime(2012, 11, 5, 23, 59, 59, 999, timezone.utc)
    expected = numpy.datetime64(datetime(2012, 10, 1, 0, 0, 0, 0, timezone.utc))
    actual = date_trunc("quarter", [dt])
    assert actual == expected, f"{actual}, {expected}"


def test_truncate_to_decade():
    dt = datetime(2012, 7, 12, 12, 14, 14, 342, timezone.utc)
    try:
        date_trunc("decade", [dt])
    except ValueError:
        pass
    except Exception as e:
        assert False, f"Unexpected exception: {e}"


def test_truncate_int64_epoch_seconds():
    values = numpy.array([1342095254], dtype=numpy.int64)
    expected = numpy.datetime64("2012-07-12T12:14:00")
    actual = date_trunc("minute", values).to_numpy(zero_copy_only=False)
    assert actual[0] == expected


def test_truncate_arrow_date32():
    values = pyarrow.array([datetime(2012, 7, 12, tzinfo=timezone.utc).date()], type=pyarrow.date32())
    expected = numpy.datetime64("2012-07-12T00:00:00")
    actual = date_trunc("day", values).to_numpy(zero_copy_only=False)
    assert actual[0] == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
