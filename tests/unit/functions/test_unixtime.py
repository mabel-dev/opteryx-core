import os
import sys
import datetime

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.expression.functions.implementations.temporal import unixtime
from opteryx.expression.functions.implementations.temporal import from_unixtimestamp


def test_unixtime_with_arrow_timestamps():
    """unixtime now works with Arrow arrays and returns lists."""
    from pyarrow import compute
    arr_str = pyarrow.array(['2020-01-01T00:00:00', '2021-01-01T12:00:00'])
    arr = compute.strptime(arr_str, "%Y-%m-%dT%H:%M:%S", "us")
    result = unixtime(arr)
    assert isinstance(result, list)
    assert result[0] == 1577836800  # 2020-01-01T00:00:00 UTC
    assert result[1] == 1609502400  # 2021-01-01T12:00:00 UTC

def test_unixtime_with_string_dates():
    """unixtime parses ISO8601 strings via Arrow."""
    arr = pyarrow.array(['2020-01-01T00:00:00', '2021-01-01T12:00:00'], type=pyarrow.string())
    result = unixtime(arr)
    assert isinstance(result, list)
    assert result[0] == 1577836800
    assert result[1] == 1609502400

def test_unixtime_with_empty_array():
    """unixtime returns empty list for empty input."""
    arr = pyarrow.array([], type=pyarrow.timestamp('us'))
    result = unixtime(arr)
    assert result == []

def test_from_unixtimestamp_single_known_value():
    """from_unixtimestamp returns list of datetime objects."""
    ts = [1572912000]  # 2019-11-05T00:00:00Z
    result = from_unixtimestamp(ts)
    assert isinstance(result, list)
    assert result[0] == datetime.datetime(2019, 11, 5, 0, 0, 0, tzinfo=datetime.timezone.utc)

def test_from_unixtimestamp_multiple_values():
    """from_unixtimestamp handles multiple timestamps."""
    ts = [
        0,                    # 1970-01-01T00:00:00
        946684800,            # 2000-01-01T00:00:00
        1609459200            # 2021-01-01T00:00:00
    ]
    expected = [
        datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
    ]
    result = from_unixtimestamp(ts)
    assert result == expected


def test_from_unixtimestamp_round_trip():
    """Round trip: timestamp ints → datetime → back."""
    ints = [1577836800, 1609459200]  # 2020-01-01, 2021-01-01
    datetimes = from_unixtimestamp(ints)
    assert datetimes[0] == datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    assert datetimes[1] == datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
