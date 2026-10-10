"""Rugo reads what Rugo writes, and a column's type does not depend on the filter.

1. write_jsonl emits timestamps with a '+00:00' offset. A column DECLARED TIMESTAMP must
   honour a zone suffix ('Z', '+HH:MM', '+HHMM', '-HH:MM') and normalise to UTC — exactly
   what CAST(... AS TIMESTAMP) does — instead of refusing it. Offset-free text is UTC.

2. An undeclared column is typed from the head of the INPUT (the first infer_sample_size
   records, before any filtering) — the same window that decides the column set and checks
   predicate literals. Typing it from the first rows the predicates KEPT made the same
   column VARCHAR unfiltered and FLOAT64 under `IS NOT NULL` / `< 60`.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from rugo import jsonl
from rugo import parquet

SPACE_MISSIONS = os.path.join(
    os.path.dirname(__file__), "..", "..", "testdata", "flat", "space_missions", "space_missions.parquet"
)
UTC = datetime.timezone.utc


def _morsel(data, **kwargs):
    with jsonl.read_jsonl(data, **kwargs) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    return morsels[0]


def _space_missions():
    with parquet.read_parquet(SPACE_MISSIONS, columns=["Lauched_at", "Price"]) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    return morsels[0]


# ---------------------------------------------------------------------------
# 1. declared TIMESTAMP honours a zone suffix
# ---------------------------------------------------------------------------


def test_round_trip_declared_timestamp_reads_written_offset():
    source = _space_missions()
    written = jsonl.write_jsonl(source)
    assert b'"1957-10-04T19:28:00+00:00"' in written

    morsel = _morsel(written, explicit_schema={"Lauched_at": "TIMESTAMP[us]"})
    assert morsel.num_rows == source.num_rows
    assert morsel.column("Lauched_at").to_pylist() == source.column("Lauched_at").to_pylist()


@pytest.mark.parametrize(
    "text",
    [
        "1957-10-04T19:28:00",
        "1957-10-04T19:28:00Z",
        "1957-10-04T19:28:00+00:00",
        "1957-10-04T19:28:00+0000",
        "1957-10-04T21:28:00+02:00",
        "1957-10-04T17:58:00-01:30",
        "1957-10-04 19:28:00+00:00",
    ],
)
def test_declared_timestamp_normalises_zone_suffix_to_utc(text):
    morsel = _morsel(('{"t":"%s"}\n' % text).encode(), explicit_schema={"t": "TIMESTAMP[us]"})
    assert morsel.column("t").to_pylist() == [datetime.datetime(1957, 10, 4, 19, 28, tzinfo=UTC)]


def test_declared_timestamp_offset_crossing_midnight():
    morsel = _morsel(b'{"t":"2024-01-01T01:00:00+02:00"}\n', explicit_schema={"t": "TIMESTAMP[s]"})
    assert morsel.column("t").to_pylist() == [datetime.datetime(2023, 12, 31, 23, 0, tzinfo=UTC)]


@pytest.mark.parametrize(
    "text",
    [
        "1957-10-04T19:28:00+25:00",  # hour out of range
        "1957-10-04T19:28:00+00:60",  # minute out of range
        "1957-10-04T19:28:00+0",  # truncated offset
        "1957-10-04T19:28:00UTC",  # not an ISO zone
        "1957-10-04Z",  # zone without a time
    ],
)
def test_declared_timestamp_refuses_malformed_zone(text):
    with pytest.raises(ValueError, match="is not a valid TIMESTAMP"):
        _morsel(('{"t":"%s"}\n' % text).encode(), explicit_schema={"t": "TIMESTAMP[us]"})


# ---------------------------------------------------------------------------
# 2. undeclared column type comes from the unfiltered head
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"columns": ["Price"]},
        {"columns": ["Price"], "predicates": [("Price", "is not null", None)]},
        {"columns": ["Price"], "predicates": [("Price", "<", 60)]},
        {"columns": ["Price"], "predicates": [("Price", "is null", None)]},
    ],
)
def test_head_all_null_column_type_does_not_depend_on_predicate(kwargs):
    # The first five records carry "Price": null, so no hint forms: VARCHAR, filtered or not.
    written = jsonl.write_jsonl(_space_missions())
    morsel = _morsel(written, **kwargs)
    assert str(morsel.schema["Price"]) == "DrakenType.VARCHAR"


def test_head_typed_column_keeps_type_when_filter_drops_head_rows():
    # Head says INT64; the filter keeps only rows past the head. Still INT64.
    data = b"".join(b'{"k":%d,"v":%d}\n' % (i, i) for i in range(20))
    unfiltered = _morsel(data)
    filtered = _morsel(data, predicates=[("v", ">=", 10)])
    assert str(unfiltered.schema["k"]) == "DrakenType.INT64"
    assert str(filtered.schema["k"]) == "DrakenType.INT64"
    assert filtered.column("k").to_pylist() == list(range(10, 20))


def test_null_head_then_numbers_is_varchar_with_and_without_filter():
    data = b'{"p":null}\n' * 5 + b'{"p":1.5}\n{"p":70.25}\n'
    for kwargs in ({}, {"predicates": [("p", "is not null", None)]}, {"predicates": [("p", "<", 60)]}):
        morsel = _morsel(data, **kwargs)
        assert str(morsel.schema["p"]) == "DrakenType.VARCHAR", kwargs
    assert _morsel(data, predicates=[("p", "<", 60)]).column("p").to_pylist() == ["1.5"]


def test_wider_sample_types_the_column_regardless_of_filter():
    data = b'{"p":null}\n' * 5 + b'{"p":1.5}\n{"p":70.25}\n'
    for kwargs in ({}, {"predicates": [("p", "<", 60)]}):
        morsel = _morsel(data, infer_sample_size=6, **kwargs)
        assert str(morsel.schema["p"]) == "DrakenType.FLOAT64", kwargs


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
