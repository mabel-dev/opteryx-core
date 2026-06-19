"""vector_retag_int64_as_timestamp64 — zero-copy INT64 -> TIMESTAMP64 retag.

Unlike vector_reinterpret_as_timestamp64 (which copies), this MOVES the source's
buffers, so the source Vector is emptied. Used by the parquet reader where the
decoded column is exclusively owned.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from draken.draken_native import vector_from_sequence, vector_retag_int64_as_timestamp64

UTC = datetime.timezone.utc


def test_retag_seconds_keeps_value_verbatim():
    v = vector_from_sequence([1370000000, 75])
    out = vector_retag_int64_as_timestamp64(v, "s").to_pylist()
    assert out == [
        datetime.datetime(2013, 5, 31, 11, 33, 20, tzinfo=UTC),
        datetime.datetime(1970, 1, 1, 0, 1, 15, tzinfo=UTC),
    ]


def test_retag_units_distinct():
    # same int, different unit tag -> different instant (verbatim, no scaling).
    assert vector_retag_int64_as_timestamp64(vector_from_sequence([1000000]), "s").to_pylist() == [
        datetime.datetime(1970, 1, 12, 13, 46, 40, tzinfo=UTC)
    ]
    assert vector_retag_int64_as_timestamp64(vector_from_sequence([1000000]), "us").to_pylist() == [
        datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
    ]


def test_source_is_emptied_after_move():
    v = vector_from_sequence([1, 2, 3])
    _ = vector_retag_int64_as_timestamp64(v, "s")
    # The move transfers ownership; the source must be left an empty husk, not a
    # dangling view onto buffers now owned by the result.
    assert v.to_pylist() == []


def test_non_int64_rejected():
    with pytest.raises(Exception):
        vector_retag_int64_as_timestamp64(vector_from_sequence([1.5, 2.5]), "s")


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
