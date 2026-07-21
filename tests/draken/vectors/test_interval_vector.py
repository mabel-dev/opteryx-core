"""IntervalVector tests: construction, arithmetic, comparison, temporal apply.

The interval model changed since this file was last touched: there is no
Vector.from_arrow, and PyArrow's three interval subtypes (month_day_nano,
month-only, day_time) no longer map onto Draken at all. The current, single
representation is draken.draken_native.vector_interval_from_sequence(list of
(months: int, ms: int) | None) — a 2-tuple, not 3. Its docstring calls the
second component "ms", but observed behavior (see test_temporal_minus_temporal
below: 31 days round-trips as 2_678_400_000_000, which is 31 days in
MICROSECONDS, not ms) shows it is actually microseconds — a doc/name
inconsistency worth fixing separately, not papered over here.

Vector has no add_vector/subtract_vector/to_arrow_interval/to_arrow_binary;
the real ops are interval_add/interval_sub (component-wise),
compare_vector(other, op) (normalized: total_us = months * 30d_in_us + us),
apply_to_temporal(interval, signum) (DATE32|TIMESTAMP64 ± INTERVAL ->
TIMESTAMP64), and temporal_minus_temporal(other) (DATE32|TIMESTAMP64 -
DATE32|TIMESTAMP64 -> INTERVAL). compare_vector no longer takes a third
"MONTH or YEAR" argument/restriction — that error and the 3-arg form are both
gone.
"""

import datetime

import pytest

from draken import draken_native
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

MICROSECONDS_PER_DAY = 24 * 60 * 60 * 1_000_000
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5


def _interval(values):
    return draken_native.vector_interval_from_sequence(values)


def test_construction_and_to_pylist():
    vec = _interval([(2, 0), (0, 1_500_000), None])
    assert vec.to_pylist() == [(2, 0), (0, 1_500_000), None]
    assert vec.length == 3
    assert vec.is_null_at(2) is True


def test_interval_add():
    left = _interval([(1, 0), None, (0, 1000)])
    right = _interval([(0, 2000), (0, 0), (0, 1000)])

    added = left.interval_add(right)
    assert added.to_pylist() == [(1, 2000), None, (0, 2000)]


def test_interval_sub():
    left = _interval([(1, 0), None, (0, 1000)])
    right = _interval([(0, 2000), (0, 0), (0, 1000)])

    subtracted = left.interval_sub(right)
    assert subtracted.to_pylist() == [(1, -2000), None, (0, 0)]


def test_interval_add_length_mismatch():
    left = _interval([(1, 0), (1, 0)])
    right = _interval([(1, 0)])

    with pytest.raises(ValueError, match="length"):
        left.interval_add(right)


def test_interval_compare_vector():
    """compare_vector normalizes months into a 30-day-month microsecond total."""
    left = _interval([(0, 2000), None])
    right = _interval([(0, 1000), (0, 1000)])

    compared = left.compare_vector(right, GT)
    assert compared.to_pylist() == [True, None]

    # A month normalizes to 30 days of microseconds, so 1 month > 29 days.
    one_month = _interval([(1, 0)])
    twenty_nine_days = _interval([(0, 29 * MICROSECONDS_PER_DAY)])
    assert one_month.compare_vector(twenty_nine_days, GT).to_pylist() == [True]


def test_apply_to_temporal_date_plus_interval():
    """DATE32 + INTERVAL(months) -> TIMESTAMP64, with calendar day-clamping."""
    dates = vector_from_sequence([datetime.date(2024, 1, 31), None], dtype=DrakenType.DATE32)
    one_month = _interval([(1, 0), (1, 0)])

    applied = dates.apply_to_temporal(one_month, 1)
    # Jan 31 + 1 month clamps to Feb 29 (2024 is a leap year), not Mar 2/3.
    assert applied.to_pylist() == [
        datetime.datetime(2024, 2, 29, tzinfo=datetime.timezone.utc),
        None,
    ]


def test_apply_to_temporal_minus():
    dates = vector_from_sequence([datetime.date(2024, 3, 15)], dtype=DrakenType.DATE32)
    interval = _interval([(0, 5 * MICROSECONDS_PER_DAY)])

    applied = dates.apply_to_temporal(interval, -1)
    assert applied.to_pylist() == [datetime.datetime(2024, 3, 10, tzinfo=datetime.timezone.utc)]


def test_apply_to_temporal_length_mismatch():
    dates = vector_from_sequence([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)], dtype=DrakenType.DATE32)
    interval = _interval([(1, 0)])

    with pytest.raises(ValueError, match="length"):
        dates.apply_to_temporal(interval, 1)


def test_temporal_minus_temporal():
    """DATE32 - DATE32 -> INTERVAL (months=0, delta in microseconds)."""
    later = vector_from_sequence([datetime.date(2024, 2, 1)], dtype=DrakenType.DATE32)
    earlier = vector_from_sequence([datetime.date(2024, 1, 1)], dtype=DrakenType.DATE32)

    diff = later.temporal_minus_temporal(earlier)
    # January has 31 days.
    assert diff.to_pylist() == [(0, 31 * MICROSECONDS_PER_DAY)]
