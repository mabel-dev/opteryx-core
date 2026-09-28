"""Regression tests: aggregate ARITHMETIC over UINT64 values >= 2^63.

The aggregate sinks (native_group_sinks.hpp) carry every integer operand in an
int64 "raw" container; a UINT64 is stored as its bit pattern, so a value >= 2^63
reads negative. That is right for MIN/MAX (copied back out at UINT64) and was
wrong for every piece of arithmetic: SUM/AVG widened the raw as signed into the
exact int128 sum, and STDDEV/VAR/MEDIAN/CORR/APPROX_PERCENTILE converted it to a
double as signed — each such value counted as value - 2^64.

Over [2^64 - 1, 1] the old code returned AVG 0.0 (should be 2^63), STDDEV 1.0
(should be ~2^63) and MEDIAN 0.0 (should be 2^63), and SUM over a single 2^63
returned -2^63 instead of raising the INT64 overflow. All arithmetic now reads a
raw through agg2_raw_as_i128 / agg2_raw_as_double.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

_SESSION = opteryx.session()

_TWO_63 = 2**63
_MAX_U64 = 2**64 - 1

# [2^64 - 1, 1] in one group (k = 1): the exact sum is 2^64, the mean 2^63.
_PAIR = (
    "(SELECT 1 AS k, CAST(18446744073709551615 AS UINT64) AS v "
    "UNION ALL SELECT 1 AS k, CAST(1 AS UINT64) AS v) AS t"
)
# A single 2^63: its SUM is past INT64.
_BIG = "(SELECT 1 AS k, CAST(9223372036854775808 AS UINT64) AS v) AS t"


def _rows(sql):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    return out


def _approx(value, expected):
    return value == pytest.approx(expected, rel=1e-12)


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT AVG(v), STDDEV(v), MEDIAN(v) FROM {_PAIR}",
        f"SELECT AVG(DISTINCT v), STDDEV(DISTINCT v), MEDIAN(DISTINCT v) FROM {_PAIR}",
    ],
)
def test_ungrouped_arithmetic_reads_uint64_unsigned(sql):
    avg, stddev, median = _rows(sql)[0]
    assert _approx(avg, _TWO_63), avg
    # population stddev of {2^64 - 1, 1} is (2^64 - 2) / 2
    assert _approx(stddev, (_MAX_U64 - 1) / 2), stddev
    assert _approx(median, _TWO_63), median


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT k, AVG(v), STDDEV(v), MEDIAN(v) FROM {_PAIR} GROUP BY k",
        f"SELECT k, AVG(DISTINCT v), STDDEV(DISTINCT v), MEDIAN(DISTINCT v) FROM {_PAIR} GROUP BY k",
    ],
)
def test_grouped_arithmetic_reads_uint64_unsigned(sql):
    k, avg, stddev, median = _rows(sql)[0]
    assert k == 1
    assert _approx(avg, _TWO_63), avg
    assert _approx(stddev, (_MAX_U64 - 1) / 2), stddev
    assert _approx(median, _TWO_63), median


def test_corr_of_uint64_with_itself_is_one():
    # The old signed read turned {2^64 - 1, 1} into {-1, 1}: still perfectly
    # correlated, so this guards the second-operand read path rather than the value.
    assert _approx(_rows(f"SELECT CORR(v, v) FROM {_PAIR}")[0][0], 1.0)


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT SUM(v) FROM {_BIG}",
        f"SELECT SUM(DISTINCT v) FROM {_BIG}",
        f"SELECT k, SUM(v) FROM {_BIG} GROUP BY k",
        f"SELECT k, SUM(DISTINCT v) FROM {_BIG} GROUP BY k",
    ],
)
def test_sum_of_uint64_past_int64_raises_overflow(sql):
    # SUM's output is INT64; a UINT64 >= 2^63 alone exceeds it. The old read
    # returned -2^63 here — a silent wrong answer instead of the overflow.
    with pytest.raises(Exception, match="SUM overflow"):
        _rows(sql)


def test_uint64_sums_within_int64_are_unchanged():
    assert _rows(
        "SELECT SUM(v), AVG(v) FROM (SELECT CAST(g AS UINT64) AS v "
        "FROM generate_series(1, 10) AS g) AS t"
    ) == [(55, 5.5)]
    assert _rows(
        "SELECT g % 2 AS k, SUM(v) FROM (SELECT g, CAST(g AS UINT64) AS v "
        "FROM generate_series(1, 10) AS g) AS t GROUP BY g % 2 ORDER BY k"
    ) == [(0, 30), (1, 25)]


def test_uint64_min_max_keep_their_bit_pattern():
    # MIN/MAX still copy the raw back out at UINT64 — the fix must not touch them.
    assert _rows(f"SELECT MIN(v), MAX(v) FROM {_PAIR}") == [(1, _MAX_U64)]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
