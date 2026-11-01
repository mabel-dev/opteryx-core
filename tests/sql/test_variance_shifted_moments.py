"""
STDDEV / VARIANCE / CORR stay accurate when values are large against their spread.

Every variance path kept Σx and Σx² and finalised Σx²/n - (Σx/n)². With values far
from zero that subtraction cancels every significant digit: STDDEV_SAMP(1e9 + {1..9})
answered 0.0 (truth 2.7386), a constant column had a non-zero deviation, and two plans
summing in different orders disagreed — which is how the single-table fuzzer found it.
The moments are now taken about a per-group shift K (the group's first value) and
re-based on merge; the finalize formulas are shift-invariant. Expected values are exact.
"""

import math
import os
import statistics
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


@pytest.mark.parametrize("base", ["0", "1e6", "1e9", "1e12"])
def test_offset_values_keep_their_spread(base):
    sql = (
        "SELECT STDDEV_SAMP(v), VAR_POP(v) FROM "
        f"(SELECT CAST({base} AS FLOAT64) + CAST(id AS FLOAT64) AS v FROM $planets) AS s"
    )
    ((sd, var),) = _rows(sql)
    values = range(1, 10)
    assert math.isclose(sd, statistics.stdev(values), rel_tol=1e-12)
    assert math.isclose(var, statistics.pvariance(values), rel_tol=1e-12)


@pytest.mark.parametrize("base", ["1e9", "1e12"])
def test_offset_values_grouped(base):
    sql = (
        "SELECT g, STDDEV_SAMP(v) FROM (SELECT id % 3 AS g, "
        f"CAST({base} AS FLOAT64) + CAST(id AS FLOAT64) AS v FROM $planets) AS s GROUP BY g"
    )
    result = dict(_rows(sql))
    for g in range(3):
        expected = statistics.stdev([i for i in range(1, 10) if i % 3 == g])
        assert math.isclose(result[g], expected, rel_tol=1e-12), (g, result[g], expected)


def test_constant_column_has_zero_deviation():
    sql = (
        "SELECT STDDEV_SAMP(v), VAR_POP(v) FROM "
        "(SELECT CAST('1234.567' AS FLOAT64) AS v FROM testdata.fuzzing.wide) AS s"
    )
    assert _rows(sql) == [(0.0, 0.0)]


def test_correlation_of_offset_values():
    sql = (
        "SELECT CORR(v, 2 * v + 1), CORR(v, -v) FROM "
        "(SELECT CAST(1e9 AS FLOAT64) + CAST(id AS FLOAT64) AS v FROM $planets) AS s"
    )
    ((pos, neg),) = _rows(sql)
    assert math.isclose(pos, 1.0, rel_tol=1e-12) and math.isclose(neg, -1.0, rel_tol=1e-12)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
