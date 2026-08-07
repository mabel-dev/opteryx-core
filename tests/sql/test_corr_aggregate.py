"""Regression tests for the CORR aggregate and the MEDIAN memory budget.

CORR(x, y) is the engine's only two-column-operand aggregate
(AggSpec2.col_idx2 → GBKind::Corr in native_group_sinks.hpp): Pearson
correlation over (x, y) pairs where BOTH values are non-NULL, always DOUBLE,
NULL when undefined (no pairs, or zero variance in either operand). Both
operands are numeric-only — DECIMAL is rejected at plan time (same
no-descale posture as MEDIAN/STDDEV).

MEDIAN is bounded by a global 512MB byte budget across all group buffers
(kMedianBudgetBytes, _agg_kernels.hpp) — it replaced a 1000-value per-group
cap that refused ordinary group sizes while bounding nothing (group count is
unbounded). The tests here pin that groups far beyond the old cap now work.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import math
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.exceptions import NotSupportedError

_SESSION = opteryx.session()


def _rows(sql):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _pearson(pairs):
    """Oracle: population Pearson r over (x, y) pairs; None when undefined."""
    n = len(pairs)
    if n == 0:
        return None
    sx = sum(x for x, _ in pairs)
    sy = sum(y for _, y in pairs)
    sxx = sum(x * x for x, _ in pairs)
    syy = sum(y * y for _, y in pairs)
    sxy = sum(x * y for x, y in pairs)
    varx = sxx / n - (sx / n) ** 2
    vary = syy / n - (sy / n) ** 2
    denom = math.sqrt(max(varx, 0.0) * max(vary, 0.0))
    if denom == 0.0:
        return None
    return (sxy / n - (sx / n) * (sy / n)) / denom


def test_ungrouped_corr_matches_oracle():
    pairs = [(r[0], r[1]) for r in _rows("SELECT id, numberOfMoons FROM $planets")]
    expected = _pearson([(float(x), float(y)) for x, y in pairs])
    (got,) = _rows("SELECT CORR(id, numberOfMoons) FROM $planets")[0:1][0]
    assert abs(got - expected) < 1e-12, (got, expected)


def test_grouped_corr_matches_oracle():
    raw = _rows("SELECT numberOfMoons > 5, id, numberOfMoons FROM $planets")
    got = {r[0]: r[1] for r in _rows(
        "SELECT numberOfMoons > 5 AS big, CORR(id, numberOfMoons) AS r "
        "FROM $planets GROUP BY numberOfMoons > 5")}
    for key in (True, False):
        pairs = [(float(r[1]), float(r[2])) for r in raw if r[0] is key]
        expected = _pearson(pairs)
        assert abs(got[key] - expected) < 1e-12, (key, got[key], expected)


def test_corr_pairwise_null_exclusion():
    # surfacePressure carries NULLs: CORR must drop the PAIR when either side
    # is NULL, matching an oracle over only the fully-populated rows.
    raw = _rows("SELECT id, surfacePressure FROM $planets")
    pairs = [(float(x), float(y)) for x, y in raw if y is not None]
    expected = _pearson(pairs)
    (got,) = _rows("SELECT CORR(id, surfacePressure) FROM $planets")[0]
    assert abs(got - expected) < 1e-12, (got, expected)


def test_corr_perfect_correlation_is_clamped():
    assert _rows("SELECT CORR(id, id) FROM $planets")[0][0] == 1.0
    assert _rows("SELECT CORR(id, 0 - id) FROM $planets")[0][0] == -1.0


def test_corr_zero_variance_is_null():
    (got,) = _rows("SELECT CORR(id, numberOfMoons * 0) FROM $planets")[0]
    assert got is None, got


def test_corr_no_pairs_is_null():
    (got,) = _rows("SELECT CORR(id, numberOfMoons) FROM $planets WHERE id > 999")[0]
    assert got is None, got


def test_corr_composes_with_scalar_functions():
    (r,) = _rows("SELECT CORR(id, numberOfMoons) FROM $planets")[0]
    (r2,) = _rows("SELECT POWER(CORR(id, numberOfMoons), 2) FROM $planets")[0]
    assert abs(r2 - r * r) < 1e-12, (r2, r * r)


def test_corr_decimal_operand_rejected():
    # $planets.gravity is DECIMAL — rejected at plan time, never descaled.
    try:
        _rows("SELECT CORR(gravity, mass) FROM $planets")
        raise AssertionError("CORR over DECIMAL did not raise")
    except NotSupportedError:
        pass


def test_corr_wrong_arity_rejected():
    try:
        _rows("SELECT CORR(id) FROM $planets")
        raise AssertionError("CORR with one argument did not raise")
    except NotSupportedError:
        pass


def test_median_group_beyond_old_cap():
    # The retired per-group cap refused >1000 values per group; the global
    # byte budget accepts them. 5000 values ungrouped, 2500 per group grouped.
    (count, med) = _rows(
        "SELECT COUNT(*), MEDIAN(g) FROM generate_series(1, 5000) AS g")[0]
    assert count == 5000 and med == 2500.5, (count, med)

    grouped = {r[0]: r[1] for r in _rows(
        "SELECT g % 2 AS parity, MEDIAN(g) FROM generate_series(1, 5000) AS g "
        "GROUP BY g % 2")}
    # odds 1..4999 → 2500; evens 2..5000 → 2501
    assert grouped == {1: 2500.0, 0: 2501.0}, grouped


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
