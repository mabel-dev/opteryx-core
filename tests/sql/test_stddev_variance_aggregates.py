"""Regression tests for STDDEV_SAMP/STDDEV_POP/VAR_POP/VAR_SAMP.

STDDEV was population stddev only (N denominator). These four round out the
family: STDDEV_POP is a pure alias for STDDEV; STDDEV_SAMP/VAR_POP/VAR_SAMP
are new finalizations over the SAME accumulated Σx/Σx²/count lanes
(agg2_update_stddev in native_group_sinks.hpp) — only the divisor (and,
for the stddev forms, the final sqrt) differs. See GBKind::StddevSamp/
VarPop/VarSamp and their emit_lane_column cases.

The N-1 (sample) forms are undefined for a group with fewer than 2 valid
rows and must return NULL — not 0, not a divide-by-zero — matching DuckDB's
stddev_samp/var_samp and the SQL standard. The N (population) forms stay
defined (0) at exactly 1 valid row, same as STDDEV always was. Expected
values below were verified against DuckDB (stddev_samp/stddev_pop/var_samp/
var_pop) directly, both formulas and the N=1 NULL/0 split.

Run as a script (CLAUDE.md §10) or under pytest.
"""

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


def _pop_var(values):
    n = len(values)
    mean = sum(values) / n
    return sum((v - mean) ** 2 for v in values) / n


def _samp_var(values):
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    return sum((v - mean) ** 2 for v in values) / (n - 1)


def test_stddev_pop_is_stddev_alias():
    # STDDEV_POP is a pure alias for STDDEV (same AggFn/GBKind under the
    # hood) — not merely "close", bit-identical.
    a = _rows("SELECT STDDEV(numberOfMoons) FROM $planets")[0][0]
    b = _rows("SELECT STDDEV_POP(numberOfMoons) FROM $planets")[0][0]
    assert a == b, (a, b)


def test_known_dataset_exact_values():
    # (VALUES (1),(2),(3),(4),(5)): mean 3, Σ(x-mean)² = 4+1+0+1+4 = 10.
    # pop var = 10/5 = 2.0, pop stddev = sqrt(2) = 1.4142135623730951
    # samp var = 10/4 = 2.5, samp stddev = sqrt(2.5) = 1.5811388300841898
    (sp, ss, vp, vs) = _rows(
        "SELECT STDDEV_POP(x), STDDEV_SAMP(x), VAR_POP(x), VAR_SAMP(x) "
        "FROM (VALUES (1),(2),(3),(4),(5)) t(x)"
    )[0]
    assert abs(sp - 1.4142135623730951) < 1e-12, sp
    assert abs(ss - 1.5811388300841898) < 1e-12, ss
    assert abs(vp - 2.0) < 1e-12, vp
    assert abs(vs - 2.5) < 1e-12, vs
    # STDDEV (no suffix) must agree with STDDEV_POP on the same input.
    (std,) = _rows("SELECT STDDEV(x) FROM (VALUES (1),(2),(3),(4),(5)) t(x)")[0]
    assert std == sp, (std, sp)


def test_single_row_group_sample_forms_are_null_population_forms_are_zero():
    # N=1: sample forms (N-1 == 0 denominator) are undefined -> NULL.
    # Population forms stay defined -> 0.0. Matches DuckDB exactly.
    (ss, vs, sp, vp) = _rows(
        "SELECT STDDEV_SAMP(x), VAR_SAMP(x), STDDEV_POP(x), VAR_POP(x) "
        "FROM (VALUES (1)) t(x)"
    )[0]
    assert ss is None, ss
    assert vs is None, vs
    assert sp == 0.0, sp
    assert vp == 0.0, vp


def test_ungrouped_matches_oracle_on_planets():
    values = [float(r[0]) for r in _rows("SELECT numberOfMoons FROM $planets")]
    (ss, sp, vs, vp) = _rows(
        "SELECT STDDEV_SAMP(numberOfMoons), STDDEV_POP(numberOfMoons), "
        "VAR_SAMP(numberOfMoons), VAR_POP(numberOfMoons) FROM $planets"
    )[0]
    assert abs(sp - _pop_var(values) ** 0.5) < 1e-9, (sp, values)
    assert abs(vp - _pop_var(values)) < 1e-9, (vp, values)
    assert abs(ss - _samp_var(values) ** 0.5) < 1e-9, (ss, values)
    assert abs(vs - _samp_var(values)) < 1e-9, (vs, values)


def test_grouped_matches_oracle_on_planets():
    raw = _rows("SELECT numberOfMoons > 5, numberOfMoons FROM $planets")
    got = {
        r[0]: (r[1], r[2], r[3], r[4])
        for r in _rows(
            "SELECT numberOfMoons > 5 AS big, STDDEV_SAMP(numberOfMoons), "
            "STDDEV_POP(numberOfMoons), VAR_SAMP(numberOfMoons), "
            "VAR_POP(numberOfMoons) FROM $planets GROUP BY numberOfMoons > 5"
        )
    }
    for key in (True, False):
        values = [float(r[1]) for r in raw if r[0] is key]
        ss, sp, vs, vp = got[key]
        assert abs(sp - _pop_var(values) ** 0.5) < 1e-9, (key, sp, values)
        assert abs(vp - _pop_var(values)) < 1e-9, (key, vp, values)
        assert abs(ss - _samp_var(values) ** 0.5) < 1e-9, (key, ss, values)
        assert abs(vs - _samp_var(values)) < 1e-9, (key, vs, values)


def test_null_ignoring():
    # surfacePressure carries NULLs — all four forms must ignore them, not
    # treat them as 0 or propagate NULL for the whole aggregate.
    values = [float(v) for (v,) in _rows(
        "SELECT surfacePressure FROM $planets WHERE surfacePressure IS NOT NULL")]
    (ss, sp) = _rows(
        "SELECT STDDEV_SAMP(surfacePressure), STDDEV_POP(surfacePressure) "
        "FROM $planets")[0]
    assert abs(sp - _pop_var(values) ** 0.5) < 1e-6, (sp, values)
    assert abs(ss - _samp_var(values) ** 0.5) < 1e-6, (ss, values)


def test_stddev_samp_decimal_operand_rejected():
    # $planets.gravity is DECIMAL — rejected at plan time for the whole
    # family, same no-descale posture as STDDEV/CORR/MEDIAN.
    for fn in ("STDDEV_SAMP", "STDDEV_POP", "VAR_SAMP", "VAR_POP"):
        try:
            _rows(f"SELECT {fn}(gravity) FROM $planets")
            raise AssertionError(f"{fn} over DECIMAL did not raise")
        except NotSupportedError:
            pass


def test_variance_is_stddev_squared():
    # Mathematical identity, not a duplicate of the oracle checks above:
    # pins that VAR_* really is the pre-sqrt of STDDEV_* on the SAME data.
    (ss, vs, sp, vp) = _rows(
        "SELECT STDDEV_SAMP(numberOfMoons), VAR_SAMP(numberOfMoons), "
        "STDDEV_POP(numberOfMoons), VAR_POP(numberOfMoons) FROM $planets"
    )[0]
    assert abs(ss * ss - vs) < 1e-9, (ss, vs)
    assert abs(sp * sp - vp) < 1e-9, (sp, vp)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
