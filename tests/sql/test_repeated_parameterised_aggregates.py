"""Regression tests: two calls to the same parameterised aggregate over the same
column, differing only in a non-column argument.

`SELECT p50, p95, p99 of one column` is the most natural use of
APPROX_PERCENTILE and it did not run. The logical planner's aggregate
de-duplication (`logical_planner_rewriter._dedup_key`) identified an aggregate by
`(function, operand[0], DISTINCT, FILTER, ORDER BY, LIMIT)` — every argument
AFTER the operand was invisible to it, so `APPROX_PERCENTILE(x, 0.5)` and
`APPROX_PERCENTILE(x, 0.95)` produced the same key and the second was dropped
without a word. The projection then asked for a column nothing computed, and the
query died in three different ways depending on how it was written:

  * bare      -> NotSupportedError "projecting a column the engine could not
                 resolve here is not supported"
  * CAST-ed   -> a raw KeyError naming internal `$derived_*` identities
  * CORR      -> SqlError "Column 'density' must appear in the GROUP BY clause"

None of the three named the real problem. Two calls on DIFFERENT columns always
worked (different `operand[0]`), which is what makes the shape here the
regression surface: SAME column, DIFFERENT parameter.

APPROX_PERCENTILE(value, percentile) and CORR(x, y) are the engine's only
aggregates that take more than one argument (reference/aggregates.json) — every
other aggregate is single-operand and cannot collide this way. ARRAY_AGG's
DISTINCT/ORDER BY/LIMIT modifiers and COUNT's FILTER were already in the key.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _one(sql):
    rows = _rows(sql)
    assert len(rows) == 1, (sql, len(rows))
    return rows[0]


# --------------------------------------------------------------------------
# APPROX_PERCENTILE — the motivating case
# --------------------------------------------------------------------------


def test_two_percentiles_same_column():
    p50, p95 = _one(
        "SELECT APPROX_PERCENTILE(diameter, 0.5), APPROX_PERCENTILE(diameter, 0.95) "
        "FROM $planets"
    )
    (want50,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.5) FROM $planets")
    (want95,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.95) FROM $planets")
    assert p50 == want50, (p50, want50)
    assert p95 == want95, (p95, want95)
    # The defect returned the p50 value for both; if the two percentiles ever
    # coincide on this column the test above stops proving anything.
    assert want50 != want95, (want50, want95)


def test_three_percentiles_same_column():
    got = _one(
        "SELECT APPROX_PERCENTILE(diameter, 0.05) a, "
        "       APPROX_PERCENTILE(diameter, 0.5) b, "
        "       APPROX_PERCENTILE(diameter, 0.95) c "
        "FROM $planets"
    )
    want = tuple(
        _one(f"SELECT APPROX_PERCENTILE(diameter, {p}) FROM $planets")[0]
        for p in ("0.05", "0.5", "0.95")
    )
    assert tuple(got) == want, (got, want)
    assert len(set(want)) == 3, want


def test_two_percentiles_same_column_grouped():
    got = {
        r[0]: (r[1], r[2])
        for r in _rows(
            "SELECT id % 3 AS bucket, "
            "       APPROX_PERCENTILE(diameter, 0.5) p50, "
            "       APPROX_PERCENTILE(diameter, 0.95) p95 "
            "FROM $planets GROUP BY id % 3"
        )
    }
    want50 = {
        r[0]: r[1]
        for r in _rows(
            "SELECT id % 3 AS bucket, APPROX_PERCENTILE(diameter, 0.5) "
            "FROM $planets GROUP BY id % 3"
        )
    }
    want95 = {
        r[0]: r[1]
        for r in _rows(
            "SELECT id % 3 AS bucket, APPROX_PERCENTILE(diameter, 0.95) "
            "FROM $planets GROUP BY id % 3"
        )
    }
    assert set(got) == set(want50) == set(want95), (got, want50, want95)
    for bucket, (p50, p95) in got.items():
        assert p50 == want50[bucket], (bucket, p50, want50[bucket])
        assert p95 == want95[bucket], (bucket, p95, want95[bucket])


def test_two_percentiles_same_column_wrapped_in_cast():
    """The CAST form failed DIFFERENTLY (a bare KeyError over stream identities),
    so it is pinned separately rather than folded into the bare case."""
    p50, p95 = _one(
        "SELECT CAST(APPROX_PERCENTILE(diameter, 0.5) AS INTEGER) p50, "
        "       CAST(APPROX_PERCENTILE(diameter, 0.95) AS INTEGER) p95 "
        "FROM $planets"
    )
    (want50,) = _one(
        "SELECT CAST(APPROX_PERCENTILE(diameter, 0.5) AS INTEGER) FROM $planets"
    )
    (want95,) = _one(
        "SELECT CAST(APPROX_PERCENTILE(diameter, 0.95) AS INTEGER) FROM $planets"
    )
    assert (p50, p95) == (want50, want95), (p50, p95, want50, want95)
    assert want50 != want95, (want50, want95)


def test_two_percentiles_same_column_grouped_and_cast():
    rows = _rows(
        "SELECT id % 3 AS bucket, "
        "       CAST(APPROX_PERCENTILE(diameter, 0.5) AS INTEGER) p50, "
        "       CAST(APPROX_PERCENTILE(diameter, 0.95) AS INTEGER) p95 "
        "FROM $planets GROUP BY id % 3"
    )
    assert len(rows) == 3, rows
    assert all(len(r) == 3 for r in rows), rows


def test_two_percentiles_different_columns_still_work():
    """Regression guard: this shape worked before the fix (different operands
    gave different dedup keys) and must keep working."""
    a, b = _one(
        "SELECT APPROX_PERCENTILE(diameter, 0.5), APPROX_PERCENTILE(mass, 0.5) "
        "FROM $planets"
    )
    (wanta,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.5) FROM $planets")
    (wantb,) = _one("SELECT APPROX_PERCENTILE(mass, 0.5) FROM $planets")
    assert (a, b) == (wanta, wantb), (a, b, wanta, wantb)


def test_percentiles_mixed_with_other_aggregates_over_same_column():
    count, mx, mn, p50, p95 = _one(
        "SELECT COUNT(*), MAX(diameter), MIN(diameter), "
        "       APPROX_PERCENTILE(diameter, 0.5), APPROX_PERCENTILE(diameter, 0.95) "
        "FROM $planets"
    )
    (want_count, want_max, want_min) = _one(
        "SELECT COUNT(*), MAX(diameter), MIN(diameter) FROM $planets"
    )
    (want50,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.5) FROM $planets")
    (want95,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.95) FROM $planets")
    assert (count, mx, mn) == (want_count, want_max, want_min)
    assert (p50, p95) == (want50, want95), (p50, p95, want50, want95)


def test_repeated_identical_percentile_still_dedups_to_one_value():
    """Same function, same column, SAME percentile: still one computation, and
    both output columns carry it. The fix widened the key; it must not have
    stopped the key from matching where it should."""
    a, b = _one(
        "SELECT APPROX_PERCENTILE(diameter, 0.5) a, "
        "       APPROX_PERCENTILE(diameter, 0.5) b FROM $planets"
    )
    (want,) = _one("SELECT APPROX_PERCENTILE(diameter, 0.5) FROM $planets")
    assert a == b == want, (a, b, want)


# --------------------------------------------------------------------------
# CORR — the same defect on the engine's other multi-argument aggregate
# --------------------------------------------------------------------------


def test_two_corrs_sharing_their_first_operand():
    a, b = _one("SELECT CORR(diameter, mass), CORR(diameter, density) FROM $planets")
    (wanta,) = _one("SELECT CORR(diameter, mass) FROM $planets")
    (wantb,) = _one("SELECT CORR(diameter, density) FROM $planets")
    assert (a, b) == (wanta, wantb), (a, b, wanta, wantb)
    assert wanta != wantb, (wanta, wantb)


def test_two_corrs_sharing_their_first_operand_grouped():
    rows = _rows(
        "SELECT id % 3 AS bucket, CORR(diameter, mass) a, CORR(diameter, density) b "
        "FROM $planets GROUP BY id % 3"
    )
    assert len(rows) == 3, rows
    for _bucket, a, b in rows:
        assert a != b or (a is None and b is None), (a, b)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
