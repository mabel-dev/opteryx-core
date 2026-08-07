"""CAST(DECIMAL AS INTEGER / DOUBLE).

DECIMAL used to be a numeric dead end: it could be rescaled to another DECIMAL
or rendered as text, and nothing else. `CAST(dec_col AS INTEGER)` was refused at
plan time — "No native CAST DECIMAL → INTEGER" — even though every other numeric
type in the engine converts freely in both directions.

Two things are defended here:

  1. The source SCALE is honoured. It lives on the bind-time ColumnType, not on
     the runtime vector, and rides into the kernel in binary_op_ctx.left_scale.
     Get that wrong and `3.7::DECIMAL(2,1)::INTEGER` returns the raw unscaled
     payload — 37 — which is a wrong answer wearing a cast's clothes.

  2. INTEGER TRUNCATES TOWARD ZERO, matching draken_cast_float64_to_int64. An
     engine where `-3.7::FLOAT64::INTEGER` and `-3.7::DECIMAL(2,1)::INTEGER`
     disagreed would be indefensible, so both must give -3 (not -4).

Both physical tiers are covered: DECIMAL (int64-backed, p<=18) and DECIMAL128
(int128-backed, p>18) are separate kernels behind one shared core.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from decimal import Decimal

import opteryx

_SESSION = opteryx.session()


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


def test_decimal_column_to_integer_applies_the_source_scale():
    """$planets.gravity is DECIMAL — the pairing that was refused outright."""
    gravity = _col("SELECT gravity AS x FROM $planets")
    got = _col("SELECT CAST(gravity AS INTEGER) AS x FROM $planets")
    assert got == [int(g) for g in gravity], got
    # The scale was applied, not ignored: an unscaled read would give 37, not 3.
    assert got[0] == 3, got[0]


def test_decimal_column_to_double_applies_the_source_scale():
    gravity = _col("SELECT gravity AS x FROM $planets")
    got = _col("SELECT CAST(gravity AS FLOAT64) AS x FROM $planets")
    assert got == [float(g) for g in gravity], got


def test_decimal_to_integer_truncates_toward_zero():
    """Toward zero, NOT floor — the float cast's convention. -3.7 -> -3."""
    negated = _col("SELECT 0 - gravity AS x FROM $planets")
    got = _col("SELECT CAST(0 - gravity AS INTEGER) AS x FROM $planets")
    assert got == [int(n) for n in negated], got
    assert negated[0] == Decimal("-3.7") and got[0] == -3, (negated[0], got[0])


def test_decimal_and_double_agree_on_truncation():
    """The whole reason for choosing truncate-toward-zero: the two numeric routes
    to INTEGER must not disagree."""
    via_decimal = _col("SELECT CAST(0 - gravity AS INTEGER) AS x FROM $planets")
    via_double = _col("SELECT CAST(CAST(0 - gravity AS FLOAT64) AS INTEGER) AS x FROM $planets")
    assert via_decimal == via_double, (via_decimal, via_double)


def test_decimal128_tier_to_integer_and_double():
    """p>18 is a physically different payload (int128) and a separate kernel."""
    assert _col(
        "SELECT CAST(CAST(gravity AS DECIMAL(22,2)) AS INTEGER) AS x FROM $planets"
    )[:3] == [3, 8, 9]
    assert _col(
        "SELECT CAST(CAST(gravity AS DECIMAL(22,2)) AS FLOAT64) AS x FROM $planets"
    )[:3] == [3.7, 8.9, 9.8]


def test_decimal_to_numeric_preserves_nulls():
    sql = (
        "SELECT CAST(d AS {t}) AS x FROM "
        "(SELECT CASE WHEN id > 4 THEN gravity ELSE NULL END AS d FROM $planets) AS s"
    )
    for target in ("INTEGER", "FLOAT64"):
        got = _col(sql.format(t=target))
        assert got[:4] == [None, None, None, None], (target, got[:4])
        assert got[4] is not None, (target, got[4])


def test_decimal_to_every_unsigned_width():
    """Same core as the INTEGER target — truncate toward zero — then a range check
    into the target width."""
    for target in ("UINT8", "UINT16", "UINT32", "UINT64"):
        assert _col(f"SELECT CAST(gravity AS {target}) AS x FROM $planets")[:4] == [
            3,
            8,
            9,
            3,
        ], target
    assert _col(
        "SELECT CAST(CAST(gravity AS DECIMAL(22,2)) AS UINT32) AS x FROM $planets"
    )[:3] == [3, 8, 9]


def test_negative_decimal_to_unsigned_raises():
    """A negative value is not an unsigned one. Loud, never wrapped."""
    import pytest

    with pytest.raises(Exception):
        _col("SELECT CAST(0 - gravity AS UINT32) AS x FROM $planets")


def test_decimal_to_numeric_works_in_a_predicate():
    """Not just projection — the same program shape has to admit in a filter."""
    assert _col("SELECT COUNT(*) AS x FROM $planets WHERE CAST(gravity AS INTEGER) > 5") == [6]
    assert _col("SELECT COUNT(*) AS x FROM $planets WHERE CAST(gravity AS FLOAT64) > 5.0") == [6]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All DECIMAL → numeric cast tests passed.")
