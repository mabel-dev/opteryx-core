"""Regression tests: CAST(... AS DECIMAL(p,s)) honours a declared scale exactly.

A declared scale is a contract, not a hint: `CAST(x AS DECIMAL(10,2))` means the
caller explicitly said "2 fractional digits". Two bugs are covered here:

  1. `1.23::DECIMAL(38,6)` used to materialise on the wrong physical tier
     (int64-backed DECIMAL instead of int128-backed DECIMAL128) because the
     bind-time literal-constant path derived (precision, scale) from the
     parsed value's own digit count instead of the declared CAST target.
     Fixed in `_materialise_constant_literal`
     (opteryx/compiled/expression/compiled_expression.pyx).

  2. A source value with MORE decimal places than the declared scale (e.g.
     CAST('1.23456' AS DECIMAL(10,2))) was silently rounded away instead of
     failing loud — inconsistent with the native decimal_to_unscaled kernel,
     which does raise for the same case ("value has more decimal places than
     the declared scale"), and with the project's fail-fast philosophy.
     Fixed in `_build_decimal_closure` (opteryx/expression/casts.pyx): plain
     CAST now raises ValueError; TRY_CAST returns NULL for that row, matching
     every other TRY_CAST parse-failure path.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from decimal import Decimal

import pytest

import opteryx

_SESSION = opteryx.session()


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


def _col_and_type(sql, colname="x"):
    out = []
    phys = None
    for morsel in _SESSION.execute_to_morsels(sql):
        v = morsel.column(colname)
        phys = str(getattr(v, "type", None))
        out.extend(v.to_pylist())
    return out, phys


# ---------------------------------------------------------------------------
# Bug 1 — literal precision/scale must come from the DECLARED target, not the
# value's own digit count.
# ---------------------------------------------------------------------------
def test_decimal_literal_promotes_to_decimal128_when_declared():
    vals, phys = _col_and_type("SELECT 1.23::DECIMAL(38,6) AS x FROM $planets LIMIT 1")
    assert "DECIMAL128" in phys, phys
    assert vals == [Decimal("1.230000")], vals


def test_decimal_literal_stays_on_decimal_tier_for_small_precision():
    vals, phys = _col_and_type("SELECT 1.23::DECIMAL(10,2) AS x FROM $planets LIMIT 1")
    assert "DECIMAL128" not in phys, phys
    assert vals == [Decimal("1.23")], vals


def test_decimal128_literal_round_trips_through_string_casts():
    # Regression for the original report: CAST to VARCHAR/VARBINARY used to
    # crash with "cast decimal->string: expected 103, got 5" because the
    # literal was mistagged as the int64 tier despite a declared precision > 18.
    vals = _col(
        "SELECT CAST(d AS VARCHAR) AS x "
        "FROM (SELECT 1.23::DECIMAL(38,6) AS d FROM $planets) LIMIT 1"
    )
    assert vals == ["1.230000"], vals
    vals = _col(
        "SELECT CAST(d AS VARBINARY) AS x "
        "FROM (SELECT 1.23::DECIMAL(38,6) AS d FROM $planets) LIMIT 1"
    )
    assert vals == [b"1.230000"], vals


# ---------------------------------------------------------------------------
# Bug 2 — a value with more decimal places than the declared scale must fail
# loud on CAST, and map to NULL on TRY_CAST. Exercised through a VARCHAR
# source so the closure path (not the inherently-approximate FLOAT->DECIMAL
# native kernel) is what runs.
# ---------------------------------------------------------------------------
def test_cast_decimal_raises_when_value_has_more_places_than_declared_scale():
    with pytest.raises(Exception, match="more decimal places than the declared scale"):
        _col(
            "SELECT CAST(x AS DECIMAL(10,2)) AS x "
            "FROM (SELECT '1.23456' AS x FROM $planets) LIMIT 1"
        )


def test_try_cast_decimal_returns_null_when_value_has_more_places_than_declared_scale():
    vals = _col(
        "SELECT TRY_CAST(x AS DECIMAL(10,2)) AS x "
        "FROM (SELECT '1.23456' AS x FROM $planets) LIMIT 1"
    )
    assert vals == [None], vals


def test_cast_decimal_honours_declared_scale_when_value_fits_exactly():
    # Fewer decimal places than declared scale: zero-pads, does not raise.
    vals = _col(
        "SELECT CAST(x AS DECIMAL(10,2)) AS x "
        "FROM (SELECT '1.2' AS x FROM $planets) LIMIT 1"
    )
    assert vals == [Decimal("1.20")], vals
    # Exactly the declared scale: passes through unchanged.
    vals = _col(
        "SELECT CAST(x AS DECIMAL(10,2)) AS x "
        "FROM (SELECT '1.23' AS x FROM $planets) LIMIT 1"
    )
    assert vals == [Decimal("1.23")], vals


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All decimal scale-fidelity tests passed.")
