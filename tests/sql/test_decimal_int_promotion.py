"""Regression tests for DECIMAL × INTEGER arithmetic and DECIMAL128 promotion.

Covers the two-part fix for `DECIMAL(p,s) op INTEGER` whose result overflows the
int64 DECIMAL tier (precision > 18 → DECIMAL128):

  1. CLOSURE PATH (draken_native.cpp decimal_*_dispatch): narrow integer operands
     (INT8/16/32) were rejected by dec_mul/dec_add/etc. Now every integer width is
     accepted with its ACTUAL digit precision (INT8→3 … INT64→19, matching the
     binder's type_unification._INT_DIGITS) and widened to INT64 stride before the
     kernels run, so the runtime result tier matches the bound schema. The genuine
     DECIMAL128 promotion (e.g. DECIMAL(10,2)*INT64) widens both operands to int128.

  2. C-NATIVE PATH (binop_dispatch.cpp draken_binop, allow-listed in
     _c_native_binop): DECIMAL128 × INT64 (either order) and cross-kind
     DECIMAL × DECIMAL128 (either order) widen the int64-backed operand to int128
     via widen_i64_to_dec128 and run the dec128_* kernels.

Every result is checked against a Python `Decimal` oracle (the SQL semantics:
add/sub scale = max; mul scale = sa+sb; div scale = max(sa+6,6), half-even),
including both operand orders, negatives, and nulls.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from decimal import Decimal, ROUND_HALF_EVEN, getcontext

import opteryx
from opteryx.compiled.expression.compiled_expression import _c_native_binop

getcontext().prec = 80

_SESSION = opteryx.session()


def _col(sql):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(b"r").to_pylist())
    return out


def _run(sql):
    """Return (values, result_physical_type_name)."""
    out = []
    phys = None
    for morsel in _SESSION.execute_to_morsels(sql):
        v = morsel.column(b"r")
        phys = getattr(getattr(v, "_nb", v), "type", None)
        out.extend(v.to_pylist())
    return out, str(phys)


def _quant(d, scale):
    return d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_EVEN)


def _assert_matches(got, expected):
    assert len(got) == len(expected), (len(got), len(expected))
    for i, (a, b) in enumerate(zip(got, expected)):
        if a is None or b is None:
            assert a is None and b is None, (i, a, b)
        else:
            assert Decimal(str(a)) == b, (i, a, b)


# Operand columns as Python values for the oracle.
_GRAV2 = [Decimal(str(x)).quantize(Decimal("0.01")) for x in _col("SELECT gravity AS r FROM $planets")]
_GRAV3 = [Decimal(str(x)).quantize(Decimal("0.001")) for x in _col("SELECT gravity AS r FROM $planets")]
_ID8 = _col("SELECT id AS r FROM $planets")                       # INT8
_ID64 = _col("SELECT CAST(id AS INTEGER) AS r FROM $planets")     # INT64


# ---------------------------------------------------------------------------
# Part 1 — closure path: DECIMAL(10,2) op INT8, both orders, result DECIMAL.
# ---------------------------------------------------------------------------
def test_closure_decimal_int8_add_sub_mul_both_orders():
    for op, fn in [("*", lambda a, b: a * b), ("+", lambda a, b: a + b), ("-", lambda a, b: a - b)]:
        got, _ = _run(f"SELECT CAST(gravity AS DECIMAL(10,2)) {op} id AS r FROM $planets")
        _assert_matches(got, [_quant(fn(a, Decimal(i)), 2) for a, i in zip(_GRAV2, _ID8)])
        got, _ = _run(f"SELECT id {op} CAST(gravity AS DECIMAL(10,2)) AS r FROM $planets")
        _assert_matches(got, [_quant(fn(Decimal(i), a), 2) for a, i in zip(_GRAV2, _ID8)])


def test_closure_decimal_int8_divide():
    # divide result scale = max(2+6, 6) = 8.
    got, _ = _run("SELECT CAST(gravity AS DECIMAL(10,2)) / id AS r FROM $planets")
    _assert_matches(got, [_quant(a / Decimal(i), 8) if i != 0 else None for a, i in zip(_GRAV2, _ID8)])


# ---------------------------------------------------------------------------
# Part 1 — closure path: DECIMAL(10,2) × INT64 promotes to DECIMAL128.
# ---------------------------------------------------------------------------
def test_closure_decimal_int64_promotes_to_decimal128():
    got, phys = _run("SELECT CAST(gravity AS DECIMAL(10,2)) * CAST(id AS INTEGER) AS r FROM $planets")
    assert "DECIMAL128" in phys, phys
    _assert_matches(got, [_quant(a * Decimal(i), 2) for a, i in zip(_GRAV2, _ID64)])


# ---------------------------------------------------------------------------
# Part 2 — c-native path: DECIMAL128 × INT64 (either order).
# ---------------------------------------------------------------------------
def test_cnative_routes_decimal128_cases():
    # PLUS..MODULO (1..5): all four DECIMAL128/int64 combinations route c-native.
    for op in (1, 2, 3, 4, 5):
        assert _c_native_binop(op, "DECIMAL128", "INT64", "DECIMAL128")
        assert _c_native_binop(op, "INT64", "DECIMAL128", "DECIMAL128")
        assert _c_native_binop(op, "DECIMAL", "DECIMAL128", "DECIMAL128")
        assert _c_native_binop(op, "DECIMAL128", "DECIMAL", "DECIMAL128")
        # narrow int and DECIMAL×INT64→DECIMAL128 stay on the closure.
        assert not _c_native_binop(op, "DECIMAL", "INT8", "DECIMAL")
        assert not _c_native_binop(op, "DECIMAL", "INT64", "DECIMAL128")


def test_cnative_decimal128_int64_add_sub_mul_both_orders():
    for op, fn in [("*", lambda a, b: a * b), ("+", lambda a, b: a + b), ("-", lambda a, b: a - b)]:
        got, phys = _run(f"SELECT CAST(gravity AS DECIMAL(20,3)) {op} CAST(id AS INTEGER) AS r FROM $planets")
        assert "DECIMAL128" in phys, phys
        _assert_matches(got, [_quant(fn(a, Decimal(i)), 3) for a, i in zip(_GRAV3, _ID64)])
        got, phys = _run(f"SELECT CAST(id AS INTEGER) {op} CAST(gravity AS DECIMAL(20,3)) AS r FROM $planets")
        assert "DECIMAL128" in phys, phys
        _assert_matches(got, [_quant(fn(Decimal(i), a), 3) for a, i in zip(_GRAV3, _ID64)])


def test_cnative_decimal128_int64_divide():
    # divide result scale = max(3+6, 6) = 9.
    got, phys = _run("SELECT CAST(gravity AS DECIMAL(20,3)) / CAST(id AS INTEGER) AS r FROM $planets")
    assert "DECIMAL128" in phys, phys
    _assert_matches(got, [_quant(a / Decimal(i), 9) if i != 0 else None for a, i in zip(_GRAV3, _ID64)])


# ---------------------------------------------------------------------------
# Part 2 — c-native path: cross-kind DECIMAL(int64) × DECIMAL128.
# ---------------------------------------------------------------------------
def test_cnative_cross_kind_decimal_decimal128():
    for op, fn, sc in [("*", lambda a, b: a * b, 5), ("+", lambda a, b: a + b, 3), ("-", lambda a, b: a - b, 3)]:
        got, phys = _run(
            f"SELECT CAST(gravity AS DECIMAL(10,2)) {op} CAST(gravity AS DECIMAL(20,3)) AS r FROM $planets"
        )
        assert "DECIMAL128" in phys, phys
        _assert_matches(got, [_quant(fn(a, b), sc) for a, b in zip(_GRAV2, _GRAV3)])


# ---------------------------------------------------------------------------
# Negatives and nulls.
# ---------------------------------------------------------------------------
def test_nulls_decimal_int8():
    got, _ = _run("SELECT CAST(gravity AS DECIMAL(10,2)) * NULLIF(id, 3) AS r FROM $planets")
    _assert_matches(got, [_quant(a * Decimal(i), 2) if i != 3 else None for a, i in zip(_GRAV2, _ID8)])


def test_negatives_decimal128_int64():
    got, phys = _run(
        "SELECT (CAST(0 AS DECIMAL(20,3)) - CAST(gravity AS DECIMAL(20,3))) * CAST(id AS INTEGER) AS r "
        "FROM $planets"
    )
    assert "DECIMAL128" in phys, phys
    _assert_matches(got, [_quant((Decimal(0) - a) * Decimal(i), 3) for a, i in zip(_GRAV3, _ID64)])


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All decimal int-promotion tests passed.")
