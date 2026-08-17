"""Regression tests for DECIMAL × INTEGER arithmetic and DECIMAL128 promotion.

Covers `DECIMAL(p,s) op INTEGER` end to end, entirely on the c-native path
(binop_dispatch.cpp draken_binop, allow-listed in _c_native_binop — there is no
Python closure fallback on the data-pipeline path; the hard-cutover posture
refuses the whole query at compile time for anything not c-native, see
engine_cutover_decisions):

  1. DECIMAL(int64) × ANY signed int width (INT8/16/32/64): draken_binop widens a
     narrow (INT8/16/32) operand to INT64 up front (widen_narrow_int_to_i64,
     sign-extending), then runs the SAME dec_*/dec128_* kernels the INT64 case
     always used — one native path per operation, not per width. Genuine
     DECIMAL128 promotion (result precision > 18, e.g. DECIMAL(10,2)*INT64) widens
     both operands to int128 (widen_i64_to_dec128) before the same kernels run.

  2. DECIMAL128 × INT64 (either order) and cross-kind DECIMAL × DECIMAL128
     (either order) widen the int64-backed operand to int128 via
     widen_i64_to_dec128 and run the dec128_* kernels.

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
# Part 1 — native path: DECIMAL(10,2) op INT8 (narrow, widened to INT64 by
# draken_binop before the dec_* kernel runs), both orders, result DECIMAL.
# ---------------------------------------------------------------------------
def test_cnative_decimal_int8_add_sub_mul_both_orders():
    for op, fn in [("*", lambda a, b: a * b), ("+", lambda a, b: a + b), ("-", lambda a, b: a - b)]:
        got, _ = _run(f"SELECT CAST(gravity AS DECIMAL(10,2)) {op} id AS r FROM $planets")
        _assert_matches(got, [_quant(fn(a, Decimal(i)), 2) for a, i in zip(_GRAV2, _ID8)])
        got, _ = _run(f"SELECT id {op} CAST(gravity AS DECIMAL(10,2)) AS r FROM $planets")
        _assert_matches(got, [_quant(fn(Decimal(i), a), 2) for a, i in zip(_GRAV2, _ID8)])


def test_cnative_decimal_int8_divide():
    # divide result scale = max(2+6, 6) = 8.
    got, _ = _run("SELECT CAST(gravity AS DECIMAL(10,2)) / id AS r FROM $planets")
    _assert_matches(got, [_quant(a / Decimal(i), 8) if i != 0 else None for a, i in zip(_GRAV2, _ID8)])


def test_cnative_decimal_int8_divide_by_zero():
    # (id - 1) is a genuine zero for Mercury (id=1) — dec_div must NULL that row
    # rather than raise (decimal_arith.h div-by-zero convention, revised
    # 2026-08-17 to match INT64/FLOAT64's non-raising division; see TPC-DS Q90).
    # Every other row must still compute normally.
    got, _ = _run("SELECT CAST(gravity AS DECIMAL(10,2)) / (id - 1) AS r FROM $planets")
    divisors = [i - 1 for i in _ID8]
    assert 0 in divisors, divisors  # sanity: the fixture actually exercises the zero row
    _assert_matches(got, [_quant(a / Decimal(d), 8) if d != 0 else None for a, d in zip(_GRAV2, divisors)])


# ---------------------------------------------------------------------------
# Part 1 — native path: DECIMAL(10,2) × INT64 promotes to DECIMAL128.
# ---------------------------------------------------------------------------
def test_cnative_decimal_int64_promotes_to_decimal128():
    got, phys = _run("SELECT CAST(gravity AS DECIMAL(10,2)) * CAST(id AS INTEGER) AS r FROM $planets")
    assert "DECIMAL128" in phys, phys
    _assert_matches(got, [_quant(a * Decimal(i), 2) for a, i in zip(_GRAV2, _ID64)])


# ---------------------------------------------------------------------------
# Part 2 — c-native path: DECIMAL128 × INT64 (either order).
# ---------------------------------------------------------------------------
def test_cnative_routes_decimal128_cases():
    # PLUS..MODULO (1..5): all four DECIMAL128/int64 combinations route c-native,
    # and so does every signed int width paired with DECIMAL, at either result tier
    # (narrow ints widen to INT64 inside draken_binop before the dec_*/dec128_*
    # kernel runs — one native path per operation, not per operand width).
    for op in (1, 2, 3, 4, 5):
        assert _c_native_binop(op, "DECIMAL128", "INT64", "DECIMAL128")
        assert _c_native_binop(op, "INT64", "DECIMAL128", "DECIMAL128")
        assert _c_native_binop(op, "DECIMAL", "DECIMAL128", "DECIMAL128")
        assert _c_native_binop(op, "DECIMAL128", "DECIMAL", "DECIMAL128")
        assert _c_native_binop(op, "DECIMAL", "INT64", "DECIMAL128")
        for width in ("INT8", "INT16", "INT32"):
            assert _c_native_binop(op, "DECIMAL", width, "DECIMAL")
            assert _c_native_binop(op, width, "DECIMAL", "DECIMAL")
            assert _c_native_binop(op, "DECIMAL", width, "DECIMAL128")
            assert _c_native_binop(op, "DECIMAL128", width, "DECIMAL128")


def test_cnative_decimal_narrow_int_promotes_to_decimal128():
    # DECIMAL(20,3) × INT8/16/32 whose result precision exceeds 18 promotes to
    # DECIMAL128, exactly like the INT64 case (test_cnative_decimal128_int64_*) —
    # the narrow int just widens one extra step (→INT64, then →INT128).
    for op, fn in [("*", lambda a, b: a * b), ("+", lambda a, b: a + b), ("-", lambda a, b: a - b)]:
        got, phys = _run(f"SELECT CAST(gravity AS DECIMAL(20,3)) {op} id AS r FROM $planets")
        assert "DECIMAL128" in phys, phys
        _assert_matches(got, [_quant(fn(a, Decimal(i)), 3) for a, i in zip(_GRAV3, _ID8)])
        got, phys = _run(f"SELECT id {op} CAST(gravity AS DECIMAL(20,3)) AS r FROM $planets")
        assert "DECIMAL128" in phys, phys
        _assert_matches(got, [_quant(fn(Decimal(i), a), 3) for a, i in zip(_GRAV3, _ID8)])


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


def test_cnative_decimal128_int64_divide_by_zero():
    # int128 tier of the same convention as test_cnative_decimal_int8_divide_by_zero
    # — this is the exact kernel (dec128_div) TPC-DS Q90 crashed in at SF0.01,
    # where an hourly bucket's row count genuinely denominates to zero.
    got, phys = _run("SELECT CAST(gravity AS DECIMAL(20,3)) / (CAST(id AS INTEGER) - 1) AS r FROM $planets")
    assert "DECIMAL128" in phys, phys
    divisors = [i - 1 for i in _ID64]
    assert 0 in divisors, divisors  # sanity: the fixture actually exercises the zero row
    _assert_matches(got, [_quant(a / Decimal(d), 9) if d != 0 else None for a, d in zip(_GRAV3, divisors)])


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
