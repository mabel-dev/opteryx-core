"""
Native + parity tests for E.3: ABS / SIGN / SQRT / ROUND via vector_math consumer.

Loads the nanobind extension without triggering opteryx/__init__.py, following
the same spec_from_file_location pattern as test_bitwise_parity.py (E.2).

Coverage:
  types:    INT8 / INT16 / INT32 / INT64 / FLOAT32 / FLOAT64
  ops:      abs / sign / sqrt / round / round_digits
  null TVL: no-null, partial-null, all-null
  edges:
    ABS:   INT*_MIN wrap, -0.0, NaN passthrough, ±Inf
    SIGN:  0, -0.0→0, NaN→null, ±Inf, large vectors
    SQRT:  negative int raises, null skips negative check, float negative→NaN
    ROUND: 0.5→0, 1.5→2, 2.5→2 (half-to-even), int identity, digits ±
  TypeError on non-Vector input
  ValueError/invalid_argument on sqrt(negative int)

Hypothesis property tests:
  abs: abs(x) >= 0 for finite non-NaN x
  abs: idempotent abs(abs(x)) == abs(x)
  round: round(n) == n for integer-valued floats in safe range
"""

import glob
import importlib.util
import math
import os

import draken.draken_native as dn
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Load vector_math extension
# ---------------------------------------------------------------------------

def _load_vector_math():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_math*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError("vector_math extension not built — run make compile")
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_math", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vm = _load_vector_math()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def i8(lst):   return dn.vector_int8_from_sequence(lst)
def i16(lst):  return dn.vector_int16_from_sequence(lst)
def i32(lst):  return dn.vector_int32_from_sequence(lst)
def i64(lst):  return dn.vector_from_sequence(lst)
def f32(lst):  return dn.vector_float32_from_sequence(lst)
def f64(lst):  return dn.vector_float64_from_sequence(lst)
def py(v):     return v.to_pylist()


INT8_MIN   = -128
INT8_MAX   = 127
INT16_MIN  = -32768
INT16_MAX  = 32767
INT32_MIN  = -2147483648
INT32_MAX  = 2147483647
INT64_MIN  = -(2**63)
INT64_MAX  = 2**63 - 1


# ===========================================================================
# ABS
# ===========================================================================

def test_abs_int64_positive():
    assert py(vm.vector_abs(i64([1, 2, 100]))) == [1, 2, 100]

def test_abs_int64_negative():
    assert py(vm.vector_abs(i64([-1, -2, -100]))) == [1, 2, 100]

def test_abs_int64_mixed():
    assert py(vm.vector_abs(i64([-3, 0, 3]))) == [3, 0, 3]

def test_abs_int64_min_wraps():
    # INT64_MIN wraps to INT64_MIN (C two's-complement convention — documented)
    assert py(vm.vector_abs(i64([INT64_MIN]))) == [INT64_MIN]

def test_abs_int8_min_wraps():
    assert py(vm.vector_abs(i8([INT8_MIN]))) == [INT8_MIN]

def test_abs_int32_min_wraps():
    assert py(vm.vector_abs(i32([INT32_MIN]))) == [INT32_MIN]

def test_abs_null_propagation():
    assert py(vm.vector_abs(i64([None, -5, None]))) == [None, 5, None]

def test_abs_all_null():
    assert py(vm.vector_abs(i64([None, None]))) == [None, None]

def test_abs_float64_basic():
    assert py(vm.vector_abs(f64([-1.5, 0.0, 2.5]))) == [1.5, 0.0, 2.5]

def test_abs_float64_neg_zero():
    assert py(vm.vector_abs(f64([-0.0]))) == [0.0]

def test_abs_float64_nan_passthrough():
    result = py(vm.vector_abs(f64([float('nan')])))
    assert math.isnan(result[0])

def test_abs_float64_inf():
    assert py(vm.vector_abs(f64([float('-inf'), float('inf')]))) == [float('inf'), float('inf')]

def test_abs_float32_basic():
    assert py(vm.vector_abs(f32([-1.5, 2.5]))) == [1.5, 2.5]

def test_abs_int8_range():
    assert py(vm.vector_abs(i8([-1, 0, 1, INT8_MAX]))) == [1, 0, 1, INT8_MAX]

def test_abs_int16_range():
    assert py(vm.vector_abs(i16([-1, 0, 1, INT16_MAX]))) == [1, 0, 1, INT16_MAX]

def test_abs_int32_range():
    assert py(vm.vector_abs(i32([-1, 0, 1, INT32_MAX]))) == [1, 0, 1, INT32_MAX]

def test_abs_non_vector_raises():
    with pytest.raises(TypeError):
        vm.vector_abs(42)

def test_abs_large():
    n = 10_000
    vals = [i - n // 2 for i in range(n)]
    assert py(vm.vector_abs(i64(vals))) == [abs(v) for v in vals]

@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1, max_size=500))
@settings(max_examples=200)
def test_abs_float64_nonneg_hypothesis(vals):
    assert all(r >= 0.0 for r in py(vm.vector_abs(f64(vals))))

@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1, max_size=200))
@settings(max_examples=100)
def test_abs_idempotent_hypothesis(vals):
    r1 = py(vm.vector_abs(f64(vals)))
    r2 = py(vm.vector_abs(f64(r1)))
    assert r1 == r2


# ===========================================================================
# SIGN
# ===========================================================================

def test_sign_int64_positive():
    assert py(vm.vector_sign(i64([1, 100, INT64_MAX]))) == [1, 1, 1]

def test_sign_int64_negative():
    assert py(vm.vector_sign(i64([-1, -100, INT64_MIN]))) == [-1, -1, -1]

def test_sign_int64_zero():
    assert py(vm.vector_sign(i64([0]))) == [0]

def test_sign_int64_mixed():
    assert py(vm.vector_sign(i64([-3, 0, 7]))) == [-1, 0, 1]

def test_sign_null_propagation():
    assert py(vm.vector_sign(i64([None, -5, None]))) == [None, -1, None]

def test_sign_float64_basic():
    assert py(vm.vector_sign(f64([-2.5, 0.0, 3.7]))) == [-1, 0, 1]

def test_sign_float64_neg_zero():
    # -0.0 is not < 0 and not > 0 → sign 0
    assert py(vm.vector_sign(f64([-0.0]))) == [0]

def test_sign_float64_nan_becomes_null():
    result = py(vm.vector_sign(f64([1.0, float('nan'), -1.0])))
    assert result == [1, None, -1]

def test_sign_float64_nan_only():
    assert py(vm.vector_sign(f64([float('nan')]))) == [None]

def test_sign_float64_nan_mixed_with_null():
    result = py(vm.vector_sign(f64([None, float('nan'), 1.0])))
    assert result == [None, None, 1]

def test_sign_float64_inf():
    assert py(vm.vector_sign(f64([float('-inf'), float('inf')]))) == [-1, 1]

def test_sign_float32_basic():
    assert py(vm.vector_sign(f32([-1.5, 0.0, 2.0]))) == [-1, 0, 1]

def test_sign_int8_range():
    assert py(vm.vector_sign(i8([-1, 0, 1]))) == [-1, 0, 1]

def test_sign_non_vector_raises():
    with pytest.raises(TypeError):
        vm.vector_sign("hello")

def test_sign_large():
    n = 10_000
    vals = [i - n // 2 for i in range(n)]
    result = py(vm.vector_sign(i64(vals)))
    expected = [1 if v > 0 else (-1 if v < 0 else 0) for v in vals]
    assert result == expected


# ===========================================================================
# SQRT
# ===========================================================================

def test_sqrt_int64_perfect_squares():
    assert py(vm.vector_sqrt(i64([0, 1, 4, 9, 16, 25, 100]))) == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0]

def test_sqrt_int64_negative_raises():
    with pytest.raises(Exception):
        vm.vector_sqrt(i64([-1]))

def test_sqrt_int64_negative_mid_vector_raises():
    with pytest.raises(Exception):
        vm.vector_sqrt(i64([4, -1, 9]))

def test_sqrt_null_skips_negative_check():
    result = py(vm.vector_sqrt(i64([None, 4, None])))
    assert result[0] is None
    assert result[1] == 2.0
    assert result[2] is None

def test_sqrt_float64_basic():
    result = py(vm.vector_sqrt(f64([0.0, 1.0, 4.0, 2.25])))
    assert result == [0.0, 1.0, 2.0, 1.5]

def test_sqrt_float64_negative_is_nan():
    result = py(vm.vector_sqrt(f64([-1.0])))
    assert math.isnan(result[0])

def test_sqrt_float64_nan_passthrough():
    result = py(vm.vector_sqrt(f64([float('nan')])))
    assert math.isnan(result[0])

def test_sqrt_float32_basic():
    result = py(vm.vector_sqrt(f32([4.0, 9.0])))
    assert result == [2.0, 3.0]

def test_sqrt_non_vector_raises():
    with pytest.raises(TypeError):
        vm.vector_sqrt(None)

def test_sqrt_large():
    n = 1_000
    vals = [i * i for i in range(n)]
    result = py(vm.vector_sqrt(i64(vals)))
    assert result == [float(i) for i in range(n)]


# ===========================================================================
# ROUND — half-to-even critical assertions
# ===========================================================================

def test_round_half_to_even_0_5():
    assert py(vm.vector_round(f64([0.5]))) == [0.0]

def test_round_half_to_even_1_5():
    assert py(vm.vector_round(f64([1.5]))) == [2.0]

def test_round_half_to_even_2_5():
    assert py(vm.vector_round(f64([2.5]))) == [2.0]

def test_round_half_to_even_neg_1_5():
    assert py(vm.vector_round(f64([-1.5]))) == [-2.0]

def test_round_half_to_even_neg_2_5():
    assert py(vm.vector_round(f64([-2.5]))) == [-2.0]

def test_round_half_to_even_3_5():
    assert py(vm.vector_round(f64([3.5]))) == [4.0]

def test_round_half_to_even_4_5():
    assert py(vm.vector_round(f64([4.5]))) == [4.0]

def test_round_normal():
    assert py(vm.vector_round(f64([1.4, 1.6, -1.4, -1.6]))) == [1.0, 2.0, -1.0, -2.0]

def test_round_int64_identity():
    vals = [1, 2, 100, -50, 0]
    assert py(vm.vector_round(i64(vals))) == vals

def test_round_int8_identity():
    assert py(vm.vector_round(i8([-1, 0, 1]))) == [-1, 0, 1]

def test_round_null_propagation():
    result = py(vm.vector_round(f64([None, 0.5, None])))
    assert result == [None, 0.0, None]

def test_round_nan_passthrough():
    result = py(vm.vector_round(f64([float('nan')])))
    assert math.isnan(result[0])

def test_round_inf_passthrough():
    result = py(vm.vector_round(f64([float('inf'), float('-inf')])))
    assert math.isinf(result[0]) and result[0] > 0
    assert math.isinf(result[1]) and result[1] < 0

def test_round_non_vector_raises():
    with pytest.raises(TypeError):
        vm.vector_round([1.5, 2.5])

def test_round_digits_zero_same_as_round():
    assert py(vm.vector_round_digits(f64([0.5, 1.5, 2.5]), 0)) == [0.0, 2.0, 2.0]

def test_round_digits_negative():
    result = py(vm.vector_round_digits(f64([15.0, 25.0, 35.0]), -1))
    assert result[0] == 20.0
    assert result[1] == 20.0  # 25 → 20 (half-to-even: 2 is even)
    assert result[2] == 40.0

def test_round_large():
    # Every i * 0.5: even half-steps (0,1,2,...) are integers → round to themselves;
    # odd half-steps (0.5, 1.5, ...) obey half-to-even.
    n = 1_000
    vals = [i * 0.5 for i in range(n)]
    result = py(vm.vector_round(f64(vals)))
    for i, r in enumerate(result):
        if i % 2 == 0:
            # integer-valued: identity
            assert r == float(i // 2), f"i={i}: expected {i//2}, got {r}"
        else:
            # x.5 case: round to nearest even integer
            lo = i // 2
            hi = lo + 1
            expected = lo if (lo % 2 == 0) else hi
            assert r == float(expected), f"i={i}: expected {expected}, got {r}"

@given(st.lists(
    st.integers(min_value=-(2**51), max_value=2**51),
    min_size=1, max_size=200))
@settings(max_examples=100)
def test_round_integer_float_identity(vals):
    result = py(vm.vector_round(f64([float(v) for v in vals])))
    assert result == [float(v) for v in vals]
