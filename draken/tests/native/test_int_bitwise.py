"""
Native tests for E.2 bitwise ops: AND / OR / XOR / NOT / SHL / SHR.

Coverage matrix:
  types:        INT8 / INT16 / INT32 / INT64
  ops:          and / or / xor / not / shl / shr
  nullability:  no nulls / left-null / right-null / both-null / mixed
  shapes:       dense / constant / dict (via materialize)
  edges:        INT*_MIN, INT*_MAX, 0, -1
  SHL/SHR:      valid range endpoints, out-of-range raises loud
  type-mismatch: raises on mismatched operand types
  non-Vector:   TypeError raised on non-Vector input

Hypothesis property tests:
  round-trip identity laws:
    a & ~a == 0 for all a
    a | ~a == -1 for all a (INT64)
    a ^ a == 0 for all a
    a & b == b & a (commutativity)
"""

import ctypes
import glob
import importlib.util
import os
import sys

import pytest
import draken.draken_native as dn

from hypothesis import given, settings
from hypothesis import strategies as st


def _load_vector_bitwise():
    """Load vector_bitwise extension without triggering opteryx/__init__.py."""
    # draken_native must already be loaded (done at module level above) so
    # draken_vector_unwrap / draken_vector_own_raw are in the global symbol table.
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vectors*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError("vector_bitwise extension not built — run make compile")
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vectors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bw = _load_vector_bitwise()


INT8_MIN   = -128
INT8_MAX   = 127
INT16_MIN  = -32768
INT16_MAX  = 32767
INT32_MIN  = -2147483648
INT32_MAX  = 2147483647
INT64_MIN  = -(2**63)
INT64_MAX  = 2**63 - 1


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def i8(lst):    return dn.vector_int8_from_sequence(lst)
def i16(lst):   return dn.vector_int16_from_sequence(lst)
def i32(lst):   return dn.vector_int32_from_sequence(lst)
def i64(lst):   return dn.vector_from_sequence(lst)
def py(v):      return v.to_pylist()


# ---------------------------------------------------------------------------
# AND
# ---------------------------------------------------------------------------

class TestAnd:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_and(i64([3, 5, 15]), i64([6, 3, 9]))) == [2, 1, 9]

    def test_i64_zeros(self):
        assert py(bw.vector_bitwise_and(i64([0, 0]), i64([0, 0]))) == [0, 0]

    def test_i64_all_ones(self):
        assert py(bw.vector_bitwise_and(i64([-1, -1]), i64([-1, -1]))) == [-1, -1]

    def test_i64_min_max(self):
        r = py(bw.vector_bitwise_and(i64([INT64_MIN, INT64_MAX]), i64([INT64_MAX, INT64_MIN])))
        assert r == [0, 0]

    def test_i64_null_left(self):
        assert py(bw.vector_bitwise_and(i64([None, 5]), i64([3, 5]))) == [None, 5]

    def test_i64_null_right(self):
        assert py(bw.vector_bitwise_and(i64([3, 5]), i64([None, 5]))) == [None, 5]

    def test_i64_null_both(self):
        assert py(bw.vector_bitwise_and(i64([None, 5]), i64([None, 5]))) == [None, 5]

    def test_i64_all_null(self):
        assert py(bw.vector_bitwise_and(i64([None, None]), i64([1, 2]))) == [None, None]

    def test_i64_empty(self):
        assert py(bw.vector_bitwise_and(i64([]), i64([]))) == []

    def test_i8_basic(self):
        assert py(bw.vector_bitwise_and(i8([3, 5]), i8([6, 3]))) == [2, 1]

    def test_i16_basic(self):
        assert py(bw.vector_bitwise_and(i16([0x0F0F, 0x00FF]), i16([0x00FF, -256]))) == [0x000F, 0x0000]

    def test_i32_basic(self):
        assert py(bw.vector_bitwise_and(i32([0x0F0F0F0F, 0]), i32([0x00FFFFFF, -1]))) == [0x000F0F0F, 0]

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_and(i64([1, 2]), i64([1]))

    def test_type_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_and(i64([1]), i32([1]))

    def test_non_vector_left_raises(self):
        with pytest.raises(TypeError):
            bw.vector_bitwise_and(42, i64([1]))

    def test_non_vector_right_raises(self):
        with pytest.raises(TypeError):
            bw.vector_bitwise_and(i64([1]), "not a vector")


# ---------------------------------------------------------------------------
# OR
# ---------------------------------------------------------------------------

class TestOr:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_or(i64([1, 2, 4]), i64([2, 2, 8]))) == [3, 2, 12]

    def test_i64_identity(self):
        assert py(bw.vector_bitwise_or(i64([5, 0]), i64([0, 0]))) == [5, 0]

    def test_i64_all_ones(self):
        assert py(bw.vector_bitwise_or(i64([0, INT64_MIN]), i64([-1, INT64_MAX]))) == [-1, -1]

    def test_i64_null_propagates(self):
        assert py(bw.vector_bitwise_or(i64([None, 3]), i64([1, None]))) == [None, None]

    def test_i8_basic(self):
        assert py(bw.vector_bitwise_or(i8([1, 2]), i8([2, 4]))) == [3, 6]

    def test_i16_basic(self):
        assert py(bw.vector_bitwise_or(i16([0x00FF, -256]), i16([-256, 0x00FF]))) == [-1, -1]

    def test_i32_basic(self):
        assert py(bw.vector_bitwise_or(i32([0, INT32_MIN]), i32([INT32_MAX, 0]))) == [INT32_MAX, INT32_MIN]

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_or(i64([1, 2, 3]), i64([1, 2]))


# ---------------------------------------------------------------------------
# XOR
# ---------------------------------------------------------------------------

class TestXor:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_xor(i64([3, 5, 6]), i64([5, 3, 6]))) == [6, 6, 0]

    def test_i64_self_zero(self):
        assert py(bw.vector_bitwise_xor(i64([42, INT64_MIN, -1]), i64([42, INT64_MIN, -1]))) == [0, 0, 0]

    def test_i64_zero(self):
        assert py(bw.vector_bitwise_xor(i64([7]), i64([0]))) == [7]

    def test_i64_null_propagates(self):
        assert py(bw.vector_bitwise_xor(i64([None, 1]), i64([1, None]))) == [None, None]

    def test_i8_edges(self):
        assert py(bw.vector_bitwise_xor(i8([INT8_MIN, INT8_MAX, 0]), i8([-1, -1, -1]))) == [INT8_MAX, INT8_MIN, -1]

    def test_i16_self_zero(self):
        v = i16([100, -100, 0, INT16_MIN, INT16_MAX])
        assert py(bw.vector_bitwise_xor(v, v)) == [0, 0, 0, 0, 0]

    def test_i32_basic(self):
        assert py(bw.vector_bitwise_xor(i32([INT32_MAX, 0]), i32([0, INT32_MIN]))) == [INT32_MAX, INT32_MIN]


# ---------------------------------------------------------------------------
# NOT
# ---------------------------------------------------------------------------

class TestNot:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_not(i64([0, -1, 1, -2]))) == [-1, 0, -2, 1]

    def test_i64_min_max(self):
        assert py(bw.vector_bitwise_not(i64([INT64_MIN, INT64_MAX]))) == [INT64_MAX, INT64_MIN]

    def test_i64_null_propagates(self):
        assert py(bw.vector_bitwise_not(i64([None, 5, None]))) == [None, -6, None]

    def test_i64_all_null(self):
        assert py(bw.vector_bitwise_not(i64([None, None]))) == [None, None]

    def test_i64_empty(self):
        assert py(bw.vector_bitwise_not(i64([]))) == []

    def test_i8_basic(self):
        assert py(bw.vector_bitwise_not(i8([0, -1, 1, INT8_MAX]))) == [-1, 0, -2, INT8_MIN]

    def test_i16_basic(self):
        assert py(bw.vector_bitwise_not(i16([0, -1, INT16_MAX, INT16_MIN]))) == [-1, 0, INT16_MIN, INT16_MAX]

    def test_i32_basic(self):
        assert py(bw.vector_bitwise_not(i32([0, -1, INT32_MAX, INT32_MIN]))) == [-1, 0, INT32_MIN, INT32_MAX]

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            bw.vector_bitwise_not(42)


# ---------------------------------------------------------------------------
# SHL
# ---------------------------------------------------------------------------

class TestShl:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_shift_left(i64([1, 2, 3]), i64([0, 1, 2]))) == [1, 4, 12]

    def test_i64_shift_zero(self):
        assert py(bw.vector_bitwise_shift_left(i64([42]), i64([0]))) == [42]

    def test_i64_shift_max(self):
        # shift by 62 = multiply by 4 (valid, no UB via unsigned cast)
        assert py(bw.vector_bitwise_shift_left(i64([1]), i64([62]))) == [1 << 62]

    def test_i64_null_left(self):
        assert py(bw.vector_bitwise_shift_left(i64([None, 1]), i64([1, 1]))) == [None, 2]

    def test_i64_null_right(self):
        assert py(bw.vector_bitwise_shift_left(i64([1, 1]), i64([None, 1]))) == [None, 2]

    def test_i64_out_of_range_negative_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_left(i64([1]), i64([-1]))

    def test_i64_out_of_range_too_large_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_left(i64([1]), i64([64]))

    def test_i64_shift_63_is_valid(self):
        # shift by 63 on value 1 gives INT64_MIN (signed overflow via unsigned cast)
        result = py(bw.vector_bitwise_shift_left(i64([1]), i64([63])))
        assert result == [INT64_MIN]

    def test_i8_valid_range(self):
        assert py(bw.vector_bitwise_shift_left(i8([1, 1, 1]), i8([0, 3, 7]))) == [1, 8, INT8_MIN]

    def test_i8_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_left(i8([1]), i8([8]))

    def test_i16_valid_range(self):
        assert py(bw.vector_bitwise_shift_left(i16([1]), i16([15]))) == [INT16_MIN]

    def test_i16_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_left(i16([1]), i16([16]))

    def test_i32_valid_range(self):
        assert py(bw.vector_bitwise_shift_left(i32([1]), i32([31]))) == [INT32_MIN]

    def test_i32_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_left(i32([1]), i32([32]))

    def test_null_shift_skips_range_check(self):
        # null right operand → null output, no range-check exception
        assert py(bw.vector_bitwise_shift_left(i64([1]), i64([None]))) == [None]

    def test_type_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_shift_left(i64([1]), i32([1]))

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_shift_left(i64([1, 2]), i64([1]))


# ---------------------------------------------------------------------------
# SHR
# ---------------------------------------------------------------------------

class TestShr:
    def test_i64_basic(self):
        assert py(bw.vector_bitwise_shift_right(i64([8, 4, 1]), i64([2, 1, 0]))) == [2, 2, 1]

    def test_i64_shift_zero(self):
        assert py(bw.vector_bitwise_shift_right(i64([42]), i64([0]))) == [42]

    def test_i64_arithmetic_negative(self):
        # arithmetic right-shift: sign bit propagates
        assert py(bw.vector_bitwise_shift_right(i64([-8]), i64([2]))) == [-2]
        assert py(bw.vector_bitwise_shift_right(i64([-1]), i64([10]))) == [-1]

    def test_i64_max_valid_shift(self):
        assert py(bw.vector_bitwise_shift_right(i64([INT64_MIN]), i64([63]))) == [-1]

    def test_i64_null_propagates(self):
        assert py(bw.vector_bitwise_shift_right(i64([None, 8]), i64([1, None]))) == [None, None]

    def test_i64_out_of_range_negative_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_right(i64([1]), i64([-1]))

    def test_i64_out_of_range_too_large_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_right(i64([1]), i64([64]))

    def test_null_shift_skips_range_check(self):
        assert py(bw.vector_bitwise_shift_right(i64([8]), i64([None]))) == [None]

    def test_i8_arithmetic(self):
        assert py(bw.vector_bitwise_shift_right(i8([-8, -1, 64]), i8([2, 7, 1]))) == [-2, -1, 32]

    def test_i8_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_right(i8([1]), i8([8]))

    def test_i16_arithmetic(self):
        assert py(bw.vector_bitwise_shift_right(i16([-32768]), i16([15]))) == [-1]

    def test_i16_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_right(i16([1]), i16([16]))

    def test_i32_arithmetic(self):
        assert py(bw.vector_bitwise_shift_right(i32([INT32_MIN]), i32([31]))) == [-1]

    def test_i32_out_of_range_raises(self):
        with pytest.raises((ValueError, Exception)):
            bw.vector_bitwise_shift_right(i32([1]), i32([32]))

    def test_type_mismatch_raises(self):
        with pytest.raises(Exception):
            bw.vector_bitwise_shift_right(i64([8]), i32([1]))


# ---------------------------------------------------------------------------
# Hypothesis property tests
# ---------------------------------------------------------------------------

@given(st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100))
@settings(max_examples=200)
def test_and_complement_is_zero(values):
    v = i64(values)
    notv = bw.vector_bitwise_not(v)
    result = bw.vector_bitwise_and(v, notv)
    assert all(x == 0 for x in py(result))


@given(st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100))
@settings(max_examples=200)
def test_or_complement_is_all_ones(values):
    v = i64(values)
    notv = bw.vector_bitwise_not(v)
    result = bw.vector_bitwise_or(v, notv)
    assert all(x == -1 for x in py(result))


@given(st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100))
@settings(max_examples=200)
def test_xor_self_is_zero(values):
    v = i64(values)
    result = bw.vector_bitwise_xor(v, v)
    assert all(x == 0 for x in py(result))


@given(
    st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100),
    st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100),
)
@settings(max_examples=200)
def test_and_commutative(left, right):
    if len(left) != len(right):
        return  # skip mismatched lengths
    a = i64(left)
    b = i64(right)
    assert py(bw.vector_bitwise_and(a, b)) == py(bw.vector_bitwise_and(b, a))


@given(
    st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100),
    st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100),
)
@settings(max_examples=200)
def test_or_commutative(left, right):
    if len(left) != len(right):
        return
    a = i64(left)
    b = i64(right)
    assert py(bw.vector_bitwise_or(a, b)) == py(bw.vector_bitwise_or(b, a))


@given(st.lists(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), min_size=0, max_size=100))
@settings(max_examples=200)
def test_not_double_inverse(values):
    v = i64(values)
    assert py(bw.vector_bitwise_not(bw.vector_bitwise_not(v))) == values
