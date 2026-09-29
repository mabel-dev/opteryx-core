"""Narrow-width (INT8/16/32) vector x int64-scalar arithmetic.

The result is the next-wider type. The scalar used to be cast to that width first,
silently truncating any scalar outside its range (INT8 + 70000), and INT32 +/* a
large scalar wrapped in int64. Now the operation is exact in int64 and the result
must fit the result width, otherwise it fails loud (per batch).
"""

import draken.draken_native as dn
import pytest

MAX = 9223372036854775807


def _v8(vals):
    return dn.vector_int8_from_sequence(vals)


def _v32(vals):
    return dn.vector_int32_from_sequence(vals)


def test_int8_scalar_out_of_result_range_raises_not_truncates():
    with pytest.raises(OverflowError, match="INT16 addition overflow"):
        _v8([100, 5]).add(70000)  # 70000 truncated to int16 would give a wrong sum
    with pytest.raises(OverflowError, match="INT16 multiplication overflow"):
        _v8([100]).mul(1000)


def test_int8_scalar_results_that_fit_are_exact():
    assert _v8([100, 5, None]).mul(300).to_pylist() == [30000, 1500, None]
    assert _v8([100, None]).sub(-200).to_pylist() == [300, None]


def test_int32_scalar_overflows_int64_raises():
    with pytest.raises(OverflowError, match="INT64 addition overflow"):
        _v32([1, None, 3]).add(MAX)
    with pytest.raises(OverflowError, match="INT64 multiplication overflow"):
        _v32([2]).mul(MAX)


def test_int32_scalar_results_that_fit_are_exact():
    assert _v32([1, None, 3]).add(1).to_pylist() == [2, None, 4]
    assert _v32([-2147483648]).mul(2147483648).to_pylist() == [-(2**31) * 2**31]


def test_scalar_div_mod_do_not_truncate_the_scalar_and_zero_divisor_raises():
    assert _v8([100, None]).div(70000).to_pylist() == [0, None]
    assert _v8([100]).mod(70000).to_pylist() == [100]
    assert _v32([1, None, 3]).mod(-1).to_pylist() == [0, None, 0]
    with pytest.raises(ValueError, match="INT64 division by zero"):
        _v32([1, None, 3]).div(0)
