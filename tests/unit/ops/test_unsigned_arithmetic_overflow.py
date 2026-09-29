"""Unsigned add / sub / mul overflow FAILS LOUD (ruling 2026-09-29, reversing E33's wrap).

`0u - 1u` used to be 2^64-1 and `UINT64 MAX + 1` used to be 0. Both are silent wrong
answers; they now raise, per batch, like the signed family. Divide / modulo by zero
raises too — see test_integer_divide_by_zero.py.
"""

import draken.draken_native as dn
import pytest

U64_MAX = 18446744073709551615


def _u8(v):
    return dn.vector_uint8_from_sequence(v)


def _u32(v):
    return dn.vector_uint32_from_sequence(v)


def _u64(v):
    return dn.vector_uint64_from_sequence(v)


def test_uint64_overflow_raises_vector_and_scalar():
    with pytest.raises(OverflowError, match="UINT64 addition overflow"):
        _u64([1, U64_MAX]).add(1)
    with pytest.raises(OverflowError, match="UINT64 addition overflow"):
        _u64([U64_MAX]).add(_u64([1]))
    with pytest.raises(OverflowError, match="UINT64 subtraction overflow"):
        _u64([3]).sub(5)
    with pytest.raises(OverflowError, match="UINT64 subtraction overflow"):
        _u64([0]).sub(_u64([1]))
    with pytest.raises(OverflowError, match="UINT64 multiplication overflow"):
        _u64([U64_MAX]).mul(2)


def test_uint64_results_that_fit_are_exact():
    assert _u64([3, 9, None]).add(1).to_pylist() == [4, 10, None]
    assert _u64([3, 9, None]).sub(2).to_pylist() == [1, 7, None]
    assert _u64([U64_MAX]).sub(1).to_pylist() == [U64_MAX - 1]
    assert _u64([U64_MAX]).mul(1).to_pylist() == [U64_MAX]
    assert _u64([U64_MAX]).add(0).to_pylist() == [U64_MAX]


def test_narrow_unsigned_subtraction_below_zero_raises():
    with pytest.raises(OverflowError, match="UINT16 subtraction overflow"):
        _u8([3, 200]).sub(_u8([5, 5]))          # 3 - 5 used to be 65534
    with pytest.raises(OverflowError, match="UINT16 subtraction overflow"):
        _u8([3, 200]).sub(5)
    with pytest.raises(OverflowError, match="UINT64 subtraction overflow"):
        _u32([3, 9]).sub(_u32([5, 5]))


def test_narrow_unsigned_results_that_fit_are_exact():
    # add / mul of narrow unsigned values fit the next width — no false overflow.
    assert _u8([3, 200, None]).add(_u8([3, 200, None])).to_pylist() == [6, 400, None]
    assert _u8([3, 200, None]).mul(_u8([3, 200, None])).to_pylist() == [9, 40000, None]
    assert _u32([3, 4000000000]).mul(_u32([3, 4000000000])).to_pylist() == [9, 16000000000000000000]
    assert _u32([3, 4000000000, None]).add(1).to_pylist() == [4, 4000000001, None]
    assert _u32([3, 9]).mul(2).to_pylist() == [6, 18]
    assert _u8([3, 9]).add(-1).to_pylist() == [2, 8]   # a negative scalar that still fits
    assert _u8([3, 200]).sub(_u8([1, 1])).to_pylist() == [2, 199]


def test_div_mod_by_zero_raises():
    with pytest.raises(ValueError, match="UINT64 division by zero"):
        _u64([3, 9, None]).div(0)
    with pytest.raises(ValueError, match="UINT64 modulo by zero"):
        _u64([3, 9, None]).mod(0)
    with pytest.raises(ValueError, match="UINT16 division by zero"):
        _u8([3, 9]).div(0)
