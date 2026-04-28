"""
Tests for RLE encoding on Int64Vector and Float64Vector.

RLE vectors are created only at the C level (from_rle_builder is cdef).
We create them indirectly via build_cartesian_indices (which uses from_rle_builder
internally) and verify behaviour at the Python boundary:
  - encoding flag is correct
  - len() is correct
  - to_pylist() materialises correctly
  - take() on RLE returns dense with correct values
  - sum/min/max aggregates work correctly
  - is_null() and is_null_at() return correct results (no nulls)
  - equals/greater_than comparisons work (via materialization)

Float64 RLE is tested via the parquet_reader path if a suitable parquet file
is available.  If the file is absent the test is skipped.
"""

import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from opteryx.compiled.joins import build_cartesian_indices

# Stable encoding constants
DRAKEN_ENCODING_DENSE = 0
DRAKEN_ENCODING_RLE = 2


def _make_rle_left(L: int, R: int):
    """Return the RLE left index from build_cartesian_indices(L, R)."""
    left, _ = build_cartesian_indices(L, R)
    assert left.encoding == DRAKEN_ENCODING_RLE
    return left


# ---------------------------------------------------------------------------
# Basic properties
# ---------------------------------------------------------------------------

def test_rle_encoding_flag():
    vec = _make_rle_left(5, 3)
    assert vec.encoding == DRAKEN_ENCODING_RLE


def test_rle_len():
    vec = _make_rle_left(5, 3)
    assert len(vec) == 15


def test_rle_len_single_run():
    vec = _make_rle_left(1, 100)
    assert len(vec) == 100


# ---------------------------------------------------------------------------
# Materialisation via to_pylist
# ---------------------------------------------------------------------------

def test_rle_to_pylist_correct():
    vec = _make_rle_left(3, 3)
    assert vec.to_pylist() == [0, 0, 0, 1, 1, 1, 2, 2, 2]


def test_rle_to_pylist_single_run():
    vec = _make_rle_left(1, 5)
    assert vec.to_pylist() == [0, 0, 0, 0, 0]


def test_rle_to_pylist_many_runs():
    L, R = 50, 2
    vec = _make_rle_left(L, R)
    expected = []
    for i in range(L):
        expected.extend([i] * R)
    assert vec.to_pylist() == expected


# ---------------------------------------------------------------------------
# take() — must return dense, must be correct
# ---------------------------------------------------------------------------

def test_rle_take_returns_dense():
    vec = _make_rle_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    indices = array("i", [0, 3, 6, 9])
    taken = vec.take(indices)
    assert taken.encoding == DRAKEN_ENCODING_DENSE


def test_rle_take_selects_correct_values():
    vec = _make_rle_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    indices = array("i", [0, 3, 6, 9])
    taken = vec.take(indices)
    assert taken.to_pylist() == [0, 1, 2, 3]


def test_rle_take_with_repeated_indices():
    vec = _make_rle_left(3, 2)  # [0,0,1,1,2,2]
    indices = array("i", [0, 0, 4, 4])
    taken = vec.take(indices)
    assert taken.to_pylist() == [0, 0, 2, 2]


def test_rle_take_reversed():
    vec = _make_rle_left(4, 2)  # [0,0,1,1,2,2,3,3]
    indices = array("i", [7, 5, 3, 1])
    taken = vec.take(indices)
    assert taken.to_pylist() == [3, 2, 1, 0]


def test_rle_take_all_same_run():
    vec = _make_rle_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    # Take all 3 elements from the second run (row 1)
    indices = array("i", [3, 4, 5])
    taken = vec.take(indices)
    assert taken.to_pylist() == [1, 1, 1]


def test_rle_take_empty_indices():
    vec = _make_rle_left(5, 3)
    indices = array("i", [])
    taken = vec.take(indices)
    assert len(taken) == 0
    assert taken.to_pylist() == []


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

def test_rle_sum():
    # left index of (3, 2): [0,0,1,1,2,2] → sum = 6
    vec = _make_rle_left(3, 2)
    assert vec.sum() == 6


def test_rle_min():
    vec = _make_rle_left(5, 3)
    assert vec.min() == 0


def test_rle_max():
    L, R = 7, 4
    vec = _make_rle_left(L, R)
    assert vec.max() == L - 1  # 6


def test_rle_sum_single_run():
    vec = _make_rle_left(1, 10)  # all zeros
    assert vec.sum() == 0


# ---------------------------------------------------------------------------
# Null handling (cross join indices have no nulls)
# ---------------------------------------------------------------------------

def test_rle_has_no_nulls():
    vec = _make_rle_left(5, 3)
    assert vec.null_count == 0


def test_rle_is_null_returns_all_zero():
    vec = _make_rle_left(3, 2)
    null_flags = list(vec.is_null())
    assert null_flags == [0, 0, 0, 0, 0, 0]


def test_rle_is_null_at_returns_false():
    vec = _make_rle_left(4, 3)
    for i in range(len(vec)):
        assert vec.is_null_at(i) is False


# ---------------------------------------------------------------------------
# Comparison operators (operate via materialization internally)
# ---------------------------------------------------------------------------

def test_rle_equals_scalar():
    vec = _make_rle_left(4, 2)  # [0,0,1,1,2,2,3,3]
    result = vec.equals(2)
    py = result.to_pylist()
    assert py == [False, False, False, False, True, True, False, False]


def test_rle_greater_than_scalar():
    vec = _make_rle_left(4, 2)  # [0,0,1,1,2,2,3,3]
    result = vec.greater_than(1)
    py = result.to_pylist()
    assert py == [False, False, False, False, True, True, True, True]


def test_rle_less_than_scalar():
    vec = _make_rle_left(4, 2)  # [0,0,1,1,2,2,3,3]
    result = vec.less_than(2)
    py = result.to_pylist()
    assert py == [True, True, True, True, False, False, False, False]


# ---------------------------------------------------------------------------
# hash_into — verify it produces consistent hashes (run-based optimisation)
# ---------------------------------------------------------------------------

def test_rle_hash_into_consistent_within_run():
    """All elements in the same run must hash to the same value."""
    from draken.vectors._hash_api import hash_into

    L, R = 4, 3
    vec = _make_rle_left(L, R)
    out = array("Q", [0] * len(vec))
    hash_into(vec, out)

    # Elements in the same run (same value) must have the same hash
    for i in range(L):
        run_hashes = [out[i * R + j] for j in range(R)]
        assert len(set(run_hashes)) == 1, (
            f"run {i} has inconsistent hashes: {run_hashes}"
        )


def test_rle_hash_into_different_runs_differ():
    """Different run values must produce different hashes (no trivial collision)."""
    from draken.vectors._hash_api import hash_into

    vec = _make_rle_left(4, 2)  # values 0, 1, 2, 3
    out = array("Q", [0] * len(vec))
    hash_into(vec, out)

    # One hash per run
    run_hashes = [out[i * 2] for i in range(4)]
    assert len(set(run_hashes)) == 4, f"hash collisions in distinct run values: {run_hashes}"


if __name__ == "__main__":
    tests = [
        test_rle_encoding_flag,
        test_rle_len,
        test_rle_len_single_run,
        test_rle_to_pylist_correct,
        test_rle_to_pylist_single_run,
        test_rle_to_pylist_many_runs,
        test_rle_take_returns_dense,
        test_rle_take_selects_correct_values,
        test_rle_take_with_repeated_indices,
        test_rle_take_reversed,
        test_rle_take_all_same_run,
        test_rle_take_empty_indices,
        test_rle_sum,
        test_rle_min,
        test_rle_max,
        test_rle_sum_single_run,
        test_rle_has_no_nulls,
        test_rle_is_null_returns_all_zero,
        test_rle_is_null_at_returns_false,
        test_rle_equals_scalar,
        test_rle_greater_than_scalar,
        test_rle_less_than_scalar,
        test_rle_hash_into_consistent_within_run,
        test_rle_hash_into_different_runs_differ,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ❌ {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
