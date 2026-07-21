"""
Tests for compressed (dict-shaped) cartesian-index vectors.

RLE as a distinct encoding tag no longer exists (see CLAUDE.md's Vector Model:
"RLE does not exist past the scan boundary" — it is expanded into one of
Dense/Constant/Dict before reaching a Draken vector). build_cartesian_indices's
left index is DICT-shaped (is_dict=True), which is what used to be labeled
DRAKEN_ENCODING_RLE and read via a `.encoding` attribute that doesn't exist on
this Vector class either. This file verifies behaviour at the Python boundary:
  - is_dict is True for the compressed left index
  - len() is correct
  - to_pylist() materialises correctly
  - take() on a dict-shaped vector returns dense with correct values
  - sum/min/max aggregates work correctly
  - is_null()/is_null_at() return correct results (no nulls)
  - equals/greater_than/less_than comparisons work (rebuilt via
    vector_from_sequence + compare_scalar, since this Vector class has no
    scalar comparison methods — only equals_vector/greater_than_vector/etc.)
  - hash() produces one hash per row, consistent within a run

This is a DIFFERENT Vector class (draken.vectors.vector.Vector, a Cython
shim) from draken.draken_native.Vector used elsewhere in this test suite —
build_cartesian_indices is real production code (opteryx.operators._operators),
not test-only construction.
"""

import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from opteryx.operators._operators import build_cartesian_indices


def _make_dict_left(L: int, R: int):
    """Return the dict-shaped left index from build_cartesian_indices(L, R)."""
    left, _ = build_cartesian_indices(L, R)
    assert left.is_dict is True
    return left


def _make_compressed_left(L: int, R: int):
    """Return the compressed left index from build_cartesian_indices(L, R),
    without assuming a specific shape — L=1 (a single unique value) is
    CONSTANT-shaped, not DICT (see CLAUDE.md's Vector Model: data_length==1
    is the Constant shape), while L>1 is DICT-shaped."""
    left, _ = build_cartesian_indices(L, R)
    assert left.is_dict or left.is_constant
    return left


def _as_draken_native(vec):
    """Rebuild as a draken.draken_native.Vector (which has compare_scalar)."""
    return vector_from_sequence(vec.to_pylist(), dtype=DrakenType.INT64)


# ---------------------------------------------------------------------------
# Basic properties
# ---------------------------------------------------------------------------

def test_dict_shape_flag():
    vec = _make_dict_left(5, 3)
    assert vec.is_dict is True
    assert vec.is_dense is False
    assert vec.is_constant is False


def test_dict_len():
    vec = _make_dict_left(5, 3)
    assert len(vec) == 15


def test_dict_len_single_run():
    vec = _make_compressed_left(1, 100)
    assert len(vec) == 100


# ---------------------------------------------------------------------------
# Materialisation via to_pylist
# ---------------------------------------------------------------------------

def test_dict_to_pylist_correct():
    vec = _make_dict_left(3, 3)
    assert vec.to_pylist() == [0, 0, 0, 1, 1, 1, 2, 2, 2]


def test_dict_to_pylist_single_run():
    vec = _make_compressed_left(1, 5)
    assert vec.to_pylist() == [0, 0, 0, 0, 0]


def test_dict_to_pylist_many_runs():
    L, R = 50, 2
    vec = _make_dict_left(L, R)
    expected = []
    for i in range(L):
        expected.extend([i] * R)
    assert vec.to_pylist() == expected


# ---------------------------------------------------------------------------
# take() — must return dense, must be correct
# ---------------------------------------------------------------------------

def test_dict_take_returns_dense():
    vec = _make_dict_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    indices = array("i", [0, 3, 6, 9])
    taken = vec.take(indices)
    assert taken.is_dense is True


def test_dict_take_selects_correct_values():
    vec = _make_dict_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    indices = array("i", [0, 3, 6, 9])
    taken = vec.take(indices)
    assert taken.to_pylist() == [0, 1, 2, 3]


def test_dict_take_with_repeated_indices():
    vec = _make_dict_left(3, 2)  # [0,0,1,1,2,2]
    indices = array("i", [0, 0, 4, 4])
    taken = vec.take(indices)
    assert taken.to_pylist() == [0, 0, 2, 2]


def test_dict_take_reversed():
    vec = _make_dict_left(4, 2)  # [0,0,1,1,2,2,3,3]
    indices = array("i", [7, 5, 3, 1])
    taken = vec.take(indices)
    assert taken.to_pylist() == [3, 2, 1, 0]


def test_dict_take_all_same_run():
    vec = _make_dict_left(4, 3)  # [0,0,0,1,1,1,2,2,2,3,3,3]
    # Take all 3 elements from the second run (row 1)
    indices = array("i", [3, 4, 5])
    taken = vec.take(indices)
    assert taken.to_pylist() == [1, 1, 1]


def test_dict_take_empty_indices():
    vec = _make_dict_left(5, 3)
    indices = array("i", [])
    taken = vec.take(indices)
    assert len(taken) == 0
    assert taken.to_pylist() == []


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

def test_dict_sum():
    # left index of (3, 2): [0,0,1,1,2,2] → sum = 6
    vec = _make_dict_left(3, 2)
    assert vec.sum() == 6


def test_dict_min():
    vec = _make_dict_left(5, 3)
    assert vec.min() == 0


def test_dict_max():
    L, R = 7, 4
    vec = _make_dict_left(L, R)
    assert vec.max() == L - 1  # 6


def test_dict_sum_single_run():
    vec = _make_compressed_left(1, 10)  # all zeros
    assert vec.sum() == 0


# ---------------------------------------------------------------------------
# Null handling (cross join indices have no nulls)
# ---------------------------------------------------------------------------

def test_dict_has_no_nulls():
    vec = _make_dict_left(5, 3)
    assert sum(vec.is_null()) == 0


def test_dict_is_null_returns_all_zero():
    vec = _make_dict_left(3, 2)
    null_flags = list(vec.is_null())
    assert null_flags == [0, 0, 0, 0, 0, 0]


def test_dict_is_null_at_returns_false():
    vec = _make_dict_left(4, 3)
    for i in range(len(vec)):
        assert vec.is_null_at(i) is False


# ---------------------------------------------------------------------------
# Comparison operators — rebuilt via draken.draken_native.Vector.compare_scalar
# (this Vector class has no scalar comparison methods of its own).
# ---------------------------------------------------------------------------

def test_dict_equals_scalar():
    vec = _as_draken_native(_make_dict_left(4, 2))  # [0,0,1,1,2,2,3,3]
    result = vec.compare_scalar(2, 0)  # op 0 = eq
    assert result.to_pylist() == [False, False, False, False, True, True, False, False]


def test_dict_greater_than_scalar():
    vec = _as_draken_native(_make_dict_left(4, 2))  # [0,0,1,1,2,2,3,3]
    result = vec.compare_scalar(1, 2)  # op 2 = gt
    assert result.to_pylist() == [False, False, False, False, True, True, True, True]


def test_dict_less_than_scalar():
    vec = _as_draken_native(_make_dict_left(4, 2))  # [0,0,1,1,2,2,3,3]
    result = vec.compare_scalar(2, 4)  # op 4 = lt
    assert result.to_pylist() == [True, True, True, True, False, False, False, False]


# ---------------------------------------------------------------------------
# hash() — verify it produces consistent hashes (run-based optimisation)
# ---------------------------------------------------------------------------

def test_hash_consistent_within_run():
    """All elements in the same run must hash to the same value."""
    L, R = 4, 3
    vec = _make_dict_left(L, R)
    out = vec.hash()

    # Elements in the same run (same value) must have the same hash
    for i in range(L):
        run_hashes = [out[i * R + j] for j in range(R)]
        assert len(set(run_hashes)) == 1, (
            f"run {i} has inconsistent hashes: {run_hashes}"
        )


def test_hash_different_runs_differ():
    """Different run values must produce different hashes (no trivial collision)."""
    vec = _make_dict_left(4, 2)  # values 0, 1, 2, 3
    out = vec.hash()

    # One hash per run
    run_hashes = [out[i * 2] for i in range(4)]
    assert len(set(run_hashes)) == 4, f"hash collisions in distinct run values: {run_hashes}"


if __name__ == "__main__":
    pytest.main([__file__])
