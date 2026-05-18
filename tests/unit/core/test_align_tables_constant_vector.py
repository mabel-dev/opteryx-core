"""
Regression tests for align_tables() with constant vectors and negative indices.

Reproduces the segfault: align_tables(left, right, la, ra) where the right morsel
contains a constant-encoded vector (e.g. from SELECT 1 AS marker) AND ra has -1
values (LEFT join unmatched rows).

Root cause: _ensure_output_null_bitmap would allocate ptr.null_bitmap on a constant
vector that still has ptr.data=NULL, creating inconsistent state that crashes on
downstream dense-buffer access.  Fixed by materializing constant vectors before take.
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from array import array as _pyarray

from draken.morsels.align import align_tables
from draken.morsels.morsel import Morsel
from draken.vectors.integer64_vector import Integer64Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.string_vector import StringVector


def _make_left(n):
    """Left morsel: n rows with a plain dense INT64 id column."""
    return Morsel.from_arrow(pa.table({"id": pa.array(list(range(n)), type=pa.int64())}))


def _make_right_int64_const(value, n):
    """Right morsel with a single constant INT64 column."""
    vec = Integer64Vector.from_constant(value, n)
    return Morsel.from_vectors([b"marker"], [vec])


def _make_right_float64_const(value, n):
    vec = Float64Vector.from_constant(value, n)
    return Morsel.from_vectors([b"score"], [vec])


def _align(left, right, left_idxs, right_idxs):
    la = _pyarray("i", left_idxs)
    ra = _pyarray("i", right_idxs)
    return align_tables(left, right, memoryview(la), memoryview(ra))


# ---------------------------------------------------------------------------
# Int64 constant + negative indices
# ---------------------------------------------------------------------------

def test_align_int64_constant_no_negative_does_not_crash():
    """Baseline: all valid indices, constant column — must not crash."""
    left = _make_left(3)
    right = _make_right_int64_const(42, 3)
    result = _align(left, right, [0, 1, 2], [0, 1, 2])
    assert result.num_rows == 3


def test_align_int64_constant_with_negatives_does_not_crash():
    """Regression: constant right column AND -1 in append_indices → must not segfault."""
    left = _make_left(5)
    right = _make_right_int64_const(42, 5)
    # rows 0,1 matched; rows 2,3,4 unmatched (-1)
    result = _align(left, right, [0, 1, 2, 3, 4], [0, 1, -1, -1, -1])
    assert result.num_rows == 5


def test_align_int64_constant_matched_rows_have_value():
    left = _make_left(3)
    right = _make_right_int64_const(99, 3)
    result = _align(left, right, [0, 1, 2], [0, 1, -1])
    marker = result.column(b"marker")
    assert marker[0] == 99
    assert marker[1] == 99


def test_align_int64_constant_unmatched_rows_are_null():
    left = _make_left(3)
    right = _make_right_int64_const(99, 3)
    result = _align(left, right, [0, 1, 2], [0, 1, -1])
    marker = result.column(b"marker")
    assert marker[2] is None


def test_align_int64_constant_all_unmatched():
    left = _make_left(4)
    right = _make_right_int64_const(7, 4)
    result = _align(left, right, [0, 1, 2, 3], [-1, -1, -1, -1])
    assert result.num_rows == 4
    marker = result.column(b"marker")
    for i in range(4):
        assert marker[i] is None


# ---------------------------------------------------------------------------
# Float64 constant + negative indices
# ---------------------------------------------------------------------------

def test_align_float64_constant_with_negatives_does_not_crash():
    left = _make_left(4)
    right = _make_right_float64_const(3.14, 4)
    result = _align(left, right, [0, 1, 2, 3], [0, -1, -1, 3])
    assert result.num_rows == 4


def test_align_float64_constant_null_semantics():
    left = _make_left(4)
    right = _make_right_float64_const(1.5, 4)
    result = _align(left, right, [0, 1, 2, 3], [0, -1, -1, 3])
    score = result.column(b"score")
    assert score[0] == pytest.approx(1.5)
    assert score[1] is None
    assert score[2] is None
    assert score[3] == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# Multiple constant columns
# ---------------------------------------------------------------------------

def test_align_multiple_constant_columns_with_negatives():
    left = _make_left(3)
    int_vec = Integer64Vector.from_constant(10, 3)
    flt_vec = Float64Vector.from_constant(2.0, 3)
    right = Morsel.from_vectors([b"a", b"b"], [int_vec, flt_vec])
    result = _align(left, right, [0, 1, 2], [-1, 1, -1])
    assert result.num_rows == 3
    a = result.column(b"a")
    b = result.column(b"b")
    assert a[0] is None
    assert a[1] == 10
    assert a[2] is None
    assert b[1] == pytest.approx(2.0)


if __name__ == "__main__":
    test_align_int64_constant_no_negative_does_not_crash()
    print("no_negative passed")
    test_align_int64_constant_with_negatives_does_not_crash()
    print("with_negatives no crash passed")
    test_align_int64_constant_matched_rows_have_value()
    print("matched value passed")
    test_align_int64_constant_unmatched_rows_are_null()
    print("unmatched null passed")
    test_align_int64_constant_all_unmatched()
    print("all_unmatched passed")
    test_align_float64_constant_with_negatives_does_not_crash()
    print("float64 no crash passed")
    test_align_float64_constant_null_semantics()
    print("float64 null semantics passed")
    test_align_multiple_constant_columns_with_negatives()
    print("multiple constant columns passed")
    print("All tests passed!")
