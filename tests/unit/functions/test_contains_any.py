"""
Tests for the Draken-native vector_contains_any.

New signature:
    vector_contains_any(ArrayVector, set) -> BoolVector
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.vector_ops import vector_contains_any
from draken.interop.arrow import vector_from_arrow


def _array_vec(rows):
    """Build an ArrayVector from a list of rows (each row is a list or None)."""
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.int64())))


def _str_array_vec(rows):
    """Build an ArrayVector of string lists."""
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.string())))


def _result(bool_vec):
    return bool_vec.to_pylist()


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


class TestListContainsAnyBasic:
    def test_single_row_match(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_any(vec, {1})) == [True]

    def test_single_row_no_match(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_any(vec, {9})) == [False]

    def test_multiple_rows_mixed(self):
        vec = _array_vec([[1, 2], [3, 4], [5, 6]])
        assert _result(vector_contains_any(vec, {1, 5})) == [True, False, True]

    def test_all_rows_match(self):
        vec = _array_vec([[10], [10, 20], [10, 20, 30]])
        assert _result(vector_contains_any(vec, {10})) == [True, True, True]

    def test_no_rows_match(self):
        vec = _array_vec([[1, 2], [3, 4]])
        assert _result(vector_contains_any(vec, {99, 100})) == [False, False]

    def test_stops_at_first_match(self):
        vec = _array_vec([[5, 1, 2, 3]])
        assert _result(vector_contains_any(vec, {5})) == [True]


# ---------------------------------------------------------------------------
# Empty inputs
# ---------------------------------------------------------------------------


class TestListContainsAnyEmpty:
    def test_empty_vector(self):
        vec = _array_vec([])
        assert _result(vector_contains_any(vec, {1})) == []

    def test_empty_items_set(self):
        vec = _array_vec([[1, 2], [3, 4]])
        assert _result(vector_contains_any(vec, set())) == [False, False]

    def test_empty_row_in_array(self):
        vec = _array_vec([[], [1, 2]])
        assert _result(vector_contains_any(vec, {1})) == [False, True]

    def test_all_empty_rows(self):
        vec = _array_vec([[], [], []])
        assert _result(vector_contains_any(vec, {1})) == [False, False, False]


# ---------------------------------------------------------------------------
# Null rows
# ---------------------------------------------------------------------------


class TestListContainsAnyNulls:
    def test_null_row_produces_false(self):
        vec = _array_vec([None, [1, 2]])
        assert _result(vector_contains_any(vec, {1})) == [False, True]

    def test_all_null_rows(self):
        vec = _array_vec([None, None])
        assert _result(vector_contains_any(vec, {1})) == [False, False]

    def test_null_among_matches(self):
        vec = _array_vec([[3, 1], None, [9]])
        assert _result(vector_contains_any(vec, {1, 9})) == [True, False, True]


# ---------------------------------------------------------------------------
# String elements
# ---------------------------------------------------------------------------


class TestListContainsAnyStrings:
    def test_string_match(self):
        vec = _str_array_vec([["apple", "banana"], ["cherry"]])
        # StringVector stores as bytes internally
        assert _result(vector_contains_any(vec, {b"banana", b"cherry"})) == [True, True]

    def test_string_no_match(self):
        vec = _str_array_vec([["foo", "bar"]])
        assert _result(vector_contains_any(vec, {b"baz"})) == [False]

    def test_mixed_rows(self):
        vec = _str_array_vec([["a", "b", "c"], ["d", "e"], ["f"]])
        assert _result(vector_contains_any(vec, {b"a", b"f"})) == [True, False, True]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
