"""
Tests for the Draken-native vector_contains_all.

New signature:
    vector_contains_all(ArrayVector, set) -> BoolVector
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.vector_ops import vector_contains_all
from opteryx.draken.interop.arrow import vector_from_arrow


def _array_vec(rows):
    """Build an ArrayVector from a list of int rows (each row is a list or None)."""
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.int64())))


def _str_array_vec(rows):
    """Build an ArrayVector of string rows."""
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.string())))


def _result(bool_vec):
    return bool_vec.to_pylist()


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


class TestListContainsAllBasic:
    def test_single_item_present(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_all(vec, {1})) == [True]

    def test_single_item_absent(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_all(vec, {9})) == [False]

    def test_all_items_present(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_all(vec, {1, 2, 3})) == [True]

    def test_some_items_missing(self):
        vec = _array_vec([[1, 2, 3]])
        assert _result(vector_contains_all(vec, {1, 4})) == [False]

    def test_multiple_rows(self):
        vec = _array_vec([[1, 2, 3], [1, 3], [1, 2, 3, 4]])
        assert _result(vector_contains_all(vec, {1, 2, 3})) == [True, False, True]

    def test_items_superset_of_row(self):
        vec = _array_vec([[1, 2]])
        assert _result(vector_contains_all(vec, {1, 2, 3})) == [False]

    def test_items_subset_of_row(self):
        vec = _array_vec([[1, 2, 3, 4, 5]])
        assert _result(vector_contains_all(vec, {2, 4})) == [True]


# ---------------------------------------------------------------------------
# Empty inputs
# ---------------------------------------------------------------------------


class TestListContainsAllEmpty:
    def test_empty_vector(self):
        vec = _array_vec([])
        assert _result(vector_contains_all(vec, {1})) == []

    def test_empty_items_trivially_true(self):
        vec = _array_vec([[1, 2], [3, 4], []])
        assert _result(vector_contains_all(vec, set())) == [True, True, True]

    def test_empty_row_nonempty_items(self):
        vec = _array_vec([[], [1, 2]])
        assert _result(vector_contains_all(vec, {1})) == [False, True]

    def test_all_empty_rows(self):
        vec = _array_vec([[], [], []])
        assert _result(vector_contains_all(vec, {1})) == [False, False, False]


# ---------------------------------------------------------------------------
# Null rows
# ---------------------------------------------------------------------------


class TestListContainsAllNulls:
    def test_null_row_produces_false(self):
        vec = _array_vec([None, [1, 2]])
        assert _result(vector_contains_all(vec, {1})) == [False, True]

    def test_all_null_rows(self):
        vec = _array_vec([None, None])
        assert _result(vector_contains_all(vec, {1})) == [False, False]

    def test_null_among_matches(self):
        vec = _array_vec([[1, 2], None, [1, 2, 3]])
        assert _result(vector_contains_all(vec, {1, 2})) == [True, False, True]

    def test_null_row_with_empty_items(self):
        # Empty items: non-null rows are True, null rows are False
        vec = _array_vec([None, [1, 2]])
        assert _result(vector_contains_all(vec, set())) == [False, True]


# ---------------------------------------------------------------------------
# Semantics
# ---------------------------------------------------------------------------


class TestListContainsAllSemantics:
    def test_requires_all_not_just_some(self):
        vec = _array_vec([[1, 2]])
        assert _result(vector_contains_all(vec, {1, 2, 3})) == [False]

    def test_extra_row_elements_ok(self):
        vec = _array_vec([[1, 2, 3, 4, 5]])
        assert _result(vector_contains_all(vec, {1, 3})) == [True]

    def test_duplicate_row_elements(self):
        vec = _array_vec([[1, 1, 2, 2]])
        assert _result(vector_contains_all(vec, {1, 2})) == [True]

    def test_single_required_item_duplicated(self):
        vec = _array_vec([[1, 1, 1]])
        assert _result(vector_contains_all(vec, {1})) == [True]


# ---------------------------------------------------------------------------
# String elements
# ---------------------------------------------------------------------------


class TestListContainsAllStrings:
    def test_all_present(self):
        vec = _str_array_vec([["apple", "banana", "cherry"]])
        assert _result(vector_contains_all(vec, {b"apple", b"cherry"})) == [True]

    def test_missing_one(self):
        vec = _str_array_vec([["apple", "banana"]])
        assert _result(vector_contains_all(vec, {b"apple", b"cherry"})) == [False]

    def test_mixed_rows(self):
        vec = _str_array_vec([["a", "b", "c"], ["a", "c"], ["b"]])
        assert _result(vector_contains_all(vec, {b"a", b"c"})) == [True, True, False]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
