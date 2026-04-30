"""
Phase 3 unit tests: AnyOp/AllOp/AtArrow operators in the Draken evaluator.

Covers:
- draken_compare: AnyOpEq / AnyOpNotEq
- draken_compare: AnyOpGt / AnyOpLt / AnyOpGtEq / AnyOpLtEq
- draken_compare: AllOpEq / AllOpNotEq
- draken_compare: AtArrow (column @> literal_list)
- draken_compare: ArrayContainsAll (column @>> literal_list)
- IS NULL on dictionary-encoded inputs through the evaluator path
"""

import os
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from draken.interop.arrow import vector_from_arrow, vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.expression import NodeType
from opteryx.expression.evaluator import draken_compare, evaluate_draken
from opteryx.models import Node
from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def bv_to_list(bv):
    return bv.to_pylist()


def _array_vec(rows):
    """Create an ArrayVector from a list of lists (with possible None rows)."""
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.int64())))


def _array_vec_str(rows):
    return vector_from_arrow(pa.array(rows, type=pa.list_(pa.string())))


def _dict_vec_float(values):
    """Create a float input from dictionary-encoded Arrow data."""
    return vector_from_arrow(pa.array(values, type=pa.float64()).dictionary_encode())


def _morsel(col_identity: str, vec):
    return Morsel.from_vectors([col_identity], [vec])


def _col(name: str, dtype=OrsoTypes.ARRAY) -> FlatColumn:
    c = FlatColumn(name=name, type=dtype)
    c.identity = name
    return c


def _identifier_node(col: FlatColumn):
    return Node(NodeType.IDENTIFIER, schema_column=col)


def _literal_node(value):
    return Node(NodeType.LITERAL, value=value)


def _comparison_node(op: str, left_node, right_node):
    n = Node(NodeType.COMPARISON_OPERATOR, value=op)
    n.left = left_node
    n.right = right_node
    return n


def _unary_node(op: str, centre_node):
    n = Node(NodeType.UNARY_OPERATOR, value=op)
    n.centre = centre_node
    return n


# ---------------------------------------------------------------------------
# AnyOp: scalar = ANY(array_column)
# convention: left=literal, right=ArrayVector column
# ---------------------------------------------------------------------------


class TestAnyOpEq:
    def test_basic(self):
        col = _array_vec([[1, 2, 3], [4, 5], [6]])
        result = draken_compare("AnyOpEq", 2, col)
        assert bv_to_list(result) == [True, False, False]

    def test_match_multiple_rows(self):
        col = _array_vec([[10], [5, 10], [1, 2]])
        result = draken_compare("AnyOpEq", 10, col)
        assert bv_to_list(result) == [True, True, False]

    def test_no_match(self):
        col = _array_vec([[1, 2], [3, 4]])
        result = draken_compare("AnyOpEq", 99, col)
        assert bv_to_list(result) == [False, False]

    def test_null_literal(self):
        col = _array_vec([[1, 2], [3, 4]])
        result = draken_compare("AnyOpEq", None, col)
        assert bv_to_list(result) == [False, False]


class TestAnyOpNotEq:
    def test_basic(self):
        col = _array_vec([[1, 1], [1, 2], [2, 2]])
        result = draken_compare("AnyOpNotEq", 1, col)
        # row0: [1,1] — no element != 1 → False; row1: 2 != 1 → True; row2: all != 1 → True
        assert bv_to_list(result) == [False, True, True]


class TestAnyOpComparisons:
    def test_gt(self):
        col = _array_vec([[1, 2], [3, 4], [1]])
        result = draken_compare("AnyOpGt", 2, col)
        # AnyOpGt: literal > ANY(row) → any element < literal (convention: col contains vals, literal is right side)
        # Actually: "2 > ANY(row)" means any row element is < 2
        # For vector_anyop_gt: literal > elem
        assert bv_to_list(result) == [True, False, True]

    def test_lt(self):
        col = _array_vec([[1, 5], [6, 7]])
        result = draken_compare("AnyOpLt", 4, col)
        # "4 < ANY(row)" means any element > 4
        assert bv_to_list(result) == [True, True]

    def test_gte(self):
        col = _array_vec([[2, 3], [4, 5]])
        result = draken_compare("AnyOpGtEq", 3, col)
        # "3 >= ANY(row)" means any element <= 3
        assert bv_to_list(result) == [True, False]

    def test_lte(self):
        col = _array_vec([[2, 3], [1, 5]])
        result = draken_compare("AnyOpLtEq", 3, col)
        # "3 <= ANY(row)" means any element >= 3
        assert bv_to_list(result) == [True, True]


# ---------------------------------------------------------------------------
# AllOp: scalar op ALL(array_column)
# ---------------------------------------------------------------------------


class TestAllOpEq:
    def test_all_equal(self):
        col = _array_vec([[5, 5], [5, 6], [5]])
        result = draken_compare("AllOpEq", 5, col)
        assert bv_to_list(result) == [True, False, True]

    def test_null_row(self):
        col = _array_vec([[5, 5], None, [5]])
        result = draken_compare("AllOpEq", 5, col)
        assert bv_to_list(result) == [True, False, True]


class TestAllOpNotEq:
    def test_none_equal(self):
        col = _array_vec([[1, 2], [3, 5], [5, 5]])
        result = draken_compare("AllOpNotEq", 5, col)
        # 5 != ALL([1,2])=True (5 not in arr), 5 != ALL([3,5])=False (5 IS in arr), 5 != ALL([5,5])=False
        assert bv_to_list(result) == [True, False, False]


# ---------------------------------------------------------------------------
# AtArrow: column @> [values]  (array column contains any of values)
# convention: left=ArrayVector, right=literal list
# ---------------------------------------------------------------------------


class TestAtArrow:
    def test_basic(self):
        col = _array_vec([[1, 2, 3], [4, 5], [6]])
        result = draken_compare("AtArrow", col, [2, 6])
        assert bv_to_list(result) == [True, False, True]

    def test_string(self):
        col = _array_vec_str([["a", "b"], ["c"], ["d", "e"]])
        result = draken_compare("AtArrow", col, {"a", "e"})
        assert bv_to_list(result) == [True, False, True]

    def test_no_match(self):
        col = _array_vec([[1, 2], [3, 4]])
        result = draken_compare("AtArrow", col, [9, 10])
        assert bv_to_list(result) == [False, False]

    def test_null_row(self):
        col = _array_vec([[1, 2], None, [3]])
        result = draken_compare("AtArrow", col, [1])
        assert bv_to_list(result) == [True, False, False]

    def test_empty_literal_set(self):
        col = _array_vec([[1, 2], [3]])
        result = draken_compare("AtArrow", col, [])
        assert bv_to_list(result) == [False, False]


# ---------------------------------------------------------------------------
# ArrayContainsAll: column must contain all values in literal_set
# ---------------------------------------------------------------------------


class TestArrayContainsAll:
    def test_all_present(self):
        col = _array_vec([[1, 2, 3], [1, 3], [2, 3]])
        result = draken_compare("ArrayContainsAll", col, [1, 3])
        assert bv_to_list(result) == [True, True, False]

    def test_empty_required_set(self):
        col = _array_vec([[1], [2, 3]])
        result = draken_compare("ArrayContainsAll", col, [])
        # Vacuously true for all non-null rows
        assert bv_to_list(result) == [True, True]
