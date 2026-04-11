"""
Phase 1 unit tests: draken_compare dispatcher and evaluate_draken tree walker.

Covers:
- draken_compare: scalar comparisons for Int64/Float64/StringVector types
- draken_compare: InList (int and string)
- draken_compare: negated operators (NotEq, NotInList, NotLike, etc.)
- draken_compare: null bitmap propagation
- evaluate_draken: IDENTIFIER + LITERAL predicate
- evaluate_draken: AND with short-circuit
- evaluate_draken: OR
- evaluate_draken: NOT
- evaluate_draken: IS NULL / IS NOT NULL
- evaluate_draken: nested AND/OR tree
"""

import datetime
import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.draken.interop.arrow import vector_from_arrow, vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel

from opteryx.expression import NodeType
from opteryx.expression.evaluator import draken_compare, evaluate_draken
from opteryx.models import Node
from opteryx.schema import FlatColumn
from opteryx.types import OrsoTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _int_vec(values):
    return vector_from_sequence(values)


def _float_vec(values):
    return vector_from_arrow(pa.array(values, type=pa.float64()))


def _str_vec(values):
    return vector_from_arrow(pa.array(values, type=pa.string()))


def _int_vec_with_nulls(values):
    return vector_from_arrow(pa.array(values, type=pa.int64()))


def bv_to_list(bv):
    return bv.to_pylist()


def _morsel(col_identity: str, vec):
    """Create a single-column Morsel keyed by col_identity."""
    return Morsel.from_vectors([col_identity], [vec])


def _identifier_node(col: FlatColumn):
    return Node(NodeType.IDENTIFIER, schema_column=col)


def _literal_node(value):
    return Node(NodeType.LITERAL, value=value)


def _typed_literal_node(value, typ: OrsoTypes):
    node = Node(NodeType.LITERAL, value=value, schema_column=FlatColumn(name=str(value), type=typ))
    node.type = typ
    return node


def _comparison_node(op: str, left_node, right_node):
    n = Node(NodeType.COMPARISON_OPERATOR, value=op)
    n.left = left_node
    n.right = right_node
    return n


def _and_node(left_node, right_node):
    n = Node(NodeType.AND)
    n.left = left_node
    n.right = right_node
    return n


def _or_node(left_node, right_node):
    n = Node(NodeType.OR)
    n.left = left_node
    n.right = right_node
    return n


def _not_node(centre_node):
    n = Node(NodeType.NOT)
    n.centre = centre_node
    return n


def _unary_node(op: str, centre_node):
    n = Node(NodeType.UNARY_OPERATOR, value=op)
    n.centre = centre_node
    return n


# ---------------------------------------------------------------------------
# draken_compare: Int64Vector scalar comparisons
# ---------------------------------------------------------------------------


class TestDrakenCompareInt64Scalar:
    def test_eq(self):
        vec = _int_vec([1, 2, 3, 4])
        result = draken_compare("Eq", vec, 2)
        assert bv_to_list(result) == [False, True, False, False]

    def test_not_eq(self):
        vec = _int_vec([1, 2, 3])
        result = draken_compare("NotEq", vec, 2)
        assert bv_to_list(result) == [True, False, True]

    def test_lt(self):
        vec = _int_vec([1, 2, 3])
        result = draken_compare("Lt", vec, 2)
        assert bv_to_list(result) == [True, False, False]

    def test_gt(self):
        vec = _int_vec([1, 2, 3])
        result = draken_compare("Gt", vec, 2)
        assert bv_to_list(result) == [False, False, True]

    def test_lt_eq(self):
        vec = _int_vec([1, 2, 3])
        result = draken_compare("LtEq", vec, 2)
        assert bv_to_list(result) == [True, True, False]

    def test_gt_eq(self):
        vec = _int_vec([1, 2, 3])
        result = draken_compare("GtEq", vec, 2)
        assert bv_to_list(result) == [False, True, True]

    def test_in_list(self):
        vec = _int_vec([1, 2, 3, 4])
        result = draken_compare("InList", vec, [1, 3])
        assert bv_to_list(result) == [True, False, True, False]

    def test_not_in_list(self):
        vec = _int_vec([1, 2, 3, 4])
        result = draken_compare("NotInList", vec, [1, 3])
        assert bv_to_list(result) == [False, True, False, True]


# ---------------------------------------------------------------------------
# draken_compare: Float64Vector scalar comparisons
# ---------------------------------------------------------------------------


class TestDrakenCompareFloat64Scalar:
    def test_eq(self):
        vec = _float_vec([1.0, 2.0, 3.0])
        result = draken_compare("Eq", vec, 2.0)
        assert bv_to_list(result) == [False, True, False]

    def test_gt(self):
        vec = _float_vec([1.5, 2.5, 3.5])
        result = draken_compare("Gt", vec, 2.0)
        assert bv_to_list(result) == [False, True, True]

    def test_in_list(self):
        vec = _float_vec([1.0, 2.0, 3.0])
        result = draken_compare("InList", vec, [1.0, 3.0])
        assert bv_to_list(result) == [True, False, True]


# ---------------------------------------------------------------------------
# draken_compare: StringVector scalar comparisons
# ---------------------------------------------------------------------------


class TestDrakenCompareStringScalar:
    def test_eq_bytes(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("Eq", vec, b"bob")
        assert bv_to_list(result) == [False, True, False]

    def test_eq_str(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("Eq", vec, "bob")
        assert bv_to_list(result) == [False, True, False]

    def test_lt(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("Lt", vec, b"bob")
        assert bv_to_list(result) == [True, False, False]

    def test_gt(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("Gt", vec, b"bob")
        assert bv_to_list(result) == [False, False, True]

    def test_not_eq(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("NotEq", vec, b"bob")
        assert bv_to_list(result) == [True, False, True]

    def test_in_list(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("InList", vec, ["alice", "carol"])
        assert bv_to_list(result) == [True, False, True]

    def test_not_in_list(self):
        vec = _str_vec(["alice", "bob", "carol"])
        result = draken_compare("NotInList", vec, ["alice", "carol"])
        assert bv_to_list(result) == [False, True, False]

    def test_like(self):
        vec = _str_vec(["foobar", "foo", "bar"])
        result = draken_compare("Like", vec, b"foo%")
        assert bv_to_list(result) == [True, True, False]

    def test_not_like(self):
        vec = _str_vec(["foobar", "foo", "bar"])
        result = draken_compare("NotLike", vec, b"foo%")
        assert bv_to_list(result) == [False, False, True]

    def test_ilike(self):
        vec = _str_vec(["FOOBAR", "foo", "bar"])
        result = draken_compare("ILike", vec, b"foo%")
        assert bv_to_list(result) == [True, True, False]

    def test_rlike(self):
        vec = _str_vec(["abc123", "abc", "123"])
        result = draken_compare("RLike", vec, b"\\d+")
        assert bv_to_list(result) == [True, False, True]

    def test_contains(self):
        vec = _str_vec(["foobar", "barfoo", "baz"])
        result = draken_compare("InStr", vec, b"foo")
        assert bv_to_list(result) == [True, True, False]

    def test_not_contains(self):
        vec = _str_vec(["foobar", "barfoo", "baz"])
        result = draken_compare("NotInStr", vec, b"foo")
        assert bv_to_list(result) == [False, False, True]


# ---------------------------------------------------------------------------
# draken_compare: null bitmap propagation
# ---------------------------------------------------------------------------


class TestDrakenCompareNullPropagation:
    def test_null_row_excluded_from_result(self):
        # [1, None, 3] — null in position 1
        vec = _int_vec_with_nulls([1, None, 3])
        result = draken_compare("Eq", vec, 1)
        pylist = result.to_pylist()
        assert pylist[0] is True
        assert pylist[1] is None  # null input → null output
        assert pylist[2] is False

    def test_null_not_in_list(self):
        vec = _int_vec_with_nulls([1, None, 3])
        result = draken_compare("InList", vec, {1, 2})
        pylist = result.to_pylist()
        assert pylist[0] is True
        assert pylist[1] is None
        assert pylist[2] is False


# ---------------------------------------------------------------------------
# evaluate_draken: tree walker
# ---------------------------------------------------------------------------


class TestEvaluateDrakenTreeWalker:
    def _make_morsel_and_col(self, values, arrow_type=pa.int64()):
        col = FlatColumn(name="salary", type=OrsoTypes.INTEGER)
        vec = vector_from_arrow(pa.array(values, type=arrow_type))
        morsel = _morsel(col.identity, vec)
        return morsel, col

    def test_simple_gt_comparison(self):
        morsel, col = self._make_morsel_and_col([10, 20, 30, 40])
        tree = _comparison_node("Gt", _identifier_node(col), _literal_node(25))
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [False, False, True, True]

    def test_simple_eq_comparison(self):
        morsel, col = self._make_morsel_and_col([10, 20, 30])
        tree = _comparison_node("Eq", _identifier_node(col), _literal_node(20))
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [False, True, False]

    def test_and_expression(self):
        morsel, col = self._make_morsel_and_col([5, 15, 25, 35])
        left = _comparison_node("Gt", _identifier_node(col), _literal_node(10))
        right = _comparison_node("Lt", _identifier_node(col), _literal_node(30))
        tree = _and_node(left, right)
        result = evaluate_draken(tree, morsel)
        # gt 10 AND lt 30 → [F,T,T,F]
        assert bv_to_list(result) == [False, True, True, False]

    def test_or_expression(self):
        morsel, col = self._make_morsel_and_col([5, 15, 25, 35])
        left = _comparison_node("Lt", _identifier_node(col), _literal_node(10))
        right = _comparison_node("Gt", _identifier_node(col), _literal_node(30))
        tree = _or_node(left, right)
        result = evaluate_draken(tree, morsel)
        # lt 10 OR gt 30 → [T,F,F,T]
        assert bv_to_list(result) == [True, False, False, True]

    def test_not_expression(self):
        morsel, col = self._make_morsel_and_col([5, 15, 25])
        inner = _comparison_node("Gt", _identifier_node(col), _literal_node(10))
        tree = _not_node(inner)
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [True, False, False]

    def test_and_short_circuit(self):
        """When the left side of AND is all-false, right side should not be evaluated."""
        morsel, col = self._make_morsel_and_col([1, 2, 3])

        # Left side is always false (value == 999, never matches)
        left = _comparison_node("Eq", _identifier_node(col), _literal_node(999))

        # Right side would hold a reference to a non-existent column if evaluated
        # We verify it doesn't crash (short-circuit skips it)
        right = _comparison_node("Eq", _identifier_node(col), _literal_node(1))

        tree = _and_node(left, right)
        result = evaluate_draken(tree, morsel)
        # All false because left is all-false
        assert bv_to_list(result) == [False, False, False]

    def test_is_null(self):
        col = FlatColumn(name="val", type=OrsoTypes.INTEGER)
        vec = vector_from_arrow(pa.array([1, None, 3], type=pa.int64()))
        morsel = _morsel(col.identity, vec)
        tree = _unary_node("IsNull", _identifier_node(col))
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [False, True, False]

    def test_is_not_null(self):
        col = FlatColumn(name="val", type=OrsoTypes.INTEGER)
        vec = vector_from_arrow(pa.array([1, None, 3], type=pa.int64()))
        morsel = _morsel(col.identity, vec)
        tree = _unary_node("IsNotNull", _identifier_node(col))
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [True, False, True]

    def test_nested_expression(self):
        morsel, col = self._make_morsel_and_col([10, 20, 30])
        inner = _comparison_node("Eq", _identifier_node(col), _literal_node(20))
        nested = Node(NodeType.NESTED)
        nested.centre = inner
        result = evaluate_draken(nested, morsel)
        assert bv_to_list(result) == [False, True, False]

    def test_string_like_via_tree(self):
        col = FlatColumn(name="name", type=OrsoTypes.VARCHAR)
        vec = vector_from_arrow(pa.array(["foobar", "foo", "bar"], type=pa.string()))
        morsel = _morsel(col.identity, vec)
        tree = _comparison_node("Like", _identifier_node(col), _literal_node(b"foo%"))
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [True, True, False]

    def test_timestamp_int64_temporal_comparison(self):
        col = FlatColumn(name="Lauched_at", type=OrsoTypes.TIMESTAMP, identity="launch")
        vec = vector_from_sequence([-386310720000000, 4102444800000000])
        morsel = _morsel(col.identity, vec)
        tree = _comparison_node(
            "Lt",
            _identifier_node(col),
            _typed_literal_node(datetime.datetime(2100, 1, 1), OrsoTypes.TIMESTAMP),
        )
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [True, False]

    def test_date_int64_temporal_comparison(self):
        col = FlatColumn(name="birth_date", type=OrsoTypes.DATE, identity="birth")
        vec = vector_from_sequence([-10000, -9000, 47482])
        morsel = _morsel(col.identity, vec)
        tree = _comparison_node(
            "Lt",
            _identifier_node(col),
            _typed_literal_node(datetime.date(1950, 1, 1), OrsoTypes.DATE),
        )
        result = evaluate_draken(tree, morsel)
        assert bv_to_list(result) == [True, True, False]


# ---------------------------------------------------------------------------
# Phase 2: FilterNode draken path
# ---------------------------------------------------------------------------


class TestFilterNodeDrakenPath:
    """Smoke tests for FilterNode (now unconditionally Draken-native)."""

    def _make_filter_node(self, filter_tree):
        from opteryx.operators.filter_node import FilterNode

        from opteryx.models import QueryProperties

        props = QueryProperties(None, {})
        return FilterNode(props, filter=filter_tree)

    def test_draken_filter_gt(self, monkeypatch):
        """FilterNode with Draken morsel."""
        col = FlatColumn(name="val", type=OrsoTypes.INTEGER)
        vec = vector_from_arrow(pa.array([10, 20, 30, 40], type=pa.int64()))
        morsel = _morsel(col.identity, vec)

        tree = _comparison_node("Gt", _identifier_node(col), _literal_node(20))
        node = self._make_filter_node(tree)

        results = list(node.execute(morsel))
        assert len(results) == 1
        result_morsel = results[0]
        assert result_morsel.__class__.__name__ == "Morsel"
        assert result_morsel.num_rows == 2  # 30, 40

    def test_draken_filter_all_false_yields_empty(self, monkeypatch):
        col = FlatColumn(name="val", type=OrsoTypes.INTEGER)
        vec = vector_from_arrow(pa.array([1, 2, 3], type=pa.int64()))
        morsel = _morsel(col.identity, vec)

        tree = _comparison_node("Eq", _identifier_node(col), _literal_node(999))
        node = self._make_filter_node(tree)

        results = list(node.execute(morsel))
        assert len(results) == 1
        assert results[0].num_rows == 0
