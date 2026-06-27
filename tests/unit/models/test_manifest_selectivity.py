"""
Tests for Manifest.estimate_selectivity.

Covers each predicate kind across three tiers:
  1. Histogram-backed (real Distogram via FileEntry.histogram_counts).
  2. NDV / null-fraction (no histogram, but min_k_hashes / null_value_counts).
  3. Textbook fallback (no per-column stats at all).

Plus compound predicates (AND / OR / NOT) and clamping.
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _schema(*names: str) -> RelationSchema:
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(name=n, column_type=INT64, identity=mint_column_identity("t", n))
            for n in names
        ],
    )


def _file(
    *,
    record_count: int = 0,
    histogram_counts: Optional[List[List[int]]] = None,
    min_values: Optional[List] = None,
    max_values: Optional[List] = None,
    null_value_counts: Optional[dict] = None,
    min_k_hashes: Optional[List[List[int]]] = None,
) -> FileEntry:
    return FileEntry(
        file_path="x",
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        histogram_counts=histogram_counts,
        min_values=min_values,
        max_values=max_values,
        null_value_counts=null_value_counts,
        min_k_hashes=min_k_hashes,
    )


def _identifier(name: str) -> Node:
    n = Node(node_type=NodeType.IDENTIFIER)
    n.value = name
    n.source_column = name
    return n


def _literal(value) -> Node:
    n = Node(node_type=NodeType.LITERAL)
    n.value = value
    return n


def _cmp(op: str, col: str, value) -> Node:
    n = Node(node_type=NodeType.COMPARISON_OPERATOR)
    n.value = op
    n.left = _identifier(col)
    n.right = _literal(value)
    return n


def _between(col: str, low, high) -> Node:
    n = Node(node_type=NodeType.BETWEEN)
    n.left = _identifier(col)
    # Mirror manifest.prune_files convention: right=lower, centre=upper.
    n.right = _literal(low)
    n.centre = _literal(high)
    return n


def _unary(op: str, col: str) -> Node:
    n = Node(node_type=NodeType.UNARY_OPERATOR)
    n.value = op
    n.centre = _identifier(col)
    return n


def _and(a: Node, b: Node) -> Node:
    n = Node(node_type=NodeType.AND)
    n.left = a
    n.right = b
    return n


def _or(a: Node, b: Node) -> Node:
    n = Node(node_type=NodeType.OR)
    n.left = a
    n.right = b
    return n


def _not(inner: Node) -> Node:
    n = Node(node_type=NodeType.NOT)
    n.centre = inner
    return n


def _histogram_manifest(
    column: str = "x",
    *,
    counts: Optional[List[int]] = None,
    col_min: float = 0.0,
    col_max: float = 100.0,
    record_count: Optional[int] = None,
    null_count: int = 0,
) -> Manifest:
    """Manifest with a single file carrying a histogram for `column`."""
    if counts is None:
        # Uniform-ish: 50 bins, 10 each → 500 rows.
        counts = [10] * 50
    rc = record_count if record_count is not None else sum(counts) + null_count
    file = _file(
        record_count=rc,
        histogram_counts=[counts],
        min_values=[col_min],
        max_values=[col_max],
        null_value_counts={0: null_count} if null_count else {0: 0},
    )
    return Manifest(files=[file], schema=_schema(column))


def _bare_manifest(column: str = "x", *, record_count: int = 100) -> Manifest:
    """Manifest with no per-column stats (no histogram, no NDV, no nulls)."""
    file = _file(record_count=record_count)
    return Manifest(files=[file], schema=_schema(column))


def _ndv_manifest(
    column: str = "x", *, ndv: int, record_count: int = 1000, null_count: int = 0
) -> Manifest:
    """Manifest with NDV (via min_k_hashes) but no histogram."""
    K = 32
    # Build a deterministic min_k_hashes vector that the KMV estimator will turn
    # back into ~ndv. Cheaper: when ndv < K, store exactly ndv hashes (exact path).
    if ndv <= K:
        hashes = list(range(1, ndv + 1))
    else:
        # KMV estimate = (K-1) * 2^64 / kth_smallest_hash → pick kth so that ratio==ndv.
        kth = int((K - 1) * (2**64) / ndv)
        hashes = list(range(1, K)) + [kth]
    file = _file(
        record_count=record_count,
        min_k_hashes=[hashes],
        null_value_counts={0: null_count},
    )
    return Manifest(files=[file], schema=_schema(column))


# ---------------------------------------------------------------------------
# Equality
# ---------------------------------------------------------------------------


class TestEquality:
    def test_eq_with_histogram(self):
        m = _histogram_manifest()  # uniform 0..100, 500 rows, 50 bins
        s = m.estimate_selectivity(_cmp("Eq", "x", 50))
        # Single bin density ≈ 10/500 = 0.02. Loose tolerance.
        assert 0.0 < s <= 0.5

    def test_eq_with_ndv_only(self):
        m = _ndv_manifest(ndv=10)
        s = m.estimate_selectivity(_cmp("Eq", "x", 7))
        assert s == pytest.approx(0.1, rel=0.01)

    def test_eq_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("Eq", "x", 1)) == 0.1

    def test_neq_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("NotEq", "x", 1)) == 0.9

    def test_neq_with_ndv(self):
        m = _ndv_manifest(ndv=4)
        s = m.estimate_selectivity(_cmp("NotEq", "x", 2))
        assert s == pytest.approx(0.75, rel=0.01)


# ---------------------------------------------------------------------------
# Range
# ---------------------------------------------------------------------------


class TestRange:
    def test_lt_with_histogram(self):
        m = _histogram_manifest()  # uniform 0..100
        s = m.estimate_selectivity(_cmp("Lt", "x", 50))
        # Should be ~0.5.
        assert 0.25 <= s <= 0.75

    def test_gt_with_histogram(self):
        m = _histogram_manifest()
        s = m.estimate_selectivity(_cmp("Gt", "x", 25))
        # Should be ~0.75 — within ±50% of textbook.
        assert 0.4 <= s <= 1.0

    def test_lt_below_min(self):
        m = _histogram_manifest(col_min=10.0, col_max=20.0)
        s = m.estimate_selectivity(_cmp("Lt", "x", 0))
        assert s == 0.0

    def test_gt_above_max(self):
        m = _histogram_manifest(col_min=10.0, col_max=20.0)
        s = m.estimate_selectivity(_cmp("Gt", "x", 999))
        assert s == 0.0

    def test_lt_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("Lt", "x", 5)) == 0.25

    def test_gteq_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("GtEq", "x", 5)) == 0.25


# ---------------------------------------------------------------------------
# IN / NOT IN
# ---------------------------------------------------------------------------


class TestInList:
    def test_in_with_histogram(self):
        m = _histogram_manifest()
        s = m.estimate_selectivity(_cmp("InList", "x", [10, 20, 30]))
        # Sum of three bin densities, each ≈ 0.02, so ≈ 0.06.
        assert 0.0 < s < 0.5

    def test_in_with_ndv(self):
        m = _ndv_manifest(ndv=20)
        s = m.estimate_selectivity(_cmp("InList", "x", [1, 2, 3, 4]))
        assert s == pytest.approx(0.2, rel=0.01)

    def test_in_capped_at_one(self):
        m = _ndv_manifest(ndv=2)
        s = m.estimate_selectivity(_cmp("InList", "x", [1, 2, 3, 4, 5]))
        assert s == 1.0

    def test_in_no_stats(self):
        m = _bare_manifest()
        s = m.estimate_selectivity(_cmp("InList", "x", [1, 2]))
        assert s == pytest.approx(0.2)

    def test_not_in_with_ndv(self):
        m = _ndv_manifest(ndv=10)
        s = m.estimate_selectivity(_cmp("NotInList", "x", [1, 2]))
        assert s == pytest.approx(0.8, rel=0.01)


# ---------------------------------------------------------------------------
# BETWEEN
# ---------------------------------------------------------------------------


class TestBetween:
    def test_between_with_histogram(self):
        m = _histogram_manifest()  # 0..100
        s = m.estimate_selectivity(_between("x", 25, 75))
        assert 0.25 <= s <= 0.75

    def test_between_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_between("x", 1, 9)) == 0.25

    def test_between_swapped_bounds(self):
        m = _histogram_manifest()
        # high passed first; helper still produces correct fraction.
        s_low_first = m.estimate_selectivity(_between("x", 25, 75))
        s_high_first = m.estimate_selectivity(_between("x", 75, 25))
        assert s_low_first == pytest.approx(s_high_first, rel=0.01)


# ---------------------------------------------------------------------------
# LIKE
# ---------------------------------------------------------------------------


class TestLike:
    def test_prefix_like(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("Like", "x", "abc%")) == 0.25

    def test_substring_like(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("Like", "x", "%abc%")) == 0.1

    def test_not_like_prefix(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_cmp("NotLike", "x", "abc%")) == 0.75


# ---------------------------------------------------------------------------
# IS NULL / IS NOT NULL
# ---------------------------------------------------------------------------


class TestNullPredicates:
    def test_is_null_with_null_fraction(self):
        m = _histogram_manifest(null_count=50, record_count=550)
        # null_fraction = 50/550 ≈ 0.091
        s = m.estimate_selectivity(_unary("IsNull", "x"))
        assert s == pytest.approx(50 / 550, rel=0.01)

    def test_is_not_null_with_null_fraction(self):
        m = _histogram_manifest(null_count=50, record_count=550)
        s = m.estimate_selectivity(_unary("IsNotNull", "x"))
        assert s == pytest.approx(1 - 50 / 550, rel=0.01)

    def test_is_null_no_stats(self):
        m = _bare_manifest()
        # No null counts → 0.05 fallback.
        assert m.estimate_selectivity(_unary("IsNull", "x")) == 0.05

    def test_is_not_null_no_stats(self):
        m = _bare_manifest()
        assert m.estimate_selectivity(_unary("IsNotNull", "x")) == 0.95


# ---------------------------------------------------------------------------
# Compound predicates
# ---------------------------------------------------------------------------


class TestCompound:
    def test_and_multiplies(self):
        m = _bare_manifest()
        a = _cmp("Lt", "x", 5)
        b = _cmp("Gt", "x", 1)
        s = m.estimate_selectivity(_and(a, b))
        assert s == pytest.approx(0.25 * 0.25)

    def test_or_complements(self):
        m = _bare_manifest()
        a = _cmp("Lt", "x", 5)
        b = _cmp("Gt", "x", 1)
        s = m.estimate_selectivity(_or(a, b))
        assert s == pytest.approx(1 - 0.75 * 0.75)

    def test_not_inverts(self):
        m = _bare_manifest()
        s = m.estimate_selectivity(_not(_cmp("Eq", "x", 1)))
        assert s == pytest.approx(0.9)

    def test_and_clamped_non_negative(self):
        # AND of low-selectivity predicates trends toward 0 but never below.
        m = _bare_manifest()
        chain = _cmp("Eq", "x", 1)  # 0.1
        for _ in range(10):
            chain = _and(chain, _cmp("Eq", "x", 1))
        s = m.estimate_selectivity(chain)
        assert 0.0 <= s <= 1.0

    def test_or_clamped_le_one(self):
        # OR of high-selectivity predicates approaches but never exceeds 1.0.
        m = _bare_manifest()
        chain = _cmp("NotEq", "x", 1)  # 0.9
        for _ in range(10):
            chain = _or(chain, _cmp("NotEq", "x", 1))
        s = m.estimate_selectivity(chain)
        assert 0.0 <= s <= 1.0


# ---------------------------------------------------------------------------
# Defensive behaviour
# ---------------------------------------------------------------------------


class TestDefensive:
    def test_unknown_node_returns_one(self):
        m = _bare_manifest()
        node = Node(node_type=NodeType.FUNCTION)
        node.value = "FOO"
        assert m.estimate_selectivity(node) == 1.0

    def test_swapped_operands(self):
        # `1 < x` should be treated like `x > 1` → 0.25 fallback (same as Gt).
        m = _bare_manifest()
        node = Node(node_type=NodeType.COMPARISON_OPERATOR)
        node.value = "Lt"
        node.left = _literal(1)
        node.right = _identifier("x")
        s = m.estimate_selectivity(node)
        assert s == 0.25

    def test_returns_float_in_unit_interval(self):
        m = _histogram_manifest()
        for op, val in [("Eq", 50), ("Lt", 50), ("Gt", 50), ("LtEq", 50), ("GtEq", 50)]:
            s = m.estimate_selectivity(_cmp(op, "x", val))
            assert 0.0 <= s <= 1.0
