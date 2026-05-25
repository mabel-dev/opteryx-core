"""
Native unit + property tests for the DRAKEN_NULL type (D.11).

Design contract (06_value_encoding.md):
  - type == NULL ⟹ every row null; no data buffer, no validity buffer.
  - Self-describing: readers short-circuit on the type tag.
  - Ops: hash → null-hash sentinel; compare/between/in_list → all-NULL bool (3VL);
         take/materialize → still null; compress → empty null; reductions → 0 / raise.

All tests import draken.draken_native directly; no import opteryx.
"""

import pytest
from hypothesis import given, settings, HealthCheck
import hypothesis.strategies as st

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def null_vec(length):
    return dn.vector_null_from_length(length)


def bool_pylist(v):
    return v.to_pylist()


# ===========================================================================
# 1. Construction and identity
# ===========================================================================

class TestNullConstruction:
    def test_type_tag(self):
        assert null_vec(5).type == dn.DrakenType.NULL

    def test_length(self):
        assert len(null_vec(10)) == 10

    def test_zero_length(self):
        v = null_vec(0)
        assert len(v) == 0
        assert v.type == dn.DrakenType.NULL

    def test_one_length(self):
        v = null_vec(1)
        assert len(v) == 1

    def test_large_length(self):
        v = null_vec(10000)
        assert len(v) == 10000


# ===========================================================================
# 2. Readback — all rows are None
# ===========================================================================

class TestNullReadback:
    def test_getitem_all_none(self):
        v = null_vec(5)
        for i in range(5):
            assert v[i] is None

    def test_getitem_negative_index(self):
        v = null_vec(3)
        assert v[-1] is None
        assert v[-3] is None

    def test_getitem_out_of_range(self):
        v = null_vec(3)
        with pytest.raises(IndexError):
            _ = v[3]
        with pytest.raises(IndexError):
            _ = v[-4]

    def test_to_pylist_all_none(self):
        v = null_vec(4)
        assert v.to_pylist() == [None, None, None, None]

    def test_to_pylist_empty(self):
        assert null_vec(0).to_pylist() == []

    @given(st.integers(min_value=0, max_value=200))
    @settings(suppress_health_check=[HealthCheck.too_slow])
    def test_to_pylist_all_none_property(self, n):
        assert null_vec(n).to_pylist() == [None] * n


# ===========================================================================
# 3. Hash — null-hash sentinel
# ===========================================================================

class TestNullHash:
    def test_hash_length(self):
        v = null_vec(5)
        assert len(v.hash()) == 5

    def test_hash_consistent(self):
        h1 = null_vec(5).hash()
        h2 = null_vec(5).hash()
        assert h1 == h2

    def test_hash_matches_null_rows_in_int_vector(self):
        """NULL type hash must equal the hash of null rows in a typed vector."""
        null_type_hash = null_vec(1).hash()[0]
        # Build an all-null int64 vector and check its hash is the same.
        int_vec = dn.vector_from_sequence([None])
        int_null_hash = int_vec.hash()[0]
        assert null_type_hash == int_null_hash

    def test_hash_empty(self):
        assert null_vec(0).hash() == []


# ===========================================================================
# 4. Compare / between / in_list → all-null bool (3VL)
# ===========================================================================

class TestNullCompareOps:
    EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

    def test_compare_scalar_all_null(self):
        v = null_vec(4)
        for op in (self.EQ, self.NE, self.GT, self.GE, self.LT, self.LE):
            result = bool_pylist(v.compare_scalar(42, op))
            assert result == [None, None, None, None], f"op={op}"

    def test_compare_scalar_any_scalar_all_null(self):
        # DRAKEN_NULL returns all-null regardless of scalar value (3VL: NULL OP x = NULL).
        result = bool_pylist(null_vec(3).compare_scalar(42, self.EQ))
        assert result == [None, None, None]

    def test_compare_vector_null_null(self):
        a = null_vec(3)
        b = null_vec(3)
        result = bool_pylist(a.compare_vector(b, self.EQ))
        assert result == [None, None, None]

    def test_compare_vector_null_typed(self):
        a = null_vec(3)
        b = dn.vector_from_sequence([1, 2, 3])
        result = bool_pylist(a.compare_vector(b, self.EQ))
        assert result == [None, None, None]

    def test_between_all_null(self):
        result = bool_pylist(null_vec(3).between(0, 10))
        assert result == [None, None, None]

    def test_in_list_all_null(self):
        result = bool_pylist(null_vec(3).in_list([1, 2, 3]))
        assert result == [None, None, None]


# ===========================================================================
# 5. take / materialize / compress
# ===========================================================================

class TestNullGatherOps:
    def test_take_returns_null(self):
        result = null_vec(5).take([0, 2, 4])
        assert result.type == dn.DrakenType.NULL
        assert len(result) == 3
        assert result.to_pylist() == [None, None, None]

    def test_take_empty(self):
        result = null_vec(5).take([])
        assert result.type == dn.DrakenType.NULL
        assert len(result) == 0

    def test_materialize_returns_null(self):
        result = null_vec(5).materialize()
        assert result.type == dn.DrakenType.NULL
        assert len(result) == 5
        assert result.to_pylist() == [None] * 5

    def test_compress_returns_empty_null(self):
        result = null_vec(5).compress()
        assert result.type == dn.DrakenType.NULL
        assert len(result) == 0

    def test_compress_empty_null(self):
        result = null_vec(0).compress()
        assert result.type == dn.DrakenType.NULL
        assert len(result) == 0


# ===========================================================================
# 6. Reductions
# ===========================================================================

class TestNullReductions:
    def test_sum_all_null_is_zero(self):
        assert null_vec(5).sum() == 0

    def test_sum_empty_is_zero(self):
        assert null_vec(0).sum() == 0

    def test_min_raises(self):
        with pytest.raises((ValueError, Exception)):
            null_vec(5).min()

    def test_max_raises(self):
        with pytest.raises((ValueError, Exception)):
            null_vec(5).max()
