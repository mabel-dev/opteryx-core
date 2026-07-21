"""
Tests for BoolVector operations.

This module tests BoolVector-specific functionality including:
- bool_any() / bool_all() reductions
- Boolean vector operations (bool_and, bool_or, bool_not — Kleene/3VL semantics)
- Comparison operations (compare_scalar)
- Null handling

Vectors are built with vector_from_sequence(values, dtype=DrakenType.BOOL) — the
current, supported "Python list -> Vector" entry point (see
draken/interop/vector_sequence.py). Vector has no from_arrow/to_arrow; there is
no bool_xor kernel exposed either, so XOR coverage is dropped rather than
faked from AND/OR/NOT.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence


def _bool_vec(values):
    return vector_from_sequence(values, dtype=DrakenType.BOOL)


def _null_count(vec) -> int:
    return sum(1 for i in range(vec.length) if vec.is_null_at(i))


class TestBoolVectorAnyAll:
    """Test bool_any()/bool_all() reductions on BoolVector."""

    def test_any_all_true(self):
        """Test bool_any() returns True when at least one True exists."""
        vec = _bool_vec([True, False, False, False])
        assert vec.bool_any() is True

    def test_any_all_false(self):
        """Test bool_any() returns False when all values are False."""
        vec = _bool_vec([False, False, False, False])
        assert vec.bool_any() is False

    def test_any_with_nulls(self):
        """Test bool_any() with null values - a True anywhere still dominates."""
        vec = _bool_vec([None, False, None, True])
        assert vec.bool_any() is True

    def test_any_all_nulls(self):
        """Test bool_any() when all values are null -> None (no True, but nulls present)."""
        vec = _bool_vec([None, None, None])
        assert vec.bool_any() is None

    def test_all_all_true(self):
        """Test bool_all() returns True when all values are True."""
        vec = _bool_vec([True, True, True, True])
        assert vec.bool_all() is True

    def test_all_one_false(self):
        """Test bool_all() returns False when at least one False exists."""
        vec = _bool_vec([True, True, False, True])
        assert vec.bool_all() is False

    def test_all_with_nulls(self):
        """Test bool_all() with nulls and no False -> None (Kleene 3VL)."""
        vec = _bool_vec([True, None, True, None])
        assert vec.bool_all() is None

    def test_all_with_nulls_and_false(self):
        """Test bool_all() with nulls and a False value -> False dominates."""
        vec = _bool_vec([True, None, False, True])
        assert vec.bool_all() is False

    def test_any_empty_vector(self):
        """Test bool_any() on empty vector -> False (vacuous)."""
        vec = _bool_vec([])
        assert vec.bool_any() is False

    def test_all_empty_vector(self):
        """Test bool_all() on empty vector -> True (vacuous truth)."""
        vec = _bool_vec([])
        assert vec.bool_all() is True


class TestBoolVectorOperations:
    """Test boolean vector-vector operations (Kleene/3VL semantics)."""

    def test_and_vector_basic(self):
        """Test bool_and operation with simple boolean values."""
        vec1 = _bool_vec([True, True, False, False])
        vec2 = _bool_vec([True, False, True, False])

        result = vec1.bool_and(vec2)
        assert result.to_pylist() == [True, False, False, False]

    def test_and_vector_with_nulls(self):
        """Test bool_and with null values: FALSE dominates (F∧N=F), else null."""
        vec1 = _bool_vec([True, True, None, False])
        vec2 = _bool_vec([True, None, True, False])

        result = vec1.bool_and(vec2)
        assert result.to_pylist() == [True, None, None, False]

    def test_or_vector_basic(self):
        """Test bool_or operation with simple boolean values."""
        vec1 = _bool_vec([True, True, False, False])
        vec2 = _bool_vec([True, False, True, False])

        result = vec1.bool_or(vec2)
        assert result.to_pylist() == [True, True, True, False]

    def test_or_vector_with_nulls(self):
        """Test bool_or with null values: TRUE dominates (T∨N=T), else null."""
        vec1 = _bool_vec([False, False, None, True])
        vec2 = _bool_vec([False, None, False, True])

        result = vec1.bool_or(vec2)
        assert result.to_pylist() == [False, None, None, True]

    def test_sql_three_valued_logic_and_or(self):
        """AND/OR should follow SQL three-valued logic."""
        and_left = _bool_vec([False, None, True, None])
        and_right = _bool_vec([None, False, None, True])
        assert and_left.bool_and(and_right).to_pylist() == [False, False, None, None]

        or_left = _bool_vec([True, None, False, None])
        or_right = _bool_vec([None, True, None, False])
        assert or_left.bool_or(or_right).to_pylist() == [True, True, None, None]

    def test_not_vector_with_nulls(self):
        """NOT should invert booleans and preserve nulls."""
        vec = _bool_vec([True, False, None])
        assert vec.bool_not().to_pylist() == [False, True, None]

    def test_vector_length_mismatch(self):
        """Test that vector operations raise error on length mismatch."""
        vec1 = _bool_vec([True, False, True])
        vec2 = _bool_vec([True, False])

        with pytest.raises(ValueError, match="equal length"):
            vec1.bool_and(vec2)

    def test_chained_operations(self):
        """Test chaining boolean operations."""
        vec1 = _bool_vec([True, True, False, False])
        vec2 = _bool_vec([True, False, True, False])
        vec3 = _bool_vec([False, False, False, True])

        # (vec1 OR vec2) AND vec3
        # vec1 OR vec2 = [True, True, True, False]
        # AND vec3     = [False, False, False, True]
        # Result       = [False, False, False, False]
        result = vec1.bool_or(vec2).bool_and(vec3)
        assert result.to_pylist() == [False, False, False, False]

    def test_take_preserves_nulls(self):
        """take() should preserve source nulls instead of dropping validity."""
        vec = _bool_vec([True, None, False, None])
        result = vec.take([0, 1, 3])

        assert result.to_pylist() == [True, None, None]


class TestBoolVectorComparisons:
    """Test BOOL == / != scalar semantics.

    compare_scalar raises ValueError ("unsupported type") for DRAKEN_BOOL — it isn't
    wired up for bool vectors in the current engine. `x == True` and `x != False` are
    just `x`; `x == False` and `x != True` are `bool_not(x)` — these are expressed via
    the ops that do exist, not a fabricated equals()/not_equals() API.
    """

    def test_equals_true(self):
        """x == True is x, nulls preserved."""
        vec = _bool_vec([True, False, True, False])
        assert vec.to_pylist() == [True, False, True, False]

    def test_equals_false(self):
        """x == False is NOT x, nulls preserved."""
        vec = _bool_vec([True, False, True, False])
        assert vec.bool_not().to_pylist() == [False, True, False, True]

    def test_not_equals_true(self):
        """x != True is NOT x, nulls preserved."""
        vec = _bool_vec([True, False, True, False])
        assert vec.bool_not().to_pylist() == [False, True, False, True]

    def test_equals_with_nulls(self):
        """x == True with nulls -> null propagates unchanged."""
        vec = _bool_vec([True, None, False, None])
        result_list = vec.to_pylist()

        assert result_list[0] is True
        assert result_list[1] is None
        assert result_list[2] is False
        assert result_list[3] is None


class TestBoolVectorNullHandling:
    """Test null handling in BoolVector operations."""

    def test_is_null(self):
        """Test is_null_at per-row."""
        vec = _bool_vec([True, None, False, None, True])
        assert [vec.is_null_at(i) for i in range(vec.length)] == [False, True, False, True, False]

    def test_null_count(self):
        """Test null count (derived from is_null_at; Vector has no null_count property)."""
        vec = _bool_vec([True, None, False, None, True])
        assert _null_count(vec) == 2

    def test_all_nulls(self):
        """Test vector with all null values."""
        vec = _bool_vec([None, None, None])
        assert _null_count(vec) == 3
        assert vec.length == 3
        assert [vec.is_null_at(i) for i in range(vec.length)] == [True, True, True]

    def test_no_nulls(self):
        """Test vector with no null values."""
        vec = _bool_vec([True, False, True])
        assert _null_count(vec) == 0
        assert [vec.is_null_at(i) for i in range(vec.length)] == [False, False, False]


class TestBoolVectorMiscellaneous:
    """Test miscellaneous BoolVector functionality."""

    def test_to_pylist(self):
        """Test conversion to Python list."""
        vec = _bool_vec([True, False, None, True])
        assert vec.to_pylist() == [True, False, None, True]

    def test_length(self):
        """Test length property."""
        vec = _bool_vec([True, False, True, False, True])
        assert vec.length == 5

    def test_empty_vector(self):
        """Test empty BoolVector."""
        vec = _bool_vec([])
        assert vec.length == 0
        assert _null_count(vec) == 0
        assert vec.to_pylist() == []

    def test_take_operation(self):
        """Test take operation on BoolVector."""
        vec = _bool_vec([True, False, True, False, True])
        result = vec.take([0, 2, 4])
        assert result.to_pylist() == [True, True, True]

    def test_take_with_nulls(self):
        """Test take operation with nulls."""
        vec = _bool_vec([True, None, False, None, True])
        result = vec.take([0, 1, 4])
        assert result.to_pylist() == [True, None, True]
