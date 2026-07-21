"""
Tests for error conditions and edge cases across vector operations.

This module tests:
- Type mismatches
- Invalid indices
- Empty vectors
- Boundary conditions
- Invalid operations

Vectors are built with vector_from_sequence(values, dtype) — the current,
supported "Python list -> Vector" entry point (see
draken/interop/vector_sequence.py). Vector has no
equals_vector/and_vector/or_vector/xor_vector/equals/is_null()/null_count —
those are compare_vector(other, op)/compare_scalar(scalar, op) (op: 0=eq
1=ne 2=gt 3=ge 4=lt 5=le), bool_and/bool_or/bool_not, bool_any/bool_all, and
is_null_at(i). There is no bool_xor.
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5


class TestVectorLengthMismatch:
    """Test error handling for vector length mismatches."""

    def test_int64_vector_comparison_length_mismatch(self):
        """Test vector-vector comparison with different lengths."""
        vec1 = vector_from_sequence([1, 2, 3], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 2], dtype=DrakenType.INT64)

        with pytest.raises(ValueError, match="length"):
            vec1.compare_vector(vec2, EQ)

    def test_float64_vector_comparison_length_mismatch(self):
        """Test float vector-vector comparison with different lengths."""
        vec1 = vector_from_sequence([1.0, 2.0, 3.0], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.0, 2.0], dtype=DrakenType.FLOAT64)

        with pytest.raises(ValueError, match="length"):
            vec1.compare_vector(vec2, EQ)

    def test_bool_vector_and_length_mismatch(self):
        """Test boolean AND operation with different lengths."""
        vec1 = vector_from_sequence([True, False, True], dtype=DrakenType.BOOL)
        vec2 = vector_from_sequence([True, False], dtype=DrakenType.BOOL)

        with pytest.raises(ValueError, match="length"):
            vec1.bool_and(vec2)

    def test_bool_vector_or_length_mismatch(self):
        """Test boolean OR operation with different lengths."""
        vec1 = vector_from_sequence([True, False], dtype=DrakenType.BOOL)
        vec2 = vector_from_sequence([True, False, True], dtype=DrakenType.BOOL)

        with pytest.raises(ValueError, match="length"):
            vec1.bool_or(vec2)


class TestEmptyVectorOperations:
    """Test operations on empty vectors."""

    def test_empty_int64_min_raises(self):
        """Test that min on empty Int64Vector raises error."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_empty_int64_max_raises(self):
        """Test that max on empty Int64Vector raises error."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        with pytest.raises(ValueError, match="empty"):
            vec.max()

    def test_empty_float64_min_raises(self):
        """Test that min on empty Float64Vector raises error."""
        vec = vector_from_sequence([], dtype=DrakenType.FLOAT64)
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_empty_float64_max_raises(self):
        """Test that max on empty Float64Vector raises error."""
        vec = vector_from_sequence([], dtype=DrakenType.FLOAT64)
        with pytest.raises(ValueError, match="empty"):
            vec.max()

    def test_empty_vector_sum(self):
        """Test sum on empty vector returns 0."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        assert vec.sum() == 0

    def test_empty_vector_comparisons(self):
        """Test comparisons on empty vector."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        result = vec.compare_scalar(5, EQ)
        assert result.to_pylist() == []

    def test_empty_vector_take(self):
        """Test take on empty vector with empty indices."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        result = vec.take([])
        assert result.length == 0

    def test_empty_bool_vector_any(self):
        """Test bool_any() on empty BoolVector -> False (vacuous)."""
        vec = vector_from_sequence([], dtype=DrakenType.BOOL)
        assert vec.bool_any() is False

    def test_empty_bool_vector_all(self):
        """Test bool_all() on empty BoolVector -> True (vacuous truth)."""
        vec = vector_from_sequence([], dtype=DrakenType.BOOL)
        assert vec.bool_all() is True


class TestInvalidIndices:
    """Test error handling for invalid indices in take operations."""

    def test_take_duplicate_indices(self):
        """Test take with duplicate indices."""
        vec = vector_from_sequence([1, 2, 3], dtype=DrakenType.INT64)
        result = vec.take([0, 0, 1, 1])
        # Should return duplicated values
        assert result.length == 4
        assert result.to_pylist() == [1, 1, 2, 2]


class TestTypeSpecificErrors:
    """Test type-specific error conditions."""

    def test_string_equals_wrong_type(self):
        """Test string comparison requires a bytes scalar."""
        vec = vector_from_sequence(["hello", "world"], dtype=DrakenType.VARCHAR)

        # Should work with bytes
        result = vec.compare_scalar(b"hello", EQ)
        assert result.to_pylist() == [True, False]

        # A str scalar (not bytes) is rejected — VARCHAR comparison requires bytes.
        with pytest.raises(ValueError, match="bytes"):
            vec.compare_scalar("hello", EQ)

    def test_all_nulls_min_max(self):
        """Test min/max on vector with all nulls raise (no value to return)."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.INT64)

        with pytest.raises(ValueError, match="all-null"):
            vec.min()
        with pytest.raises(ValueError, match="all-null"):
            vec.max()


class TestBoundaryConditions:
    """Test boundary conditions for various operations."""

    def test_single_element_operations(self):
        """Test operations on single-element vector."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)

        assert vec.sum() == 42
        assert vec.min() == 42
        assert vec.max() == 42
        assert vec.compare_scalar(42, EQ).to_pylist() == [True]
        assert vec.is_null_at(0) is False

    def test_two_element_comparisons(self):
        """Test comparisons on two-element vector."""
        vec = vector_from_sequence([1, 2], dtype=DrakenType.INT64)

        assert vec.compare_scalar(2, LT).to_pylist() == [True, False]
        assert vec.compare_scalar(1, GT).to_pylist() == [False, True]

    def test_vector_vector_empty_comparison(self):
        """Test vector-vector comparison with empty vectors."""
        vec1 = vector_from_sequence([], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([], dtype=DrakenType.INT64)

        # Should not raise, just return empty
        result = vec1.compare_vector(vec2, EQ)
        assert result.length == 0

    def test_large_vector_operations(self):
        """Test operations on large vectors."""
        size = 10000
        vec = vector_from_sequence(list(range(size)), dtype=DrakenType.INT64)

        assert vec.length == size
        assert vec.sum() == sum(range(size))
        assert vec.min() == 0
        assert vec.max() == size - 1

    def test_max_int64_value(self):
        """Test with maximum int64 values."""
        max_val = 2**63 - 1
        vec = vector_from_sequence([max_val, max_val - 1, max_val - 2], dtype=DrakenType.INT64)

        assert vec.max() == max_val
        assert vec.min() == max_val - 2

    def test_min_int64_value(self):
        """Test with minimum int64 values."""
        min_val = -(2**63)
        vec = vector_from_sequence([min_val, min_val + 1, min_val + 2], dtype=DrakenType.INT64)

        assert vec.min() == min_val
        assert vec.max() == min_val + 2


class TestComparisonEdgeCases:
    """Test edge cases in comparison operations."""

    def test_equals_all_match(self):
        """Test equals when all values match."""
        vec = vector_from_sequence([5, 5, 5, 5], dtype=DrakenType.INT64)
        result = vec.compare_scalar(5, EQ)
        assert result.to_pylist() == [True, True, True, True]

    def test_equals_none_match(self):
        """Test equals when no values match."""
        vec = vector_from_sequence([1, 2, 3, 4], dtype=DrakenType.INT64)
        result = vec.compare_scalar(5, EQ)
        assert result.to_pylist() == [False, False, False, False]

    def test_vector_vector_identical(self):
        """Test vector-vector comparison with identical vectors."""
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, EQ)
        assert result.to_pylist() == [True, True, True, True, True]

    def test_vector_vector_all_different(self):
        """Test vector-vector comparison with completely different values."""
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([6, 7, 8, 9, 10], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, EQ)
        assert result.to_pylist() == [False, False, False, False, False]


class TestFloatSpecialValues:
    """Test handling of special float values."""

    def test_float_infinity(self):
        """Test operations with infinity values."""
        vec = vector_from_sequence([float("inf"), 1.0, -float("inf"), 2.0], dtype=DrakenType.FLOAT64)

        assert vec.max() == float("inf")
        assert vec.min() == -float("inf")

    def test_float_nan_handling(self):
        """Test operations with NaN values."""
        vec = vector_from_sequence([1.0, float("nan"), 3.0], dtype=DrakenType.FLOAT64)

        result = vec.to_pylist()
        assert result[0] == 1.0
        assert math.isnan(result[1])
        assert result[2] == 3.0

    def test_float_very_small_values(self):
        """Test operations with very small float values."""
        vec = vector_from_sequence([1e-100, 1e-200, 1e-300], dtype=DrakenType.FLOAT64)

        assert vec.length == 3
        # Just verify operations don't crash
        vec.sum()
        vec.min()
        vec.max()


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
