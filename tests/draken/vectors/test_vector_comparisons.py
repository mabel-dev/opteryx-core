"""Tests for vector-vector comparison operations.

This module tests the vector-vector and vector-scalar comparison operations
for Int64Vector and Float64Vector via compare_vector(other, op) /
compare_scalar(scalar, op), where op is 0=eq 1=ne 2=gt 3=ge 4=lt 5=le. Vector
has no equals_vector/not_equals_vector/greater_than_vector/etc. — those are
all the single compare_vector/compare_scalar entry point, dispatched by op
code (see draken/draken_native.cpp).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5


class TestInt64VectorComparisons:
    """Test Int64Vector vector-vector and vector-scalar comparison operations."""

    def test_equals_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, EQ)
        assert result.to_pylist() == [True, False, True, False, False]

    def test_not_equals_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, NE)
        assert result.to_pylist() == [False, True, False, True, True]

    def test_greater_than_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, GT)
        assert result.to_pylist() == [False, False, False, True, False]

    def test_greater_than_or_equals_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, GE)
        assert result.to_pylist() == [True, False, True, True, False]

    def test_less_than_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, LT)
        assert result.to_pylist() == [False, True, False, False, True]

    def test_less_than_or_equals_vector(self):
        vec1 = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 3, 3, 2, 6], dtype=DrakenType.INT64)

        result = vec1.compare_vector(vec2, LE)
        assert result.to_pylist() == [True, True, True, False, True]

    def test_vector_length_mismatch(self):
        vec1 = vector_from_sequence([1, 2, 3], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 2], dtype=DrakenType.INT64)

        with pytest.raises(ValueError, match="lengths must match"):
            vec1.compare_vector(vec2, EQ)

    def test_scalar_comparisons_still_work(self):
        vec = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)

        assert vec.compare_scalar(3, EQ).to_pylist() == [False, False, True, False, False]
        assert vec.compare_scalar(3, NE).to_pylist() == [True, True, False, True, True]
        assert vec.compare_scalar(3, GT).to_pylist() == [False, False, False, True, True]
        assert vec.compare_scalar(3, LT).to_pylist() == [True, True, False, False, False]


class TestFloat64VectorComparisons:
    """Test Float64Vector vector-vector and vector-scalar comparison operations."""

    def test_equals_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, EQ)
        assert result.to_pylist() == [True, False, True, False, False]

    def test_not_equals_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, NE)
        assert result.to_pylist() == [False, True, False, True, True]

    def test_greater_than_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, GT)
        assert result.to_pylist() == [False, False, False, True, False]

    def test_greater_than_or_equals_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, GE)
        assert result.to_pylist() == [True, False, True, True, False]

    def test_less_than_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, LT)
        assert result.to_pylist() == [False, True, False, False, True]

    def test_less_than_or_equals_vector(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 3.0, 3.3, 2.0, 6.0], dtype=DrakenType.FLOAT64)

        result = vec1.compare_vector(vec2, LE)
        assert result.to_pylist() == [True, True, True, False, True]

    def test_vector_length_mismatch(self):
        vec1 = vector_from_sequence([1.5, 2.7, 3.3], dtype=DrakenType.FLOAT64)
        vec2 = vector_from_sequence([1.5, 2.7], dtype=DrakenType.FLOAT64)

        with pytest.raises(ValueError, match="lengths must match"):
            vec1.compare_vector(vec2, EQ)

    def test_scalar_comparisons_still_work(self):
        vec = vector_from_sequence([1.5, 2.7, 3.3, 4.1, 5.9], dtype=DrakenType.FLOAT64)

        assert vec.compare_scalar(3.3, EQ).to_pylist() == [False, False, True, False, False]
        assert vec.compare_scalar(3.3, NE).to_pylist() == [True, True, False, True, True]
        assert vec.compare_scalar(3.3, GT).to_pylist() == [False, False, False, True, True]
        assert vec.compare_scalar(3.3, LT).to_pylist() == [True, True, False, False, False]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
