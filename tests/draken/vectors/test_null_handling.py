"""
Tests for null handling across all vector types.

This module tests comprehensive null handling including:
- Null detection (is_null_at, per-row)
- Null counts (derived from is_null_at; Vector has no null_count property)
- Operations with nulls
- Edge cases (all nulls, no nulls, mixed)

Vectors are built with vector_from_sequence(values, dtype) — the current,
supported "Python list -> Vector" entry point (see
draken/interop/vector_sequence.py). Vector has no from_arrow/to_arrow, no
is_null()/null_count/equals()/equals_vector()/and_vector() — those are
is_null_at(i), compare_scalar(value, op)/compare_vector(other, op)
(op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le), and bool_and/bool_or/bool_not.
"""

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

EQ = 0


def _null_mask(vec):
    return [vec.is_null_at(i) for i in range(vec.length)]


def _null_count(vec) -> int:
    return sum(1 for i in range(vec.length) if vec.is_null_at(i))


class TestInt64NullHandling:
    """Test null handling in Int64Vector."""

    def test_is_null_basic(self):
        """Test is_null_at per-row."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_null_count(self):
        """Test null count."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        assert _null_count(vec) == 2

    def test_no_nulls(self):
        """Test vector with no nulls."""
        vec = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        assert _null_count(vec) == 0
        assert _null_mask(vec) == [False] * 5

    def test_all_nulls(self):
        """Test vector with all nulls."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.INT64)
        assert _null_count(vec) == 3
        assert _null_mask(vec) == [True, True, True]

    def test_comparison_with_nulls(self):
        """Test compare_scalar propagates nulls (SQL 3VL)."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        result_list = vec.compare_scalar(3, EQ).to_pylist()

        assert result_list[0] is False  # 1 != 3
        assert result_list[1] is None  # null
        assert result_list[2] is True  # 3 == 3
        assert result_list[3] is None  # null
        assert result_list[4] is False  # 5 != 3

    def test_take_preserves_nulls(self):
        """Test that take() on Int64Vector preserves nulls."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        result = vec.take([0, 1, 3])
        assert result.to_pylist() == [1, None, None]

    def test_to_pylist_with_nulls(self):
        """Test to_pylist preserves None values."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        assert vec.to_pylist() == [1, None, 3, None, 5]


class TestFloat64NullHandling:
    """Test null handling in Float64Vector."""

    def test_is_null_basic(self):
        """Test is_null_at per-row."""
        vec = vector_from_sequence([1.5, None, 3.5, None, 5.5], dtype=DrakenType.FLOAT64)
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_null_count(self):
        """Test null count."""
        vec = vector_from_sequence([1.5, None, 3.5, None, 5.5], dtype=DrakenType.FLOAT64)
        assert _null_count(vec) == 2

    def test_aggregations_skip_nulls(self):
        """sum() treats null as 0 in its running total; min()/max() skip nulls entirely."""
        vec = vector_from_sequence([1.0, None, 3.0, None, 5.0], dtype=DrakenType.FLOAT64)

        assert vec.sum() == pytest.approx(9.0)  # 1 + 0 + 3 + 0 + 5
        assert vec.min() == pytest.approx(1.0)  # nulls excluded, not treated as 0
        assert vec.max() == pytest.approx(5.0)

    def test_all_nulls_aggregations(self):
        """sum() of an all-null vector is 0; min()/max() have no value to return."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.FLOAT64)

        assert vec.sum() == pytest.approx(0.0)
        with pytest.raises(ValueError, match="all-null"):
            vec.min()
        with pytest.raises(ValueError, match="all-null"):
            vec.max()

    def test_take_preserves_nulls(self):
        """Test that take() preserves null values."""
        vec = vector_from_sequence([1.5, None, 3.5, None, 5.5], dtype=DrakenType.FLOAT64)
        result = vec.take([0, 1, 3])

        assert result.to_pylist() == [1.5, None, None]
        assert _null_count(result) == 2


class TestStringNullHandling:
    """Test null handling in StringVector."""

    def test_is_null_basic(self):
        """Test is_null_at on strings."""
        vec = vector_from_sequence(["hello", None, "world", None, "test"], dtype=DrakenType.VARCHAR)
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_null_count(self):
        """Test null count."""
        vec = vector_from_sequence(["hello", None, "world", None, "test"], dtype=DrakenType.VARCHAR)
        assert _null_count(vec) == 2

    def test_to_pylist_with_nulls(self):
        """Test to_pylist preserves None values (VARCHAR decodes to str)."""
        vec = vector_from_sequence(["hello", None, "world", None, "test"], dtype=DrakenType.VARCHAR)
        assert vec.to_pylist() == ["hello", None, "world", None, "test"]

    def test_take_preserves_nulls(self):
        """Test that take() preserves null values."""
        vec = vector_from_sequence(["hello", None, "world", None, "test"], dtype=DrakenType.VARCHAR)
        result = vec.take([0, 1, 3])

        assert result.to_pylist() == ["hello", None, None]
        assert _null_count(result) == 2

    def test_equals_with_nulls(self):
        """Test compare_scalar (equality) with nulls."""
        vec = vector_from_sequence(["hello", None, "world", None, "hello"], dtype=DrakenType.VARCHAR)
        result_list = vec.compare_scalar(b"hello", EQ).to_pylist()

        assert result_list[0] is True
        assert result_list[2] is False
        assert result_list[4] is True


class TestBoolNullHandling:
    """Test null handling in BoolVector."""

    def test_is_null_basic(self):
        """Test is_null_at per-row."""
        vec = vector_from_sequence([True, None, False, None, True], dtype=DrakenType.BOOL)
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_null_count(self):
        """Test null count."""
        vec = vector_from_sequence([True, None, False, None, True], dtype=DrakenType.BOOL)
        assert _null_count(vec) == 2

    def test_any_with_only_nulls(self):
        """Test bool_any() when all values are null -> None (Kleene 3VL, not False)."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.BOOL)
        assert vec.bool_any() is None

    def test_all_with_only_nulls(self):
        """Test bool_all() when all values are null -> None (Kleene 3VL, not False)."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.BOOL)
        assert vec.bool_all() is None

    def test_boolean_ops_with_nulls(self):
        """Test bool_and handles nulls (Kleene 3VL) without crashing."""
        vec1 = vector_from_sequence([True, None, False, True], dtype=DrakenType.BOOL)
        vec2 = vector_from_sequence([True, True, None, False], dtype=DrakenType.BOOL)

        result = vec1.bool_and(vec2)
        assert result.length == 4
        assert result.to_pylist() == [True, None, False, False]


class TestTemporalNullHandling:
    """Test null handling in temporal vectors (Date32, Timestamp)."""

    def test_date32_null_count(self):
        """Test null count on a DATE32 vector."""
        vec = vector_from_sequence(
            [datetime.date(2020, 1, 1), None, datetime.date(2020, 1, 2), None, datetime.date(2020, 1, 3)],
            dtype=DrakenType.DATE32,
        )
        assert _null_count(vec) == 2

    def test_date32_is_null(self):
        """Test is_null_at on a DATE32 vector."""
        vec = vector_from_sequence(
            [datetime.date(2020, 1, 1), None, datetime.date(2020, 1, 2), None, datetime.date(2020, 1, 3)],
            dtype=DrakenType.DATE32,
        )
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_date32_aggregations_with_nulls(self):
        """Test min/max skip nulls on a DATE32 vector."""
        vec = vector_from_sequence(
            [datetime.date(2020, 1, 3), None, datetime.date(2020, 1, 5), None, datetime.date(2020, 1, 1)],
            dtype=DrakenType.DATE32,
        )
        assert vec.min() == datetime.date(2020, 1, 1)
        assert vec.max() == datetime.date(2020, 1, 5)

    def test_timestamp_null_count(self):
        """Test null count on a TIMESTAMP64 vector."""
        vec = vector_from_sequence(
            [datetime.datetime(2020, 1, 1), None, datetime.datetime(2020, 1, 2), None, datetime.datetime(2020, 1, 3)],
            dtype=DrakenType.TIMESTAMP64,
        )
        assert _null_count(vec) == 2

    def test_timestamp_is_null(self):
        """Test is_null_at on a TIMESTAMP64 vector."""
        vec = vector_from_sequence(
            [datetime.datetime(2020, 1, 1), None, datetime.datetime(2020, 1, 2), None, datetime.datetime(2020, 1, 3)],
            dtype=DrakenType.TIMESTAMP64,
        )
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_timestamp_aggregations_with_nulls(self):
        """Test min/max skip nulls on a TIMESTAMP64 vector."""
        vec = vector_from_sequence(
            [datetime.datetime(2020, 1, 3), None, datetime.datetime(2020, 1, 5), None, datetime.datetime(2020, 1, 1)],
            dtype=DrakenType.TIMESTAMP64,
        )
        assert vec.min() == datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
        assert vec.max() == datetime.datetime(2020, 1, 5, tzinfo=datetime.timezone.utc)


class TestNullHandlingEdgeCases:
    """Test edge cases in null handling."""

    def test_empty_vector_null_count(self):
        """Test null count on empty vectors."""
        for dtype in (DrakenType.INT64, DrakenType.FLOAT64, DrakenType.BOOL):
            vec = vector_from_sequence([], dtype=dtype)
            assert _null_count(vec) == 0

    def test_single_null_value(self):
        """Test vector with single null value."""
        vec = vector_from_sequence([None], dtype=DrakenType.INT64)

        assert vec.length == 1
        assert _null_count(vec) == 1
        assert _null_mask(vec) == [True]

    def test_single_non_null_value(self):
        """Test vector with single non-null value."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)

        assert vec.length == 1
        assert _null_count(vec) == 0
        assert _null_mask(vec) == [False]

    def test_alternating_nulls(self):
        """Test vector with alternating null/non-null pattern."""
        vec = vector_from_sequence([1, None, 2, None, 3, None, 4, None], dtype=DrakenType.INT64)

        assert _null_count(vec) == 4
        assert _null_mask(vec) == [False, True, False, True, False, True, False, True]

    def test_consecutive_nulls(self):
        """Test vector with consecutive null values."""
        vec = vector_from_sequence([1, 2, None, None, None, 3, 4], dtype=DrakenType.INT64)

        assert _null_count(vec) == 3
        assert _null_mask(vec) == [False, False, True, True, True, False, False]

    def test_null_at_boundaries(self):
        """Test nulls at start and end of vector."""
        vec = vector_from_sequence([None, 1, 2, 3, None], dtype=DrakenType.INT64)

        assert _null_count(vec) == 2
        assert _null_mask(vec) == [True, False, False, False, True]

    def test_vector_vector_comparison_with_nulls(self):
        """Test vector-vector comparisons with nulls (compare_vector)."""
        vec1 = vector_from_sequence([1, None, 3, 4, None], dtype=DrakenType.INT64)
        vec2 = vector_from_sequence([1, 2, None, 4, None], dtype=DrakenType.INT64)

        result_list = vec1.compare_vector(vec2, EQ).to_pylist()

        assert result_list[0] is True  # 1 == 1
        assert result_list[3] is True  # 4 == 4


if __name__ == "__main__":
    pytest.main([__file__])
