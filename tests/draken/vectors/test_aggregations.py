"""
Tests for vector aggregation operations (sum, min, max).

This module tests aggregation operations across different vector types
with various edge cases including nulls, empty vectors, and mixed values.

Vectors are built with vector_from_sequence(values, dtype) — the current,
supported "Python list -> Vector" entry point (see
draken/interop/vector_sequence.py). Vector has no from_arrow. min()/max() are
not supported at all for VARCHAR (raises ValueError), and sum() likewise
rejects non-numeric types with ValueError ("unsupported type"), not TypeError.
DATE32/TIMESTAMP64 min()/max() return datetime.date/datetime.datetime, not
raw day/microsecond offsets.
"""

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence

EPOCH = datetime.date(1970, 1, 1)


def _date(days_since_epoch):
    return EPOCH + datetime.timedelta(days=days_since_epoch)


def _ts(us_since_epoch):
    return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(
        microseconds=us_since_epoch
    )


class TestInt64Aggregations:
    """Test aggregation operations on Int64Vector."""

    def test_sum_basic(self):
        """Test basic sum operation."""
        vec = vector_from_sequence([1, 2, 3, 4, 5], dtype=DrakenType.INT64)
        assert vec.sum() == 15

    def test_sum_with_nulls(self):
        """Test sum with null values - nulls should be ignored."""
        vec = vector_from_sequence([1, None, 3, None, 5], dtype=DrakenType.INT64)
        # Sum should ignore nulls: 1 + 3 + 5 = 9
        assert vec.sum() == 9

    def test_sum_all_nulls(self):
        """Test sum when all values are null."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.INT64)
        # Sum of all nulls should be 0
        assert vec.sum() == 0

    def test_sum_negative_values(self):
        """Test sum with negative values."""
        vec = vector_from_sequence([-5, -3, 2, 4, -1], dtype=DrakenType.INT64)
        assert vec.sum() == -3

    def test_sum_single_value(self):
        """Test sum with single value."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)
        assert vec.sum() == 42

    def test_min_basic(self):
        """Test basic min operation."""
        vec = vector_from_sequence([5, 2, 8, 1, 9], dtype=DrakenType.INT64)
        assert vec.min() == 1

    def test_min_with_nulls(self):
        """Test min with null values - nulls should be ignored."""
        vec = vector_from_sequence([5, None, 8, 1, None], dtype=DrakenType.INT64)
        assert vec.min() == 1

    def test_min_negative_values(self):
        """Test min with negative values."""
        vec = vector_from_sequence([5, -3, 2, -10, 4], dtype=DrakenType.INT64)
        assert vec.min() == -10

    def test_min_single_value(self):
        """Test min with single value."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)
        assert vec.min() == 42

    def test_min_empty_raises(self):
        """Test that min on empty vector raises ValueError."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_max_basic(self):
        """Test basic max operation."""
        vec = vector_from_sequence([5, 2, 8, 1, 9], dtype=DrakenType.INT64)
        assert vec.max() == 9

    def test_max_with_nulls(self):
        """Test max with null values - nulls should be ignored."""
        vec = vector_from_sequence([5, None, 8, None, 9], dtype=DrakenType.INT64)
        assert vec.max() == 9

    def test_max_negative_values(self):
        """Test max with negative values."""
        vec = vector_from_sequence([-5, -3, -2, -10, -4], dtype=DrakenType.INT64)
        assert vec.max() == -2

    def test_max_single_value(self):
        """Test max with single value."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)
        assert vec.max() == 42

    def test_max_empty_raises(self):
        """Test that max on empty vector raises ValueError."""
        vec = vector_from_sequence([], dtype=DrakenType.INT64)
        with pytest.raises(ValueError, match="empty"):
            vec.max()


class TestFloat64Aggregations:
    """Test aggregation operations on Float64Vector."""

    def test_sum_basic(self):
        """Test basic sum operation."""
        vec = vector_from_sequence([1.5, 2.5, 3.0, 4.0, 5.0], dtype=DrakenType.FLOAT64)
        assert vec.sum() == pytest.approx(16.0)

    def test_sum_with_nulls(self):
        """Test sum with null values."""
        vec = vector_from_sequence([1.5, None, 3.5, None, 5.0], dtype=DrakenType.FLOAT64)
        assert vec.sum() == pytest.approx(10.0)

    def test_sum_all_nulls(self):
        """Test sum when all values are null."""
        vec = vector_from_sequence([None, None, None], dtype=DrakenType.FLOAT64)
        assert vec.sum() == pytest.approx(0.0)

    def test_sum_negative_and_positive(self):
        """Test sum with mixed negative and positive values."""
        vec = vector_from_sequence([-1.5, 2.5, -3.0, 4.0, -1.0], dtype=DrakenType.FLOAT64)
        assert vec.sum() == pytest.approx(1.0)

    def test_sum_very_small_values(self):
        """Test sum with very small floating point values."""
        vec = vector_from_sequence([0.1, 0.2, 0.3], dtype=DrakenType.FLOAT64)
        # Use approx due to floating point arithmetic
        assert vec.sum() == pytest.approx(0.6)

    def test_min_basic(self):
        """Test basic min operation."""
        vec = vector_from_sequence([5.5, 2.2, 8.8, 1.1, 9.9], dtype=DrakenType.FLOAT64)
        assert vec.min() == pytest.approx(1.1)

    def test_min_with_nulls(self):
        """Test min with null values."""
        vec = vector_from_sequence([5.5, None, 1.1, None, 9.9], dtype=DrakenType.FLOAT64)
        assert vec.min() == pytest.approx(1.1)

    def test_min_negative_values(self):
        """Test min with negative values."""
        vec = vector_from_sequence([5.5, -3.3, 2.2, -10.1, 4.4], dtype=DrakenType.FLOAT64)
        assert vec.min() == pytest.approx(-10.1)

    def test_min_empty_raises(self):
        """Test that min on empty vector raises ValueError."""
        vec = vector_from_sequence([], dtype=DrakenType.FLOAT64)
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_max_basic(self):
        """Test basic max operation."""
        vec = vector_from_sequence([5.5, 2.2, 8.8, 1.1, 9.9], dtype=DrakenType.FLOAT64)
        assert vec.max() == pytest.approx(9.9)

    def test_max_with_nulls(self):
        """Test max with null values."""
        vec = vector_from_sequence([5.5, None, 9.9, None, 1.1], dtype=DrakenType.FLOAT64)
        assert vec.max() == pytest.approx(9.9)

    def test_max_negative_values(self):
        """Test max with all negative values."""
        vec = vector_from_sequence([-5.5, -3.3, -2.2, -10.1, -4.4], dtype=DrakenType.FLOAT64)
        assert vec.max() == pytest.approx(-2.2)

    def test_max_empty_raises(self):
        """Test that max on empty vector raises ValueError."""
        vec = vector_from_sequence([], dtype=DrakenType.FLOAT64)
        with pytest.raises(ValueError, match="empty"):
            vec.max()


class TestConstantAggregations:
    """Test aggregation helpers on constant-valued vectors."""

    def test_constant_numeric_sum(self):
        vec = vector_from_sequence([3, 3, None, 3], dtype=DrakenType.INT64)
        assert vec.sum() == 9

    def test_constant_numeric_min_max(self):
        vec = vector_from_sequence([2.5, 2.5, None], dtype=DrakenType.FLOAT64)
        assert vec.min() == pytest.approx(2.5)
        assert vec.max() == pytest.approx(2.5)

    def test_constant_string_min_max_not_supported(self):
        """min()/max() are not implemented for VARCHAR in the current engine."""
        vec = vector_from_sequence(["alpha", "alpha", None], dtype=DrakenType.VARCHAR)
        with pytest.raises(ValueError, match="unsupported type"):
            vec.min()
        with pytest.raises(ValueError, match="unsupported type"):
            vec.max()

    def test_constant_sum_rejects_non_numeric(self):
        vec = vector_from_sequence(["alpha", "alpha"], dtype=DrakenType.VARCHAR)
        with pytest.raises(ValueError, match="unsupported type"):
            vec.sum()

    def test_constant_min_all_null_raises(self):
        vec = vector_from_sequence([None, None], dtype=DrakenType.INT64)
        with pytest.raises(ValueError, match="all-null"):
            vec.min()


class TestDate32Aggregations:
    """Test aggregation operations on Date32Vector."""

    def test_min_basic(self):
        """Test basic min operation on dates."""
        vec = vector_from_sequence([_date(50), _date(10), _date(30), _date(20)], dtype=DrakenType.DATE32)
        assert vec.min() == _date(10)

    def test_min_with_nulls(self):
        """Test min with null date values."""
        vec = vector_from_sequence([_date(50), None, _date(10), None, _date(30)], dtype=DrakenType.DATE32)
        assert vec.min() == _date(10)

    def test_max_basic(self):
        """Test basic max operation on dates."""
        vec = vector_from_sequence([_date(50), _date(10), _date(30), _date(20)], dtype=DrakenType.DATE32)
        assert vec.max() == _date(50)

    def test_max_with_nulls(self):
        """Test max with null date values."""
        vec = vector_from_sequence([_date(50), None, _date(30), None, _date(10)], dtype=DrakenType.DATE32)
        assert vec.max() == _date(50)


class TestTimestampAggregations:
    """Test aggregation operations on TimestampVector."""

    def test_min_basic(self):
        """Test basic min operation on timestamps."""
        vec = vector_from_sequence([_ts(5000), _ts(1000), _ts(3000), _ts(2000)], dtype=DrakenType.TIMESTAMP64)
        assert vec.min() == _ts(1000)

    def test_min_with_nulls(self):
        """Test min with null timestamp values."""
        vec = vector_from_sequence([_ts(5000), None, _ts(1000), None, _ts(3000)], dtype=DrakenType.TIMESTAMP64)
        assert vec.min() == _ts(1000)

    def test_max_basic(self):
        """Test basic max operation on timestamps."""
        vec = vector_from_sequence([_ts(5000), _ts(1000), _ts(3000), _ts(2000)], dtype=DrakenType.TIMESTAMP64)
        assert vec.max() == _ts(5000)

    def test_max_with_nulls(self):
        """Test max with null timestamp values."""
        vec = vector_from_sequence([_ts(5000), None, _ts(3000), None, _ts(1000)], dtype=DrakenType.TIMESTAMP64)
        assert vec.max() == _ts(5000)


class TestAggregationEdgeCases:
    """Test edge cases for aggregation operations."""

    def test_sum_overflow_safety(self):
        """Test sum with large values doesn't overflow."""
        vec = vector_from_sequence([10**15, 10**15, 10**15], dtype=DrakenType.INT64)
        assert vec.sum() == 3 * (10**15)

    def test_single_value_aggregations(self):
        """Test all aggregations with single value."""
        vec = vector_from_sequence([42], dtype=DrakenType.INT64)
        assert vec.sum() == 42
        assert vec.min() == 42
        assert vec.max() == 42

    def test_two_value_aggregations(self):
        """Test all aggregations with two values."""
        vec = vector_from_sequence([10, 20], dtype=DrakenType.INT64)
        assert vec.sum() == 30
        assert vec.min() == 10
        assert vec.max() == 20

    def test_identical_values(self):
        """Test aggregations when all values are identical."""
        vec = vector_from_sequence([7, 7, 7, 7, 7], dtype=DrakenType.INT64)
        assert vec.sum() == 35
        assert vec.min() == 7
        assert vec.max() == 7

    def test_zero_values(self):
        """Test aggregations with zero values."""
        vec = vector_from_sequence([0, 0, 0, 0], dtype=DrakenType.INT64)
        assert vec.sum() == 0
        assert vec.min() == 0
        assert vec.max() == 0

    def test_mixed_zeros_and_values(self):
        """Test aggregations with mix of zeros and non-zero values."""
        vec = vector_from_sequence([0, 5, 0, 10, 0], dtype=DrakenType.INT64)
        assert vec.sum() == 15
        assert vec.min() == 0
        assert vec.max() == 10


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
