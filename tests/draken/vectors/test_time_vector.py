"""
Tests for TimeVector (TIME64) operations.

This module tests TimeVector functionality including:
- Basic operations
- Null handling
- take()

Vectors are built with draken.draken_native.vector_time64_from_sequence(values,
unit), which takes datetime.time (or None) elements — not raw microsecond ints
— and is called directly rather than through vector_from_sequence's dispatch
table, which only wires up TIME/TIME32 (see draken/interop/vector_sequence.py's
docstring: per-type quirks need the typed constructor directly). Vector has no
from_arrow/to_arrow, no is_null()/null_count (use is_null_at per-row), and no
comparison methods for TIME64 in the current engine.
"""

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken import draken_native


def _time_us(microseconds: int) -> datetime.time:
    return (datetime.datetime.min + datetime.timedelta(microseconds=microseconds)).time()


def _time64_vec(microsecond_values):
    return draken_native.vector_time64_from_sequence(
        [None if v is None else _time_us(v) for v in microsecond_values], "us"
    )


def _null_mask(vec):
    return [vec.is_null_at(i) for i in range(vec.length)]


def _null_count(vec) -> int:
    return sum(1 for i in range(vec.length) if vec.is_null_at(i))


class TestTimeVectorBasics:
    """Test basic TimeVector operations."""

    def test_creation(self):
        """Test creating a TIME64 vector."""
        vec = _time64_vec([10000, 20000, 30000, 40000])

        assert vec.length == 4
        assert _null_count(vec) == 0

    def test_to_pylist(self):
        """Test conversion to Python list (datetime.time elements)."""
        vec = _time64_vec([10000, 20000, 30000])
        assert vec.to_pylist() == [_time_us(10000), _time_us(20000), _time_us(30000)]

    def test_to_pylist_with_nulls(self):
        """Test conversion to Python list with nulls."""
        vec = _time64_vec([10000, None, 30000, None])
        assert vec.to_pylist() == [_time_us(10000), None, _time_us(30000), None]

    def test_length(self):
        """Test length property."""
        vec = _time64_vec([10000, 20000, 30000, 40000, 50000])
        assert vec.length == 5

    def test_empty_vector(self):
        """Test empty TimeVector."""
        vec = _time64_vec([])
        assert vec.length == 0
        assert _null_count(vec) == 0


class TestTimeVectorNullHandling:
    """Test null handling in TimeVector."""

    def test_null_count(self):
        """Test null count."""
        vec = _time64_vec([10000, None, 30000, None, 50000])
        assert _null_count(vec) == 2

    def test_is_null(self):
        """Test is_null_at per-row."""
        vec = _time64_vec([10000, None, 30000, None, 50000])
        assert _null_mask(vec) == [False, True, False, True, False]

    def test_all_nulls(self):
        """Test vector with all null values."""
        vec = _time64_vec([None, None, None])
        assert _null_count(vec) == 3
        assert vec.length == 3
        assert _null_mask(vec) == [True, True, True]

    def test_no_nulls(self):
        """Test vector with no null values."""
        vec = _time64_vec([10000, 20000, 30000])
        assert _null_count(vec) == 0
        assert _null_mask(vec) == [False, False, False]


class TestTimeVectorTake:
    """Test take() on TimeVector."""

    def test_take_basic(self):
        """Test basic take operation (take() accepts a plain list of indices)."""
        vec = _time64_vec([10000, 20000, 30000, 40000, 50000])
        result = vec.take([0, 2, 4])
        assert result.to_pylist() == [_time_us(10000), _time_us(30000), _time_us(50000)]

    def test_take_with_nulls(self):
        """Test take operation preserves nulls."""
        vec = _time64_vec([10000, None, 30000, None, 50000])
        result = vec.take([0, 1, 4])
        assert result.to_pylist() == [_time_us(10000), None, _time_us(50000)]

    def test_take_single_index(self):
        """Test take with single index."""
        vec = _time64_vec([10000, 20000, 30000])
        result = vec.take([1])
        assert result.to_pylist() == [_time_us(20000)]

    def test_take_all_indices(self):
        """Test take with all indices."""
        vec = _time64_vec([10000, 20000, 30000])
        result = vec.take([0, 1, 2])
        assert result.to_pylist() == [_time_us(10000), _time_us(20000), _time_us(30000)]


class TestTimeVectorEdgeCases:
    """Test edge cases for TimeVector."""

    def test_single_value(self):
        """Test vector with single value."""
        vec = _time64_vec([10000])
        assert vec.length == 1
        assert vec.to_pylist() == [_time_us(10000)]

    def test_duplicate_values(self):
        """Test vector with duplicate values."""
        vec = _time64_vec([10000, 10000, 10000])
        assert vec.to_pylist() == [_time_us(10000)] * 3

    def test_very_large_time_values(self):
        """Test with large time values (near end-of-day)."""
        max_time = 86400000000 - 1  # 24 hours in microseconds, minus 1
        vec = _time64_vec([0, max_time // 2, max_time])

        assert vec.length == 3
        assert vec.to_pylist()[0] == _time_us(0)

    def test_sequential_times(self):
        """Test with sequential time values."""
        times = list(range(0, 50000, 10000))
        vec = _time64_vec(times)

        assert vec.length == len(times)
        assert vec.to_pylist() == [_time_us(t) for t in times]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
