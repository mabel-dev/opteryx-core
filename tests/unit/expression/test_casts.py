"""Tests for cast operation kernels."""

import numpy as np
import pyarrow as pa
import pytest
from orso.types import OrsoTypes

from opteryx.expression.casts import (
    cast_to_int,
    cast_to_double,
    cast_to_varchar,
    cast_to_blob,
    try_cast,
    cast,
)


class TestCastToInt:
    """Test INTEGER cast operations."""

    def test_int_to_int_identity(self):
        """Casting int to int returns same values."""
        arr = np.array([1, 2, 3, None], dtype=object)
        result = cast_to_int(arr)
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3
        assert result[3] is None

    def test_string_to_int(self):
        """Cast string array to int."""
        arr = np.array(["1", "2", "3", None], dtype=object)
        result = cast_to_int(arr)
        # cast_to_int returns PyArrow array/scalars for string input
        assert int(result[0]) == 1
        assert int(result[1]) == 2
        assert int(result[2]) == 3

    def test_float_to_int(self):
        """Cast float to int (truncates)."""
        arr = np.array([1.5, 2.7, 3.1, None], dtype=object)
        result = cast_to_int(arr)
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3
        assert result[3] is None


class TestCastToDouble:
    """Test DOUBLE cast operations."""

    def test_double_to_double_identity(self):
        """Casting double to double returns same values."""
        arr = np.array([1.5, 2.7, 3.1], dtype=np.float64)
        result = cast_to_double(arr)
        np.testing.assert_array_equal(result, arr)

    def test_int_to_double(self):
        """Cast int to double."""
        arr = np.array([1, 2, 3], dtype=np.int64)
        result = cast_to_double(arr)
        expected = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        np.testing.assert_array_equal(result, expected)

    def test_string_to_double(self):
        """Cast string array to double."""
        arr = np.array(["1.5", "2.7", "3.1", None], dtype=object)
        result = cast_to_double(arr)
        np.testing.assert_almost_equal(result[0], 1.5)
        np.testing.assert_almost_equal(result[1], 2.7)
        np.testing.assert_almost_equal(result[2], 3.1)
        # Note: None becomes NaN, not None
        assert np.isnan(result[3])


class TestCastToVarchar:
    """Test VARCHAR cast operations."""

    def test_int_to_varchar(self):
        """Cast int array to varchar."""
        arr = np.array([1, 2, 3], dtype=np.int64)
        result = cast_to_varchar(arr)
        # cast_to_varchar with int64 optimized path returns PyArrow BinaryScalars
        assert bytes(result[0]) == b'1'
        assert bytes(result[1]) == b'2'
        assert bytes(result[2]) == b'3'

    def test_double_to_varchar(self):
        """Cast double array to varchar."""
        arr = np.array([1.5, 2.7, 3.1], dtype=np.float64)
        result = cast_to_varchar(arr)
        # Just verify it doesn't crash and produces strings
        assert len(result) == 3

    def test_varchar_with_null(self):
        """VARCHAR cast handles nulls correctly."""
        arr = np.array(["hello", None, "world"], dtype=object)
        result = cast_to_varchar(arr)
        assert result[0] == "hello"
        assert result[1] is None
        assert result[2] == "world"


class TestCastToBlob:
    """Test BLOB cast operations."""

    def test_int_to_blob(self):
        """Cast int array to blob."""
        arr = np.array([1, 2, 3], dtype=np.int64)
        result = cast_to_blob(arr)
        # Just verify it doesn't crash
        assert len(result) == 3

    def test_double_to_blob(self):
        """Cast double array to blob."""
        arr = np.array([1.5, 2.7, 3.1], dtype=np.float64)
        result = cast_to_blob(arr)
        assert len(result) == 3


class TestTryCast:
    """Test TRY_CAST operations (safe casting with null on error)."""

    def test_try_cast_int_valid(self):
        """TRY_CAST with valid values."""
        caster = try_cast("INTEGER")
        arr = np.array(["1", "2", "3"], dtype=object)
        result = caster(arr)
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3

    def test_try_cast_int_invalid_returns_none(self):
        """TRY_CAST returns None for invalid values instead of raising."""
        caster = try_cast("INTEGER")
        arr = np.array(["1", "not_a_number", "3"], dtype=object)
        result = caster(arr)
        assert result[0] == 1
        assert result[1] is None  # Invalid value → None
        assert result[2] == 3

    def test_try_cast_double_invalid_returns_none(self):
        """TRY_CAST to DOUBLE returns None for invalid values."""
        caster = try_cast("DOUBLE")
        arr = np.array(["1.5", "not_a_number", "3.1"], dtype=object)
        result = caster(arr)
        assert result[0] == pytest.approx(1.5)
        assert result[1] is None  # Invalid value → None
        assert result[2] == pytest.approx(3.1)


class TestCastFactory:
    """Test the cast() factory function."""

    def test_cast_int(self):
        """cast() factory for INTEGER."""
        caster = cast("INTEGER")
        arr = np.array(["1", "2", "3"], dtype=object)
        result = caster(arr)
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3

    def test_cast_double(self):
        """cast() factory for DOUBLE."""
        caster = cast("DOUBLE")
        arr = np.array(["1.5", "2.7", "3.1"], dtype=object)
        result = caster(arr)
        assert result[0] == pytest.approx(1.5)
        assert result[1] == pytest.approx(2.7)
        assert result[2] == pytest.approx(3.1)

    def test_cast_varchar(self):
        """cast() factory for VARCHAR."""
        caster = cast("VARCHAR")
        arr = np.array([1, 2, 3], dtype=np.int64)
        result = caster(arr)
        assert str(result[0]) == "1"
        assert str(result[1]) == "2"
        assert str(result[2]) == "3"
