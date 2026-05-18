"""Tests for vector.between() single-pass range comparison.

Covers:
- All 4 bound combinations (inclusive/exclusive on each side)
- Null propagation (NULL in → NULL out)
- Boundary values (exact lower, exact upper)
- Out-of-range values
- Constant-encoded vectors
- Integer64Vector, Float64Vector, TimestampVector, Date32Vector, IntegerVector
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import datetime

import pyarrow as pa
import pytest

from draken import Vector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _int64(values):
    return Vector.from_arrow(pa.array(values, type=pa.int64()))


def _float64(values):
    return Vector.from_arrow(pa.array(values, type=pa.float64()))


def _timestamp(values, unit="us"):
    return Vector.from_arrow(pa.array(values, type=pa.timestamp(unit)))


def _date32(values):
    return Vector.from_arrow(pa.array(values, type=pa.date32()))


# ---------------------------------------------------------------------------
# Integer64Vector
# ---------------------------------------------------------------------------


class TestInt64Between:
    DATA = [1, 5, 10, 15, 20]

    def test_inclusive_inclusive(self):
        vec = _int64(self.DATA)
        result = vec.between(5, 15, True, True)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_exclusive_exclusive(self):
        vec = _int64(self.DATA)
        result = vec.between(5, 15, False, False)
        assert result.to_pylist() == [False, False, True, False, False]

    def test_inclusive_exclusive(self):
        vec = _int64(self.DATA)
        result = vec.between(5, 15, True, False)
        assert result.to_pylist() == [False, True, True, False, False]

    def test_exclusive_inclusive(self):
        vec = _int64(self.DATA)
        result = vec.between(5, 15, False, True)
        assert result.to_pylist() == [False, False, True, True, False]

    def test_exact_lower_boundary_inclusive(self):
        vec = _int64([5])
        assert vec.between(5, 10, True, True).to_pylist() == [True]

    def test_exact_lower_boundary_exclusive(self):
        vec = _int64([5])
        assert vec.between(5, 10, False, True).to_pylist() == [False]

    def test_exact_upper_boundary_inclusive(self):
        vec = _int64([10])
        assert vec.between(5, 10, True, True).to_pylist() == [True]

    def test_exact_upper_boundary_exclusive(self):
        vec = _int64([10])
        assert vec.between(5, 10, True, False).to_pylist() == [False]

    def test_all_below_range(self):
        vec = _int64([1, 2, 3])
        result = vec.between(10, 20, True, True)
        assert result.to_pylist() == [False, False, False]

    def test_all_above_range(self):
        vec = _int64([30, 40, 50])
        result = vec.between(10, 20, True, True)
        assert result.to_pylist() == [False, False, False]

    def test_null_propagation(self):
        vec = _int64([5, None, 15])
        result = vec.between(1, 20, True, True)
        assert result.to_pylist() == [True, None, True]

    def test_all_nulls(self):
        vec = _int64([None, None, None])
        result = vec.between(0, 100, True, True)
        assert result.to_pylist() == [None, None, None]

    def test_null_outside_range(self):
        # NULL rows should be NULL regardless of whether the value would pass
        vec = _int64([None])
        result = vec.between(5, 10, True, True)
        assert result.to_pylist() == [None]

    def test_empty_vector(self):
        vec = _int64([])
        result = vec.between(0, 10, True, True)
        assert result.to_pylist() == []

    def test_single_element_match(self):
        vec = _int64([7])
        assert vec.between(5, 10, True, True).to_pylist() == [True]

    def test_negative_range(self):
        vec = _int64([-10, -5, 0, 5, 10])
        result = vec.between(-7, -2, True, True)
        assert result.to_pylist() == [False, True, False, False, False]

    def test_constant_encoded_match(self):
        from draken.vectors.integer64_vector import Integer64Vector
        vec = Integer64Vector.from_constant(7, 5)
        result = vec.between(5, 10, True, True)
        assert result.to_pylist() == [True, True, True, True, True]

    def test_constant_encoded_no_match(self):
        from draken.vectors.integer64_vector import Integer64Vector
        vec = Integer64Vector.from_constant(20, 3)
        result = vec.between(5, 10, True, True)
        assert result.to_pylist() == [False, False, False]

    def test_constant_encoded_null(self):
        from draken.vectors.integer64_vector import Integer64Vector
        vec = Integer64Vector.from_constant(0, 4, is_null=True)
        result = vec.between(0, 10, True, True)
        assert result.to_pylist() == [None, None, None, None]

    def test_matches_two_pass_reference(self):
        """between() must agree with separate >= and <= comparisons."""
        data = list(range(-20, 21))
        vec = _int64(data)
        result_between = vec.between(3, 17, True, True)
        result_ref = vec.greater_than_or_equals(3).and_vector(vec.less_than_or_equals(17))
        assert result_between.to_pylist() == result_ref.to_pylist()


# ---------------------------------------------------------------------------
# Float64Vector
# ---------------------------------------------------------------------------


class TestFloat64Between:
    DATA = [1.0, 5.0, 10.0, 15.0, 20.0]

    def test_inclusive_inclusive(self):
        vec = _float64(self.DATA)
        result = vec.between(5.0, 15.0, True, True)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_exclusive_exclusive(self):
        vec = _float64(self.DATA)
        result = vec.between(5.0, 15.0, False, False)
        assert result.to_pylist() == [False, False, True, False, False]

    def test_fractional_bounds(self):
        vec = _float64([4.9, 5.0, 5.1, 9.9, 10.0, 10.1])
        result = vec.between(5.0, 10.0, True, True)
        assert result.to_pylist() == [False, True, True, True, True, False]

    def test_null_propagation(self):
        vec = _float64([5.0, None, 15.0])
        result = vec.between(1.0, 20.0, True, True)
        assert result.to_pylist() == [True, None, True]

    def test_empty_vector(self):
        vec = _float64([])
        assert vec.between(0.0, 1.0, True, True).to_pylist() == []

    def test_matches_two_pass_reference(self):
        data = [float(x) / 2 for x in range(-40, 41)]
        vec = _float64(data)
        result_between = vec.between(3.0, 17.0, True, False)
        result_ref = vec.greater_than_or_equals(3.0).and_vector(vec.less_than(17.0))
        assert result_between.to_pylist() == result_ref.to_pylist()


# ---------------------------------------------------------------------------
# TimestampVector
# ---------------------------------------------------------------------------


class TestTimestampBetween:
    # timestamps as microseconds since epoch (pa.timestamp('us'))
    _DATES = [
        datetime.datetime(2020, 1, 1),
        datetime.datetime(2021, 6, 15),
        datetime.datetime(2022, 12, 31),
        datetime.datetime(2023, 3, 1),
        datetime.datetime(2024, 7, 4),
    ]

    def _ts_us(self, dt):
        """Convert datetime to microseconds since epoch."""
        epoch = datetime.datetime(1970, 1, 1)
        return int((dt - epoch).total_seconds() * 1_000_000)

    def test_inclusive_inclusive(self):
        vec = _timestamp(self._DATES)
        lo = self._ts_us(datetime.datetime(2021, 1, 1))
        hi = self._ts_us(datetime.datetime(2023, 12, 31))
        result = vec.between(lo, hi, True, True)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_exclusive_exclusive(self):
        vec = _timestamp(self._DATES)
        lo = self._ts_us(datetime.datetime(2021, 6, 15))
        hi = self._ts_us(datetime.datetime(2023, 3, 1))
        result = vec.between(lo, hi, False, False)
        assert result.to_pylist() == [False, False, True, False, False]

    def test_null_propagation(self):
        dates = [self._DATES[0], None, self._DATES[2]]
        vec = _timestamp(dates)
        lo = self._ts_us(datetime.datetime(2019, 1, 1))
        hi = self._ts_us(datetime.datetime(2025, 1, 1))
        result = vec.between(lo, hi, True, True)
        assert result.to_pylist() == [True, None, True]

    def test_empty_vector(self):
        vec = _timestamp([])
        assert vec.between(0, 1, True, True).to_pylist() == []

    def test_matches_two_pass_reference(self):
        vec = _timestamp(self._DATES)
        lo = self._ts_us(datetime.datetime(2021, 1, 1))
        hi = self._ts_us(datetime.datetime(2023, 12, 31))
        result_between = vec.between(lo, hi, True, True)
        result_ref = vec.greater_than_or_equals(lo).and_vector(vec.less_than_or_equals(hi))
        assert result_between.to_pylist() == result_ref.to_pylist()


# ---------------------------------------------------------------------------
# Date32Vector
# ---------------------------------------------------------------------------


class TestDate32Between:
    _DATES = [
        datetime.date(2020, 1, 1),
        datetime.date(2021, 6, 15),
        datetime.date(2022, 12, 31),
        datetime.date(2023, 3, 1),
        datetime.date(2024, 7, 4),
    ]

    def _days(self, d):
        """Convert date to days since epoch."""
        return (d - datetime.date(1970, 1, 1)).days

    def test_inclusive_inclusive(self):
        vec = _date32(self._DATES)
        lo = self._days(datetime.date(2021, 1, 1))
        hi = self._days(datetime.date(2023, 12, 31))
        result = vec.between(lo, hi, True, True)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_exclusive_exclusive(self):
        vec = _date32(self._DATES)
        lo = self._days(datetime.date(2021, 6, 15))
        hi = self._days(datetime.date(2023, 3, 1))
        result = vec.between(lo, hi, False, False)
        assert result.to_pylist() == [False, False, True, False, False]

    def test_null_propagation(self):
        dates = [self._DATES[0], None, self._DATES[2]]
        vec = _date32(dates)
        lo = self._days(datetime.date(2019, 1, 1))
        hi = self._days(datetime.date(2025, 1, 1))
        result = vec.between(lo, hi, True, True)
        assert result.to_pylist() == [True, None, True]

    def test_empty_vector(self):
        vec = _date32([])
        assert vec.between(0, 1000, True, True).to_pylist() == []

    def test_matches_two_pass_reference(self):
        vec = _date32(self._DATES)
        lo = self._days(datetime.date(2021, 1, 1))
        hi = self._days(datetime.date(2023, 12, 31))
        result_between = vec.between(lo, hi, True, True)
        result_ref = vec.greater_than_or_equals(lo).and_vector(vec.less_than_or_equals(hi))
        assert result_between.to_pylist() == result_ref.to_pylist()


# ---------------------------------------------------------------------------
# IntegerVector (mixed-width)
# ---------------------------------------------------------------------------


class TestIntegerVectorBetween:
    """IntegerVector stores int8/16/32/64 — between() delegates to _compare_scalar."""

    def test_int32_inclusive(self):
        vec = Vector.from_arrow(pa.array([1, 5, 10, 15, 20], type=pa.int32()))
        result = vec.between(5, 15, True, True)
        assert result.to_pylist() == [False, True, True, True, False]

    def test_int16_exclusive(self):
        vec = Vector.from_arrow(pa.array([1, 5, 10, 15, 20], type=pa.int16()))
        result = vec.between(5, 15, False, False)
        assert result.to_pylist() == [False, False, True, False, False]

    def test_int8_null_propagation(self):
        vec = Vector.from_arrow(pa.array([5, None, 15], type=pa.int8()))
        result = vec.between(1, 20, True, True)
        assert result.to_pylist() == [True, None, True]

    def test_matches_two_pass_reference(self):
        data = list(range(-20, 21))
        vec = Vector.from_arrow(pa.array(data, type=pa.int32()))
        result_between = vec.between(3, 17, True, True)
        result_ref = vec.greater_than_or_equals(3).and_vector(vec.less_than_or_equals(17))
        assert result_between.to_pylist() == result_ref.to_pylist()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
