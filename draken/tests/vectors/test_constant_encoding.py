"""
Constant-encoding tests for all vector types that support from_constant.

Constant vectors hold a single repeated value (or null) and report it for every
logical row without storing N copies.  The paths through sum/min/max/any/all
and subscript differ from the dense path — this module covers them.

Types with from_constant: Int64, Float64, Float32, Integer, Bool, String,
                          Date32, Timestamp, Time, Decimal.
Interval has no from_constant; it is skipped here.
"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
import pytest

from draken import Vector
from draken.vectors.bool_vector import BoolVector
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float32_vector import Float32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.integer_vector import IntegerVector
from draken.vectors.string_vector import StringVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector
from draken.vectors._decimal_vector import DecimalVector


# ---------------------------------------------------------------------------
# Int64
# ---------------------------------------------------------------------------

class TestInt64Constant:
    def test_sum(self):
        assert Int64Vector.from_constant(7, 4).sum() == 28

    def test_min_max(self):
        v = Int64Vector.from_constant(42, 5)
        assert v.min() == 42
        assert v.max() == 42

    def test_all_null_sum_zero(self):
        assert Int64Vector.from_constant(None, 3, is_null=True).sum() == 0

    def test_all_null_min_raises(self):
        with pytest.raises(ValueError):
            Int64Vector.from_constant(None, 3, is_null=True).min()

    def test_all_null_max_raises(self):
        with pytest.raises(ValueError):
            Int64Vector.from_constant(None, 3, is_null=True).max()

    def test_null_count(self):
        assert Int64Vector.from_constant(5, 4).null_count == 0
        assert Int64Vector.from_constant(None, 4, is_null=True).null_count == 4

    def test_length(self):
        assert len(Int64Vector.from_constant(1, 10)) == 10

    def test_to_pylist(self):
        assert Int64Vector.from_constant(9, 3).to_pylist() == [9, 9, 9]
        assert Int64Vector.from_constant(None, 2, is_null=True).to_pylist() == [None, None]

    def test_subscript(self):
        v = Int64Vector.from_constant(99, 5)
        assert v[0] == 99
        assert v[4] == 99

    def test_zero_length(self):
        v = Int64Vector.from_constant(1, 0)
        assert len(v) == 0
        assert v.sum() == 0


# ---------------------------------------------------------------------------
# Float64
# ---------------------------------------------------------------------------

class TestFloat64Constant:
    def test_sum(self):
        assert Float64Vector.from_constant(2.5, 4).sum() == pytest.approx(10.0)

    def test_min_max(self):
        v = Float64Vector.from_constant(3.14, 5)
        assert v.min() == pytest.approx(3.14)
        assert v.max() == pytest.approx(3.14)

    def test_all_null_sum_zero(self):
        assert Float64Vector.from_constant(None, 3, is_null=True).sum() == pytest.approx(0.0)

    def test_null_count(self):
        assert Float64Vector.from_constant(1.0, 4).null_count == 0
        assert Float64Vector.from_constant(None, 4, is_null=True).null_count == 4

    def test_to_pylist(self):
        assert Float64Vector.from_constant(1.5, 2).to_pylist() == [1.5, 1.5]


# ---------------------------------------------------------------------------
# Float32
# ---------------------------------------------------------------------------

class TestFloat32Constant:
    def test_sum(self):
        assert Float32Vector.from_constant(2.0, 5).sum() == pytest.approx(10.0)

    def test_min_max(self):
        v = Float32Vector.from_constant(7.5, 3)
        assert v.min() == pytest.approx(7.5)
        assert v.max() == pytest.approx(7.5)

    def test_null_count(self):
        assert Float32Vector.from_constant(None, 3, is_null=True).null_count == 3

    def test_to_pylist(self):
        result = Float32Vector.from_constant(1.0, 3).to_pylist()
        assert all(abs(x - 1.0) < 1e-6 for x in result)


# ---------------------------------------------------------------------------
# Integer (small int)
# ---------------------------------------------------------------------------

class TestIntegerConstant:
    def test_sum(self):
        assert IntegerVector.from_constant(7, 3).sum() == 21

    def test_min_max(self):
        v = IntegerVector.from_constant(42, 4)
        assert v.min() == 42
        assert v.max() == 42

    def test_null_count(self):
        assert IntegerVector.from_constant(None, 5, is_null=True).null_count == 5

    def test_to_pylist(self):
        assert IntegerVector.from_constant(3, 4).to_pylist() == [3, 3, 3, 3]


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------

class TestBoolConstant:
    def test_sum_all_true(self):
        assert BoolVector.from_constant(True, 5).sum() == 5

    def test_sum_all_false(self):
        assert BoolVector.from_constant(False, 5).sum() == 0

    def test_sum_all_null(self):
        assert BoolVector.from_constant(None, 3, is_null=True).sum() == 0

    def test_min_all_true(self):
        assert BoolVector.from_constant(True, 4).min() == 1

    def test_min_all_false(self):
        assert BoolVector.from_constant(False, 4).min() == 0

    def test_min_all_null_raises(self):
        with pytest.raises(ValueError):
            BoolVector.from_constant(None, 3, is_null=True).min()

    def test_max_all_true(self):
        assert BoolVector.from_constant(True, 4).max() == 1

    def test_max_all_false(self):
        assert BoolVector.from_constant(False, 4).max() == 0

    def test_max_all_null_raises(self):
        with pytest.raises(ValueError):
            BoolVector.from_constant(None, 3, is_null=True).max()

    def test_any_true(self):
        assert BoolVector.from_constant(True, 5).any() == 1

    def test_any_false(self):
        assert BoolVector.from_constant(False, 5).any() == 0

    def test_any_null(self):
        assert BoolVector.from_constant(None, 3, is_null=True).any() == 0

    def test_all_true(self):
        assert BoolVector.from_constant(True, 5).all() == 1

    def test_all_false(self):
        assert BoolVector.from_constant(False, 5).all() == 0

    def test_all_null(self):
        # all-null constant: vacuously true (no non-null False values)
        assert BoolVector.from_constant(None, 3, is_null=True).all() == 1

    def test_null_count(self):
        assert BoolVector.from_constant(True, 4).null_count == 0
        assert BoolVector.from_constant(None, 4, is_null=True).null_count == 4

    def test_to_pylist_true(self):
        assert BoolVector.from_constant(True, 3).to_pylist() == [True, True, True]

    def test_to_pylist_false(self):
        assert BoolVector.from_constant(False, 2).to_pylist() == [False, False]

    def test_to_pylist_null(self):
        assert BoolVector.from_constant(None, 2, is_null=True).to_pylist() == [None, None]


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------

class TestStringConstant:
    def test_min_max(self):
        v = StringVector.from_constant("hello", 4)
        assert v.min() == b"hello"
        assert v.max() == b"hello"

    def test_null_min_max(self):
        v = StringVector.from_constant(None, 3, is_null=True)
        assert v.min() is None
        assert v.max() is None

    def test_sum_raises(self):
        with pytest.raises(NotImplementedError):
            StringVector.from_constant("x", 3).sum()

    def test_null_count(self):
        assert StringVector.from_constant("hi", 5).null_count == 0
        assert StringVector.from_constant(None, 5, is_null=True).null_count == 5

    def test_to_pylist(self):
        assert StringVector.from_constant("abc", 3).to_pylist() == [b"abc", b"abc", b"abc"]

    def test_length(self):
        assert len(StringVector.from_constant("z", 7)) == 7


# ---------------------------------------------------------------------------
# Date32
# ---------------------------------------------------------------------------

class TestDate32Constant:
    def test_min_max(self):
        v = Date32Vector.from_constant(100, 5)
        assert v.min() == 100
        assert v.max() == 100

    def test_sum(self):
        assert Date32Vector.from_constant(10, 4).sum() == 40

    def test_null_min_raises(self):
        with pytest.raises(ValueError):
            Date32Vector.from_constant(None, 3, is_null=True).min()

    def test_null_sum_zero(self):
        assert Date32Vector.from_constant(None, 3, is_null=True).sum() == 0

    def test_null_count(self):
        assert Date32Vector.from_constant(5, 4).null_count == 0
        assert Date32Vector.from_constant(None, 4, is_null=True).null_count == 4

    def test_length(self):
        assert len(Date32Vector.from_constant(1, 6)) == 6


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

class TestTimestampConstant:
    def test_min_max(self):
        v = TimestampVector.from_constant(5000, 3)
        assert v.min() == 5000
        assert v.max() == 5000

    def test_sum(self):
        assert TimestampVector.from_constant(1000, 3).sum() == 3000

    def test_null_min_raises(self):
        with pytest.raises(ValueError):
            TimestampVector.from_constant(None, 3, is_null=True).min()

    def test_null_sum_zero(self):
        assert TimestampVector.from_constant(None, 3, is_null=True).sum() == 0

    def test_null_count(self):
        assert TimestampVector.from_constant(100, 5).null_count == 0
        assert TimestampVector.from_constant(None, 5, is_null=True).null_count == 5

    def test_length(self):
        assert len(TimestampVector.from_constant(1, 8)) == 8


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------

class TestTimeConstant:
    def test_min_max_time64(self):
        v = TimeVector.from_constant(2000, 4, is_time64=True)
        assert v.min() == 2000
        assert v.max() == 2000

    def test_sum_time64(self):
        assert TimeVector.from_constant(500, 3, is_time64=True).sum() == 1500

    def test_min_max_time32(self):
        v = TimeVector.from_constant(45, 3, is_time64=False)
        assert v.min() == 45
        assert v.max() == 45

    def test_null_min_raises(self):
        with pytest.raises(ValueError):
            TimeVector.from_constant(None, 3, is_null=True, is_time64=True).min()

    def test_null_sum_zero(self):
        assert TimeVector.from_constant(None, 3, is_null=True, is_time64=True).sum() == 0

    def test_null_count(self):
        assert TimeVector.from_constant(100, 4, is_time64=True).null_count == 0
        assert TimeVector.from_constant(None, 4, is_null=True, is_time64=True).null_count == 4


# ---------------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------------

class TestDecimalConstant:
    def test_sum(self):
        v = DecimalVector.from_constant(Decimal("2.50"), 4)
        assert v.sum() == Decimal("10.00")

    def test_min_max(self):
        v = DecimalVector.from_constant(Decimal("3.75"), 3)
        assert v.min() == Decimal("3.75")
        assert v.max() == Decimal("3.75")

    def test_null_sum_zero(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        assert v.sum() == Decimal(0)

    def test_null_min_raises(self):
        with pytest.raises(ValueError):
            DecimalVector.from_constant(None, 3, is_null=True).min()

    def test_null_count(self):
        assert DecimalVector.from_constant(Decimal("1.0"), 4).null_count == 0
        assert DecimalVector.from_constant(None, 4, is_null=True).null_count == 4


# ---------------------------------------------------------------------------
# Cross-type: constant round-trips through to_arrow then back
# ---------------------------------------------------------------------------

class TestConstantArrowRoundTrip:
    """Constant vectors converted to Arrow and back must preserve values."""

    def test_int64_roundtrip(self):
        v = Int64Vector.from_constant(42, 5)
        arr = v.to_arrow()
        v2 = Vector.from_arrow(arr)
        assert v2.to_pylist() == [42, 42, 42, 42, 42]

    def test_bool_roundtrip(self):
        v = BoolVector.from_constant(True, 4)
        arr = v.to_arrow()
        v2 = Vector.from_arrow(arr)
        assert v2.to_pylist() == [True, True, True, True]

    def test_string_roundtrip(self):
        v = StringVector.from_constant("draken", 3)
        arr = v.to_arrow()
        v2 = Vector.from_arrow(arr)
        assert v2.to_pylist() == [b"draken", b"draken", b"draken"]

    def test_int64_null_roundtrip(self):
        v = Int64Vector.from_constant(None, 3, is_null=True)
        arr = v.to_arrow()
        v2 = Vector.from_arrow(arr)
        assert v2.null_count == 3
        assert v2.to_pylist() == [None, None, None]
