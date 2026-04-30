"""
GC buffer lifecycle tests for all vector types.

Each vector type that wraps Arrow buffers zero-copy must retain buffer references
so they are not freed when the source pa.array goes out of scope.  Vectors that
copy data at construction time (Decimal, Interval month_day_nano) are also covered
as regression guards.

Pattern for every case:
  1. Build the vector inside a helper so the pa.array reference is local.
  2. Force a full GC cycle.
  3. Allocate scratch memory to clobber pages the GC might have released.
  4. Assert that reads (to_pylist / aggregations) still return the expected values.
"""

import gc
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
import pytest

from draken import Vector


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _churn():
    """Allocate and immediately discard memory to clobber freed pages."""
    _ = [bytes(1024) * 512 for _ in range(200)]


def _make_and_drop(arrow_array):
    """Build a Vector from an Arrow array, then let the array go out of scope."""
    return Vector.from_arrow(arrow_array)


# ---------------------------------------------------------------------------
# Int64
# ---------------------------------------------------------------------------

class TestInt64GC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([10, 20, 30, 40, 50], type=pa.int64()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [10, 20, 30, 40, 50]
        assert vec.sum() == 150
        assert vec.min() == 10
        assert vec.max() == 50

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([1, None, 3, None, 5], type=pa.int64()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [1, None, 3, None, 5]
        assert vec.null_count == 2
        assert vec.sum() == 9


# ---------------------------------------------------------------------------
# Float64
# ---------------------------------------------------------------------------

class TestFloat64GC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([1.1, 2.2, 3.3], type=pa.float64()))
        gc.collect(); _churn()
        vals = vec.to_pylist()
        assert len(vals) == 3
        assert abs(vals[0] - 1.1) < 1e-9
        assert vec.sum() == pytest.approx(6.6)

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([1.0, None, 3.0], type=pa.float64()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [1.0, None, 3.0]
        assert vec.null_count == 1


# ---------------------------------------------------------------------------
# Float32
# ---------------------------------------------------------------------------

class TestFloat32GC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([1.0, 2.0, 3.0], type=pa.float32()))
        gc.collect(); _churn()
        assert vec.sum() == pytest.approx(6.0)
        assert vec.min() == pytest.approx(1.0)
        assert vec.max() == pytest.approx(3.0)

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([1.0, None, 3.0], type=pa.float32()))
        gc.collect(); _churn()
        assert vec.null_count == 1
        assert vec.sum() == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# Integer (small int — int8/int16/int32)
# ---------------------------------------------------------------------------

class TestIntegerGC:
    @pytest.mark.parametrize("pa_type", [pa.int8(), pa.int16(), pa.int32()])
    def test_dense_no_nulls(self, pa_type):
        vec = _make_and_drop(pa.array([5, 10, 15], type=pa_type))
        gc.collect(); _churn()
        assert vec.to_pylist() == [5, 10, 15]
        assert vec.sum() == 30

    @pytest.mark.parametrize("pa_type", [pa.int8(), pa.int16(), pa.int32()])
    def test_dense_with_nulls(self, pa_type):
        vec = _make_and_drop(pa.array([5, None, 15], type=pa_type))
        gc.collect(); _churn()
        assert vec.null_count == 1
        assert vec.to_pylist() == [5, None, 15]


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------

class TestBoolGC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([True, False, True, True], type=pa.bool_()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [True, False, True, True]
        assert vec.sum() == 3

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([True, None, False], type=pa.bool_()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [True, None, False]
        assert vec.null_count == 1


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------

class TestStringGC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array(["alpha", "beta", "gamma"], type=pa.string()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [b"alpha", b"beta", b"gamma"]
        assert vec.min() == b"alpha"
        assert vec.max() == b"gamma"

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array(["alpha", None, "gamma"], type=pa.string()))
        gc.collect(); _churn()
        assert vec.to_pylist() == [b"alpha", None, b"gamma"]
        assert vec.null_count == 1

    def test_long_strings(self):
        # Ensures offsets buffer is also retained.
        data = [f"value_{i:05d}" for i in range(100)]
        vec = _make_and_drop(pa.array(data, type=pa.string()))
        gc.collect(); _churn()
        result = vec.to_pylist()
        assert len(result) == 100
        assert result[0] == b"value_00000"
        assert result[-1] == b"value_00099"


# ---------------------------------------------------------------------------
# Date32
# ---------------------------------------------------------------------------

class TestDate32GC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([10, 20, 30], type=pa.date32()))
        gc.collect(); _churn()
        assert vec.min() == 10
        assert vec.max() == 30
        assert vec.sum() == 60

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([10, None, 30], type=pa.date32()))
        gc.collect(); _churn()
        assert vec.null_count == 1
        assert vec.min() == 10
        assert vec.max() == 30


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

class TestTimestampGC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(pa.array([1000, 2000, 3000], type=pa.timestamp("us")))
        gc.collect(); _churn()
        assert vec.min() == 1000
        assert vec.max() == 3000
        assert vec.sum() == 6000

    def test_dense_with_nulls(self):
        vec = _make_and_drop(pa.array([1000, None, 3000], type=pa.timestamp("us")))
        gc.collect(); _churn()
        assert vec.null_count == 1
        assert vec.min() == 1000
        assert vec.max() == 3000

    @pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
    def test_timestamp_units(self, unit):
        vec = _make_and_drop(pa.array([100, 200, 300], type=pa.timestamp(unit)))
        gc.collect(); _churn()
        assert vec.min() == 100
        assert vec.max() == 300


# ---------------------------------------------------------------------------
# Time (regression: was missing _arrow_data_buf before today's fix)
# ---------------------------------------------------------------------------

class TestTimeGC:
    def test_time64_no_nulls(self):
        vec = _make_and_drop(pa.array([1000, 2000, 3000], type=pa.time64("us")))
        gc.collect(); _churn()
        assert vec.min() == 1000
        assert vec.max() == 3000
        assert vec.sum() == 6000

    def test_time64_with_nulls(self):
        vec = _make_and_drop(
            pa.array([3000, None, 1000, None, 2000], type=pa.time64("us"))
        )
        gc.collect(); _churn()
        assert vec.null_count == 2
        assert vec.min() == 1000
        assert vec.max() == 3000

    def test_time32_no_nulls(self):
        vec = _make_and_drop(pa.array([10, 20, 30], type=pa.time32("s")))
        gc.collect(); _churn()
        assert vec.min() == 10
        assert vec.max() == 30

    def test_time32_with_nulls(self):
        vec = _make_and_drop(pa.array([10, None, 30], type=pa.time32("s")))
        gc.collect(); _churn()
        assert vec.null_count == 1


# ---------------------------------------------------------------------------
# Decimal (copies on construction — GC test is a regression guard)
# ---------------------------------------------------------------------------

class TestDecimalGC:
    def test_dense_no_nulls(self):
        vec = _make_and_drop(
            pa.array([Decimal("1.50"), Decimal("2.50"), Decimal("3.00")],
                     type=pa.decimal128(10, 2))
        )
        gc.collect(); _churn()
        assert vec.sum() == Decimal("7.00")
        assert vec.min() == Decimal("1.50")
        assert vec.max() == Decimal("3.00")

    def test_dense_with_nulls(self):
        vec = _make_and_drop(
            pa.array([Decimal("1.50"), None, Decimal("3.00")],
                     type=pa.decimal128(10, 2))
        )
        gc.collect(); _churn()
        assert vec.null_count == 1
        assert vec.sum() == Decimal("4.50")


# ---------------------------------------------------------------------------
# Interval (copies on construction via from_arrow_interval — regression guard)
# ---------------------------------------------------------------------------

class TestIntervalGC:
    def test_no_nulls(self):
        vec = _make_and_drop(pa.array(
            [(1, 0, 0), (2, 0, 0), (3, 0, 0)],
            type=pa.month_day_nano_interval(),
        ))
        gc.collect(); _churn()
        months, micros = vec.sum()
        assert months == 6
        assert micros == 0

    def test_with_nulls(self):
        vec = _make_and_drop(pa.array(
            [(1, 0, 0), None, (3, 0, 0)],
            type=pa.month_day_nano_interval(),
        ))
        gc.collect(); _churn()
        assert vec.sum() is not None
        months, _ = vec.sum()
        assert months == 4


# ---------------------------------------------------------------------------
# Array (list-of-int64 — 4 buffer refs must all survive GC)
# ---------------------------------------------------------------------------

class TestArrayVectorGC:
    def test_no_nulls(self):
        vec = _make_and_drop(pa.array(
            [[1, 2], [3, 4, 5], [6]], type=pa.list_(pa.int64())
        ))
        gc.collect(); _churn()
        result = vec.to_pylist()
        assert result == [[1, 2], [3, 4, 5], [6]]

    def test_with_nulls(self):
        vec = _make_and_drop(pa.array(
            [[1, 2], None, [6]], type=pa.list_(pa.int64())
        ))
        gc.collect(); _churn()
        result = vec.to_pylist()
        assert result == [[1, 2], None, [6]]
        assert vec.null_count == 1

    def test_large_child_values(self):
        # Forces the child vector's buffer to also be retained.
        data = [list(range(i, i + 5)) for i in range(50)]
        vec = _make_and_drop(pa.array(data, type=pa.list_(pa.int64())))
        gc.collect(); _churn()
        result = vec.to_pylist()
        assert result[0] == [0, 1, 2, 3, 4]
        assert result[-1] == [49, 50, 51, 52, 53]
