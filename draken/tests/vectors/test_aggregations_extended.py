"""
Extended aggregation tests for vector types not covered by test_aggregations.py.

Adds coverage for:
  - Float32Vector  (sum/min/max)
  - BoolVector     (sum/min/max/any/all)
  - StringVector   (min/max + sum raises)
  - DecimalVector  (sum/min/max)
  - IntervalVector (sum/min/max)
  - ArrayVector    (sum/min/max all raise)
  - Date32 / Timestamp sum
  - null_count across types
  - Cross-type empty + all-null edge cases
"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
import pytest

from draken import Vector


# ---------------------------------------------------------------------------
# Float32
# ---------------------------------------------------------------------------

class TestFloat32Aggregations:
    def test_sum_basic(self):
        vec = Vector.from_arrow(pa.array([1.5, 2.5, 3.0], type=pa.float32()))
        assert vec.sum() == pytest.approx(7.0)

    def test_sum_with_nulls(self):
        vec = Vector.from_arrow(pa.array([1.5, None, 2.5, None, 4.0], type=pa.float32()))
        assert vec.sum() == pytest.approx(8.0)

    def test_sum_all_nulls(self):
        vec = Vector.from_arrow(pa.array([None, None, None], type=pa.float32()))
        assert vec.sum() == pytest.approx(0.0)

    def test_min_basic(self):
        vec = Vector.from_arrow(pa.array([5.5, 2.2, 8.8, 1.1], type=pa.float32()))
        assert vec.min() == pytest.approx(1.1)

    def test_min_with_nulls(self):
        vec = Vector.from_arrow(pa.array([5.5, None, 1.1, None], type=pa.float32()))
        assert vec.min() == pytest.approx(1.1)

    def test_min_negative(self):
        vec = Vector.from_arrow(pa.array([3.0, -2.0, 0.0, -10.5], type=pa.float32()))
        assert vec.min() == pytest.approx(-10.5)

    def test_min_empty_raises(self):
        vec = Vector.from_arrow(pa.array([], type=pa.float32()))
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_max_basic(self):
        vec = Vector.from_arrow(pa.array([5.5, 2.2, 8.8, 1.1], type=pa.float32()))
        assert vec.max() == pytest.approx(8.8)

    def test_max_with_nulls(self):
        vec = Vector.from_arrow(pa.array([5.5, None, 9.9, None], type=pa.float32()))
        assert vec.max() == pytest.approx(9.9)

    def test_max_empty_raises(self):
        vec = Vector.from_arrow(pa.array([], type=pa.float32()))
        with pytest.raises(ValueError, match="empty"):
            vec.max()


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------

class TestBoolAggregations:
    def test_sum_counts_true(self):
        vec = Vector.from_arrow(pa.array([True, False, True, True, False], type=pa.bool_()))
        assert vec.sum() == 3

    def test_sum_with_nulls(self):
        vec = Vector.from_arrow(pa.array([True, None, True, False, None], type=pa.bool_()))
        assert vec.sum() == 2

    def test_sum_all_false(self):
        vec = Vector.from_arrow(pa.array([False, False, False], type=pa.bool_()))
        assert vec.sum() == 0

    def test_sum_all_nulls(self):
        vec = Vector.from_arrow(pa.array([None, None, None], type=pa.bool_()))
        assert vec.sum() == 0

    def test_min_basic(self):
        vec = Vector.from_arrow(pa.array([True, False, True], type=pa.bool_()))
        assert vec.min() == 0

    def test_min_all_true(self):
        vec = Vector.from_arrow(pa.array([True, True, True], type=pa.bool_()))
        assert vec.min() == 1

    def test_min_with_nulls(self):
        vec = Vector.from_arrow(pa.array([True, None, False, None], type=pa.bool_()))
        assert vec.min() == 0

    def test_min_empty_raises(self):
        vec = Vector.from_arrow(pa.array([], type=pa.bool_()))
        with pytest.raises(ValueError, match="empty"):
            vec.min()

    def test_min_all_null_raises(self):
        vec = Vector.from_arrow(pa.array([None, None], type=pa.bool_()))
        with pytest.raises(ValueError, match="all-null"):
            vec.min()

    def test_max_basic(self):
        vec = Vector.from_arrow(pa.array([False, True, False], type=pa.bool_()))
        assert vec.max() == 1

    def test_max_all_false(self):
        vec = Vector.from_arrow(pa.array([False, False, False], type=pa.bool_()))
        assert vec.max() == 0

    def test_max_with_nulls(self):
        vec = Vector.from_arrow(pa.array([False, None, True, None], type=pa.bool_()))
        assert vec.max() == 1

    def test_max_empty_raises(self):
        vec = Vector.from_arrow(pa.array([], type=pa.bool_()))
        with pytest.raises(ValueError, match="empty"):
            vec.max()

    def test_max_all_null_raises(self):
        vec = Vector.from_arrow(pa.array([None, None], type=pa.bool_()))
        with pytest.raises(ValueError, match="all-null"):
            vec.max()

    def test_any_true(self):
        vec = Vector.from_arrow(pa.array([False, False, True, False], type=pa.bool_()))
        assert vec.any() == 1

    def test_any_all_false(self):
        vec = Vector.from_arrow(pa.array([False, False, False], type=pa.bool_()))
        assert vec.any() == 0

    def test_all_true(self):
        vec = Vector.from_arrow(pa.array([True, True, True], type=pa.bool_()))
        assert vec.all() == 1

    def test_all_with_false(self):
        vec = Vector.from_arrow(pa.array([True, False, True], type=pa.bool_()))
        assert vec.all() == 0


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------

class TestStringAggregations:
    def test_min_basic(self):
        vec = Vector.from_arrow(pa.array(["banana", "apple", "cherry"], type=pa.string()))
        assert vec.min() == b"apple"

    def test_max_basic(self):
        vec = Vector.from_arrow(pa.array(["banana", "apple", "cherry"], type=pa.string()))
        assert vec.max() == b"cherry"

    def test_min_with_nulls(self):
        vec = Vector.from_arrow(pa.array(["zebra", None, "apple", None, "mango"], type=pa.string()))
        assert vec.min() == b"apple"

    def test_max_with_nulls(self):
        vec = Vector.from_arrow(pa.array(["zebra", None, "apple", None, "mango"], type=pa.string()))
        assert vec.max() == b"zebra"

    def test_lex_order_prefix(self):
        # Shorter prefix is lex-smaller than its extensions.
        vec = Vector.from_arrow(pa.array(["abc", "abcd", "ab"], type=pa.string()))
        assert vec.min() == b"ab"
        assert vec.max() == b"abcd"

    def test_lex_order_prefix_reverse_order(self):
        # Same content, different insertion order — must produce same answer.
        vec = Vector.from_arrow(pa.array(["ab", "abcd", "abc"], type=pa.string()))
        assert vec.min() == b"ab"
        assert vec.max() == b"abcd"

    def test_min_max_empty_returns_none(self):
        vec = Vector.from_arrow(pa.array([], type=pa.string()))
        assert vec.min() is None
        assert vec.max() is None

    def test_min_max_all_null_returns_none(self):
        vec = Vector.from_arrow(pa.array([None, None], type=pa.string()))
        assert vec.min() is None
        assert vec.max() is None

    def test_single_value(self):
        vec = Vector.from_arrow(pa.array(["solo"], type=pa.string()))
        assert vec.min() == b"solo"
        assert vec.max() == b"solo"

    def test_sum_raises(self):
        vec = Vector.from_arrow(pa.array(["a", "b"], type=pa.string()))
        with pytest.raises(NotImplementedError, match="not supported"):
            vec.sum()


# ---------------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------------

class TestDecimalAggregations:
    def test_sum_basic(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("1.50"), Decimal("2.50"), Decimal("3.00")],
                     type=pa.decimal128(10, 2))
        )
        assert vec.sum() == Decimal("7.00")

    def test_sum_with_nulls(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("1.50"), None, Decimal("2.50"), None],
                     type=pa.decimal128(10, 2))
        )
        assert vec.sum() == Decimal("4.00")

    def test_sum_all_nulls(self):
        vec = Vector.from_arrow(
            pa.array([None, None, None], type=pa.decimal128(10, 2))
        )
        assert vec.sum() == Decimal(0)

    def test_min_basic(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("5.50"), Decimal("1.10"), Decimal("3.30")],
                     type=pa.decimal128(10, 2))
        )
        assert vec.min() == Decimal("1.10")

    def test_max_basic(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("5.50"), Decimal("1.10"), Decimal("3.30")],
                     type=pa.decimal128(10, 2))
        )
        assert vec.max() == Decimal("5.50")

    def test_min_with_nulls(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("5.50"), None, Decimal("1.10"), None],
                     type=pa.decimal128(10, 2))
        )
        assert vec.min() == Decimal("1.10")

    def test_max_with_nulls(self):
        vec = Vector.from_arrow(
            pa.array([None, Decimal("9.90"), None, Decimal("1.10")],
                     type=pa.decimal128(10, 2))
        )
        assert vec.max() == Decimal("9.90")

    def test_negative_values(self):
        vec = Vector.from_arrow(
            pa.array([Decimal("-5.00"), Decimal("3.50"), Decimal("-10.25")],
                     type=pa.decimal128(10, 2))
        )
        assert vec.min() == Decimal("-10.25")
        assert vec.max() == Decimal("3.50")
        assert vec.sum() == Decimal("-11.75")

    def test_empty_raises(self):
        vec = Vector.from_arrow(pa.array([], type=pa.decimal128(10, 2)))
        with pytest.raises(ValueError, match="empty"):
            vec.min()
        with pytest.raises(ValueError, match="empty"):
            vec.max()

    def test_all_null_raises(self):
        vec = Vector.from_arrow(pa.array([None, None], type=pa.decimal128(10, 2)))
        with pytest.raises(ValueError, match="all-null"):
            vec.min()
        with pytest.raises(ValueError, match="all-null"):
            vec.max()


# ---------------------------------------------------------------------------
# Interval
# ---------------------------------------------------------------------------

class TestIntervalAggregations:
    def test_min_picks_smallest_microseconds(self):
        # (months, days, nanoseconds) — Draken stores months + microseconds.
        vec = Vector.from_arrow(pa.array(
            [(0, 5, 0), (0, 1, 0), (0, 3, 0)],
            type=pa.month_day_nano_interval(),
        ))
        result = vec.min()
        assert isinstance(result, tuple)
        assert len(result) == 2
        # 1 day < 3 days < 5 days
        assert result[1] == 1 * 86400 * 1_000_000

    def test_max_picks_largest_microseconds(self):
        vec = Vector.from_arrow(pa.array(
            [(0, 5, 0), (0, 1, 0), (0, 3, 0)],
            type=pa.month_day_nano_interval(),
        ))
        result = vec.max()
        assert result[1] == 5 * 86400 * 1_000_000

    def test_sum_adds_components(self):
        vec = Vector.from_arrow(pa.array(
            [(1, 0, 0), (2, 0, 0), (3, 0, 0)],
            type=pa.month_day_nano_interval(),
        ))
        months, micros = vec.sum()
        assert months == 6
        assert micros == 0

    def test_sum_with_nulls_skips(self):
        vec = Vector.from_arrow(pa.array(
            [(1, 0, 0), None, (2, 0, 0), None],
            type=pa.month_day_nano_interval(),
        ))
        months, _micros = vec.sum()
        assert months == 3

    def test_min_max_all_null_returns_none(self):
        vec = Vector.from_arrow(pa.array(
            [None, None], type=pa.month_day_nano_interval(),
        ))
        assert vec.min() is None
        assert vec.max() is None

    def test_sum_all_null_returns_none(self):
        vec = Vector.from_arrow(pa.array(
            [None, None], type=pa.month_day_nano_interval(),
        ))
        assert vec.sum() is None


# ---------------------------------------------------------------------------
# Array
# ---------------------------------------------------------------------------

class TestArrayAggregations:
    def _vec(self):
        return Vector.from_arrow(pa.array([[1, 2], [3], [4, 5, 6]], type=pa.list_(pa.int64())))

    def test_sum_raises(self):
        with pytest.raises(NotImplementedError, match="not supported"):
            self._vec().sum()

    def test_min_raises(self):
        with pytest.raises(NotImplementedError, match="not supported"):
            self._vec().min()

    def test_max_raises(self):
        with pytest.raises(NotImplementedError, match="not supported"):
            self._vec().max()


# ---------------------------------------------------------------------------
# Date32 / Timestamp sum (existing file only covers min/max)
# ---------------------------------------------------------------------------

class TestDate32Sum:
    def test_sum_basic(self):
        vec = Vector.from_arrow(pa.array([10, 20, 30], type=pa.date32()))
        assert vec.sum() == 60

    def test_sum_with_nulls(self):
        vec = Vector.from_arrow(pa.array([10, None, 20, None, 30], type=pa.date32()))
        assert vec.sum() == 60

    def test_sum_all_null(self):
        vec = Vector.from_arrow(pa.array([None, None], type=pa.date32()))
        assert vec.sum() == 0


class TestTimestampSum:
    def test_sum_basic(self):
        vec = Vector.from_arrow(pa.array([1000, 2000, 3000], type=pa.timestamp("us")))
        assert vec.sum() == 6000

    def test_sum_with_nulls(self):
        vec = Vector.from_arrow(
            pa.array([1000, None, 2000, None, 3000], type=pa.timestamp("us"))
        )
        assert vec.sum() == 6000


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------

class TestTimeAggregations:
    """Aggregations on TimeVector (time64 microseconds-since-midnight)."""

    def test_min_basic(self):
        vec = Vector.from_arrow(pa.array([3000, 1000, 2000], type=pa.time64("us")))
        assert vec.min() == 1000

    def test_max_basic(self):
        vec = Vector.from_arrow(pa.array([3000, 1000, 2000], type=pa.time64("us")))
        assert vec.max() == 3000

    def test_sum_basic(self):
        vec = Vector.from_arrow(pa.array([1000, 2000, 3000], type=pa.time64("us")))
        assert vec.sum() == 6000

    def test_min_with_nulls(self):
        vec = Vector.from_arrow(
            pa.array([3000, None, 1000, None, 2000], type=pa.time64("us"))
        )
        assert vec.min() == 1000

    def test_arrow_buffer_kept_alive(self):
        # Regression: TimeVector.from_arrow used to drop the Arrow data buffer,
        # so the underlying memory could be reused and reads returned garbage.
        import gc

        def _make():
            return Vector.from_arrow(
                pa.array([1000, 2000, 3000], type=pa.time64("us"))
            )

        vec = _make()
        gc.collect()
        # Force allocator churn that would have clobbered the freed buffer.
        _junk = [bytes(64) * 100 for _ in range(1000)]
        assert vec.min() == 1000
        assert vec.max() == 3000
        assert vec.sum() == 6000


# ---------------------------------------------------------------------------
# null_count across types (count-of-non-null = len(vec) - null_count)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("arr,expected_nulls", [
    (pa.array([1, 2, 3, 4], type=pa.int64()), 0),
    (pa.array([1, None, 3, None], type=pa.int64()), 2),
    (pa.array([None, None, None], type=pa.int64()), 3),
    (pa.array([1.0, None, 3.0], type=pa.float64()), 1),
    (pa.array([1.0, None, 3.0], type=pa.float32()), 1),
    (pa.array([True, False, None], type=pa.bool_()), 1),
    (pa.array(["a", None, "b"], type=pa.string()), 1),
    (pa.array([10, None, 20], type=pa.date32()), 1),
    (pa.array([1000, None, 2000], type=pa.timestamp("us")), 1),
    (pa.array([Decimal("1.0"), None, Decimal("2.0")], type=pa.decimal128(10, 2)), 1),
])
def test_null_count(arr, expected_nulls):
    vec = Vector.from_arrow(arr)
    assert vec.null_count == expected_nulls
    # count(non-null) is derivable from len + null_count
    assert len(vec) - vec.null_count == len(arr) - expected_nulls


def test_null_count_no_nulls_at_all():
    """Vectors built without a null bitmap should report zero nulls cheaply."""
    vec = Vector.from_arrow(pa.array(list(range(100)), type=pa.int64()))
    assert vec.null_count == 0


# ---------------------------------------------------------------------------
# Cross-type empty + all-null
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("arrow_type", [
    pa.int64(), pa.float64(), pa.float32(),
    pa.date32(), pa.timestamp("us"), pa.decimal128(10, 2),
])
def test_min_empty_raises_across_numeric_types(arrow_type):
    vec = Vector.from_arrow(pa.array([], type=arrow_type))
    with pytest.raises(ValueError, match="empty"):
        vec.min()


@pytest.mark.parametrize("arrow_type", [
    pa.int64(), pa.float64(), pa.float32(),
    pa.date32(), pa.timestamp("us"), pa.decimal128(10, 2),
])
def test_max_empty_raises_across_numeric_types(arrow_type):
    vec = Vector.from_arrow(pa.array([], type=arrow_type))
    with pytest.raises(ValueError, match="empty"):
        vec.max()


@pytest.mark.parametrize("arrow_type", [
    pa.int64(), pa.float64(), pa.float32(),
    pa.date32(), pa.timestamp("us"),
])
def test_sum_all_null_returns_zero_across_numeric_types(arrow_type):
    vec = Vector.from_arrow(pa.array([None, None, None], type=arrow_type))
    assert vec.sum() == 0


@pytest.mark.parametrize("arrow_type", [
    pa.int64(), pa.float64(), pa.float32(),
])
def test_min_all_null_raises_across_numeric_types(arrow_type):
    vec = Vector.from_arrow(pa.array([None, None], type=arrow_type))
    with pytest.raises(ValueError, match="all-null"):
        vec.min()


# ---------------------------------------------------------------------------
# Aggregation invariants
# ---------------------------------------------------------------------------

class TestAggregationInvariants:
    def test_min_le_max(self):
        vec = Vector.from_arrow(pa.array([7, 3, 9, 1, 5, 8, 2], type=pa.int64()))
        assert vec.min() <= vec.max()

    def test_sum_equals_python_sum(self):
        values = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]
        vec = Vector.from_arrow(pa.array(values, type=pa.int64()))
        assert vec.sum() == sum(values)

    def test_sum_skips_nulls_matches_filtered_python_sum(self):
        values = [3, None, 4, None, 5, 9, None, 6]
        vec = Vector.from_arrow(pa.array(values, type=pa.int64()))
        assert vec.sum() == sum(v for v in values if v is not None)
