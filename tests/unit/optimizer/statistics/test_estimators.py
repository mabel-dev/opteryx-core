"""
Comprehensive tests for statistics estimation primitives.

Tests cover:
- ColumnRange intersection and width calculations
- ColumnStatistics selectivity estimation (uniform and histogram-backed)
- CardinalityEstimator for GROUP BY and JOINs
"""

import pytest
from opteryx.planner.optimizer.statistics import (
    ColumnRange,
    ColumnStatistics,
    RelationStatistics,
)


class TestColumnRange:
    """Tests for ColumnRange class."""

    def test_range_creation(self):
        """Test creating a range with bounds."""
        r = ColumnRange(lower_bound=10, upper_bound=100)
        assert r.lower_bound == 10
        assert r.upper_bound == 100

    def test_range_open_ended_lower(self):
        """Test range with open lower bound."""
        r = ColumnRange(upper_bound=100)
        assert r.lower_bound is None
        assert r.upper_bound == 100

    def test_range_open_ended_upper(self):
        """Test range with open upper bound."""
        r = ColumnRange(lower_bound=10)
        assert r.lower_bound == 10
        assert r.upper_bound is None

    def test_range_width_calculation(self):
        """Test width calculation for numeric ranges."""
        r = ColumnRange(lower_bound=10, upper_bound=100)
        assert r.width() == 90.0

    def test_range_width_with_floats(self):
        """Test width calculation with float bounds."""
        r = ColumnRange(lower_bound=10.5, upper_bound=20.5)
        assert r.width() == 10.0

    def test_range_width_with_no_bounds(self):
        """Test width returns None when bounds are missing."""
        r1 = ColumnRange()
        assert r1.width() is None

        r2 = ColumnRange(lower_bound=10)
        assert r2.width() is None

        r3 = ColumnRange(upper_bound=100)
        assert r3.width() is None

    def test_range_width_with_strings(self):
        """Test width returns None for non-numeric ranges."""
        r = ColumnRange(lower_bound="a", upper_bound="z")
        assert r.width() is None

    def test_range_width_negative(self):
        """Test width with negative numbers."""
        r = ColumnRange(lower_bound=-100, upper_bound=-10)
        assert r.width() == 90.0

    def test_range_width_crossing_zero(self):
        """Test width for range that crosses zero."""
        r = ColumnRange(lower_bound=-50, upper_bound=50)
        assert r.width() == 100.0

    def test_range_intersection_both_bounded(self):
        """Test intersection of two fully bounded ranges."""
        r1 = ColumnRange(lower_bound=10, upper_bound=100)
        r2 = ColumnRange(lower_bound=50, upper_bound=150)

        result = r1.intersect(r2)
        assert result.lower_bound == 50
        assert result.upper_bound == 100

    def test_range_intersection_no_overlap(self):
        """Test intersection of non-overlapping ranges."""
        r1 = ColumnRange(lower_bound=10, upper_bound=50)
        r2 = ColumnRange(lower_bound=60, upper_bound=100)

        result = r1.intersect(r2)
        assert result.lower_bound == 60
        assert result.upper_bound == 50  # Invalid range (lower > upper)

    def test_range_intersection_one_open_lower(self):
        """Test intersection when one range has open lower bound."""
        r1 = ColumnRange(upper_bound=100)
        r2 = ColumnRange(lower_bound=50, upper_bound=150)

        result = r1.intersect(r2)
        assert result.lower_bound == 50
        assert result.upper_bound == 100

    def test_range_intersection_one_open_upper(self):
        """Test intersection when one range has open upper bound."""
        r1 = ColumnRange(lower_bound=10)
        r2 = ColumnRange(lower_bound=50, upper_bound=150)

        result = r1.intersect(r2)
        assert result.lower_bound == 50
        assert result.upper_bound == 150

    def test_range_intersection_both_open(self):
        """Test intersection when both ranges are open on same side."""
        r1 = ColumnRange(lower_bound=None, upper_bound=100)
        r2 = ColumnRange(lower_bound=None, upper_bound=150)

        result = r1.intersect(r2)
        assert result.lower_bound is None
        assert result.upper_bound == 100

    def test_range_intersection_identical(self):
        """Test intersection of identical ranges."""
        r = ColumnRange(lower_bound=10, upper_bound=100)
        result = r.intersect(r)

        assert result.lower_bound == 10
        assert result.upper_bound == 100


# RelationStatistics.columns is keyed by opaque column identity (bytes), not by
# name — names are not unique across a plan.
_AGE = b"tes_age_00000001"
_NAME = b"tes_nam_00000002"


class TestColumnStatistics:
    """Tests for ColumnStatistics class."""

    def test_column_statistics_creation(self):
        """Test creating column statistics."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            distinct_count=100,
            value_range=ColumnRange(lower_bound=0, upper_bound=120),
        )
        assert col.column_name == "age"
        assert col.data_type == "int"
        assert col.distinct_count == 100


class TestRelationStatistics:
    """Tests for RelationStatistics class."""

    def test_relation_statistics_creation(self):
        """Test creating relation statistics."""
        col1 = ColumnStatistics(
            column_name="age",
            data_type="int",
            distinct_count=100,
            value_range=ColumnRange(lower_bound=0, upper_bound=120),
        )
        col2 = ColumnStatistics(
            column_name="name",
            data_type="string",
        )
        stats = RelationStatistics(row_count=10000, columns={_AGE: col1, _NAME: col2})

        assert stats.row_count == 10000
        assert len(stats.columns) == 2

    def test_relation_statistics_get_column(self):
        """Test retrieving column statistics."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
        )
        stats = RelationStatistics(row_count=10000, columns={_AGE: col})

        retrieved = stats.get_column(_AGE)
        assert retrieved is not None
        assert retrieved.column_name == "age"

    def test_relation_statistics_get_nonexistent_column(self):
        """Test retrieving nonexistent column."""
        stats = RelationStatistics(row_count=10000, columns={})
        retrieved = stats.get_column(_AGE)
        assert retrieved is None

    def test_relation_statistics_copy(self):
        """Test copying relation statistics."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
        )
        stats = RelationStatistics(row_count=10000, columns={_AGE: col})
        stats_copy = stats.copy()

        assert stats_copy.row_count == 10000
        assert stats_copy is not stats
        assert stats_copy.columns is not stats.columns

    def test_relation_statistics_with_row_count(self):
        """Test updating row count."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
        )
        stats = RelationStatistics(row_count=10000, columns={_AGE: col})
        new_stats = stats.with_row_count(5000)

        assert stats.row_count == 10000  # Original unchanged
        assert new_stats.row_count == 5000

    def test_relation_statistics_update_column_range(self):
        """Test updating column range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=120),
        )
        stats = RelationStatistics(row_count=10000, columns={_AGE: col})

        new_range = ColumnRange(lower_bound=18, upper_bound=65)
        new_stats = stats.update_column_range(_AGE, new_range)

        assert stats.columns[_AGE].value_range.lower_bound == 0  # Original unchanged
        assert new_stats.columns[_AGE].value_range.lower_bound == 18


class TestCardinalityFunctions:
    """Tests for the pure cardinality functions in cost_estimation."""

    def test_estimate_after_filter_basic(self):
        from opteryx.planner.cost_estimation import estimate_after_filter
        assert estimate_after_filter(1000, 0.1) == 100
        assert estimate_after_filter(1000, 1.0) == 1000
        assert estimate_after_filter(1000, 0.0) == 1  # floored at 1
        assert estimate_after_filter(0, 0.5) == 1     # floored at 1

    def test_estimate_after_filter_rejects_negative(self):
        import pytest
        from opteryx.planner.cost_estimation import estimate_after_filter
        with pytest.raises(ValueError):
            estimate_after_filter(-1, 0.5)
        with pytest.raises(ValueError):
            estimate_after_filter(100, -0.1)

    def test_estimate_group_by_cardinality_known_ndvs(self):
        from opteryx.planner.cost_estimation import estimate_group_by_cardinality
        # 100 input rows, two group keys with NDV 3 and 4 -> 12, capped by input.
        assert estimate_group_by_cardinality(100, [3, 4]) == 12
        # Cap at input rows when product exceeds it.
        assert estimate_group_by_cardinality(10, [50, 50]) == 10

    def test_estimate_group_by_cardinality_unknown_ndvs(self):
        from opteryx.planner.cost_estimation import estimate_group_by_cardinality
        # Unknown NDV falls back to input_rows / 2 per missing key.
        assert estimate_group_by_cardinality(100, [None]) == 50
        assert estimate_group_by_cardinality(100, []) == 1
        assert estimate_group_by_cardinality(0, [10]) == 1

