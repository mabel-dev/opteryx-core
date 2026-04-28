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
    PredicateType,
    Predicate,
    CardinalityEstimator,
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

    def test_estimate_selectivity_full_range(self):
        """Test selectivity estimation for full range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            distinct_count=100,
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        selectivity = col.estimate_selectivity(predicate_lower=0, predicate_upper=100)
        assert selectivity == 1.0

    def test_estimate_selectivity_half_range(self):
        """Test selectivity estimation for half the range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        selectivity = col.estimate_selectivity(predicate_lower=0, predicate_upper=50)
        assert selectivity == pytest.approx(0.5, abs=0.01)

    def test_estimate_selectivity_quarter_range(self):
        """Test selectivity estimation for quarter of the range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        selectivity = col.estimate_selectivity(predicate_lower=25, predicate_upper=75)
        assert selectivity == pytest.approx(0.5, abs=0.01)

    def test_estimate_selectivity_no_bounds(self):
        """Test selectivity with no predicate bounds returns 1.0."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        selectivity = col.estimate_selectivity()
        assert selectivity == 1.0

    def test_estimate_selectivity_no_range_info(self):
        """Test selectivity when column has no range info."""
        col = ColumnStatistics(
            column_name="name",
            data_type="string",
        )
        selectivity = col.estimate_selectivity(predicate_lower="a", predicate_upper="z")
        assert selectivity == 0.1  # Conservative default

    def test_estimate_selectivity_outside_range(self):
        """Test selectivity for predicate outside column range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        # Predicate asking for ages 200-300, but column only has 0-100
        selectivity = col.estimate_selectivity(predicate_lower=200, predicate_upper=300)
        assert selectivity == 0.0

    def test_estimate_selectivity_partial_overlap(self):
        """Test selectivity for predicate partially overlapping column range."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        # Predicate 50-150, column is 0-100, intersection is 50-100 (width 50)
        selectivity = col.estimate_selectivity(predicate_lower=50, predicate_upper=150)
        assert selectivity == pytest.approx(0.5, abs=0.01)

    def test_estimate_selectivity_lower_bound_only(self):
        """Test selectivity with only lower bound."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        # age >= 75
        selectivity = col.estimate_selectivity(predicate_lower=75)
        assert selectivity == pytest.approx(0.25, abs=0.01)

    def test_estimate_selectivity_upper_bound_only(self):
        """Test selectivity with only upper bound."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        # age <= 25
        selectivity = col.estimate_selectivity(predicate_upper=25)
        assert selectivity == pytest.approx(0.25, abs=0.01)


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
        stats = RelationStatistics(row_count=10000, columns={"age": col1, "name": col2})

        assert stats.row_count == 10000
        assert len(stats.columns) == 2

    def test_relation_statistics_get_column(self):
        """Test retrieving column statistics."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
        )
        stats = RelationStatistics(row_count=10000, columns={"age": col})

        retrieved = stats.get_column("age")
        assert retrieved is not None
        assert retrieved.column_name == "age"

    def test_relation_statistics_get_nonexistent_column(self):
        """Test retrieving nonexistent column."""
        stats = RelationStatistics(row_count=10000, columns={})
        retrieved = stats.get_column("age")
        assert retrieved is None

    def test_relation_statistics_copy(self):
        """Test copying relation statistics."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
        )
        stats = RelationStatistics(row_count=10000, columns={"age": col})
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
        stats = RelationStatistics(row_count=10000, columns={"age": col})
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
        stats = RelationStatistics(row_count=10000, columns={"age": col})

        new_range = ColumnRange(lower_bound=18, upper_bound=65)
        new_stats = stats.update_column_range("age", new_range)

        assert stats.columns["age"].value_range.lower_bound == 0  # Original unchanged
        assert new_stats.columns["age"].value_range.lower_bound == 18


class TestPredicate:
    """Tests for Predicate class."""

    def test_predicate_range(self):
        """Test creating a range predicate."""
        pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=18,
            upper_bound=65,
        )
        assert pred.column_name == "age"
        assert pred.predicate_type == PredicateType.RANGE
        assert pred.lower_bound == 18

    def test_predicate_equality(self):
        """Test creating an equality predicate."""
        pred = Predicate(
            column_name="status",
            predicate_type=PredicateType.EQUALITY,
            lower_bound="active",
        )
        assert pred.column_name == "status"
        assert pred.predicate_type == PredicateType.EQUALITY

    def test_predicate_in_list(self):
        """Test creating an IN_LIST predicate."""
        pred = Predicate(
            column_name="region",
            predicate_type=PredicateType.IN_LIST,
            values=["US", "CA", "MX"],
        )
        assert pred.column_name == "region"
        assert pred.values == ["US", "CA", "MX"]

    def test_predicate_to_range_bounds_range(self):
        """Test converting RANGE predicate to bounds."""
        pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=18,
            upper_bound=65,
        )
        lower, upper = pred.to_range_bounds()
        assert lower == 18
        assert upper == 65

    def test_predicate_to_range_bounds_equality(self):
        """Test converting EQUALITY predicate to bounds."""
        pred = Predicate(
            column_name="status",
            predicate_type=PredicateType.EQUALITY,
            lower_bound="active",
        )
        lower, upper = pred.to_range_bounds()
        assert lower == "active"
        assert upper == "active"

    def test_predicate_to_range_bounds_in_list(self):
        """Test converting IN_LIST predicate to bounds."""
        pred = Predicate(
            column_name="region",
            predicate_type=PredicateType.IN_LIST,
            values=[10, 20, 30],
        )
        lower, upper = pred.to_range_bounds()
        assert lower == 10
        assert upper == 30


class TestCardinalityEstimator:
    """Tests for CardinalityEstimator class."""

    def test_estimate_after_filter_half_rows(self):
        """Test cardinality after filter removing half the rows."""
        estimator = CardinalityEstimator()
        stats = RelationStatistics(row_count=10000, columns={})

        output = estimator.estimate_after_filter(stats, selectivity=0.5)
        assert output == 5000

    def test_estimate_after_filter_minimum_one_row(self):
        """Test that cardinality is at least 1."""
        estimator = CardinalityEstimator()
        stats = RelationStatistics(row_count=10000, columns={})

        output = estimator.estimate_after_filter(stats, selectivity=0.00001)
        assert output >= 1

    def test_estimate_after_filter_zero_selectivity(self):
        """Test filter with zero selectivity still returns at least 1."""
        estimator = CardinalityEstimator()
        stats = RelationStatistics(row_count=10000, columns={})

        output = estimator.estimate_after_filter(stats, selectivity=0.0)
        assert output >= 1

    def test_estimate_group_by_no_columns(self):
        """Test GROUP BY with no columns returns 1."""
        estimator = CardinalityEstimator()
        stats = RelationStatistics(row_count=10000, columns={})

        output = estimator.estimate_group_by_cardinality(stats, group_columns=[])
        assert output == 1

    def test_estimate_group_by_single_column(self):
        """Test GROUP BY with single column."""
        estimator = CardinalityEstimator()
        col = ColumnStatistics(
            column_name="region",
            data_type="string",
            distinct_count=50,
        )
        stats = RelationStatistics(row_count=10000, columns={"region": col})

        output = estimator.estimate_group_by_cardinality(stats, group_columns=["region"])
        assert output == 50

    def test_estimate_group_by_multiple_columns(self):
        """Test GROUP BY with multiple columns."""
        estimator = CardinalityEstimator()
        cols = {
            "region": ColumnStatistics(
                column_name="region",
                data_type="string",
                distinct_count=5,
            ),
            "category": ColumnStatistics(
                column_name="category",
                data_type="string",
                distinct_count=10,
            ),
        }
        stats = RelationStatistics(row_count=10000, columns=cols)

        output = estimator.estimate_group_by_cardinality(
            stats, group_columns=["region", "category"]
        )
        # 5 * 10 = 50
        assert output == 50

    def test_estimate_group_by_cardinality_exceeds_input_rows(self):
        """Test GROUP BY cardinality capped at input rows."""
        estimator = CardinalityEstimator()
        cols = {
            "col1": ColumnStatistics(
                column_name="col1",
                data_type="int",
                distinct_count=1000,
            ),
            "col2": ColumnStatistics(
                column_name="col2",
                data_type="int",
                distinct_count=500,
            ),
        }
        stats = RelationStatistics(row_count=100, columns=cols)

        output = estimator.estimate_group_by_cardinality(
            stats, group_columns=["col1", "col2"]
        )
        # min(1000 * 500, 100) = 100
        assert output == 100

    def test_estimate_group_by_no_cardinality_info(self):
        """Test GROUP BY when column has no cardinality info."""
        estimator = CardinalityEstimator()
        col = ColumnStatistics(
            column_name="region",
            data_type="string",
        )
        stats = RelationStatistics(row_count=10000, columns={"region": col})

        output = estimator.estimate_group_by_cardinality(stats, group_columns=["region"])
        # Falls back to input_rows // 2 = 5000
        assert output == 5000

    def test_estimate_join_cardinality_inner(self):
        """Test inner join cardinality estimation."""
        estimator = CardinalityEstimator()

        left_col = ColumnStatistics(
            column_name="customer_id",
            data_type="int",
            distinct_count=50000,
        )
        right_col = ColumnStatistics(
            column_name="c_id",
            data_type="int",
            distinct_count=100000,
        )

        left_stats = RelationStatistics(row_count=1000000, columns={"customer_id": left_col})
        right_stats = RelationStatistics(row_count=100000, columns={"c_id": right_col})

        output = estimator.estimate_join_cardinality(
            left_stats,
            right_stats,
            left_key="customer_id",
            right_key="c_id",
            join_type="inner",
        )

        # (1M * 100K) / max(50K, 100K) = 100M / 100K = 1M
        assert output == 1000000

    def test_estimate_join_cardinality_left(self):
        """Test left join cardinality is at least left input rows."""
        estimator = CardinalityEstimator()

        left_col = ColumnStatistics(
            column_name="customer_id",
            data_type="int",
            distinct_count=50000,
        )
        right_col = ColumnStatistics(
            column_name="c_id",
            data_type="int",
            distinct_count=1000,
        )

        left_stats = RelationStatistics(row_count=100000, columns={"customer_id": left_col})
        right_stats = RelationStatistics(row_count=5000, columns={"c_id": right_col})

        output = estimator.estimate_join_cardinality(
            left_stats,
            right_stats,
            left_key="customer_id",
            right_key="c_id",
            join_type="left",
        )

        # Should be at least 100000 (left input rows)
        assert output >= 100000

    def test_estimate_join_cardinality_right(self):
        """Test right join cardinality is at least right input rows."""
        estimator = CardinalityEstimator()

        left_col = ColumnStatistics(
            column_name="customer_id",
            data_type="int",
            distinct_count=1000,
        )
        right_col = ColumnStatistics(
            column_name="c_id",
            data_type="int",
            distinct_count=50000,
        )

        left_stats = RelationStatistics(row_count=5000, columns={"customer_id": left_col})
        right_stats = RelationStatistics(row_count=100000, columns={"c_id": right_col})

        output = estimator.estimate_join_cardinality(
            left_stats,
            right_stats,
            left_key="customer_id",
            right_key="c_id",
            join_type="right",
        )

        # Should be at least 100000 (right input rows)
        assert output >= 100000

    def test_estimate_join_cardinality_outer(self):
        """Test outer join cardinality is at least max of inputs."""
        estimator = CardinalityEstimator()

        left_col = ColumnStatistics(
            column_name="customer_id",
            data_type="int",
            distinct_count=50000,
        )
        right_col = ColumnStatistics(
            column_name="c_id",
            data_type="int",
            distinct_count=100000,
        )

        left_stats = RelationStatistics(row_count=100000, columns={"customer_id": left_col})
        right_stats = RelationStatistics(row_count=200000, columns={"c_id": right_col})

        output = estimator.estimate_join_cardinality(
            left_stats,
            right_stats,
            left_key="customer_id",
            right_key="c_id",
            join_type="outer",
        )

        # Should be at least 200000 (max of inputs)
        assert output >= 200000


class TestHistogramBacking:
    """Tests for histogram-backed selectivity estimation."""

    def _create_distogram_from_bins(self):
        """Helper to create a simple distogram."""
        from opteryx.third_party.maki_nage.distogram import load

        # Create a distogram from 100 bins, min=0, max=100
        # Simulating uniform distribution: 1000 rows across 100 bins = 10 rows per bin
        bins = [(i + 0.5, 10) for i in range(100)]
        return load(bins, 0.0, 100.0)

    def test_estimate_selectivity_with_histogram_no_histogram(self):
        """Test that None is returned when no histogram available."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=None,  # No histogram
            _total_rows=1000,
        )

        selectivity = col.estimate_selectivity_with_histogram(
            predicate_lower=25, predicate_upper=75
        )
        assert selectivity is None

    def test_estimate_selectivity_with_histogram_no_total_rows(self):
        """Test that None is returned when total_rows is None."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=None,  # Missing total rows
        )

        selectivity = col.estimate_selectivity_with_histogram(
            predicate_lower=25, predicate_upper=75
        )
        assert selectivity is None

    def test_estimate_selectivity_with_histogram_full_range(self):
        """Test selectivity for full range with histogram."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        selectivity = col.estimate_selectivity_with_histogram()
        assert selectivity == 1.0

    def test_estimate_selectivity_with_histogram_half_range(self):
        """Test selectivity for half the range with histogram."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        # Range [0, 50] is half of [0, 100]
        selectivity = col.estimate_selectivity_with_histogram(
            predicate_lower=0, predicate_upper=50
        )

        # Should be approximately 0.5
        assert selectivity == pytest.approx(0.5, abs=0.05)

    def test_estimate_selectivity_with_histogram_quarter_range(self):
        """Test selectivity for quarter range with histogram."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        # Range [25, 75] is half of [0, 100]
        selectivity = col.estimate_selectivity_with_histogram(
            predicate_lower=25, predicate_upper=75
        )

        # Should be approximately 0.5
        assert selectivity == pytest.approx(0.5, abs=0.05)

    def test_estimate_selectivity_with_histogram_lower_bound_only(self):
        """Test selectivity with only lower bound."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        # age >= 75, should be approximately 0.25
        selectivity = col.estimate_selectivity_with_histogram(predicate_lower=75)
        assert selectivity == pytest.approx(0.25, abs=0.05)

    def test_estimate_selectivity_with_histogram_upper_bound_only(self):
        """Test selectivity with only upper bound."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        # age <= 25, should be approximately 0.25
        selectivity = col.estimate_selectivity_with_histogram(predicate_upper=25)
        assert selectivity == pytest.approx(0.25, abs=0.05)

    def test_estimate_selectivity_with_histogram_outside_bounds(self):
        """Test selectivity for range outside histogram bounds."""
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            histogram=self._create_distogram_from_bins(),
            _total_rows=1000,
        )

        # Range [200, 300] is outside [0, 100]
        selectivity = col.estimate_selectivity_with_histogram(
            predicate_lower=200, predicate_upper=300
        )

        # Should return None because outside bounds
        assert selectivity is None

