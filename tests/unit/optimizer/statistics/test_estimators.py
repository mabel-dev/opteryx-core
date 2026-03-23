"""
Comprehensive tests for statistics estimation primitives.

Tests cover:
- ColumnRange intersection and width calculations
- ColumnStatistics selectivity estimation
- SelectivityEstimator with single and multiple predicates
- CardinalityEstimator for GROUP BY and JOINs
- RangeEstimator for column bound narrowing
- Exponential dampening factor behavior
"""

import pytest
from opteryx.planner.optimizer.statistics import (
    ColumnRange,
    ColumnStatistics,
    RelationStatistics,
    PredicateType,
    Predicate,
    SelectivityEstimator,
    CardinalityEstimator,
    RangeEstimator,
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


class TestSelectivityEstimator:
    """Tests for SelectivityEstimator class."""

    def test_default_dampening_factor(self):
        """Test default dampening factor."""
        estimator = SelectivityEstimator()
        assert estimator.dampening_factor == 0.75

    def test_custom_dampening_factor(self):
        """Test custom dampening factor."""
        estimator = SelectivityEstimator(dampening_factor=0.9)
        assert estimator.dampening_factor == 0.9

    def test_invalid_dampening_factor_zero(self):
        """Test that dampening factor of 0 is rejected."""
        with pytest.raises(ValueError):
            SelectivityEstimator(dampening_factor=0.0)

    def test_invalid_dampening_factor_negative(self):
        """Test that negative dampening factor is rejected."""
        with pytest.raises(ValueError):
            SelectivityEstimator(dampening_factor=-0.5)

    def test_invalid_dampening_factor_greater_than_one(self):
        """Test that dampening factor > 1 is rejected."""
        with pytest.raises(ValueError):
            SelectivityEstimator(dampening_factor=1.5)

    def test_estimate_single_predicate_equality(self):
        """Test single predicate estimation for equality."""
        estimator = SelectivityEstimator()
        col = ColumnStatistics(
            column_name="status",
            data_type="string",
            distinct_count=10,
        )
        stats = RelationStatistics(row_count=1000, columns={"status": col})

        pred = Predicate(
            column_name="status",
            predicate_type=PredicateType.EQUALITY,
            lower_bound="active",
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        # Should be 1 / 10 = 0.1
        assert selectivity == pytest.approx(0.1, abs=0.01)

    def test_estimate_single_predicate_range(self):
        """Test single predicate estimation for range."""
        estimator = SelectivityEstimator()
        col = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        stats = RelationStatistics(row_count=1000, columns={"age": col})

        pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=0,
            upper_bound=50,
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        assert selectivity == pytest.approx(0.5, abs=0.01)

    def test_estimate_single_predicate_in_list(self):
        """Test single predicate estimation for IN_LIST."""
        estimator = SelectivityEstimator()
        col = ColumnStatistics(
            column_name="region",
            data_type="string",
            distinct_count=50,
        )
        stats = RelationStatistics(row_count=1000, columns={"region": col})

        pred = Predicate(
            column_name="region",
            predicate_type=PredicateType.IN_LIST,
            values=["US", "CA", "MX"],
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        # 3 / 50 = 0.06
        assert selectivity == pytest.approx(0.06, abs=0.01)

    def test_estimate_single_predicate_in_list_exceeds_cardinality(self):
        """Test IN_LIST selectivity capped at 1.0."""
        estimator = SelectivityEstimator()
        col = ColumnStatistics(
            column_name="region",
            data_type="string",
            distinct_count=2,
        )
        stats = RelationStatistics(row_count=1000, columns={"region": col})

        pred = Predicate(
            column_name="region",
            predicate_type=PredicateType.IN_LIST,
            values=["US", "CA", "MX", "UK", "DE"],
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        assert selectivity == 1.0

    def test_estimate_single_predicate_like(self):
        """Test single predicate estimation for LIKE."""
        estimator = SelectivityEstimator()
        col = ColumnStatistics(
            column_name="name",
            data_type="string",
        )
        stats = RelationStatistics(row_count=1000, columns={"name": col})

        pred = Predicate(
            column_name="name",
            predicate_type=PredicateType.LIKE,
            pattern="A%",
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        # Conservative default for LIKE
        assert selectivity == 0.1

    def test_estimate_single_predicate_no_column_stats(self):
        """Test single predicate estimation when column has no stats."""
        estimator = SelectivityEstimator()
        stats = RelationStatistics(row_count=1000, columns={})

        pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=18,
            upper_bound=65,
        )
        selectivity = estimator.estimate_single_predicate(pred, stats)

        # Conservative default when no stats
        assert selectivity == 0.1

    def test_estimate_multiple_predicates_no_dampening_first_only(self):
        """Test that first predicate in multiple predicates has no dampening."""
        estimator = SelectivityEstimator(dampening_factor=0.75)
        col1 = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        stats = RelationStatistics(row_count=1000, columns={"age": col1})

        pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=0,
            upper_bound=50,
        )

        single_sel = estimator.estimate_single_predicate(pred, stats)
        multi_sel = estimator.estimate_multiple_predicates([pred], stats)

        # First predicate should be same in both
        assert single_sel == multi_sel

    def test_estimate_multiple_predicates_with_dampening(self):
        """Test multiple predicates with dampening applied."""
        estimator = SelectivityEstimator(dampening_factor=0.75)

        col1 = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        col2 = ColumnStatistics(
            column_name="salary",
            data_type="int",
            distinct_count=1000,
        )
        stats = RelationStatistics(
            row_count=10000,
            columns={"age": col1, "salary": col2}
        )

        pred1 = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=18,
            upper_bound=65,
        )
        pred2 = Predicate(
            column_name="salary",
            predicate_type=PredicateType.RANGE,
            lower_bound=50000,
            upper_bound=150000,
        )

        combined_sel = estimator.estimate_multiple_predicates([pred1, pred2], stats)

        # First predicate (age): (65 - 18) / (100 - 0) = 47/100 = 0.47
        sel1 = 0.47
        # Second predicate (salary): 0.1 * 0.75 = 0.075
        sel2 = 0.1 * 0.75

        expected = sel1 * sel2
        assert combined_sel == pytest.approx(expected, abs=0.01)

    def test_estimate_multiple_predicates_three_predicates(self):
        """Test dampening with three predicates."""
        estimator = SelectivityEstimator(dampening_factor=0.5)

        cols = {
            "col1": ColumnStatistics(
                column_name="col1",
                data_type="int",
                value_range=ColumnRange(lower_bound=0, upper_bound=100),
            ),
            "col2": ColumnStatistics(
                column_name="col2",
                data_type="int",
                distinct_count=10,
            ),
            "col3": ColumnStatistics(
                column_name="col3",
                data_type="int",
                distinct_count=10,
            ),
        }
        stats = RelationStatistics(row_count=10000, columns=cols)

        predicates = [
            Predicate(
                column_name="col1",
                predicate_type=PredicateType.RANGE,
                lower_bound=0,
                upper_bound=50,
            ),
            Predicate(
                column_name="col2",
                predicate_type=PredicateType.EQUALITY,
                lower_bound="value1",
            ),
            Predicate(
                column_name="col3",
                predicate_type=PredicateType.EQUALITY,
                lower_bound="value2",
            ),
        ]

        combined_sel = estimator.estimate_multiple_predicates(predicates, stats)

        # sel1: 50/100 = 0.5 (no dampening)
        # sel2: (1/10) * 0.5 = 0.05 (dampened)
        # sel3: (1/10) * 0.5 = 0.05 (dampened)
        expected = 0.5 * 0.05 * 0.05
        assert combined_sel == pytest.approx(expected, abs=0.001)

    def test_estimate_multiple_predicates_empty_list(self):
        """Test multiple predicates with empty list."""
        estimator = SelectivityEstimator()
        stats = RelationStatistics(row_count=1000, columns={})

        combined_sel = estimator.estimate_multiple_predicates([], stats)

        assert combined_sel == 1.0


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


class TestRangeEstimator:
    """Tests for RangeEstimator class."""

    def test_narrow_range_for_range_predicate(self):
        """Test narrowing range with RANGE predicate."""
        estimator = RangeEstimator()

        original = ColumnRange(lower_bound=0, upper_bound=120)
        predicate = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=18,
            upper_bound=65,
        )

        result = estimator.narrow_range_for_predicate(original, predicate)

        assert result.lower_bound == 18
        assert result.upper_bound == 65

    def test_narrow_range_for_equality_predicate(self):
        """Test narrowing range with EQUALITY predicate."""
        estimator = RangeEstimator()

        original = ColumnRange(lower_bound=0, upper_bound=120)
        predicate = Predicate(
            column_name="age",
            predicate_type=PredicateType.EQUALITY,
            lower_bound=42,
        )

        result = estimator.narrow_range_for_predicate(original, predicate)

        assert result.lower_bound == 42
        assert result.upper_bound == 42

    def test_narrow_range_for_in_list_predicate(self):
        """Test narrowing range with IN_LIST predicate."""
        estimator = RangeEstimator()

        original = ColumnRange(lower_bound=0, upper_bound=1000)
        predicate = Predicate(
            column_name="value",
            predicate_type=PredicateType.IN_LIST,
            values=[10, 50, 100],
        )

        result = estimator.narrow_range_for_predicate(original, predicate)

        assert result.lower_bound == 10
        assert result.upper_bound == 100

    def test_narrow_range_for_like_predicate(self):
        """Test narrowing range with LIKE predicate (no narrowing)."""
        estimator = RangeEstimator()

        original = ColumnRange(lower_bound="a", upper_bound="z")
        predicate = Predicate(
            column_name="name",
            predicate_type=PredicateType.LIKE,
            pattern="A%",
        )

        result = estimator.narrow_range_for_predicate(original, predicate)

        # LIKE doesn't narrow range
        assert result.lower_bound == "a"
        assert result.upper_bound == "z"

    def test_narrow_range_partial_overlap(self):
        """Test narrowing range with partial overlap."""
        estimator = RangeEstimator()

        original = ColumnRange(lower_bound=0, upper_bound=100)
        predicate = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=50,
            upper_bound=150,
        )

        result = estimator.narrow_range_for_predicate(original, predicate)

        assert result.lower_bound == 50
        assert result.upper_bound == 100


class TestIntegration:
    """Integration tests combining multiple estimation components."""

    def test_filter_followed_by_group_by(self):
        """Test cardinality flow through filter then GROUP BY."""
        selectivity_est = SelectivityEstimator()
        cardinality_est = CardinalityEstimator()

        # Original table
        col1 = ColumnStatistics(
            column_name="age",
            data_type="int",
            value_range=ColumnRange(lower_bound=0, upper_bound=100),
        )
        col2 = ColumnStatistics(
            column_name="region",
            data_type="string",
            distinct_count=10,
        )
        input_stats = RelationStatistics(
            row_count=100000,
            columns={"age": col1, "region": col2}
        )

        # Apply filter: age > 50
        filter_pred = Predicate(
            column_name="age",
            predicate_type=PredicateType.RANGE,
            lower_bound=50,
        )
        filter_selectivity = selectivity_est.estimate_single_predicate(
            filter_pred, input_stats
        )
        filtered_rows = cardinality_est.estimate_after_filter(
            input_stats, selectivity=filter_selectivity
        )

        # Apply GROUP BY region
        filtered_stats = input_stats.with_row_count(filtered_rows)
        grouped_rows = cardinality_est.estimate_group_by_cardinality(
            filtered_stats, group_columns=["region"]
        )

        # Expected: 100K * 0.5 = 50K after filter
        # Then GROUP BY region: min(10, 50K) = 10
        assert filtered_rows == 50000
        assert grouped_rows == 10

    def test_multiple_filters_with_dampening(self):
        """Test multiple filters applied with dampening."""
        selectivity_est = SelectivityEstimator(dampening_factor=0.75)
        cardinality_est = CardinalityEstimator()

        cols = {
            "age": ColumnStatistics(
                column_name="age",
                data_type="int",
                value_range=ColumnRange(lower_bound=0, upper_bound=100),
            ),
            "salary": ColumnStatistics(
                column_name="salary",
                data_type="int",
                value_range=ColumnRange(lower_bound=0, upper_bound=500000),
            ),
            "status": ColumnStatistics(
                column_name="status",
                data_type="string",
                distinct_count=5,
            ),
        }
        input_stats = RelationStatistics(row_count=100000, columns=cols)

        predicates = [
            Predicate("age", PredicateType.RANGE, lower_bound=30, upper_bound=60),
            Predicate("salary", PredicateType.RANGE, lower_bound=100000, upper_bound=400000),
            Predicate("status", PredicateType.EQUALITY, lower_bound="active"),
        ]

        combined_selectivity = selectivity_est.estimate_multiple_predicates(
            predicates, input_stats
        )
        output_rows = cardinality_est.estimate_after_filter(
            input_stats, selectivity=combined_selectivity
        )

        # Should apply dampening to predicates 2 and 3
        # Result should be much smaller than 100K but positive
        assert 0 < output_rows < 100000

    def test_range_narrowing_in_statistics_update(self):
        """Test how ranges narrow as data flows through operators."""
        range_est = RangeEstimator()

        original_col = ColumnStatistics(
            column_name="age",
            data_type="int",
            distinct_count=100,
            value_range=ColumnRange(lower_bound=0, upper_bound=120),
        )

        # Apply first filter: age >= 18
        pred1 = Predicate("age", PredicateType.RANGE, lower_bound=18)
        range1 = range_est.narrow_range_for_predicate(
            original_col.value_range, pred1
        )
        assert range1.lower_bound == 18
        assert range1.upper_bound == 120

        # Apply second filter: age <= 65
        pred2 = Predicate("age", PredicateType.RANGE, upper_bound=65)
        range2 = range_est.narrow_range_for_predicate(range1, pred2)
        assert range2.lower_bound == 18
        assert range2.upper_bound == 65

        # Now the range is narrower, selectivity would be more accurate
        # for subsequent predicates
