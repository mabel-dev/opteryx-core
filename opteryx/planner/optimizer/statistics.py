"""
Statistics estimation primitives for cardinality and range predictions.

Supports:
- Single predicate estimation with histogram backing
- Multiple predicate estimation with selectivity dampening
- Range narrowing for column bounds
- Cardinality estimation for GROUP BY and joins
"""

from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from typing import Optional
from typing import Tuple
from typing import Union


@dataclass(frozen=True)
class ColumnRange:
    """Represents the range of values in a column."""

    lower_bound: Optional[Union[int, float, str]] = None
    upper_bound: Optional[Union[int, float, str]] = None

    def intersect(self, other: "ColumnRange") -> "ColumnRange":
        """Compute intersection of two ranges."""
        if self.lower_bound is None and other.lower_bound is None:
            new_lower = None
        elif self.lower_bound is None:
            new_lower = other.lower_bound
        elif other.lower_bound is None:
            new_lower = self.lower_bound
        else:
            new_lower = max(self.lower_bound, other.lower_bound)

        if self.upper_bound is None and other.upper_bound is None:
            new_upper = None
        elif self.upper_bound is None:
            new_upper = other.upper_bound
        elif other.upper_bound is None:
            new_upper = self.upper_bound
        else:
            new_upper = min(self.upper_bound, other.upper_bound)

        return ColumnRange(new_lower, new_upper)

    def width(self) -> Optional[float]:
        """Estimate the span of the range (for numeric types)."""
        if self.lower_bound is None or self.upper_bound is None:
            return None
        try:
            return float(self.upper_bound) - float(self.lower_bound)
        except (TypeError, ValueError):
            return None


@dataclass
class ColumnStatistics:
    """Statistics for a single column."""

    column_name: str
    data_type: str

    # Cardinality: number of distinct values
    distinct_count: Optional[int] = None

    # Range of values
    value_range: ColumnRange = ColumnRange()

    # Distribution information (histogram, sketch, etc.)
    # For now: None, will be extended with histogram/sketch backing
    histogram: Optional[object] = None

    def estimate_selectivity(
        self, predicate_lower: Optional[float] = None, predicate_upper: Optional[float] = None
    ) -> float:
        """
        Estimate selectivity of a range predicate on this column.

        Args:
            predicate_lower: Lower bound of predicate (inclusive)
            predicate_upper: Upper bound of predicate (inclusive)

        Returns:
            Estimated selectivity in [0, 1]
        """
        if predicate_lower is None and predicate_upper is None:
            return 1.0

        # No histogram—estimate using uniform distribution assumption
        range_width = self.value_range.width()
        if range_width is None or range_width == 0:
            return 0.1  # Conservative default

        # Compute intersection of predicate range with column range
        predicate_range = ColumnRange(predicate_lower, predicate_upper)
        intersection = self.value_range.intersect(predicate_range)

        intersection_width = intersection.width()
        if intersection_width is None:
            return 0.1

        # Selectivity = intersection_width / range_width
        selectivity = intersection_width / range_width
        return max(0.0, min(1.0, selectivity))


@dataclass
class RelationStatistics:
    """Statistics for an entire relation/intermediate result."""

    row_count: int
    columns: dict[str, ColumnStatistics]

    def copy(self) -> "RelationStatistics":
        """Create a shallow copy with new column dict."""
        return RelationStatistics(
            row_count=self.row_count, columns={k: v for k, v in self.columns.items()}
        )

    def get_column(self, column_name: str) -> Optional[ColumnStatistics]:
        """Retrieve statistics for a column."""
        return self.columns.get(column_name)

    def with_row_count(self, new_count: int) -> "RelationStatistics":
        """Return a copy with updated row count."""
        return replace(self, row_count=new_count)

    def update_column_range(self, column_name: str, new_range: ColumnRange) -> "RelationStatistics":
        """Return a copy with an updated column range."""
        new_stats = self.copy()
        col_stats = new_stats.columns.get(column_name)
        if col_stats:
            new_col = replace(col_stats, value_range=new_range)
            new_stats.columns[column_name] = new_col
        return new_stats


class PredicateType(Enum):
    """Types of predicates we can estimate."""

    RANGE = "range"  # col >= x AND col <= y
    EQUALITY = "equality"  # col = x
    IN_LIST = "in_list"  # col IN (x, y, z)
    LIKE = "like"  # col LIKE pattern (string)


@dataclass(frozen=True)
class Predicate:
    """Represents a single filter predicate for estimation."""

    column_name: str
    predicate_type: PredicateType
    lower_bound: Optional[Union[int, float, str]] = None
    upper_bound: Optional[Union[int, float, str]] = None
    values: Optional[list] = None  # For IN_LIST
    pattern: Optional[str] = None  # For LIKE

    def to_range_bounds(self) -> Tuple[Optional[float], Optional[float]]:
        """Convert predicate to lower/upper bounds if possible."""
        if self.predicate_type == PredicateType.RANGE:
            return (self.lower_bound, self.upper_bound)
        elif self.predicate_type == PredicateType.EQUALITY:
            return (self.lower_bound, self.lower_bound)
        elif self.predicate_type == PredicateType.IN_LIST:
            # Conservative: assume it covers some range
            return (
                min(self.values) if self.values else None,
                max(self.values) if self.values else None,
            )
        else:
            return (None, None)


class SelectivityEstimator:
    """
    Estimates selectivity of single and multiple predicates.

    Uses exponential dampening when applying multiple predicates to
    account for correlation between filters on different columns.
    """

    # Dampening factor applied to each successive predicate on different columns
    # Range: (0, 1]. Lower = more pessimistic about subsequent predicates
    DEFAULT_DAMPENING_FACTOR = 0.75

    def __init__(self, dampening_factor: float = DEFAULT_DAMPENING_FACTOR):
        """
        Args:
            dampening_factor: Multiplier applied to each successive predicate's
                            selectivity. Accounts for correlation assumptions.
                            Typical range: 0.5 - 0.9
        """
        if not 0 < dampening_factor <= 1.0:
            raise ValueError(f"Dampening factor must be in (0, 1], got {dampening_factor}")
        self.dampening_factor = dampening_factor

    def estimate_single_predicate(self, predicate: Predicate, stats: RelationStatistics) -> float:
        """
        Estimate selectivity of a single predicate using column statistics.

        Args:
            predicate: The filter predicate
            stats: Statistics of the relation being filtered

        Returns:
            Estimated selectivity in [0, 1]
        """
        col_stats = stats.get_column(predicate.column_name)
        if col_stats is None:
            return 0.1  # Conservative default if no stats

        if predicate.predicate_type == PredicateType.EQUALITY:
            # Selectivity ≈ 1 / distinct_count
            if col_stats.distinct_count and col_stats.distinct_count > 0:
                return 1.0 / col_stats.distinct_count
            return 0.1

        elif predicate.predicate_type == PredicateType.RANGE:
            lower, upper = predicate.to_range_bounds()
            return col_stats.estimate_selectivity(lower, upper)

        elif predicate.predicate_type == PredicateType.IN_LIST:
            # Selectivity ≈ |values| / distinct_count
            if col_stats.distinct_count and col_stats.distinct_count > 0 and predicate.values:
                return min(1.0, len(predicate.values) / col_stats.distinct_count)
            return 0.1

        elif predicate.predicate_type == PredicateType.LIKE:
            # Conservative estimate for LIKE patterns
            return 0.1

        return 0.1

    def estimate_multiple_predicates(
        self, predicates: list[Predicate], stats: RelationStatistics
    ) -> float:
        """
        Estimate selectivity of multiple predicates using exponential dampening.

        Assumes predicates are on different columns. For predicates on the same
        column, they should be merged into a single range predicate first.

        The first predicate uses its actual selectivity. Each successive predicate
        on a different column has its selectivity dampened to account for
        correlation between filters.

        Example with dampening_factor=0.75:
            - Filter 1: s1 = 0.10 (10%)
            - Filter 2: s2_raw = 0.20, but s2 = 0.20 * 0.75 = 0.15 (15%)
            - Filter 3: s3_raw = 0.15, but s3 = 0.15 * 0.75 = 0.1125 (11.25%)
            - Total: 0.10 * 0.15 * 0.1125 ≈ 0.00169 (0.169%)

        Args:
            predicates: List of filter predicates on different columns
            stats: Statistics of the relation being filtered

        Returns:
            Estimated combined selectivity in [0, 1]
        """
        if not predicates:
            return 1.0

        combined_selectivity = 1.0

        for i, predicate in enumerate(predicates):
            selectivity = self.estimate_single_predicate(predicate, stats)

            # Apply dampening to all but the first predicate
            if i > 0:
                selectivity *= self.dampening_factor

            combined_selectivity *= selectivity

        return max(0.0, min(1.0, combined_selectivity))


class CardinalityEstimator:
    """
    Estimates cardinality of intermediate results and transformations.
    """

    def estimate_after_filter(self, input_stats: RelationStatistics, selectivity: float) -> int:
        """
        Estimate row count after applying a filter.

        Args:
            input_stats: Input relation statistics
            selectivity: Estimated selectivity of filter (0 to 1)

        Returns:
            Estimated output row count
        """
        output_count = int(input_stats.row_count * selectivity)
        return max(1, output_count)  # At least 1 row

    def estimate_group_by_cardinality(
        self, input_stats: RelationStatistics, group_columns: list[str]
    ) -> int:
        """
        Estimate cardinality after GROUP BY.

        The output row count is at most the product of the cardinalities
        of the grouping columns (if they're independent).

        Args:
            input_stats: Input relation statistics
            group_columns: Names of columns in GROUP BY

        Returns:
            Estimated output row count
        """
        if not group_columns:
            return 1  # GROUP BY with no columns returns 1 row

        # Product of distinct counts
        cardinality = 1
        for col_name in group_columns:
            col_stats = input_stats.get_column(col_name)
            if col_stats and col_stats.distinct_count:
                cardinality *= col_stats.distinct_count
            else:
                # No cardinality info—assume half the input rows
                cardinality *= input_stats.row_count // 2

        # Output can't exceed input rows
        return min(cardinality, input_stats.row_count)

    def estimate_join_cardinality(
        self,
        left_stats: RelationStatistics,
        right_stats: RelationStatistics,
        left_key: str,
        right_key: str,
        join_type: str = "inner",
    ) -> int:
        """
        Estimate output cardinality of a join.

        For inner joins with uniform distribution assumption:
            output = (left_rows * right_rows) / max(left_cardinality, right_cardinality)

        Args:
            left_stats: Statistics of left input
            right_stats: Statistics of right input
            left_key: Name of join column on left
            right_key: Name of join column on right
            join_type: "inner", "left", "right", "outer"

        Returns:
            Estimated output row count
        """
        left_col = left_stats.get_column(left_key)
        right_col = right_stats.get_column(right_key)

        left_cardinality = left_col.distinct_count if left_col else left_stats.row_count
        right_cardinality = right_col.distinct_count if right_col else right_stats.row_count

        # Assume uniform distribution of join keys
        # Output rows = (left_rows * right_rows) / max(left_cardinality, right_cardinality)
        max_cardinality = max(left_cardinality, right_cardinality, 1)
        join_selectivity = 1.0 / max_cardinality

        inner_join_output = int(left_stats.row_count * right_stats.row_count * join_selectivity)
        inner_join_output = max(1, inner_join_output)

        # Adjust for join type
        if join_type == "inner":
            return inner_join_output
        elif join_type == "left":
            # Left join output >= left input rows
            return max(left_stats.row_count, inner_join_output)
        elif join_type == "right":
            # Right join output >= right input rows
            return max(right_stats.row_count, inner_join_output)
        elif join_type == "outer":
            # Outer join output >= max of inputs
            return max(left_stats.row_count, right_stats.row_count, inner_join_output)
        else:
            return inner_join_output


class RangeEstimator:
    """
    Estimates column value ranges after filtering.
    """

    def narrow_range_for_predicate(
        self, original_range: ColumnRange, predicate: Predicate
    ) -> ColumnRange:
        """
        Narrow a column's value range based on a filter predicate.

        Args:
            original_range: Current range of the column
            predicate: Filter predicate

        Returns:
            Narrowed range (intersection of original and predicate bounds)
        """
        if predicate.predicate_type == PredicateType.RANGE:
            lower, upper = predicate.to_range_bounds()
            predicate_range = ColumnRange(lower, upper)
            return original_range.intersect(predicate_range)

        elif predicate.predicate_type == PredicateType.EQUALITY:
            # After equality, range is just that single value
            value = predicate.lower_bound
            return ColumnRange(value, value)

        elif predicate.predicate_type == PredicateType.IN_LIST:
            # Range is from min to max of the values
            if predicate.values:
                return ColumnRange(min(predicate.values), max(predicate.values))
            return original_range

        elif predicate.predicate_type == PredicateType.LIKE:
            # Can't narrow range for LIKE without pattern analysis
            return original_range

        return original_range

    def narrow_range_for_multiple_predicates(
        self, original_range: ColumnRange, predicates: list[Predicate]
    ) -> ColumnRange:
        """
        Narrow a column's value range based on multiple predicates.

        Only applies predicates that mention the column being narrowed.

        Args:
            original_range: Current range of the column
            predicates: Filter predicates (may or may not mention the column)

        Returns:
            Narrowed range
        """
        result_range = original_range
        for pred in predicates:
            # Check if this predicate affects the column
            # (In a real implementation, we'd track which column the predicate is on)
            if hasattr(pred, "affects_column") and pred.affects_column(original_range.lower_bound):
                result_range = self.narrow_range_for_predicate(result_range, pred)

        return result_range
