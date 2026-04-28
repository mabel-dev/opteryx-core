"""
Statistics estimation primitives for cardinality predictions.

Supports:
- Column and relation statistics with histogram backing
- Cardinality estimation for GROUP BY and joins
"""

from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from typing import TYPE_CHECKING
from typing import Optional
from typing import Tuple
from typing import Union

if TYPE_CHECKING:
    from opteryx.third_party.maki_nage import Distogram


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
    # Can be a Distogram from maki_nage library for histogram-backed estimation
    histogram: Optional[object] = None

    # Total rows in the relation (needed for selectivity calculation with histogram)
    _total_rows: Optional[int] = None

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

    def estimate_selectivity_with_histogram(
        self, predicate_lower: Optional[float] = None, predicate_upper: Optional[float] = None
    ) -> Optional[float]:
        """
        Estimate selectivity of a range predicate using histogram (distogram).

        Uses the distogram if available to compute exact selectivity based on
        the actual distribution. Falls back to None if histogram is unavailable.

        Args:
            predicate_lower: Lower bound of predicate (inclusive)
            predicate_upper: Upper bound of predicate (inclusive)

        Returns:
            Estimated selectivity in [0, 1], or None if histogram unavailable
        """
        if self.histogram is None or self._total_rows is None or self._total_rows == 0:
            return None

        if predicate_lower is None and predicate_upper is None:
            return 1.0

        # Import distogram functions
        from opteryx.third_party.maki_nage.distogram import count
        from opteryx.third_party.maki_nage.distogram import count_up_to

        distogram = self.histogram

        try:
            total_count = count(distogram)
            if total_count is None or total_count == 0:
                return None

            # Compute count within range [predicate_lower, predicate_upper]
            if predicate_lower is not None and predicate_upper is not None:
                # Range predicate: col >= lower AND col <= upper
                count_upper = count_up_to(distogram, predicate_upper)
                count_lower = count_up_to(distogram, predicate_lower)

                if count_upper is None or count_lower is None:
                    return None

                # count_up_to includes the value, so we subtract from count_lower
                range_count = count_upper - count_lower
            elif predicate_lower is not None:
                # Lower bound only: col >= lower
                count_upper = count(distogram)
                count_lower = count_up_to(distogram, predicate_lower)

                if count_upper is None or count_lower is None:
                    return None

                range_count = count_upper - count_lower
            else:
                # Upper bound only: col <= upper
                count_upper = count_up_to(distogram, predicate_upper)
                if count_upper is None:
                    return None
                range_count = count_upper

            selectivity = range_count / total_count
            return max(0.0, min(1.0, selectivity))

        except (TypeError, AttributeError, ValueError):
            # Histogram format or values incompatible
            return None


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


