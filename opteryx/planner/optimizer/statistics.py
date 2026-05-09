"""
Statistics estimation primitives for cardinality predictions.

Supports:
- Column and relation statistics with histogram backing
- Cardinality estimation for GROUP BY and joins
"""

from dataclasses import dataclass
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Optional
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

    null_fraction: Optional[float] = None


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




