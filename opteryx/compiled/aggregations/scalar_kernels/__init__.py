"""
Scalar aggregation kernels.

This package exposes the non-grouped / scalar aggregation helpers from the
consolidated scalar kernels extension module.
"""

from ._definitions import (
    ApproximateCountState,
    ApproximateMedianState,
    ApproximatePercentileState,
    ArrayAggState,
    approximate_count,
    approximate_count_draken,
    approximate_median,
    approximate_median_draken,
    approximate_percentile,
    approximate_percentile_draken,
    count_distinct,
    count_distinct_draken,
)

__all__ = [
    "ArrayAggState",
    "ApproximateCountState",
    "ApproximateMedianState",
    "ApproximatePercentileState",
    "approximate_count",
    "approximate_count_draken",
    "approximate_median",
    "approximate_median_draken",
    "approximate_percentile",
    "approximate_percentile_draken",
    "count_distinct",
    "count_distinct_draken",
]
