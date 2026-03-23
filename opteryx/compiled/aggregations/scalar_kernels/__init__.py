"""
Scalar aggregation kernels.

This package exposes the non-grouped / scalar aggregation helpers from the
consolidated scalar kernels extension module.
"""

from ._definitions import ApproximateCountState
from ._definitions import ApproximateMedianState
from ._definitions import ApproximatePercentileState
from ._definitions import ArrayAggState
from ._definitions import approximate_count
from ._definitions import approximate_count_draken
from ._definitions import approximate_median
from ._definitions import approximate_median_draken
from ._definitions import approximate_percentile
from ._definitions import approximate_percentile_draken
from ._definitions import count_distinct
from ._definitions import count_distinct_draken

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
