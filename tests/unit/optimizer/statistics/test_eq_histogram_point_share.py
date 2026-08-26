# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The eq histogram tier must be able to move in BOTH directions off uniform.

Until 2026-08-26 ``_selectivity_eq``'s histogram tier ended in
``min(density, max(1/ndv, density/ndv))``. Since ``density <= 1`` the max()
always resolved to ``1/ndv``, so the whole expression was ``min(density,
1/ndv)`` -- a ceiling at the uniform estimate. The histogram could lower an
equality estimate but never raise one, which made frequency skew structurally
invisible (the 2026-08-21 estimator audit's "eq tier ceilinged by 1/ndv"
defect, observed live in the build-3247 JOB regression: a value holding 36% of
a 215-NDV column estimated at 0.47%).

The replacement, ``_bin_mass_point_share``, scales the bin-width probe's mass
to ONE value's share of its bin (``density * bin_count / ndv``, raw density
when ``ndv <= bin_count`` or NDV is unknown). These tests pin both directions:
a dominant value must estimate ABOVE uniform, and a value in a high-NDV column
must not be handed its whole bin.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# `selectivity` participates in an import cycle with `planner.optimizer`.
import opteryx.planner.optimizer  # noqa: F401

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation.selectivity import estimate_selectivity
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.third_party.maki_nage import distogram as dg

_COL = b"tes_col_00000001"

_BINS = 32
_ROWS = 100_000


def _skewed_histogram():
    """One value holds 36% of the rows; the rest spread over [0, 214].

    The shape of the JOB regression's ``company_name.country_code``: NDV 215,
    '[us]' at 36.1%.
    """
    h = dg.Distogram(bin_count=_BINS)
    for _ in range(36_000):
        dg.update(h, 100.0)
    for i in range(_ROWS - 36_000):
        dg.update(h, float(i % 215))
    return h


def _uniform_histogram(ndv: int):
    """Rows spread evenly over [0, ndv)."""
    h = dg.Distogram(bin_count=_BINS)
    for i in range(_ROWS):
        dg.update(h, float(i % ndv))
    return h


def _stats(histogram, ndv: int) -> RelationStatistics:
    col = ColumnStatistics(
        column_name="col",
        data_type="int",
        distinct_count=ndv,
        histogram=histogram,
    )
    return RelationStatistics(row_count_estimate=_ROWS, columns={_COL: col})


def _identifier() -> Node:
    n = Node(node_type=NodeType.IDENTIFIER)
    n.schema_column = type("_S", (), {"identity": _COL})()
    return n


def _eq(value) -> Node:
    n = Node(node_type=NodeType.COMPARISON_OPERATOR)
    n.value = "Eq"
    n.left = _identifier()
    n.right = Node(NodeType.LITERAL, value=value)
    return n


def _in_list(values) -> Node:
    n = Node(node_type=NodeType.COMPARISON_OPERATOR)
    n.value = "InList"
    n.left = _identifier()
    n.right = Node(NodeType.LITERAL, value=tuple(values))
    return n


def test_a_dominant_value_estimates_above_uniform():
    """The regression case: skew must be able to RAISE the estimate.

    True selectivity of the dominant value is 0.36; uniform (1/215) is 0.0047.
    Under the old ceiling this returned exactly uniform. The share is still an
    under-estimate (the value is averaged against its bin-mates), but it must
    sit well above uniform.
    """
    ndv = 215
    stats = _stats(_skewed_histogram(), ndv)
    selectivity = estimate_selectivity(_eq(100), stats)
    assert selectivity > 1.0 / ndv, (
        f"dominant value estimated {selectivity:.4f}, at or below uniform "
        f"{1.0 / ndv:.4f} -- the 1/ndv ceiling is back"
    )


def test_the_share_is_the_probe_mass_scaled_by_bins_over_ndv():
    """Pin WHAT the answer is, not just its direction."""
    ndv = 215
    histogram = _skewed_histogram()
    stats = _stats(histogram, ndv)
    bin_width = (histogram.max - histogram.min) / histogram.bin_count

    probe = (
        dg.count_up_to(histogram, 100.0 + bin_width / 2.0)
        - dg.count_up_to(histogram, 100.0 - bin_width / 2.0)
    ) / float(histogram.count())
    expected = probe * histogram.bin_count / ndv

    assert estimate_selectivity(_eq(100), stats) == pytest.approx(expected)


def test_a_high_ndv_column_is_not_handed_its_whole_bin():
    """A 50,000-NDV uniform column: the whole bin is ~1/32 of the rows, the
    honest per-value share is ~1/50,000. Raw bin density (the
    _selectivity_starts_with posture, option (b)) would be ~1,500x over."""
    ndv = 50_000
    stats = _stats(_uniform_histogram(ndv), ndv)
    selectivity = estimate_selectivity(_eq(25_000), stats)
    assert selectivity < 5.0 / ndv, (
        f"estimated {selectivity:.6f} for one value of {ndv:,} -- the probe's "
        "whole-bin mass is leaking through unscaled"
    )


def test_low_ndv_passes_the_probe_through_unscaled():
    """ndv <= bin_count: a bin holds ~one value, the probe IS the answer."""
    ndv = 8
    histogram = _uniform_histogram(ndv)
    stats = _stats(histogram, ndv)
    bin_width = (histogram.max - histogram.min) / histogram.bin_count
    expected = (
        dg.count_up_to(histogram, 3.0 + bin_width / 2.0)
        - dg.count_up_to(histogram, 3.0 - bin_width / 2.0)
    ) / float(histogram.count())
    assert estimate_selectivity(_eq(3), stats) == pytest.approx(expected)


def test_in_list_single_member_agrees_with_eq():
    """`x IN (v)` and `x = v` are the same predicate and must price the same.

    Before 2026-08-26 the in-list histogram tier accumulated RAW bin mass
    while eq ceilinged at 1/ndv -- the two spellings disagreed in both
    directions.
    """
    stats = _stats(_skewed_histogram(), 215)
    eq = estimate_selectivity(_eq(100), stats)
    in_one = estimate_selectivity(_in_list([100]), stats)
    assert in_one == pytest.approx(eq)


def test_in_list_accumulates_per_member_shares():
    stats = _stats(_skewed_histogram(), 215)
    a = estimate_selectivity(_eq(100), stats)
    b = estimate_selectivity(_eq(10), stats)
    both = estimate_selectivity(_in_list([100, 10]), stats)
    assert both == pytest.approx(a + b)


if __name__ == "__main__":  # pragma: no cover
    import sys as _sys

    _sys.exit(pytest.main([__file__, "-v"]))
