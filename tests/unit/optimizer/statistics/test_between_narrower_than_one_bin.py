# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A BETWEEN window narrower than one histogram bin must not fabricate a near-zero.

``count_up_to`` interpolates linearly INSIDE a bin. Probing the raw bounds of a
window narrower than a bin therefore returns that bin's mass scaled by
``window_width / bin_gap`` -- a confident number derived from a linearity the
histogram never claimed to have. The histogram's real resolution is one bin; a
window below that is an ABSENCE of information, and this module's posture for
absent information is "assume no reduction", never "assume nothing matches".

Found 2026-08-22 on a live 4,765,263-row netflow relation:

    WHERE src_addr <<= '192.168.0.0/16'   -- rewritten to a BETWEEN

is 65,536 wide. The column's histogram had 50 bins spanning the uint32 domain
(~74.8M per bin), so the window covered 0.09% of ONE bin and both probes landed
inside it. Estimated selectivity 6.34e-06 against a measured 0.783 -- 123,000x
under. That took the scan estimate to 9 rows against 1,279,999 actual, and the
join above it inherited the 9 and estimated 3 rows against 2.56 billion.

The fix is the probe ``_selectivity_eq``/``_selectivity_in_list`` already apply
to a point: widen to one bin width about the window's centre and take the bin's
average density.

Note this matches the eq tier's histogram PROBE, not its final answer -- the eq
tier then ceilings the density at ~1/ndv (skew undetectable; a separately logged
defect from the 2026-08-21 estimator audit). So ``BETWEEN x AND x`` and ``= x``
still disagree, and the test below pins the probe rather than asserting a
continuity that does not hold.
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

_SRC = b"net_src_kNjxTk2T"

# 192.168.0.0/16 as the rewriter emits it.
_LO = 3232235520
_HI = 3232301055
_UINT32_MAX = 4294967295


def _histogram():
    """50 bins over the uint32 domain, with the bulk of the mass inside 192.168/16.

    Mirrors the shape of the relation that exposed this: one dominant host
    (192.168.0.85), a 192.168.4.x cluster, and a thin tail of public addresses
    spread across the whole domain.
    """
    h = dg.Distogram(bin_count=50)
    for _ in range(80_000):
        dg.update(h, float(_LO + 85))
    for i in range(20_000):
        dg.update(h, float(_LO + 1024 + (i % 256)))
    # thin public tail, deterministic and spread across the domain
    for i in range(20_000):
        dg.update(h, float((i * 214_013 + 2_531_011) % _UINT32_MAX))
    return h


def _stats(histogram):
    col = ColumnStatistics(
        column_name="src_addr",
        data_type="int",
        distinct_count=10_653,
        histogram=histogram,
    )
    return RelationStatistics(row_count_estimate=120_000, columns={_SRC: col})


def _identifier():
    node = Node(NodeType.IDENTIFIER, source_column="src_addr")
    node.schema_column = Node(NodeType.IDENTIFIER, identity=_SRC)
    return node


def _between(lo, hi):
    # BETWEEN carries its bounds as `right` (low) and `centre` (high) -- see
    # `_selectivity_between`. `centre` is a real operand here, not decoration.
    return Node(
        NodeType.BETWEEN,
        left=_identifier(),
        right=Node(NodeType.LITERAL, value=lo),
        centre=Node(NodeType.LITERAL, value=hi),
    )


def test_between_narrower_than_a_bin_is_not_a_fabricated_near_zero():
    """The regression: 0.09% of a bin must not estimate at ~1e-06."""
    stats = _stats(_histogram())
    selectivity = estimate_selectivity(_between(_LO, _HI), stats)

    # measured truth for this fixture is ~0.83; the pre-fix answer was ~1e-06.
    assert selectivity > 0.1, (
        f"BETWEEN over 0.09% of one bin estimated {selectivity:.3e} -- the "
        "sub-bin interpolation is fabricating a near-zero again"
    )


def test_the_widened_probe_is_one_bin_width_about_the_centre():
    """Pin WHAT the widened answer is, not just that it stopped being tiny."""
    histogram = _histogram()
    stats = _stats(histogram)
    bin_width = (histogram.max - histogram.min) / histogram.bin_count
    centre = (_LO + _HI) / 2.0

    expected = (
        dg.count_up_to(histogram, centre + bin_width / 2.0)
        - dg.count_up_to(histogram, centre - bin_width / 2.0)
    ) / float(histogram.count())

    assert estimate_selectivity(_between(_LO, _HI), stats) == pytest.approx(expected)


def test_the_eq_tier_ndv_ceiling_is_not_applied_to_a_range():
    """A BETWEEN spans many distinct values, so 1/ndv is not a bound on it.

    Guards against someone "restoring continuity" with ``=`` by copying the eq
    tier's ``min(density, 1/ndv)`` ceiling over here -- that would reintroduce a
    near-zero on any wide-domain column with a large NDV.
    """
    stats = _stats(_histogram())
    selectivity = estimate_selectivity(_between(_LO, _HI), stats)
    assert selectivity > 1.0 / 10_653


def test_a_window_wider_than_a_bin_is_left_alone():
    """The widening must not touch windows the histogram CAN resolve."""
    histogram = _histogram()
    stats = _stats(histogram)
    span = histogram.max - histogram.min
    bin_width = span / histogram.bin_count

    lo = histogram.min + bin_width
    hi = lo + bin_width * 10

    expected = (dg.count_up_to(histogram, hi) - dg.count_up_to(histogram, lo)) / float(
        histogram.count()
    )
    assert estimate_selectivity(_between(lo, hi), stats) == pytest.approx(expected)


if __name__ == "__main__":  # pragma: no cover
    import sys as _sys

    _sys.exit(pytest.main([__file__, "-v"]))
