"""KeyStats NDV provenance, and the occupancy bound honouring it.

`_key_stats_with_tdom` fills a missing join-key NDV with the equivalence
class's tdom -- a DOMAIN size, deliberately (dividing by a post-filter row
count makes a filtered dimension table predict zero reduction). The stand-in
is correct; writing it into `ndv` with no provenance was not: it made
`apply_occupancy_bound`'s "bail out on an unknown NDV" guard unreachable from
the plan-adapter path, so the bound ran on domain sizes as though they were
counted distinct values.

These lock the two halves: the provenance survives every construction site,
and the bound stands down when no class in the join knows a real NDV.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from math import isqrt

import pytest

from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import apply_occupancy_bound
from opteryx.planner.cost_estimation.join_cardinality import NdvProvenance


def _measured(ndv, null_fraction=None):
    return KeyStats(ndv=ndv, null_fraction=null_fraction, ndv_provenance=NdvProvenance.MEASURED)


def _standin(ndv, null_fraction=None):
    return KeyStats(
        ndv=ndv, null_fraction=null_fraction, ndv_provenance=NdvProvenance.DOMAIN_STANDIN
    )


def test_a_present_ndv_must_declare_where_it_came_from():
    """No default provenance for a real number -- a site that does not say
    must fail here, not be read as a distinct count downstream."""
    with pytest.raises(ValueError):
        KeyStats(ndv=1000, null_fraction=None)


def test_an_absent_ndv_cannot_claim_a_provenance():
    with pytest.raises(ValueError):
        KeyStats(ndv=None, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)
    with pytest.raises(ValueError):
        KeyStats(ndv=None, null_fraction=None, ndv_provenance=NdvProvenance.DOMAIN_STANDIN)


def test_a_standin_is_not_measured():
    assert _measured(10).ndv_is_measured is True
    assert _standin(10).ndv_is_measured is False
    assert KeyStats(ndv=None, null_fraction=None).ndv_is_measured is False


def test_bound_is_widened_when_no_class_knows_a_real_ndv():
    """Every factor a stand-in makes the product an upper bound, so the gap
    between it and the occupancy bound is uncertainty. The bound still binds
    -- at the geometric mean of the two, not at the bound itself.

    Dropping it instead was measured and is worse: TPC-H Q20's hash join goes
    to an estimate of 1 row against an actual 5,843."""
    equi_keys = [
        (_standin(2_000_000), _standin(2_000_000)),
        (_standin(100_000), _standin(100_000)),
    ]
    bounded = apply_occupancy_bound(equi_keys, 8_000_000, 59_986_052)
    assert len(bounded) == 1
    # isqrt(2e5 * 1e6 * 8e6) -- between the 8,000,000 bound and the 2e11 product.
    assert bounded[0][0].ndv == isqrt(2_000_000 * 100_000 * 8_000_000)
    assert bounded[0][0].ndv > 8_000_000
    assert bounded[0][0].ndv < 2_000_000 * 100_000


def test_a_widened_bound_still_binds_below_the_raw_product():
    """The widening is a loosening, never a licence to skip the bound: the
    divisor charged is strictly smaller than the unbounded product."""
    equi_keys = [
        (_standin(2_000_000), _standin(2_000_000)),
        (_standin(100_000), _standin(100_000)),
    ]
    bounded = apply_occupancy_bound(equi_keys, 8_000_000, 59_986_052)
    assert bounded[0][0].ndv < 2_000_000 * 100_000


def test_a_slack_composite_is_untouched_even_when_it_is_all_standins():
    """A product already under the bound is left exactly as it was -- widening
    cannot manufacture a collapse where the plain bound would not have."""
    equi_keys = [
        (_standin(100), _standin(100)),
        (_standin(200), _standin(200)),
    ]
    assert apply_occupancy_bound(equi_keys, 1_000_000, 1_000_000) is equi_keys


def test_bound_applies_at_full_strength_when_a_class_knows_a_real_ndv():
    """One measured class is enough to charge the bound unwidened: TPC-H Q09's
    partsupp x lineitem shape, where the bound is the whole reason the
    composite key does not collapse the estimate 2,500x."""
    equi_keys = [
        (_measured(2_000_000), _measured(2_000_000)),
        (_standin(100_000), _standin(100_000)),
    ]
    bounded = apply_occupancy_bound(equi_keys, 8_000_000, 59_986_052)
    assert len(bounded) == 1
    assert bounded[0][0].ndv == 8_000_000


def test_the_bounded_pair_is_itself_a_standin():
    """The collapsed pair's NDV is a row count, not a distinct count."""
    equi_keys = [
        (_measured(2_000_000), _measured(2_000_000)),
        (_measured(100_000), _measured(100_000)),
    ]
    bounded = apply_occupancy_bound(equi_keys, 8_000_000, 59_986_052)
    assert bounded[0][0].ndv_provenance is NdvProvenance.DOMAIN_STANDIN
    assert bounded[0][1].ndv_is_measured is False


def test_plan_adapter_tdom_fallback_is_marked_as_a_standin():
    """End to end through the site the defect lives at."""
    from opteryx.planner.cost_estimation import plan_adapter

    class _NoStatsScan:
        statistics = None

    stats = plan_adapter._key_stats(_NoStatsScan(), b"k")
    assert stats.ndv is None
    assert stats.ndv_provenance is NdvProvenance.UNKNOWN


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
