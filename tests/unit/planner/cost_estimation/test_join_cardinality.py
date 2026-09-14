"""Unit tests for opteryx.planner.cost_estimation.join_cardinality."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import NdvProvenance
from opteryx.planner.cost_estimation import estimate_join_cardinality


def _stats(ndv, null_fraction=0.0, live_ndv=None):
    """``ndv`` is the key DOMAIN; ``live_ndv`` is what the relation holds now.

    Only semi/anti reads the live count. Leaving it None is the "no live NDV
    known" case, where semi/anti must DECLINE rather than invent a fraction.
    """
    provenance = NdvProvenance.UNKNOWN if ndv is None else NdvProvenance.MEASURED
    return KeyStats(
        ndv=ndv, null_fraction=null_fraction, ndv_provenance=provenance, live_ndv=live_ndv
    )


# ---------------------------------------------------------------------------
# Cross join
# ---------------------------------------------------------------------------


def test_cross_join_no_keys():
    # 100 × 50 = 5000
    assert estimate_join_cardinality(100, 50, "cross", []) == 5000


def test_cross_join_with_extra_selectivity():
    # 100 × 50 × 0.5 = 2500
    assert estimate_join_cardinality(
        100, 50, "cross", [], extra_predicates_selectivity=0.5
    ) == 2500


def test_cross_join_ignores_equi_keys():
    # cross is unconditional; equi_keys are not consulted
    assert estimate_join_cardinality(
        100, 50, "cross", [(_stats(10), _stats(10))]
    ) == 5000


# ---------------------------------------------------------------------------
# Inner join — single key
# ---------------------------------------------------------------------------


def test_inner_single_key():
    # 1000 × 500 / max(100, 200) = 500000 / 200 = 2500
    out = estimate_join_cardinality(
        1000, 500, "inner", [(_stats(100), _stats(200))]
    )
    assert out == 2500


def test_inner_single_key_smaller_ndv_drives_selectivity():
    # max(NDV) — not min — is the divisor
    out = estimate_join_cardinality(
        100, 100, "inner", [(_stats(10), _stats(50))]
    )
    # 100 × 100 / 50 = 200
    assert out == 200


# ---------------------------------------------------------------------------
# Inner join — multi key (independence)
# ---------------------------------------------------------------------------


def test_inner_multi_key_multiplies_selectivities():
    # 1000 × 1000 / (max(10,10) × max(20,20)) = 1_000_000 / 200 = 5000
    out = estimate_join_cardinality(
        1000,
        1000,
        "inner",
        [(_stats(10), _stats(10)), (_stats(20), _stats(20))],
    )
    assert out == 5000


# ---------------------------------------------------------------------------
# Null-fraction reduction
# ---------------------------------------------------------------------------


def test_inner_null_fraction_halves_when_one_side_half_null():
    # baseline: 1000 × 1000 / 100 = 10_000
    base = estimate_join_cardinality(
        1000, 1000, "inner", [(_stats(100), _stats(100))]
    )
    assert base == 10_000

    # 50% nulls on left → eff_left = 500 → 500 × 1000 / 100 = 5000
    halved = estimate_join_cardinality(
        1000,
        1000,
        "inner",
        [(_stats(100, null_fraction=0.5), _stats(100))],
    )
    assert halved == 5000


# ---------------------------------------------------------------------------
# NDV-None fallback
# ---------------------------------------------------------------------------


def test_inner_ndv_none_left_uses_fallback():
    # selectivity = 0.1 → 100 × 100 × 0.1 = 1000
    out = estimate_join_cardinality(
        100, 100, "inner", [(_stats(None), _stats(50))]
    )
    assert out == 1000


def test_inner_ndv_none_right_uses_fallback():
    out = estimate_join_cardinality(
        100, 100, "inner", [(_stats(50), _stats(None))]
    )
    assert out == 1000


def test_inner_ndv_none_both_uses_fallback():
    out = estimate_join_cardinality(
        100, 100, "inner", [(_stats(None), _stats(None))]
    )
    assert out == 1000


# ---------------------------------------------------------------------------
# Floor at 1
# ---------------------------------------------------------------------------


def test_inner_clamped_to_one_when_ndv_huge():
    # 10 × 10 / 1_000_000 = 0.0001 → floored to 1
    out = estimate_join_cardinality(
        10, 10, "inner", [(_stats(1_000_000), _stats(1_000_000))]
    )
    assert out == 1


def test_anti_does_not_read_the_inner_estimate():
    """The old arm was `max(0, left - inner)`, which collapsed to the floor
    whenever inner >= left -- the common case. A semi/anti join emits LEFT ROWS,
    not matched pairs, so the inner estimate is not the right quantity at all.
    Here the right covers the whole (tiny) domain, so nothing is anti-matched
    and the answer is the floor for a REASON, not by collapse."""
    out = estimate_join_cardinality(
        100, 100, "anti", [(_stats(1, live_ndv=1), _stats(1, live_ndv=1))]
    )
    assert out == 1
    # ... and the semi is everything, which `max(0, left - inner)` could never
    # have paired with.
    assert estimate_join_cardinality(
        100, 100, "semi", [(_stats(1, live_ndv=1), _stats(1, live_ndv=1))]
    ) == 100


# ---------------------------------------------------------------------------
# extra_predicates_selectivity
# ---------------------------------------------------------------------------


def test_inner_extra_predicates_halves():
    base = estimate_join_cardinality(
        1000, 500, "inner", [(_stats(100), _stats(200))]
    )
    halved = estimate_join_cardinality(
        1000,
        500,
        "inner",
        [(_stats(100), _stats(200))],
        extra_predicates_selectivity=0.5,
    )
    assert halved == base // 2


def test_cross_extra_predicates_halves():
    halved = estimate_join_cardinality(
        100, 50, "cross", [], extra_predicates_selectivity=0.5
    )
    assert halved == 2500


# ---------------------------------------------------------------------------
# Outer joins
# ---------------------------------------------------------------------------


def test_left_outer_floors_at_left_rows():
    # inner = 1; left_rows = 100 → result = 100
    out = estimate_join_cardinality(
        100, 100, "left outer", [(_stats(1_000_000), _stats(1_000_000))]
    )
    assert out == 100


def test_left_outer_uses_inner_when_larger():
    # inner = 100×100/10 = 1000 > left_rows=100
    out = estimate_join_cardinality(
        100, 100, "left outer", [(_stats(10), _stats(10))]
    )
    assert out == 1000


def test_right_outer_floors_at_right_rows():
    out = estimate_join_cardinality(
        100, 250, "right outer", [(_stats(1_000_000), _stats(1_000_000))]
    )
    assert out == 250


def test_full_outer_when_no_matches():
    # tiny inner → full ≈ inner + (left - inner) + (right - inner)
    out = estimate_join_cardinality(
        100, 200, "full outer", [(_stats(1_000_000), _stats(1_000_000))]
    )
    # inner = 100×200/1_000_000 = 0.02 → 0.02 + 99.98 + 199.98 = 299.98 → 299
    assert out == 299


def test_full_outer_many_to_many_is_at_least_inner():
    # inner = 100×100/1 = 10_000 (many-to-many: every row matches every row).
    # A full outer join CONTAINS the inner join, so it can never be smaller.
    # The old formula max(l + r - inner, l, r) returned 100 here.
    inner = estimate_join_cardinality(100, 100, "inner", [(_stats(1), _stats(1))])
    full = estimate_join_cardinality(100, 100, "full outer", [(_stats(1), _stats(1))])
    assert inner == 10_000
    assert full >= inner
    assert full == 10_000


def test_full_outer_at_least_both_inputs():
    # Every left row and every right row appears at least once (matched or
    # null-extended), so full outer >= max(l, r) regardless of key stats.
    for left_ndv, right_ndv in ((1, 1), (100, 100), (1_000_000, 1_000_000), (None, None)):
        out = estimate_join_cardinality(
            300, 700, "full outer", [(_stats(left_ndv), _stats(right_ndv))]
        )
        assert out >= 700, f"ndv=({left_ndv}, {right_ndv}) gave {out}"


def test_full_outer_at_least_inner():
    # full outer >= inner across a sweep of shapes, including many-to-many.
    for l, r, ndv in ((100, 100, 1), (1000, 500, 10), (50, 5000, 100), (10, 10, 1_000_000)):
        keys = [(_stats(ndv), _stats(ndv))]
        inner = estimate_join_cardinality(l, r, "inner", keys)
        full = estimate_join_cardinality(l, r, "full outer", keys)
        assert full >= inner, f"l={l} r={r} ndv={ndv}: full {full} < inner {inner}"


# ---------------------------------------------------------------------------
# Semi / anti
# ---------------------------------------------------------------------------


def test_semi_and_anti_decline_without_a_live_ndv():
    """No live NDV means no match fraction. The estimate must fall back to the
    left row count -- the only sound bound -- rather than fabricating one."""
    keys = [(_stats(1_000_000), _stats(1_000_000))]
    assert estimate_join_cardinality(1000, 1000, "semi", keys) == 1000
    assert estimate_join_cardinality(1000, 1000, "anti", keys) == 1000


def test_right_covering_the_whole_domain_matches_everything():
    """The limit case: the right holds every value the key can take, so every
    left row matches. This is the answer a declared foreign key asserts, arrived
    at by the formula rather than by a special branch."""
    keys = [(_stats(50_000, live_ndv=50_000), _stats(50_000, live_ndv=50_000))]
    assert estimate_join_cardinality(1000, 9999, "semi", keys) == 1000
    assert estimate_join_cardinality(1000, 9999, "anti", keys) == 1  # floored zero


def test_a_filtered_right_side_matches_proportionally():
    """The right holds a quarter of the domain, so a quarter of the left matches."""
    keys = [(_stats(40_000, live_ndv=10_000), _stats(40_000, live_ndv=10_000))]
    assert estimate_join_cardinality(1000, 9999, "semi", keys) == 250
    assert estimate_join_cardinality(1000, 9999, "anti", keys) == 750


def test_semi_and_anti_sum_to_the_left_row_count():
    """The complement is exact: every left row either matched or did not."""
    keys = [(_stats(40_000, live_ndv=17_351), _stats(40_000, live_ndv=17_351))]
    semi = estimate_join_cardinality(4000, 9999, "semi", keys)
    anti = estimate_join_cardinality(4000, 9999, "anti", keys)
    assert semi + anti == 4000


def test_the_right_side_is_measured_against_the_domain_not_the_left():
    """TPC-H Q21's anti at SF100. A filtered fact table still holds far more
    distinct keys than the left has ROWS, so the containment form
    min(1, ndv_right/ndv_left) pins at 1.0 and reports anti = 0 against an
    actual 396,100. Measuring against the domain gives a usable number."""
    keys = [(_stats(150_000_000, live_ndv=147_188_758),
             _stats(150_000_000, live_ndv=147_188_758))]
    anti = estimate_join_cardinality(15_012_825, 600_000_000, "anti", keys)
    assert 250_000 < anti < 500_000, anti


def test_null_left_keys_survive_anti_and_never_match_semi():
    """A NULL key matches nothing, so it is excluded from semi and MUST come
    back in anti. The complement is what puts it back; writing anti directly as
    left * (1 - fraction) would drop it."""
    keys = [(_stats(1000, null_fraction=0.5, live_ndv=1000),
             _stats(1000, live_ndv=1000))]
    # Right covers the whole domain: every NON-NULL left row matches.
    assert estimate_join_cardinality(1000, 1000, "semi", keys) == 500
    # The 500 NULL-keyed rows are exactly what anti emits.
    assert estimate_join_cardinality(1000, 1000, "anti", keys) == 500


def test_not_distinct_treats_null_as_an_ordinary_value():
    """INTERSECT / EXCEPT compare with IS NOT DISTINCT FROM, where NULL equals
    itself -- so a NULL left key DOES match and is not excluded."""
    keys = [(_stats(1000, null_fraction=0.5, live_ndv=1000),
             _stats(1000, live_ndv=1000))]
    assert estimate_join_cardinality(1000, 1000, "semi not-distinct", keys) == 1000
    assert estimate_join_cardinality(1000, 1000, "anti not-distinct", keys) == 1


def test_not_in_emits_nothing_when_the_right_key_holds_a_null():
    """NOT IN propagates UNKNOWN: one NULL on the right and every comparison is
    UNKNOWN, so the join emits no rows at all."""
    nulls = [(_stats(1000, live_ndv=500), _stats(1000, null_fraction=0.01, live_ndv=500))]
    assert estimate_join_cardinality(1000, 1000, "anti null-aware", nulls) == 1

    # An UNKNOWN null fraction cannot establish that, so it must not be assumed.
    unknown = [(_stats(1000, live_ndv=500),
                KeyStats(ndv=1000, null_fraction=None,
                         ndv_provenance=NdvProvenance.MEASURED, live_ndv=500))]
    assert estimate_join_cardinality(1000, 1000, "anti null-aware", unknown) == 500


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_negative_rows_raises():
    with pytest.raises(ValueError):
        estimate_join_cardinality(-1, 10, "inner", [(_stats(10), _stats(10))])
    with pytest.raises(ValueError):
        estimate_join_cardinality(10, -1, "inner", [(_stats(10), _stats(10))])


def test_unknown_join_type_raises():
    with pytest.raises(ValueError):
        estimate_join_cardinality(10, 10, "diagonal", [(_stats(10), _stats(10))])


def test_negative_extra_selectivity_raises():
    with pytest.raises(ValueError):
        estimate_join_cardinality(
            10, 10, "inner", [(_stats(10), _stats(10))], extra_predicates_selectivity=-0.1
        )


if __name__ == "__main__":
    import sys as _sys

    _sys.exit(pytest.main([__file__, "-v"]))
