# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The estimator's shared helpers and constants have exactly ONE definition.

Phase 1.6 of the 2026-08-21 estimator remediation reconciled three places
where the build-side chooser and the cardinality estimator silently priced
the same join or predicate differently:

  A. Composite-key NDV composition: statistics_refresh took max() across a
     side's key-column NDVs while join_ordering._key_ndv took min() -- the
     two read DIFFERENT NDVs for the same join. Ruling: max (the standard
     conservative lower bound on a composite key's NDV), in one shared
     helper `composite_key_ndv`.

  B. The occupancy bound existed as two diverging copies that each claimed
     to mirror the other: dpccp multiplied max(left.ndv, right.ndv) per
     class (the divisor _key_selectivity actually applies) and bailed on
     unknown NDV; statistics_refresh multiplied only left.ndv. Ruling: the
     dpccp form, as one shared `apply_occupancy_bound`.

  C. Fallback selectivity constants were declared independently with
     different values (range 0.5 vs 0.25, LIKE 0.3 vs 0.25/0.1, InStr 0.3
     vs 0.1), so predicate order could flip on whether stats were attached.
     Ruling: the stats-informed module's values, defined once in
     `fallback_selectivity`.

These tests pin the single-source property with object-identity assertions:
a reintroduced local copy changes `is` results even if its values start out
identical.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# The optimizer package must initialize before cost_estimation.selectivity is
# imported directly: selectivity imports optimizer.statistics, whose package
# __init__ imports the strategies, and strategies.predicate_ordering imports
# selectivity back -- a pre-existing cycle that resolves only in this order.
from opteryx.planner.optimizer import statistics_refresh
from opteryx.planner.optimizer.strategies import join_ordering
from opteryx.planner.optimizer.strategies import predicate_ordering as strategy_predicate_ordering
import sys as _sys

from opteryx.planner.cost_estimation import fallback_selectivity
from opteryx.planner.cost_estimation import join_cardinality
from opteryx.planner.cost_estimation import selectivity as selectivity_module

# The package __init__ re-exports the dpccp FUNCTION under the module's own
# name, so attribute access yields the function; fetch the module itself.
import opteryx.planner.cost_estimation.dpccp  # noqa: F401  (registers the module)

dpccp_module = _sys.modules["opteryx.planner.cost_estimation.dpccp"]
from opteryx.planner.cost_estimation.join_cardinality import KeyStats
from opteryx.planner.cost_estimation.join_cardinality import NdvProvenance
from opteryx.planner.cost_estimation.join_cardinality import apply_occupancy_bound
from opteryx.planner.cost_estimation.join_cardinality import composite_key_ndv


# --- single-source identity ------------------------------------------------


def test_composite_key_ndv_is_the_single_composition():
    """Both consumers bind the ONE helper, not local copies."""
    assert statistics_refresh.composite_key_ndv is composite_key_ndv
    assert join_ordering.composite_key_ndv is composite_key_ndv


def test_occupancy_bound_is_the_single_function():
    """dpccp and statistics_refresh call the ONE bound; the local copies are gone."""
    assert dpccp_module.apply_occupancy_bound is apply_occupancy_bound
    assert statistics_refresh.apply_occupancy_bound is apply_occupancy_bound
    assert "_apply_occupancy_bound" not in vars(dpccp_module)
    assert "_apply_occupancy_bound" not in vars(statistics_refresh)


def test_fallback_constants_have_one_definition():
    """selectivity.py, join_cardinality and the ordering strategy all read
    fallback_selectivity's values -- no independently declared duplicates."""
    assert (
        join_cardinality.EQ_UNKNOWN_NDV_FALLBACK
        is fallback_selectivity.EQ_UNKNOWN_NDV_FALLBACK
    )
    assert (
        selectivity_module._EQ_UNKNOWN_NDV_FALLBACK
        is fallback_selectivity.EQ_UNKNOWN_NDV_FALLBACK
    )
    assert (
        selectivity_module._LIKE_PREFIX_SELECTIVITY
        is fallback_selectivity.LIKE_PREFIX_SELECTIVITY
    )
    assert (
        selectivity_module._LIKE_INFIX_SELECTIVITY
        is fallback_selectivity.LIKE_INFIX_SELECTIVITY
    )
    assert (
        selectivity_module._RANGE_FALLBACK_SELECTIVITY
        is fallback_selectivity.RANGE_FALLBACK_SELECTIVITY
    )
    assert (
        strategy_predicate_ordering.DEFAULT_SELECTIVITY
        is fallback_selectivity.DEFAULT_SELECTIVITY
    )


def test_default_selectivity_table_is_built_from_the_scalars():
    """The operator table cannot drift from the stats path's constants."""
    table = fallback_selectivity.DEFAULT_SELECTIVITY
    eq = fallback_selectivity.EQ_UNKNOWN_NDV_FALLBACK
    rng = fallback_selectivity.RANGE_FALLBACK_SELECTIVITY
    prefix = fallback_selectivity.LIKE_PREFIX_SELECTIVITY
    infix = fallback_selectivity.LIKE_INFIX_SELECTIVITY
    assert table["Eq"] == eq
    assert table["NotEq"] == 1.0 - eq
    for op in ("Gt", "GtEq", "Lt", "LtEq"):
        assert table[op] == rng
    for op in ("Like", "ILike", "RLike"):
        assert table[op] == prefix
    for op in ("NotLike", "NotILike", "NotRLike"):
        assert table[op] == 1.0 - prefix
    for op in ("InStr", "IInStr"):
        assert table[op] == infix
    for op in ("NotInStr", "NotIInStr"):
        assert table[op] == 1.0 - infix


# --- helper semantics ------------------------------------------------------


def test_composite_key_ndv_takes_max_of_the_known_ndvs():
    """Ruling A: max, the conservative lower bound -- never min."""
    assert composite_key_ndv([100, 7, 5000]) == 5000
    assert composite_key_ndv([None, 7, None]) == 7
    assert composite_key_ndv([42]) == 42


def test_composite_key_ndv_returns_none_when_nothing_is_known():
    assert composite_key_ndv([]) is None
    assert composite_key_ndv([None, None]) is None


def test_occupancy_bound_multiplies_the_larger_side_per_class():
    """Ruling B: the per-class factor is max(left.ndv, right.ndv) -- the exact
    divisor _key_selectivity applies. A left-only product (the old refresh
    form) would be 10 x 10 = 100, under the 500-row bound, and the composite
    would slip through unbounded; the real product 1000 x 1000 = 1e6 exceeds
    it and must collapse to a single bounded pair."""
    equi_keys = [
        (KeyStats(ndv=10, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=1000, null_fraction=0.2, ndv_provenance=NdvProvenance.MEASURED)),
        (KeyStats(ndv=10, null_fraction=0.1, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=1000, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
    ]
    bounded = apply_occupancy_bound(equi_keys, 500, 800)
    assert len(bounded) == 1
    left_stat, right_stat = bounded[0]
    assert left_stat.ndv == 500 and right_stat.ndv == 500
    # Null fractions keep their worst-case-per-side composition.
    assert left_stat.null_fraction == 0.1
    assert right_stat.null_fraction == 0.2


def test_occupancy_bound_leaves_a_slack_composite_alone():
    equi_keys = [
        (KeyStats(ndv=10, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=10, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
        (KeyStats(ndv=10, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=10, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
    ]
    assert apply_occupancy_bound(equi_keys, 500, 800) is equi_keys


def test_occupancy_bound_bails_on_unknown_ndv():
    """_key_selectivity charges a flat fallback for an unknown NDV; there is no
    product to bound, and inventing one would overwrite that fallback."""
    equi_keys = [
        (KeyStats(ndv=1000, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=1000, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
        (KeyStats(ndv=None, null_fraction=None), KeyStats(ndv=1000, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
    ]
    assert apply_occupancy_bound(equi_keys, 5, 5) is equi_keys


def test_occupancy_bound_ignores_a_single_class():
    equi_keys = [
        (KeyStats(ndv=10**9, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED), KeyStats(ndv=10**9, null_fraction=None, ndv_provenance=NdvProvenance.MEASURED)),
    ]
    assert apply_occupancy_bound(equi_keys, 5, 5) is equi_keys


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
