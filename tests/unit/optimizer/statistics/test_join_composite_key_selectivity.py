# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Composite equi-join keys must have EVERY key pair's selectivity applied.

Found during the same stats-system review that produced
test_scan_filter_pushdown.py's post-pushdown tests: `_join_stats` built
`equi_keys` from only `left_columns[0]` / `right_columns[0]`, silently
dropping every join-key column past the first. `estimate_join_cardinality`
already multiplies per-key selectivities correctly given a full list (see
join_cardinality.py's independence-assumption docstring), and
`_intersect_join_keys` a few lines below already loops over every pair --
only the cardinality estimate itself was under-using the signal. Net effect:
a composite-key join (`ON a.x = b.x AND a.y = b.y`) was estimated as if only
`x` mattered, overestimating the join and risking a false ResultTooLargeError
rejection for a routine multi-column-key join.

The correction has two halves, and they pull in opposite directions:

  * every key class's selectivity must be applied (above), and
  * their product must not claim more distinct key tuples than there are rows
    to hold them (`_apply_occupancy_bound`).

Without the second half the first overshoots: TPC-H Q09's
`(ps_partkey, ps_suppkey)` multiplies to a 2e9 domain over 800,000 partsupp
rows and estimates a 6,001,215-row join at 2,400. One test below covers each
half -- the multiplication is exercised at a row count where the bound does
not bind, so neither property can mask a regression in the other.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _join_stats

# Join keys reach _join_stats as raw column identities (opaque bytes) -- see
# test_join_keystats_null_fraction.py for why name-shaped keys would silently
# hide a dead-lookup bug.
_LK1 = b"tes_lk_00000001"
_LK2 = b"tes_lk_00000002"
_RK1 = b"tes_rk_00000003"
_RK2 = b"tes_rk_00000004"


def _join_node(n_keys):
    return SimpleNamespace(
        type="inner",
        left_columns=[_LK1, _LK2][:n_keys],
        right_columns=[_RK1, _RK2][:n_keys],
    )


def _estimate(n_keys, rows):
    """Both sides `rows` rows; each key column has NDV 100 (per-key selectivity
    1/100), independent of the other key."""
    left = RelationStatistics(
        row_count=rows,
        columns={
            _LK1: ColumnStatistics(column_name="lk1", data_type="INTEGER", distinct_count=100, null_fraction=0.0),
            _LK2: ColumnStatistics(column_name="lk2", data_type="INTEGER", distinct_count=100, null_fraction=0.0),
        },
    )
    right = RelationStatistics(
        row_count=rows,
        columns={
            _RK1: ColumnStatistics(column_name="rk1", data_type="INTEGER", distinct_count=100, null_fraction=0.0),
            _RK2: ColumnStatistics(column_name="rk2", data_type="INTEGER", distinct_count=100, null_fraction=0.0),
        },
    )
    child_stats = [(left, "left"), (right, "right")]
    return _join_stats(_join_node(n_keys), child_stats).row_count


def test_second_key_column_further_reduces_the_estimate():
    """The original guard: every key column's selectivity must be applied.

    Sized so the composite domain (100 * 100 = 10,000) fits inside the row
    count, which keeps the occupancy bound out of the way and leaves the pure
    multiplication on display.
    """
    single_key = _estimate(1, rows=1_000_000)
    composite_key = _estimate(2, rows=1_000_000)
    # Single key: 1e6 * 1e6 * (1/100) = 1e10.
    assert single_key == pytest.approx(1e10, rel=0.05), single_key
    # Composite key: the second column's 1/100 selectivity must ALSO apply --
    # 1e6 * 1e6 * (1/100) * (1/100) = 1e8. If the second key is silently
    # dropped (the bug), composite_key == single_key instead.
    assert composite_key < single_key, (
        f"second join-key column did not reduce the estimate at all "
        f"({composite_key} == {single_key}) -- _join_stats is only using "
        f"the first equi-join key"
    )
    assert composite_key == pytest.approx(1e8, rel=0.05), composite_key


def test_composite_domain_cannot_exceed_the_rows_that_hold_it():
    """Multiplying per-column domains can claim more key tuples than exist.

    Two NDV-100 key columns over 1000-row relations multiply to a composite
    domain of 10,000 -- ten distinct key tuples for every row available to hold
    one. `_apply_occupancy_bound` caps the domain at the row count, so the
    estimate is 1000 * 1000 / 1000 = 1000 rather than the 100 that the
    unbounded product gives.

    This is what TPC-H Q09 hit at scale: (ps_partkey, ps_suppkey) multiplied to
    2e9 against 800,000 partsupp rows, estimating a 6,001,215-row join at 2,400
    and putting six million rows on the BUILD side of three consecutive joins.

    The bound is an upper bound on the DOMAIN, so it can only raise the
    estimate. That is the safe direction: Q09 is a direct demonstration that
    under-estimating a join inverts build-side selection, while over-estimating
    costs a larger hash table.
    """
    bounded = _estimate(2, rows=1000)
    assert bounded == pytest.approx(1000, rel=0.05), bounded
    # The unbounded product would give 1000 * 1000 * (1/100) * (1/100) = 100.
    assert bounded > 100, (
        f"composite domain was not bounded by the row count ({bounded}) -- a "
        f"1000-row relation cannot hold 10,000 distinct key tuples"
    )


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
