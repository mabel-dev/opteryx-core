"""A filter must reduce NDV — but only the LIVE count, never the key domain.

A filter drops ROWS. A distinct value disappears only when every row carrying
it is dropped, so NDV falls far more slowly than the row count
(`surviving_distinct_count`), and it can never exceed the row count that
remains (`_cap_ndvs`). Before this, neither happened: `l_orderkey` came out of
a scan reporting 60,000 distinct values against 20,058 rows.

The second half of this file is the trap. Scaling `distinct_count` in place
silently fed a POST-filter number to the join-key divisor, which is documented
in `_build_equiv_tdoms` as required to be PRE-filter -- "a filter removes ROWS,
not the values the key column could hold". The measured effect on TPC-DS Q54's
shape was that a filtered dimension stopped predicting any reduction and DPccp
put the UNFILTERED 100,000-row customer table first: the leading join went from
8,206 rows to 2,150,243. `base_distinct_count` is what keeps both numbers
available, and these tests pin each to its own consumer.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.cost_estimation import surviving_distinct_count
from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _equi_key_classes
from opteryx.planner.optimizer.statistics_refresh import _scale_ndvs

KEY = b"key_left________"
OTHER_KEY = b"key_right_______"


def _relation(rows, ndv, base_ndv=None, base_rows=None, key=KEY):
    return RelationStatistics(
        row_count_estimate=rows,
        columns={
            key: ColumnStatistics(
                column_name="k",
                data_type="INT64",
                distinct_count=ndv,
                base_distinct_count=base_ndv,
                value_range=ColumnRange(),
            )
        },
        base_row_count=base_rows,
    )


def test_a_value_survives_while_any_of_its_rows_does():
    """NDV must not be scaled by the row ratio. 100 distinct values spread over
    600,000 rows keep essentially all of themselves through a 50% filter."""
    assert surviving_distinct_count(100, 600_000, 0.5) == 100
    # A near-unique column has nothing to hide behind and falls with the rows.
    assert surviving_distinct_count(1000, 1000, 0.1) == 99


def test_scaling_never_grows_or_invents():
    assert surviving_distinct_count(None, 5000, 0.5) is None      # unknown stays unknown
    assert surviving_distinct_count(10, 1_000_000, 0.999) == 10   # cannot grow
    assert surviving_distinct_count(1000, 5000, 1.0) == 1000      # nothing removed
    assert surviving_distinct_count(1000, 5000, 0.0) == 1         # floored, never 0


def test_scaling_reduces_the_live_count_and_keeps_the_domain():
    base = _relation(600_000, ndv=150_000)
    scaled = _scale_ndvs(base.columns, base, 0.63)[KEY]

    assert scaled.distinct_count < 150_000, "the live count must respond to the filter"
    assert scaled.domain_distinct_count == 150_000, "the domain is not a filterable thing"


def test_a_filter_does_not_move_the_join_divisor():
    """THE REGRESSION. The divisor is a property of the key DOMAIN, so filtering
    one side must not move it -- otherwise the filter's selectivity is charged a
    second time inside the divisor and a filtered dimension predicts no
    reduction at all."""
    right = _relation(100_000, ndv=100_000, key=OTHER_KEY)

    unfiltered = _relation(200_000, ndv=200_000)
    # Same relation after a filter: rows and the live NDV both fell, the domain
    # did not -- exactly what _scale_ndvs produces.
    filtered = _relation(20_000, ndv=20_000, base_ndv=200_000, base_rows=200_000)

    before = _equi_key_classes([KEY], [OTHER_KEY], unfiltered, right)[0]
    after = _equi_key_classes([KEY], [OTHER_KEY], filtered, right)[0]

    assert max(k.ndv for k in before) == max(k.ndv for k in after), (
        f"filtering moved the divisor {max(k.ndv for k in before)} -> "
        f"{max(k.ndv for k in after)}; the post-filter NDV reached the divisor"
    )
    assert max(k.ndv for k in after) == 200_000


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
