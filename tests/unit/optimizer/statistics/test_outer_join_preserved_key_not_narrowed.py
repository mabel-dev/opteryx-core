# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""An outer join must not narrow its PRESERVED side's key statistics.

`_intersect_join_keys` replaces both equi-key columns' ranges with their
intersection and their NDV with the smaller of the two. That is right for an
inner join, where every output row matched. An outer join emits its preserved
rows whether or not they matched, so a preserved key keeps values from outside
the intersection and distinct values the other side never had.

Narrowing it anyway under-claimed the estimate, and made the propagated range
say something the join does not produce -- a consumer that reads those ranges as
truth and transports them onto the opposite leg's scan would drop matching rows.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.optimizer.statistics import ColumnRange, ColumnStatistics, RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _intersect_join_keys

LEFT_KEY = b"left_key"
RIGHT_KEY = b"right_key"


def _side(identity, lower, upper, ndv):
    return RelationStatistics(
        columns={
            identity: ColumnStatistics(
                column_name="k",
                data_type="INTEGER",
                distinct_count=ndv,
                value_range=ColumnRange(lower_bound=lower, upper_bound=upper),
            )
        },
        row_count_estimate=1000,
    )


def _intersected(estimator_type):
    left = _side(LEFT_KEY, 0, 100, 100)
    right = _side(RIGHT_KEY, 40, 60, 20)
    merged = dict(left.columns)
    merged.update(right.columns)
    return _intersect_join_keys(
        merged, left, right, [LEFT_KEY], [RIGHT_KEY], estimator_type
    )


def test_inner_join_narrows_both_keys():
    out = _intersected("inner")
    for key in (LEFT_KEY, RIGHT_KEY):
        assert out[key].value_range == ColumnRange(lower_bound=40, upper_bound=60), key
        assert out[key].distinct_count == 20, key


@pytest.mark.parametrize(
    "estimator_type, narrowed, preserved, preserved_range, preserved_ndv",
    [
        ("left", RIGHT_KEY, LEFT_KEY, ColumnRange(lower_bound=0, upper_bound=100), 100),
        ("right", LEFT_KEY, RIGHT_KEY, ColumnRange(lower_bound=40, upper_bound=60), 20),
    ],
)
def test_one_sided_outer_join_narrows_only_the_matched_side(
    estimator_type, narrowed, preserved, preserved_range, preserved_ndv
):
    out = _intersected(estimator_type)

    assert out[narrowed].value_range == ColumnRange(lower_bound=40, upper_bound=60)
    assert out[narrowed].distinct_count == 20

    # The preserved key keeps exactly what it arrived with. (The right side's own
    # range IS the intersection here, so its NDV is what shows the difference.)
    assert out[preserved].value_range == preserved_range
    assert out[preserved].distinct_count == preserved_ndv


def test_full_outer_join_narrows_neither_key():
    out = _intersected("outer")
    assert out[LEFT_KEY].value_range == ColumnRange(lower_bound=0, upper_bound=100)
    assert out[LEFT_KEY].distinct_count == 100
    assert out[RIGHT_KEY].value_range == ColumnRange(lower_bound=40, upper_bound=60)
    assert out[RIGHT_KEY].distinct_count == 20


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
