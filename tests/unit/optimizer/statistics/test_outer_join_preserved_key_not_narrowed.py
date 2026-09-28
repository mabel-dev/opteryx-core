# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""An outer join must not narrow its PRESERVED side's key statistics.

The join's key narrowing (`intersect_join_keys`,
src/cpp/planner/statistics_refresh.hpp) replaces both equi-key columns' ranges with their
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

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext

LEFT_KEY = "left_key"
RIGHT_KEY = "right_key"

# The estimator's spelling of each side policy -> the join type that reaches it.
_JOIN_TYPE = {"inner": "inner", "left": "left outer", "right": "right outer", "outer": "full outer"}


def _side(plan_context, identity, lower, upper, ndv):
    return StatisticsInput(
        plan_context.columns,
        row_count_estimate=1000,
        column_stats={identity: {"distinct_count": ndv, "value_range": (lower, upper)}},
    )


class _Out:
    """The join's key-column statistics: (value range, NDV) by key label."""

    def __init__(self, store, nid, identities):
        self._store = store
        self._nid = nid
        self._identities = identities

    def __getitem__(self, key):
        identity = self._identities[key]
        return self._store.value_range(self._nid, identity), self._store.distinct_count(self._nid, identity)


def _intersected(estimator_type):
    """The join's native propagator run alone over the two seeded legs."""
    plan_context = PlanContext()
    columns = plan_context.columns
    identities = {
        LEFT_KEY: columns.relation_column("l", "k").identity,
        RIGHT_KEY: columns.relation_column("r", "k").identity,
    }

    join = JoinStep()
    join.type = _JOIN_TYPE[estimator_type]
    join.left_columns = [identities[LEFT_KEY]]
    join.right_columns = [identities[RIGHT_KEY]]

    plan = LogicalPlan(plan_context)
    left_nid = plan.add_node(ScanStep())
    right_nid = plan.add_node(ScanStep())
    join_nid = plan.add_node(join)
    plan.add_edge(left_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)

    store = plan_context.statistics
    store.seed(left_nid, _side(plan_context, identities[LEFT_KEY], 0, 100, 100))
    store.seed(right_nid, _side(plan_context, identities[RIGHT_KEY], 40, 60, 20))
    store.compute(plan, join_nid)
    return _Out(store, join_nid, identities)


def test_inner_join_narrows_both_keys():
    out = _intersected("inner")
    for key in (LEFT_KEY, RIGHT_KEY):
        value_range, distinct_count = out[key]
        assert value_range == (40, 60), key
        assert distinct_count == 20, key


@pytest.mark.parametrize(
    "estimator_type, narrowed, preserved, preserved_range, preserved_ndv",
    [
        ("left", RIGHT_KEY, LEFT_KEY, (0, 100), 100),
        ("right", LEFT_KEY, RIGHT_KEY, (40, 60), 20),
    ],
)
def test_one_sided_outer_join_narrows_only_the_matched_side(
    estimator_type, narrowed, preserved, preserved_range, preserved_ndv
):
    out = _intersected(estimator_type)

    assert out[narrowed] == ((40, 60), 20)

    # The preserved key keeps exactly what it arrived with. (The right side's own
    # range IS the intersection here, so its NDV is what shows the difference.)
    assert out[preserved] == (preserved_range, preserved_ndv)


def test_full_outer_join_narrows_neither_key():
    out = _intersected("outer")
    assert out[LEFT_KEY] == ((0, 100), 100)
    assert out[RIGHT_KEY] == ((40, 60), 20)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
