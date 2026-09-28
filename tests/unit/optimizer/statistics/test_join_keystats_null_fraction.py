# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-6: join cardinality must consume join-key null fractions.

The join's statistics propagator builds ``KeyStats`` from each join key's column
statistics (``equi_key_classes`` in src/cpp/planner/statistics_refresh.hpp).
The null fraction was previously hard-coded to ``None`` even when the column
carried one, so the estimator's null discount (``_effective_rows``) never fired.
These tests pin the wiring: a null-heavy join key now reduces the estimated
output cardinality.
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


def _estimate(left_null_fraction):
    """Estimate inner-join row_count with the left key carrying the given null
    fraction; both sides 1000 rows, NDV 100 (so per-key selectivity 1/100).

    Join keys reach the join as raw column *identities* minted in the query's
    ColumnTable, which is also how the statistics store keys columns. Anything
    name-shaped would not resolve -- and would silently re-create the
    dead-lookup bug where every join-key NDV read returned None and the
    estimator fell back to tdom."""
    plan_context = PlanContext()
    columns = plan_context.columns
    left_key = columns.relation_column("l", "lk").identity
    right_key = columns.relation_column("r", "rk").identity

    join = JoinStep()
    join.type = "inner"
    join.left_columns = [left_key]
    join.right_columns = [right_key]

    plan = LogicalPlan(plan_context)
    left_nid = plan.add_node(ScanStep())
    right_nid = plan.add_node(ScanStep())
    join_nid = plan.add_node(join)
    plan.add_edge(left_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)

    store = plan_context.statistics
    store.seed(
        left_nid,
        StatisticsInput(
            columns,
            row_count_estimate=1000,
            column_stats={left_key: {"distinct_count": 100, "null_fraction": left_null_fraction}},
        ),
    )
    store.seed(
        right_nid,
        StatisticsInput(
            columns,
            row_count_estimate=1000,
            column_stats={right_key: {"distinct_count": 100, "null_fraction": 0.0}},
        ),
    )
    store.compute(plan, join_nid)
    return store.row_count(join_nid)


def test_null_fraction_halves_effective_rows():
    # 1000 * 1000 * (1/100) = 10000 with no nulls; the 0.5 null key discounts the
    # left side's effective rows by half -> ~5000.
    baseline = _estimate(0.0)
    half_null = _estimate(0.5)
    assert baseline == pytest.approx(10000, rel=0.05), baseline
    assert half_null < baseline
    assert half_null == pytest.approx(baseline * 0.5, rel=0.05), (half_null, baseline)


def test_none_null_fraction_behaves_like_zero():
    assert _estimate(None) == _estimate(0.0)


def test_all_null_key_column_collapses_cardinality():
    # A wholly-null join key matches nothing; estimate must shrink, not crash.
    all_null = _estimate(1.0)
    assert all_null < _estimate(0.0)
    assert all_null >= 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
