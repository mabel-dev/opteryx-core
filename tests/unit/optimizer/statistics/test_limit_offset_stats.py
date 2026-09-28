"""LIMIT's row estimate must account for OFFSET.

OFFSET consumes rows before LIMIT counts: `LIMIT 10 OFFSET 1_000_000` over a
1_000_005-row input returns 5 rows, not 10. The Limit estimate once read only
``node.limit``, so any offset-heavy pagination query was estimated at the full
limit — and an OFFSET with no LIMIT was ignored outright.

Provenance follows the metric/estimate lingo (src/cpp/planner/stats_store.hpp): the subtraction
and min are exact arithmetic, so the output inherits the INPUT's provenance —
a metric input stays a metric, an estimate stays an estimate.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.plan_steps import LimitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext


class _Out:
    """The Limit node's statistics, read from the store."""

    def __init__(self, store, nid):
        self.row_count = store.row_count(nid)
        self.row_count_metric = store.row_count_metric(nid)
        self.row_count_estimate = store.row_count_estimate(nid)
        self.row_count_is_metric = self.row_count_metric is not None


def _limit_stats(limit, offset, rows, metric=True):
    """Run the Limit node's native propagator alone over a seeded child."""
    plan_context = PlanContext()
    node = LimitStep()
    node.limit = limit
    node.offset = offset

    plan = LogicalPlan(plan_context)
    child_nid = plan.add_node(ScanStep())
    limit_nid = plan.add_node(node)
    plan.add_edge(child_nid, limit_nid)

    if metric:
        stats = StatisticsInput(plan_context.columns, row_count_metric=rows)
    else:
        stats = StatisticsInput(plan_context.columns, row_count_estimate=rows)
    store = plan_context.statistics
    store.seed(child_nid, stats)
    store.compute(plan, limit_nid)
    return _Out(store, limit_nid)


def test_offset_within_input_leaves_fewer_rows_than_the_limit():
    """The motivating case: LIMIT 10 OFFSET 1_000_000 over 1_000_005 rows is 5."""
    out = _limit_stats(10, 1_000_000, 1_000_005)
    assert out.row_count == 5


def test_offset_plus_limit_past_end_returns_the_remainder():
    out = _limit_stats(50, 80, 100)
    assert out.row_count == 20


def test_offset_past_end_returns_zero_rows():
    out = _limit_stats(10, 200, 100)
    assert out.row_count == 0


def test_offset_with_no_limit_emits_everything_past_the_offset():
    out = _limit_stats(None, 30, 100)
    assert out.row_count == 70


def test_no_offset_is_unchanged():
    out = _limit_stats(10, None, 100)
    assert out.row_count == 10


def test_limit_larger_than_input_with_no_offset_is_unchanged():
    out = _limit_stats(500, None, 100)
    assert out.row_count == 100


def test_metric_input_stays_a_metric():
    """Exact arithmetic over a metric input preserves METRIC provenance."""
    out = _limit_stats(10, 1_000_000, 1_000_005, metric=True)
    assert out.row_count_is_metric
    assert out.row_count_metric == 5


def test_estimate_input_stays_an_estimate():
    out = _limit_stats(10, 1_000_000, 1_000_005, metric=False)
    assert not out.row_count_is_metric
    assert out.row_count_estimate == 5


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
