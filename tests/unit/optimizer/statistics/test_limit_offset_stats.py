"""LIMIT's row estimate must account for OFFSET.

OFFSET consumes rows before LIMIT counts: `LIMIT 10 OFFSET 1_000_000` over a
1_000_005-row input returns 5 rows, not 10. `_limit_stats` read only
``node.limit``, so any offset-heavy pagination query was estimated at the full
limit — and an OFFSET with no LIMIT was ignored outright.

Provenance follows the metric/estimate lingo (statistics.py): the subtraction
and min are exact arithmetic, so the output inherits the INPUT's provenance —
a metric input stays a metric, an estimate stays an estimate.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _limit_stats


def _limit_node(limit=None, offset=None):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
    node.limit = limit
    node.offset = offset
    return node


def _child(rows, metric=True):
    if metric:
        stats = RelationStatistics(columns={}, row_count_metric=rows)
    else:
        stats = RelationStatistics(columns={}, row_count_estimate=rows)
    return [(stats, "child")]


def test_offset_within_input_leaves_fewer_rows_than_the_limit():
    """The motivating case: LIMIT 10 OFFSET 1_000_000 over 1_000_005 rows is 5."""
    out = _limit_stats(_limit_node(limit=10, offset=1_000_000), _child(1_000_005))
    assert out.row_count == 5


def test_offset_plus_limit_past_end_returns_the_remainder():
    out = _limit_stats(_limit_node(limit=50, offset=80), _child(100))
    assert out.row_count == 20


def test_offset_past_end_returns_zero_rows():
    out = _limit_stats(_limit_node(limit=10, offset=200), _child(100))
    assert out.row_count == 0


def test_offset_with_no_limit_emits_everything_past_the_offset():
    out = _limit_stats(_limit_node(limit=None, offset=30), _child(100))
    assert out.row_count == 70


def test_no_offset_is_unchanged():
    out = _limit_stats(_limit_node(limit=10, offset=None), _child(100))
    assert out.row_count == 10


def test_limit_larger_than_input_with_no_offset_is_unchanged():
    out = _limit_stats(_limit_node(limit=500, offset=None), _child(100))
    assert out.row_count == 100


def test_metric_input_stays_a_metric():
    """Exact arithmetic over a metric input preserves METRIC provenance."""
    out = _limit_stats(_limit_node(limit=10, offset=1_000_000), _child(1_000_005, metric=True))
    assert out.row_count_is_metric
    assert out.row_count_metric == 5


def test_estimate_input_stays_an_estimate():
    out = _limit_stats(_limit_node(limit=10, offset=1_000_000), _child(1_000_005, metric=False))
    assert not out.row_count_is_metric
    assert out.row_count_estimate == 5


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
