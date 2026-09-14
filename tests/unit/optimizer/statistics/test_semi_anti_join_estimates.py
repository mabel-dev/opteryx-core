"""Semi/anti joins reduce, and the estimator has to say so.

`_join_stats` used to return `left.row_count` for all five semi/anti spellings,
justified by a comment about COLUMNS ("semi/anti emit only left-side columns;
right contributes nothing") -- true, and not a statement about the row count.
It asserted that a join whose whole purpose is to reduce reduces nothing, and
reported `key_count=0` while the node carried keys.

See docs/SEMI_ANTI_CARDINALITY_DESIGN.md. The model measures the right side's
LIVE key set against the shared key DOMAIN.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _join_stats

LEFT_KEY = b"left_key________"
RIGHT_KEY = b"right_key_______"


class _Node:
    """The fields `_join_stats` reads off a FilterJoinNode."""

    def __init__(self, join_type):
        self.type = join_type
        self.left_columns = [_Ident(LEFT_KEY)]
        self.right_columns = [_Ident(RIGHT_KEY)]


class _Ident:
    def __init__(self, identity):
        self.schema_column = _Col(identity)
        self.node_type = None


class _Col:
    def __init__(self, identity):
        self.identity = identity


def _relation(rows, key, ndv, base_ndv=None, lower=None, upper=None, nulls=None):
    return RelationStatistics(
        row_count_estimate=rows,
        columns={
            key: ColumnStatistics(
                column_name="k",
                data_type="INT64",
                distinct_count=ndv,
                base_distinct_count=base_ndv,
                value_range=ColumnRange(lower, upper),
                null_fraction=nulls,
            )
        },
    )


def _estimate(join_type, left, right):
    notes = []
    stats = _join_stats(_Node(join_type), [(left, "left"), (right, "right")], "nid", notes)
    return stats, notes[0]


def _sides(left_rows=1000, live=2500, domain=10_000, **kw):
    left = _relation(left_rows, LEFT_KEY, ndv=left_rows, base_ndv=domain, **kw)
    right = _relation(50_000, RIGHT_KEY, ndv=live, base_ndv=domain)
    return left, right


def test_a_semi_join_is_not_estimated_as_non_reducing():
    left, right = _sides()
    stats, note = _estimate("left semi", left, right)
    # The right holds a quarter of the key domain.
    assert stats.row_count == 250, stats.row_count
    assert note["row_count"] == 250


def test_an_anti_join_is_the_complement():
    left, right = _sides()
    semi, _ = _estimate("left semi", left, right)
    anti, _ = _estimate("left anti", left, right)
    assert semi.row_count + anti.row_count == 1000


def test_the_key_count_is_reported():
    """It was hardcoded to 0, so telemetry could not tell a keyed semi join from
    a keyless one."""
    left, right = _sides()
    _, note = _estimate("left anti", left, right)
    assert note["key_count"] == 1


def test_an_anti_join_does_not_narrow_its_left_key():
    """§4.3. ANTI emits the left rows that did NOT match, whose keys lie in the
    COMPLEMENT of the intersection. `_NARROWABLE_JOIN_SIDES` defaults to
    narrowing BOTH sides, so without an explicit entry the anti join would
    publish the intersected range -- describing a relation it never produces,
    and the range is transported onto scans."""
    left = _relation(1000, LEFT_KEY, ndv=1000, base_ndv=10_000, lower=0, upper=100)
    right = _relation(50_000, RIGHT_KEY, ndv=2500, base_ndv=10_000, lower=50, upper=200)

    anti, _ = _estimate("left anti", left, right)
    assert anti.columns[LEFT_KEY].value_range.lower_bound == 0, (
        "the anti join's left key was narrowed to the MATCHING range"
    )
    assert anti.columns[LEFT_KEY].value_range.upper_bound == 100

    # SEMI is the opposite case: it emits only matched rows, so its left key IS
    # bounded by the intersection.
    semi, _ = _estimate("left semi", left, right)
    assert semi.columns[LEFT_KEY].value_range.lower_bound == 50


def test_only_left_columns_survive():
    left, right = _sides()
    stats, _ = _estimate("left semi", left, right)
    assert RIGHT_KEY not in stats.columns


def test_an_unknown_live_ndv_declines_rather_than_inventing():
    """No live NDV, no match fraction. Falling back to the left row count is the
    honest answer; a fabricated fraction is not."""
    left = _relation(1000, LEFT_KEY, ndv=None, base_ndv=10_000)
    right = _relation(50_000, RIGHT_KEY, ndv=None, base_ndv=10_000)
    stats, _ = _estimate("left anti", left, right)
    assert stats.row_count == 1000


def test_the_row_count_is_an_estimate_never_a_metric():
    left, right = _sides()
    stats, _ = _estimate("left semi", left, right)
    assert not stats.row_count_is_metric


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
