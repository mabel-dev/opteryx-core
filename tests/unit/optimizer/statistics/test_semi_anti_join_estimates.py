"""Semi/anti joins reduce, and the estimator has to say so.

The join estimate used to return `left.row_count` for all five semi/anti spellings,
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

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import MaterializedCteRefStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.expression import NodeType
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64


class _Query:
    """One query's context: the two join-key columns minted in its ColumnTable."""

    def __init__(self):
        self.plan_context = PlanContext()
        self.left_key = self.plan_context.columns.relation_column("l", "k", column_type=INT64)
        self.right_key = self.plan_context.columns.relation_column("r", "k", column_type=INT64)


def _relation(query, rows, key, ndv, base_ndv=None, lower=None, upper=None, nulls=None):
    """(key column, statistics) of a one-key relation."""
    fields = {"distinct_count": ndv, "base_distinct_count": base_ndv, "null_fraction": nulls}
    if lower is not None or upper is not None:
        fields["value_range"] = (lower, upper)
    return StatisticsInput(
        query.plan_context.columns, row_count_estimate=rows, column_stats={key.identity: fields}
    )


def _identifier(query, column):
    """A join key as the binder leaves it: an identifier bound to its column."""
    identifier = LogicalColumn(
        node_type=NodeType.IDENTIFIER, source_column=column.name, arena=query.plan_context.expressions
    )
    identifier.schema_column = column
    return identifier


class _Stats:
    """The join node's statistics, read from the store."""

    def __init__(self, query, nid):
        store = query.plan_context.statistics
        self.row_count = store.row_count(nid)
        self.row_count_is_metric = store.row_count_metric(nid) is not None
        self._store = store
        self._nid = nid

    def has_column(self, column):
        return self._store.has_column(self._nid, column.identity)

    def value_range(self, column):
        return self._store.value_range(self._nid, column.identity)


def _estimate(query, join_type, left, right):
    """(join statistics, the join's telemetry note).

    The two legs are shared-CTE references stamped with `left` / `right`, so a
    full refresh -- the only path that records the join note -- reads exactly
    those statistics as its inputs."""
    plan_context = query.plan_context
    store = plan_context.statistics

    # the CTE bodies: nodes of their own plan, holding the seeded statistics
    bodies = LogicalPlan(plan_context)
    for cte_key, stats in (("left", left), ("right", right)):
        body_nid = bodies.add_node(ScanStep())
        store.seed(body_nid, stats)
        store.set_cte(cte_key, body_nid)

    join = JoinStep()
    join.type = join_type
    join.left_columns = [_identifier(query, query.left_key)]
    join.right_columns = [_identifier(query, query.right_key)]

    plan = LogicalPlan(plan_context)
    legs = {}
    for cte_key in ("left", "right"):
        ref = MaterializedCteRefStep()
        ref.cte_key = cte_key
        legs[cte_key] = plan.add_node(ref)
    join_nid = plan.add_node(join)
    plan.add_edge(legs["left"], join_nid, EdgeRole.LEFT)
    plan.add_edge(legs["right"], join_nid, EdgeRole.RIGHT)

    telemetry = QueryTelemetry.detached()
    refresh_statistics(plan, plan_context, telemetry)
    (note,) = [n for n in telemetry._reading["join_estimates"] if n["nid"] == join_nid]
    return _Stats(query, join_nid), note


def _sides(query, left_rows=1000, live=2500, domain=10_000, **kw):
    left = _relation(query, left_rows, query.left_key, ndv=left_rows, base_ndv=domain, **kw)
    right = _relation(query, 50_000, query.right_key, ndv=live, base_ndv=domain)
    return left, right


def test_a_semi_join_is_not_estimated_as_non_reducing():
    query = _Query()
    left, right = _sides(query)
    stats, note = _estimate(query, "left semi", left, right)
    # The right holds a quarter of the key domain.
    assert stats.row_count == 250, stats.row_count
    assert note["row_count"] == 250


def test_an_anti_join_is_the_complement():
    query = _Query()
    left, right = _sides(query)
    semi, _ = _estimate(query, "left semi", left, right)
    anti, _ = _estimate(query, "left anti", left, right)
    assert semi.row_count + anti.row_count == 1000


def test_the_key_count_is_reported():
    """It was hardcoded to 0, so telemetry could not tell a keyed semi join from
    a keyless one."""
    query = _Query()
    left, right = _sides(query)
    _, note = _estimate(query, "left anti", left, right)
    assert note["key_count"] == 1


def test_an_anti_join_does_not_narrow_its_left_key():
    """§4.3. ANTI emits the left rows that did NOT match, whose keys lie in the
    COMPLEMENT of the intersection. `_NARROWABLE_JOIN_SIDES` defaults to
    narrowing BOTH sides, so without an explicit entry the anti join would
    publish the intersected range -- describing a relation it never produces,
    and the range is transported onto scans."""
    query = _Query()
    left = _relation(query, 1000, query.left_key, ndv=1000, base_ndv=10_000, lower=0, upper=100)
    right = _relation(query, 50_000, query.right_key, ndv=2500, base_ndv=10_000, lower=50, upper=200)

    anti, _ = _estimate(query, "left anti", left, right)
    assert anti.value_range(query.left_key)[0] == 0, (
        "the anti join's left key was narrowed to the MATCHING range"
    )
    assert anti.value_range(query.left_key)[1] == 100

    # SEMI is the opposite case: it emits only matched rows, so its left key IS
    # bounded by the intersection.
    semi, _ = _estimate(query, "left semi", left, right)
    assert semi.value_range(query.left_key)[0] == 50


def test_only_left_columns_survive():
    query = _Query()
    left, right = _sides(query)
    stats, _ = _estimate(query, "left semi", left, right)
    assert not stats.has_column(query.right_key)


def test_an_unknown_live_ndv_declines_rather_than_inventing():
    """No live NDV, no match fraction. Falling back to the left row count is the
    honest answer; a fabricated fraction is not."""
    query = _Query()
    left = _relation(query, 1000, query.left_key, ndv=None, base_ndv=10_000)
    right = _relation(query, 50_000, query.right_key, ndv=None, base_ndv=10_000)
    stats, _ = _estimate(query, "left anti", left, right)
    assert stats.row_count == 1000


def test_the_row_count_is_an_estimate_never_a_metric():
    query = _Query()
    left, right = _sides(query)
    stats, _ = _estimate(query, "left semi", left, right)
    assert not stats.row_count_is_metric


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
