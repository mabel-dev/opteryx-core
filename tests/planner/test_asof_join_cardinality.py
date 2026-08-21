"""An ASOF join's estimated cardinality is the LEFT input's row count.

ASOF is left-preserving: the operator emits exactly one row per left row,
null-filled when nothing matches (AsofJoinNode's docstring, and
tests/operators/test_asof_join.py). The statistics pass did not know that.
"asof" matched none of the mapped join types in `_join_stats`, and the optional
partition key is the ONLY thing that populates `left_columns`/`right_columns`
(a MATCH_CONDITION fills `asof_left_column`/`asof_right_column` instead), so a
no-ON ASOF fell through to the keyless branch and was estimated at the CROSS
PRODUCT of its inputs. A five-row ASOF against a fifty-row relation was refused
before reading any data at "340,244,459,546 rows".

Also covered here: a LIMIT pushed INTO a scan. LimitPushdownStrategy deletes the
Limit node once the connector supports the pushdown, and the scan's statistics
used to keep reporting the full relation size — the reason the refusal above
quoted the two tables' FULL cardinalities despite `LIMIT 5` / `LIMIT 50`.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.exceptions import ResultTooLargeError
from opteryx.models import ExecutionContext
from opteryx.models import QueryTelemetry
from opteryx.planner import bind_statement
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.result_size_guard import check_estimated_result_size
from opteryx.utils import random_string

# Over the `sql_select_limit` default (1,073,741,824) as a PRODUCT, comfortably
# under it as a single side — so a cross-product estimate is refused and a
# left-preserving one is not.
_BIG = 100_000

_NO_ON = """
SELECT p.name, p2.name AS m
  FROM $planets AS p
  ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity >= p2.gravity)
"""

_WITH_KEY = """
SELECT p.name, p2.name AS m
  FROM $planets AS p
  ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity >= p2.gravity) USING (id)
"""

_CROSS = "SELECT p.name, p2.name FROM $planets AS p CROSS JOIN $planets AS p2"


def _bound(sql):
    query_id = random_string(32)
    telemetry = QueryTelemetry(query_id)
    bound, _clean_sql, _ast = bind_statement(
        operation=sql,
        parameters=None,
        visibility_filters=None,
        execution_context=ExecutionContext(memberships=["opteryx"]),
        query_id=query_id,
        telemetry=telemetry,
    )
    return do_optimizer(bound, telemetry, scan_stats_cache={}, shared_ctes={}), telemetry


def _inflate_scans(plan, rows):
    """Make every scan claim `rows` rows, as a METRIC.

    $planets is small enough that a cross product of it clears no limit at all,
    so the guard could never be exercised against it at its real size. The
    schema row count is the same field `_scan_base_stats` and the guard's own
    `_declared_row_count` read for a manifest-less relation.
    """
    for nid, node in plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Scan:
            continue
        assert node.manifest is None, "virtual scan grew a manifest; inflate that instead"
        node.schema.row_count_metric = rows
        node.schema.row_count_estimate = None
    plan.statistics_are_stale = True
    return plan


def _join_and_child_rows(plan):
    plan = refresh_statistics(plan)
    joins = [
        node for _nid, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Join
    ]
    assert len(joins) == 1, f"expected exactly one Join node, got {len(joins)}"
    join_nid = [
        nid for nid, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Join
    ][0]
    # Mirror `_split_join_children`: the "left"-labelled leg, or — when the
    # labels did not survive the optimizer — the first child in insertion order.
    edges = list(plan.outgoing_edges(join_nid))
    labelled = [target for _s, target, label in edges if label == "left"]
    left_nid = labelled[0] if labelled else edges[0][1]
    return joins[0].statistics, plan[left_nid].statistics


@pytest.mark.parametrize("sql", [_NO_ON, _WITH_KEY], ids=["no_on", "using_key"])
def test_asof_cardinality_is_the_left_row_count(sql):
    plan, _telemetry = _bound(sql)
    join_stats, left_stats = _join_and_child_rows(plan)
    assert join_stats.row_count == left_stats.row_count, (
        f"ASOF estimated {join_stats.row_count} rows from a {left_stats.row_count}-row "
        "left input — ASOF emits exactly one row per left row"
    )


def test_asof_left_count_provenance_survives():
    """A metric left count makes the ASOF count a metric — it is EQUAL to the
    left count, not bounded by it, so no heuristic touched the number."""
    plan, _telemetry = _bound(_NO_ON)
    join_stats, left_stats = _join_and_child_rows(plan)
    assert left_stats.row_count_is_metric, "left leg is not metric; test no longer covers this"
    assert join_stats.row_count_is_metric


def test_no_on_asof_over_large_relations_is_not_refused():
    plan, telemetry = _bound(_NO_ON)
    plan = _inflate_scans(plan, _BIG)
    limit = 1_073_741_824
    # Would raise if the estimate were the cross product (_BIG ** 2 = 10**10).
    plan = check_estimated_result_size(plan, limit, telemetry=telemetry, scan_stats_cache={})
    exit_nid = plan.get_exit_points()[0]
    # Not just "wasn't refused" — the number the guard read has to be the left
    # count. A cross product that merely lost its metric provenance would slip
    # past the guard while still poisoning join ordering and EXPLAIN.
    assert plan[exit_nid].statistics.row_count == _BIG, (
        f"exit estimated {plan[exit_nid].statistics.row_count} rows from a "
        f"{_BIG}-row left input"
    )


def test_cross_join_over_the_same_relations_is_still_refused():
    """Control: the guard IS armed at this scale, so the test above is not
    passing because nothing was checked."""
    plan, telemetry = _bound(_CROSS)
    plan = _inflate_scans(plan, _BIG)
    with pytest.raises(ResultTooLargeError):
        check_estimated_result_size(plan, 1_073_741_824, telemetry=telemetry, scan_stats_cache={})


def test_pushed_down_scan_limit_caps_the_scan_row_count():
    """LimitPushdownStrategy removes the Limit node; the cap must not vanish
    with it."""
    from opteryx.connectors import DiskConnector

    import opteryx

    opteryx.register_workspace("testdata", DiskConnector)
    plan, _telemetry = _bound("SELECT * FROM (SELECT * FROM testdata.planets LIMIT 3) AS t")
    plan = refresh_statistics(plan)
    scans = [
        node for _nid, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Scan
    ]
    assert len(scans) == 1
    assert scans[0].limit == 3, "limit was not pushed into the scan; test covers nothing"
    assert scans[0].statistics.row_count == 3, (
        f"scan with a pushed LIMIT 3 reports {scans[0].statistics.row_count} rows"
    )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
