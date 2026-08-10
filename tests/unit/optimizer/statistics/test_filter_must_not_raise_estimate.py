"""A filter can only REDUCE cardinality, so no estimate may rise when one is added.

The single-table fuzzer found this as a refusal, not a slow plan:

    SELECT flag, grp_wide, AVG(row_id) OVER (PARTITION BY flag) AS w
    FROM testdata.fuzzing.wide WHERE flag = TRUE
      -> ResultTooLargeError: estimated to return 2,000,000,000 rows

against a 200,000-row relation — and the same query WITHOUT the WHERE ran fine.
An aggregate window is planned as a self-join of the relation against a grouped
aggregate of it, so both halves of the fault live on the join estimate path:

  * `_aggregate_stats` left the single group key's NDV unset, throwing away the
    one NDV a group-by always knows exactly (one output row per distinct key).
  * `_equi_key_classes` pooled both sides' NDVs and value-range spans before
    reducing, so `flag = TRUE` (ndv 1, range [True, True]) pinned tdom to 1 for
    the WHOLE class and turned |L| x |R| / tdom into a full cross product.

Both are estimate-only defects, so they are asserted against the estimator
rather than a row count.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _equi_key_classes

KEY = b"tes_grp_aTElGxhx"
OTHER_KEY = b"tes_grp_SasR64jX"


def _relation(rows, ndv, lower=None, upper=None, base=None, key=KEY):
    return RelationStatistics(
        row_count=rows,
        columns={
            key: ColumnStatistics(
                column_name="grp_wide",
                data_type="INT64",
                distinct_count=ndv,
                value_range=ColumnRange(lower, upper),
            )
        },
        base_row_count=base,
    )


def test_one_sided_ndv_does_not_collapse_the_key_domain():
    """tdom stands in for max(ndv_left, ndv_right); a known NDV on ONE side is
    not that maximum, and adopting it makes the join a cross product."""
    # `WHERE grp_wide = 5`: one distinct value survives on the left. The right
    # side reports no NDV at all.
    left = _relation(20_000, ndv=1, lower=5, upper=5, base=200_000)
    right = _relation(100_000, ndv=None, key=OTHER_KEY)

    ((left_key, right_key),) = _equi_key_classes([KEY], [OTHER_KEY], left, right)

    assert left_key.ndv == right_key.ndv, "tdom is one domain, applied to both sides"
    assert left_key.ndv > 1, (
        f"tdom collapsed to {left_key.ndv}: the filtered side's NDV was adopted as the "
        "whole key domain, so the join estimates as |L| x |R|"
    )


def test_a_narrow_range_on_one_side_does_not_cap_the_other():
    """A value-range span bounds the NDV of the column it came from, not the
    other side's. Intersecting the two produces the size of the MATCHING
    domain while the row counts stay un-intersected -- and that error only ever
    runs one way, inflating the estimate exactly when a filter narrows a side."""
    unfiltered = _relation(200_000, ndv=None, lower=0, upper=49_999, base=200_000)
    filtered = _relation(20_000, ndv=None, lower=5, upper=5, base=200_000)
    right = _relation(100_000, ndv=50_000, lower=0, upper=49_999, key=OTHER_KEY)

    without_filter = _equi_key_classes([KEY], [OTHER_KEY], unfiltered, right)[0][0].ndv
    with_filter = _equi_key_classes([KEY], [OTHER_KEY], filtered, right)[0][0].ndv

    assert with_filter == without_filter, (
        f"narrowing one side's range moved tdom {without_filter} -> {with_filter}; "
        "the other side's domain is unchanged by a filter that isn't on it"
    )


def test_both_sides_known_still_take_the_maximum():
    """The per-side split must not disturb the ordinary case: when both sides
    report an NDV, tdom is still the larger of the two (Ebergen 2022 3.2)."""
    left = _relation(200_000, ndv=10_000)
    right = _relation(800_000, ndv=200_000, key=OTHER_KEY)

    ((left_key, _),) = _equi_key_classes([KEY], [OTHER_KEY], left, right)

    assert left_key.ndv == 200_000


def _exit_estimate(sql):
    """Row-count estimate the `sql_select_limit` guard would read for `sql`."""
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    parsed = sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(
        plan, execution_context=ctx, query_id=str(uuid.uuid4()), telemetry=telemetry
    )
    refreshed = refresh_statistics(bound)

    (exit_point,) = refreshed.get_exit_points()
    assert refreshed[exit_point].node_type == LogicalPlanStepType.Exit
    return refreshed[exit_point].statistics.row_count


@pytest.mark.skipif(
    not os.path.isdir("testdata/fuzzing/wide"),
    reason="testdata/fuzzing not generated (dev/generate_fuzz_testdata.py)",
)
@pytest.mark.parametrize(
    "predicate",
    ["flag = TRUE", "row_id > 5", "grp_wide = 5", "cat = 'a'"],
)
def test_filter_under_an_aggregate_window_cannot_raise_the_estimate(predicate):
    window = (
        "SELECT flag, grp_wide, AVG(row_id) OVER (PARTITION BY flag) AS w "
        "FROM testdata.fuzzing.wide"
    )
    unfiltered = _exit_estimate(window)
    filtered = _exit_estimate(f"{window} WHERE {predicate}")

    assert filtered <= unfiltered, (
        f"WHERE {predicate} raised the estimate {unfiltered} -> {filtered}; a filter "
        "can only reduce cardinality"
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
