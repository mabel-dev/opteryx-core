"""A filter can only REDUCE cardinality, so no estimate may rise when one is added.

The single-table fuzzer found this as a refusal, not a slow plan:

    SELECT flag, grp_wide, AVG(row_id) OVER (PARTITION BY flag) AS w
    FROM testdata.fuzzing.wide WHERE flag = TRUE
      -> ResultTooLargeError: estimated to return 2,000,000,000 rows

against a 200,000-row relation — and the same query WITHOUT the WHERE ran fine.
An aggregate window is planned as a self-join of the relation against a grouped
aggregate of it, so both halves of the fault live on the join estimate path:

  * the Aggregate estimate left the single group key's NDV unset, throwing away the
    one NDV a group-by always knows exactly (one output row per distinct key).
  * the join's key classes (`equi_key_classes`) pooled both sides' NDVs and value-range spans before
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

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext


class _Query:
    """One query's context and its two join-key columns."""

    def __init__(self):
        self.plan_context = PlanContext()
        self.key = self.plan_context.columns.relation_column("l", "grp_wide").identity
        self.other_key = self.plan_context.columns.relation_column("r", "grp_wide").identity


def _relation(query, rows, ndv, lower=None, upper=None, base=None, key=None):
    fields = {"distinct_count": ndv}
    if lower is not None or upper is not None:
        fields["value_range"] = (lower, upper)
    return StatisticsInput(
        query.plan_context.columns,
        row_count_estimate=rows,
        column_stats={query.key if key is None else key: fields},
        base_row_count=base,
    )


def _divisor(query, left, right):
    """The number `key_selectivity` actually divides by.

    These tests are about the DIVISOR -- tdom, standing in for
    max(ndv_left, ndv_right) -- not about which slot carries it. The key
    classes' KeyStats are internal to the native join propagator; what they
    produce is the inner join's row count, |L| x |R| / max(ndv_l, ndv_r) for one
    null-free key class, so the divisor is recovered from it (exactly: these
    fixtures divide evenly).
    """
    join = JoinStep()
    join.type = "inner"
    join.left_columns = [query.key]
    join.right_columns = [query.other_key]
    plan = LogicalPlan(query.plan_context)
    left_nid = plan.add_node(ScanStep())
    right_nid = plan.add_node(ScanStep())
    join_nid = plan.add_node(join)
    plan.add_edge(left_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)
    store = query.plan_context.statistics
    store.seed(left_nid, left)
    store.seed(right_nid, right)
    store.compute(plan, join_nid)
    return store.row_count(left_nid) * store.row_count(right_nid) / store.row_count(join_nid)


def test_one_sided_ndv_does_not_collapse_the_key_domain():
    """tdom stands in for max(ndv_left, ndv_right); a known NDV on ONE side is
    not that maximum, and adopting it makes the join a cross product."""
    query = _Query()
    # `WHERE grp_wide = 5`: one distinct value survives on the left. The right
    # side reports no NDV at all.
    left = _relation(query, 20_000, ndv=1, lower=5, upper=5, base=200_000)
    right = _relation(query, 100_000, ndv=None, key=query.other_key)

    divisor = _divisor(query, left, right)

    # The filtered side's own NDV is 1 and says so -- that is its honest number.
    # What must not happen is that 1 becoming the DIVISOR for the whole class.
    # (The per-side KeyStats NDV -- `left_key.ndv == 1` -- is internal to the
    # native propagator and not observable; only the divisor it feeds is.)
    assert divisor > 1, (
        f"tdom collapsed to {divisor}: the filtered side's NDV "
        "was adopted as the whole key domain, so the join estimates as |L| x |R|"
    )


def test_a_narrow_range_on_one_side_does_not_cap_the_other():
    """A value-range span bounds the NDV of the column it came from, not the
    other side's. Intersecting the two produces the size of the MATCHING
    domain while the row counts stay un-intersected -- and that error only ever
    runs one way, inflating the estimate exactly when a filter narrows a side."""
    query = _Query()
    unfiltered = _relation(query, 200_000, ndv=None, lower=0, upper=49_999, base=200_000)
    filtered = _relation(query, 20_000, ndv=None, lower=5, upper=5, base=200_000)
    right = _relation(query, 100_000, ndv=50_000, lower=0, upper=49_999, key=query.other_key)

    without_filter = _divisor(query, unfiltered, right)
    with_filter = _divisor(query, filtered, right)

    assert with_filter == without_filter, (
        f"narrowing one side's range moved tdom {without_filter} -> {with_filter}; "
        "the other side's domain is unchanged by a filter that isn't on it"
    )


def test_both_sides_known_still_take_the_maximum():
    """The per-side split must not disturb the ordinary case: when both sides
    report an NDV, tdom is still the larger of the two (Ebergen 2022 3.2)."""
    query = _Query()
    left = _relation(query, 200_000, ndv=10_000)
    right = _relation(query, 800_000, ndv=200_000, key=query.other_key)

    assert _divisor(query, left, right) == 200_000
    # (Each side reporting the NDV it was actually given, not the pair's
    # maximum -- `(left_key.ndv, right_key.ndv) == (10_000, 200_000)` -- is
    # internal to the native propagator and not observable.)


def _exit_estimate(sql):
    """Row-count estimate the `sql_select_limit` guard would read for `sql`."""
    plan_context = PlanContext()
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

    telemetry = QueryTelemetry.detached()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    parsed = sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast, plan_context=plan_context)
    plan = do_resolve_relations(plan, ctes, telemetry, plan_context=plan_context)
    plan = do_plan_rewrite(plan, telemetry, plan_context=plan_context)
    bound = do_bind_phase(
        plan, execution_context=ctx, query_id=str(uuid.uuid4()), telemetry=telemetry, 
    plan_context=plan_context)
    refreshed = refresh_statistics(bound, plan_context)

    (exit_point,) = refreshed.get_exit_points()
    assert refreshed[exit_point].node_type == LogicalPlanStepType.Exit
    return plan_context.statistics.row_count(exit_point)


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
