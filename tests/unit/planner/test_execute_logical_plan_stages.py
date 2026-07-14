"""`execute_logical_plan` must run the same planning stages as `query_planner`.

`execute_logical_plan` is the entry point for callers that build a LogicalPlan directly
instead of submitting SQL text (the OData service). It skipped `do_plan_rewrite`, on the
reasoning that an externally-built plan has no SQL constructs to lower.

That reasoning breaks the moment the relation resolver expands a VIEW into the plan: the
view body is arbitrary SQL, so it can carry `IN (<subquery>)`, INTERSECT, EXCEPT. The
rewriter is what *lowers* those into semi/anti joins — there is no physical operator for
an InSubQuery — so skipping it doesn't forfeit an optimisation, it makes any view
containing one fail at execution.
"""

import opteryx
import pytest

from opteryx.planner import execute_logical_plan
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.models import QueryTelemetry
from opteryx.third_party import sqloxide

# Self-referencing IN-subquery: the shape a view body expands to once the resolver
# splices it into an externally-supplied plan. The alias is required — a dataset
# referenced twice in one query must be aliased.
IN_SUBQUERY_SQL = (
    "SELECT name FROM $planets WHERE id IN (SELECT p2.id FROM $planets AS p2 WHERE p2.gravity > 9)"
)


def _raw_logical_plan(sql):
    """Parse + logical-plan only — deliberately stopping before the rewriter."""
    ast = sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx")
    result = do_logical_planning_phase(ast[0])
    return result[0] if isinstance(result, tuple) else next(iter(result))


def _node_types(plan):
    return [str(plan[nid].node_type).split(".")[-1] for nid in plan.nodes()]


def _names(morsels):
    out = []
    for morsel in morsels:
        morsel.materialize()
        out += morsel.column("name").to_pylist()
    return sorted(out)


def test_in_subquery_is_lowered_to_a_semi_join():
    # An un-lowered plan carries the InSubQuery inside the Filter's condition and has no
    # Join node at all. Nothing downstream can execute that.
    before = _raw_logical_plan(IN_SUBQUERY_SQL)
    assert "Join" not in _node_types(before)

    after = do_plan_rewrite(_raw_logical_plan(IN_SUBQUERY_SQL), QueryTelemetry("test"))

    join_types = [
        str(after[nid].type) for nid in after.nodes() if "Join" in str(after[nid].node_type)
    ]
    assert join_types == ["left semi"], f"expected a left semi join, got {join_types}"


def test_execute_logical_plan_runs_an_in_subquery():
    # The regression: without do_plan_rewrite this raised, because the InSubQuery survived
    # into the physical plan with no operator able to evaluate it.
    morsels, _ = execute_logical_plan(_raw_logical_plan(IN_SUBQUERY_SQL))

    assert _names(morsels) == ["Earth", "Jupiter", "Neptune"]


def test_logical_plan_path_agrees_with_the_sql_path():
    """The two entry points must not disagree about what a query means."""
    morsels, _ = execute_logical_plan(_raw_logical_plan(IN_SUBQUERY_SQL))
    via_logical_plan = _names(morsels)

    via_sql = _names(opteryx.session().execute_to_morsels(IN_SUBQUERY_SQL))

    assert via_logical_plan == via_sql


def test_plans_without_subqueries_are_unaffected():
    """The added stage is a no-op for the plans the OData service builds itself."""
    morsels, _ = execute_logical_plan(
        _raw_logical_plan("SELECT name FROM $planets WHERE gravity > 9")
    )

    assert _names(morsels) == ["Earth", "Jupiter", "Neptune"]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
