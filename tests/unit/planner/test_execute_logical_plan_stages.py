"""`execute_logical_plan` must run the same planning stages as `query_planner`.

`execute_logical_plan` is the entry point for callers that build a LogicalPlan directly
instead of submitting SQL text (the OData service). It skipped the lowering stages, on the
reasoning that an externally-built plan has no SQL constructs to lower.

That reasoning breaks the moment the relation resolver expands a VIEW into the plan: the
view body is arbitrary SQL, so it can carry `IN (<subquery>)`, INTERSECT, EXCEPT. There is
no physical operator for an InSubQuery, so skipping those stages doesn't forfeit an
optimisation, it makes any view containing one fail at execution.

These tests deliberately assert on OUTCOME (does it execute, does it agree with the SQL
path) rather than on which stage does the lowering. That split is not stable: INTERSECT and
EXCEPT are lowered by `do_plan_rewrite`, while `IN`/`EXISTS`/scalar subqueries moved to the
OPTIMIZER when decorrelation went post-bind (2026-07-26) and the rewriter's decorrelation
strategy was deleted. A test pinning `do_plan_rewrite` to emitting the semi join outlived
that move and was removed rather than updated — `test_execute_logical_plan_runs_an_in_subquery`
below is the regression it was really protecting.
"""

import opteryx
import pytest

from opteryx.planner import execute_logical_plan
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.sql_rewriter import do_sql_rewrite
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


def _names(morsels):
    out = []
    for morsel in morsels:
        morsel.materialize()
        out += morsel.column("name").to_pylist()
    return sorted(out)


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
