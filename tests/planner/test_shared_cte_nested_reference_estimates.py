"""Every reference to a shared CTE must carry the body's estimate — including
references that are NOT in the top-level plan graph.

`shared_cte._refs_of` used to find references with a FLAT scan of each consumer
plan. References routinely live in an EMBEDDED plan instead: an expression
subquery holds a whole LogicalPlan off to the side, and at the point the
optimizer coordinates shared bodies those side plans have not yet been spliced
into the main graph. The flat scan therefore yielded NOTHING for the TPC-DS Q14
shape, and both consumers of `_refs_of` silently no-opped:

  - `stamp_reference_estimates` stamped no `cte_statistics`, so every reference
    fell back to statistics_refresh's UNKNOWN stand-in (1,000,000 rows) and that
    stand-in multiplied through the joins above it;
  - `coordinate_shared_cte` gates on `if refs:`, so shared-body predicate
    pushdown and projection narrowing were skipped entirely.

The fix traverses the plan FOREST — the resolver's own `iter_plan_forest`, the
same rule it counts references with when it decides to share the body at all.

These shapes are the reduction of Q14: references reached only through an
expression subquery (a scalar select-list subquery, an OR-ed EXISTS, and Q14's
own IN-plus-scalar-subquery-under-UNION-ALL form).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.models import ExecutionContext
from opteryx.models import QueryTelemetry
from opteryx.planner import bind_statement
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.relation_resolver import iter_plan_forest
from opteryx.utils import random_string

_SCALAR_SELECT_LIST = """
WITH c AS (SELECT id, name FROM $planets)
SELECT (SELECT COUNT(*) FROM c WHERE id < 5) AS a,
       (SELECT COUNT(*) FROM c WHERE id > 7) AS b
  FROM $planets p LIMIT 1
"""

_OR_EXISTS = """
WITH c AS (SELECT id, name FROM $planets)
SELECT p.name FROM $planets p
 WHERE EXISTS (SELECT 1 FROM c WHERE c.id = p.id AND c.id < 5)
    OR EXISTS (SELECT 1 FROM c WHERE c.id = p.id AND c.id > 7)
"""

# The Q14 form: three UNION ALL branches, each reading one CTE through an IN
# subquery and another through a scalar subquery.
_Q14_SHAPE = """
WITH x AS (SELECT id FROM $planets), av AS (SELECT AVG(gravity) AS g FROM $planets)
SELECT 's' AS ch, COUNT(*) AS n FROM $planets p
 WHERE p.id IN (SELECT id FROM x) AND p.gravity > (SELECT g FROM av)
UNION ALL
SELECT 'c', COUNT(*) FROM $planets p
 WHERE p.id IN (SELECT id FROM x) AND p.gravity > (SELECT g FROM av)
UNION ALL
SELECT 'w', COUNT(*) FROM $planets p
 WHERE p.id IN (SELECT id FROM x) AND p.gravity > (SELECT g FROM av)
"""


def _optimized(sql):
    """(plan, shared_ctes) for `sql`, taken through bind and the optimizer —
    the same two calls `query_planner` makes, stopping before physical planning
    because the property under test lives on the logical reference node."""
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
    shared = getattr(bound, "shared_ctes", None) or {}
    plan = do_optimizer(bound, telemetry, scan_stats_cache={}, shared_ctes=shared)
    return plan, (getattr(plan, "shared_ctes", None) or shared)


def _references(plan, shared):
    for candidate in [plan, *shared.values()]:
        for member in iter_plan_forest(candidate):
            for _nid, node in member.nodes(True):
                if node.node_type == LogicalPlanStepType.MaterializedCteRef:
                    yield node


@pytest.mark.parametrize(
    "sql, expected_bodies, expected_refs",
    [
        (_SCALAR_SELECT_LIST, 1, 2),
        (_OR_EXISTS, 1, 2),
        (_Q14_SHAPE, 2, 6),
    ],
    ids=["scalar_select_list", "or_exists", "q14_shape"],
)
def test_every_nested_cte_reference_is_stamped(sql, expected_bodies, expected_refs):
    plan, shared = _optimized(sql)
    assert len(shared) == expected_bodies, (
        f"expected {expected_bodies} shared CTE bodies, got {sorted(shared)} — the "
        "shape under test no longer shares its CTEs, so it no longer covers the defect"
    )
    references = list(_references(plan, shared))
    assert len(references) == expected_refs, (
        f"expected {expected_refs} MaterializedCteRef leaves, got {len(references)}"
    )
    unstamped = [n for n in references if n.properties.get("cte_statistics") is None]
    assert not unstamped, (
        f"{len(unstamped)} of {len(references)} references carry no cte_statistics — "
        "_refs_of is not reaching references held in embedded (expression-subquery) "
        "plans, so every one of them falls to the 1,000,000-row UNKNOWN stand-in"
    )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
