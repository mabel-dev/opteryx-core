# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Estimate telemetry describes the FINAL plan (architect ruling 2026-09-24).

The optimizer refreshes statistics several times per query. Each refresh used
to record ``estimated_row_counts`` / ``predicate_estimates`` /
``join_estimates`` — formatting and costing every predicate — and each
overwrote the last, so all but one was wasted, and the survivor could describe
a plan that no longer existed: a query whose main plan reads only shared CTEs
(TPC-H Q11) reported the CTE BODY's estimates, because the body's optimize()
refresh ran last and the result-size guard, which finds no scans to trust in
the main plan, never refreshed it.

Now the optimizer's refreshes record nothing, and planning records one refresh
of the plan that actually runs.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.planner.plan_context import PlanContext

_TPCH = "testdata/tpch_001"


def _optimize(sql):
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry.detached()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    parsed = sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=str(uuid.uuid4()), telemetry=telemetry)
    return do_optimizer(bound, telemetry, PlanContext()), telemetry


@pytest.mark.skipif(not os.path.isdir(_TPCH), reason=f"{_TPCH} not populated")
def test_optimizer_refreshes_record_no_estimate_telemetry():
    """A join query makes the optimizer estimate, and says so — but records
    nothing itself; that is left to the one final-plan refresh."""
    optimized, telemetry = _optimize(
        "SELECT n_name, r_name FROM testdata.tpch_001.nation"
        " JOIN testdata.tpch_001.region ON n_regionkey = r_regionkey"
        " WHERE r_name = 'ASIA'"
    )

    assert optimized.statistics_estimated_by_optimizer is True
    assert "estimated_row_counts" not in telemetry._reading
    assert "predicate_estimates" not in telemetry._reading
    assert "join_estimates" not in telemetry._reading


@pytest.mark.skipif(not os.path.isdir(_TPCH), reason=f"{_TPCH} not populated")
def test_cte_only_main_plan_reports_its_own_estimates():
    """TPC-H Q11's shape: the main plan reads only shared CTEs, so the
    result-size guard does not refresh it. Its estimates must be the main
    plan's (MaterializedCteRef leaves), not the CTE body's (Scans)."""
    session = opteryx.session()
    session.plan(
        "WITH t AS (SELECT n_regionkey, COUNT(*) AS c FROM testdata.tpch_001.nation"
        " GROUP BY n_regionkey), s AS (SELECT SUM(c) AS total FROM t)"
        " SELECT n_regionkey, c FROM t JOIN s WHERE c > total * 0.1"
    )
    entries = session._telemetry._reading["estimated_row_counts"]
    node_types = {e["node_type"] for e in entries}

    assert "MaterializedCteRef" in node_types
    assert "Scan" not in node_types
