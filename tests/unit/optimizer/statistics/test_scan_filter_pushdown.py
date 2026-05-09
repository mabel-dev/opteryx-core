# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Phase 3 regression test: refresh applies leaf-local filter selectivity to
Scan.statistics.row_count.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


def _build_refreshed_plan(sql):
    """Parse SQL through bind phase, run refresh_statistics, return plan."""
    import uuid

    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_plan_rewrite(plan, ctes, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=query_id, telemetry=telemetry)
    return refresh_statistics(bound)


def _scan_row_counts(plan):
    """Map relation_name -> Scan.statistics.row_count for every Scan in plan."""
    from opteryx.planner.logical_planner import LogicalPlanStepType

    out = {}
    for nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            rel = getattr(node, "relation", None) or getattr(node, "alias", None)
            stats = getattr(node, "statistics", None)
            if rel and stats is not None:
                out[rel] = stats.row_count
    return out


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_filter_above_scan_reduces_row_count():
    plan = _build_refreshed_plan(
        "SELECT * FROM testdata.tpch_001.nation WHERE n_regionkey = 1"
    )
    rows = _scan_row_counts(plan)
    nation = rows.get("testdata.tpch_001.nation")
    assert nation is not None
    # Manifest count is 25; with a 1/NDV eq selectivity, expect a fraction of 25.
    assert nation < 25, f"expected filter to reduce nation rows; got {nation}"


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_no_filter_leaves_row_count_at_manifest():
    plan = _build_refreshed_plan("SELECT * FROM testdata.tpch_001.nation")
    rows = _scan_row_counts(plan)
    nation = rows.get("testdata.tpch_001.nation")
    assert nation == 25, f"expected unfiltered manifest count; got {nation}"
