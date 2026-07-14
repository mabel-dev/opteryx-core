"""Phase 6 regression test: _filter_stats does not double-count selectivity
for conjuncts already folded into Scan.statistics.row_count by Phase 3's
upward-walk in _scan_stats.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


def _build_refreshed_plan(sql):
    """Parse SQL through bind phase, run refresh_statistics, return plan."""
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=query_id, telemetry=telemetry)
    return refresh_statistics(bound)


def _stats_by_node_type(plan):
    """Map node_type name -> list of statistics.row_count for that type."""
    out = {}
    for nid, node in plan.nodes(True):
        st = getattr(node, "statistics", None)
        if st is None:
            continue
        out.setdefault(node.node_type.name, []).append(st.row_count)
    return out


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_filter_above_scan_does_not_double_count():
    """Filter directly above a Scan with a single-relation predicate must
    not reduce row count below the Scan's filtered row count."""
    plan = _build_refreshed_plan(
        "SELECT * FROM testdata.tpch_001.nation WHERE n_regionkey = 1"
    )
    by_type = _stats_by_node_type(plan)
    scan_rows = by_type.get("Scan", [])
    filter_rows = by_type.get("Filter", [])
    assert scan_rows, "expected at least one Scan node with statistics"
    assert filter_rows, "expected at least one Filter node with statistics"
    # The Filter sits directly above the Scan and its conjunct is folded
    # into the Scan's row_count by Phase 3. The Filter must therefore
    # propagate the same row count, not multiply by selectivity again.
    assert min(filter_rows) >= min(scan_rows), (
        f"Filter row count {min(filter_rows)} dropped below Scan row count "
        f"{min(scan_rows)} — _filter_stats is double-counting selectivity"
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_no_filter_above_scan_unchanged():
    """Sanity: with no filter, Filter and Scan stats should both equal the
    manifest count (no Filter node may exist; just verify no regression)."""
    plan = _build_refreshed_plan("SELECT * FROM testdata.tpch_001.nation")
    by_type = _stats_by_node_type(plan)
    scan_rows = by_type.get("Scan", [])
    assert scan_rows, "expected a Scan node with statistics"
    assert min(scan_rows) == 25, f"expected manifest count 25; got {min(scan_rows)}"
