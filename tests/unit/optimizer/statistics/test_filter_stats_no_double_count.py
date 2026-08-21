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

    telemetry = QueryTelemetry.detached()
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
def test_filter_above_cross_join_applies_selectivity_once():
    """A single-relation conjunct above a CROSS join is folded into its Scan
    by the upward walk (cross joins are transparent to it). The Filter must
    then skip that conjunct — before the fold registry, _filter_stats
    re-derived "already folded" with a downward walk that stopped at ANY
    join, saw nothing folded, and applied the same selectivity a second time.
    """
    plan = _build_refreshed_plan(
        "SELECT * FROM testdata.tpch_001.nation, testdata.tpch_001.region "
        "WHERE n_regionkey = 1"
    )
    by_type = _stats_by_node_type(plan)
    scan_rows = by_type.get("Scan", [])
    join_rows = by_type.get("Join", [])
    filter_rows = by_type.get("Filter", [])
    assert len(scan_rows) == 2, f"expected two Scan nodes; got {scan_rows}"
    assert join_rows and filter_rows, "expected Join and Filter statistics"
    # The conjunct folded into the nation Scan (25 rows -> fewer); region (5
    # rows) is untouched.
    assert min(scan_rows) < 25, f"conjunct was not folded into a Scan: {scan_rows}"
    assert 5 in scan_rows, f"region scan should be unfiltered at 5 rows: {scan_rows}"
    # The cross join is the product of the (already filtered) scans, and the
    # Filter adds NOTHING on top — its one conjunct is already in the fold
    # registry. filter < join means the selectivity was applied twice.
    assert min(filter_rows) == min(join_rows), (
        f"Filter row count {min(filter_rows)} != Join row count {min(join_rows)} "
        "— the folded conjunct's selectivity was applied a second time"
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_self_join_filter_folds_into_one_scan_only():
    """Two Scans of the same relation both name-match a conjunct on that
    relation. Before fold claiming was keyed to the scan NODE, both folded it
    and its selectivity was squared. Exactly one Scan may claim the fold; the
    Filter then applies nothing further.
    """
    plan = _build_refreshed_plan(
        "SELECT nation.n_name FROM testdata.tpch_001.nation "
        "CROSS JOIN testdata.tpch_001.nation AS n2 "
        "WHERE nation.n_regionkey = 1"
    )
    by_type = _stats_by_node_type(plan)
    scan_rows = by_type.get("Scan", [])
    join_rows = by_type.get("Join", [])
    filter_rows = by_type.get("Filter", [])
    assert len(scan_rows) == 2, f"expected two Scan nodes; got {scan_rows}"
    assert join_rows and filter_rows, "expected Join and Filter statistics"
    # One scan folds the conjunct; its twin must keep the full 25-row count.
    assert max(scan_rows) == 25, (
        f"both self-join Scans folded the same conjunct (rows {scan_rows}) "
        "— selectivity squared"
    )
    assert min(scan_rows) < 25, f"conjunct was not folded into either Scan: {scan_rows}"
    # And the Filter must not apply it a third time.
    assert min(filter_rows) == min(join_rows), (
        f"Filter row count {min(filter_rows)} != Join row count {min(join_rows)} "
        "— the folded conjunct's selectivity was re-applied"
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
