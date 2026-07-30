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


def _build_optimized_and_refreshed_plan(sql):
    """Parse SQL through the FULL optimizer (including PredicatePushdown), then
    run refresh_statistics, return plan.

    Unlike `_build_refreshed_plan`, this reaches statistics_refresh the same
    way the real query path does (see planner/__init__.py:
    do_optimizer -> check_estimated_result_size -> refresh_statistics), so a
    selective single-table equality predicate is no longer a Filter node by
    the time refresh runs -- PredicatePushdownStrategy has already removed it
    and attached the condition to Scan.predicates instead.
    """
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
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
    optimized = do_optimizer(bound, telemetry)
    return refresh_statistics(optimized)


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


# ── post-pushdown: the predicate's Filter node is gone by the time refresh runs ──
#
# Regression for the bug reported 2026-07-29: `WHERE project = 'x'` against a
# billion-row table estimated ~the full table (no reduction at all), because
# PredicatePushdownStrategy had already deleted the Filter node and moved the
# condition onto Scan.predicates before refresh_statistics ran. The leaf-local
# walk in _scan_stats only finds conjuncts still attached to a Filter node, so
# a pushed-down predicate silently contributed zero selectivity to row_count.


def _find_scan(plan):
    from opteryx.planner.logical_planner import LogicalPlanStepType

    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            return node
    return None


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_pushed_down_equality_predicate_still_reduces_row_count():
    plan = _build_optimized_and_refreshed_plan(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    )
    scan = _find_scan(plan)
    assert scan is not None, "expected a Scan node"
    # Prove this test is actually exercising the post-pushdown path, not a
    # Filter node that happened to survive optimization.
    assert scan.predicates, (
        "expected n_name = 'BRAZIL' to have been pushed onto Scan.predicates "
        "-- if this is empty, the optimizer stopped pushing this predicate and "
        "this test is no longer reproducing the reported bug"
    )
    assert scan.statistics.row_count < 25, (
        f"pushed-down equality predicate did not reduce the estimate; "
        f"got {scan.statistics.row_count} (full manifest count is 25)"
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_distinct_over_pushed_down_predicate_is_not_full_table():
    plan = _build_optimized_and_refreshed_plan(
        "SELECT DISTINCT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    )
    from opteryx.planner.logical_planner import LogicalPlanStepType

    exit_row_count = None
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Exit:
            exit_row_count = node.statistics.row_count
    assert exit_row_count is not None, "expected an Exit node with statistics"
    # Only one nation is named BRAZIL; the estimate must reflect that it was
    # filtered before DISTINCT, not the unfiltered 25-row table.
    assert exit_row_count < 25, (
        f"DISTINCT over a selective pushed-down filter estimated {exit_row_count} "
        f"rows -- expected it to inherit the filtered (small) row count"
    )


# ── DISTINCT's NDV estimate must be scoped to its own output columns ────────────
#
# Second bug found in the same investigation: _distinct_stats multiplied NDVs
# across every column still attached to RelationStatistics (Project/pass-through
# nodes don't narrow it), not just the columns actually being distinct-ed. That
# product overflows and gets capped straight back at the input row count --
# masked in the reported incident because the pushed-predicate bug above left
# input_rows wrong too, but a real gap on its own: a `SELECT DISTINCT` over a
# genuinely low-cardinality column should estimate near its own NDV, not
# whatever unrelated high-cardinality columns (ids, comments, ...) happen to
# still be attached to the underlying relation's stats.


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_distinct_ndv_scoped_to_selected_column_not_whole_relation():
    # nation has 25 rows; n_regionkey has 5 distinct values, n_comment is
    # (near-)unique per row. Distinct-ing on just n_regionkey must not be
    # dragged up towards 25 by n_comment's NDV still sitting in the relation's
    # column stats.
    plan = _build_optimized_and_refreshed_plan(
        "SELECT DISTINCT n_regionkey FROM testdata.tpch_001.nation"
    )
    from opteryx.planner.logical_planner import LogicalPlanStepType

    exit_row_count = None
    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Exit:
            exit_row_count = node.statistics.row_count
    assert exit_row_count is not None, "expected an Exit node with statistics"
    assert exit_row_count < 25, (
        f"DISTINCT on a single low-cardinality column estimated {exit_row_count} "
        f"rows -- expected it near n_regionkey's own NDV (5), not dragged up by "
        f"unrelated columns still attached to the relation's stats"
    )
