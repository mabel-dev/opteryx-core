# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Plan-shape ("golden") tests for optimizer decisions.

These pin the optimizer's *observable* rewrites at the logical-plan level, so a
rule that silently stops firing (or starts firing differently) fails CI instead
of only changing performance. The result batteries validate query *results*;
these validate the plan that produces them.

Assertions are predicate-style (node present/absent, attribute equals) rather
than full-text snapshots: structural predicates survive cosmetic plan churn and
say exactly which decision regressed when they fail.

Each test names the strategy it guards. If you intentionally change a rule and a
test here fails, update the assertion in the same change and say why.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.relation_resolver import do_resolve_relations
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _optimized_plan(sql: str):
    """Parse, rewrite, bind and fully optimize ``sql``; return the logical plan.

    Mirrors the optimizer entry path in opteryx/planner/__init__.py so the plan
    we assert on is the same one the physical planner consumes.
    """
    telemetry = QueryTelemetry.detached()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(
        plan,
        execution_context=ctx,
        query_id=query_id,
        telemetry=telemetry,
    )
    return do_optimizer(bound, telemetry)


def _nodes(plan, step_type):
    return [n for _, n in plan.nodes(True) if n.node_type == step_type]


def _step_counts(plan):
    counts: dict = {}
    for _, n in plan.nodes(True):
        counts[n.node_type] = counts.get(n.node_type, 0) + 1
    return counts


def _join_types(plan):
    return [getattr(n, "type", None) for n in _nodes(plan, LogicalPlanStepType.Join)]


def _scan_relations(plan):
    return [getattr(n, "relation", None) for n in _nodes(plan, LogicalPlanStepType.Scan)]


# ---------------------------------------------------------------------------
# StatisticsOnlyResponseStrategy
# ---------------------------------------------------------------------------


def test_count_star_answered_from_statistics():
    # COUNT(*) with no filter/group/join is answered from manifest record counts:
    # the real Scan is rewritten to the virtual `$one_row` and the aggregate is
    # replaced by a literal projection — zero data is read.
    plan = _optimized_plan("SELECT COUNT(*) FROM testdata.tpch_001.lineitem")
    counts = _step_counts(plan)
    assert counts.get(LogicalPlanStepType.Join, 0) == 0
    assert counts.get(LogicalPlanStepType.Filter, 0) == 0
    assert "$one_row" in _scan_relations(plan), _scan_relations(plan)


# ---------------------------------------------------------------------------
# PredicatePushdownStrategy
# ---------------------------------------------------------------------------


def test_predicate_pushed_into_scan():
    # A simple comparison filter is pushed onto the Scan and the standalone
    # Filter node disappears.
    plan = _optimized_plan(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1"
    )
    assert _step_counts(plan).get(LogicalPlanStepType.Filter, 0) == 0
    scans = _nodes(plan, LogicalPlanStepType.Scan)
    assert any(getattr(s, "predicates", None) for s in scans), "scan carries no predicates"


# ---------------------------------------------------------------------------
# CrossJoinFilterPushdownStrategy
# ---------------------------------------------------------------------------


def test_cross_join_with_equality_becomes_inner():
    # CROSS JOIN with an equi-predicate in WHERE is converted to an INNER JOIN,
    # avoiding cartesian materialisation.
    plan = _optimized_plan(
        "SELECT p.name FROM testdata.planets p "
        "CROSS JOIN testdata.satellites s WHERE p.id = s.planetId"
    )
    types = _join_types(plan)
    assert types == ["inner"], types


# ---------------------------------------------------------------------------
# JoinRewriteStrategy  (outer join + IS NULL  ->  anti join)
# ---------------------------------------------------------------------------


def test_outer_join_is_null_becomes_anti_join():
    plan = _optimized_plan(
        "SELECT p.name FROM testdata.planets p "
        "LEFT JOIN testdata.satellites s ON p.id = s.planetId "
        "WHERE s.planetId IS NULL"
    )
    types = _join_types(plan)
    assert types == ["left anti"], types


# ---------------------------------------------------------------------------
# Semi-join planning  (IN (subquery)  ->  left semi)
# ---------------------------------------------------------------------------


def test_in_subquery_becomes_semi_join():
    plan = _optimized_plan(
        "SELECT name FROM $planets WHERE id IN (SELECT planetId FROM testdata.satellites)"
    )
    types = _join_types(plan)
    assert types == ["left semi"], types


# ---------------------------------------------------------------------------
# OperatorFusionStrategy  (Order + Limit  ->  HeapSort)
# ---------------------------------------------------------------------------


def test_order_by_limit_fuses_to_heap_sort():
    plan = _optimized_plan("SELECT name FROM $planets ORDER BY id DESC LIMIT 3")
    counts = _step_counts(plan)
    assert counts.get(LogicalPlanStepType.HeapSort, 0) == 1, counts
    assert counts.get(LogicalPlanStepType.Order, 0) == 0, counts
    assert counts.get(LogicalPlanStepType.Limit, 0) == 0, counts


# ---------------------------------------------------------------------------
# LimitPushdownStrategy  (WP-1: limit must stay ABOVE a row-multiplying join)
# ---------------------------------------------------------------------------


def test_limit_over_outer_join_stays_above_join():
    # Regression guard for WP-1: a LIMIT over an outer join must not be pushed
    # below the join nor absorbed into a scan. Exactly one Limit node survives,
    # its child is the Join, and no Scan carries a pushed limit.
    plan = _optimized_plan(
        "SELECT p.name FROM testdata.planets p "
        "LEFT JOIN testdata.satellites s ON p.id = s.planetId LIMIT 5"
    )
    limits = _nodes(plan, LogicalPlanStepType.Limit)
    assert len(limits) == 1, _step_counts(plan)

    # the Limit's single child (ingoing edge) is the Join
    limit_nid = next(
        nid for nid, n in plan.nodes(True) if n.node_type == LogicalPlanStepType.Limit
    )
    child_types = [plan[cid].node_type for cid, _, _ in plan.ingoing_edges(limit_nid)]
    assert child_types == [LogicalPlanStepType.Join], child_types

    scan_limits = [getattr(s, "limit", None) for s in _nodes(plan, LogicalPlanStepType.Scan)]
    assert all(lim is None for lim in scan_limits), scan_limits


def test_limit_over_bare_scan_is_pushed_into_scan():
    # The valid pushdown still happens: a LIMIT directly over a single Scan is
    # absorbed into the Scan (limit set) and the Limit node is removed.
    plan = _optimized_plan("SELECT n_name FROM testdata.tpch_001.nation LIMIT 5")
    assert _step_counts(plan).get(LogicalPlanStepType.Limit, 0) == 0, _step_counts(plan)
    scans = _nodes(plan, LogicalPlanStepType.Scan)
    assert any(getattr(s, "limit", None) == 5 for s in scans), [
        getattr(s, "limit", None) for s in scans
    ]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
