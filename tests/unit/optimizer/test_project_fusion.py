# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""ProjectFusionStrategy: fuse two consecutive Project nodes into one.

Plan-shape assertions follow the pattern in test_optimizer_plan_snapshots.py;
result assertions run the SQL end-to-end and compare against an equivalent
single-layer query to prove fusion doesn't change the answer.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
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


def _optimized_plan(sql: str):
    """Parse, rewrite, bind and fully optimize ``sql``; return the logical plan.

    Mirrors the helper in test_optimizer_plan_snapshots.py (duplicated rather
    than imported so this file doesn't depend on sibling test collection order).
    """
    telemetry = QueryTelemetry()
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


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        if m.num_rows:
            names = [n.decode() if isinstance(n, bytes) else n for n in m.column_names]
            cols = {n: m.column(n.encode()).to_pylist() for n in names}
            for i in range(m.num_rows):
                out.append({n: cols[n][i] for n in names})
    return out


# ---------------------------------------------------------------------------
# Nested subquery, each layer adding a computed column
# ---------------------------------------------------------------------------


def test_nested_projects_fuse_to_one():
    plan = _optimized_plan(
        "SELECT id, name, total, CEILING(total * 0.1) AS billed FROM "
        "(SELECT id, name, mass * 2 AS total FROM $planets) AS inner_q"
    )
    counts = _step_counts(plan)
    assert counts.get(LogicalPlanStepType.Project, 0) == 1, counts


def test_nested_projects_fusion_preserves_results():
    fused = _rows(
        "SELECT id, name, total, CEILING(total * 0.1) AS billed FROM "
        "(SELECT id, name, mass * 2 AS total FROM $planets) AS inner_q "
        "ORDER BY id"
    )
    flat = _rows(
        "SELECT id, name, mass * 2 AS total, CEILING((mass * 2) * 0.1) AS billed "
        "FROM $planets ORDER BY id"
    )
    assert fused == flat


# ---------------------------------------------------------------------------
# Shared computed expression referenced 2+ times -> hoisted, not duplicated
# ---------------------------------------------------------------------------


def test_multiply_referenced_expression_is_hoisted_not_fused_away():
    plan = _optimized_plan(
        "SELECT id, expensive + 1 AS a, expensive * 2 AS b, expensive AS c FROM "
        "(SELECT id, mass * 1.5 AS expensive FROM $planets) AS inner_q"
    )
    counts = _step_counts(plan)
    assert counts.get(LogicalPlanStepType.Project, 0) == 1, counts
    projects = _nodes(plan, LogicalPlanStepType.Project)
    assert getattr(projects[0], "hoisted_columns", None), "expected a hoisted column"


def test_multiply_referenced_expression_fusion_preserves_results():
    fused = _rows(
        "SELECT id, expensive + 1 AS a, expensive * 2 AS b, expensive AS c FROM "
        "(SELECT id, mass * 1.5 AS expensive FROM $planets) AS inner_q "
        "ORDER BY id"
    )
    flat = _rows(
        "SELECT id, (mass * 1.5) + 1 AS a, (mass * 1.5) * 2 AS b, mass * 1.5 AS c "
        "FROM $planets ORDER BY id"
    )
    assert fused == flat


# ---------------------------------------------------------------------------
# Trivial rename chain -> fuses, no hoisting needed
# ---------------------------------------------------------------------------


def test_trivial_rename_chain_fuses_without_hoisting():
    plan = _optimized_plan(
        "SELECT c AS z FROM (SELECT b AS c FROM (SELECT id AS b FROM $planets)) AS x"
    )
    counts = _step_counts(plan)
    assert counts.get(LogicalPlanStepType.Project, 0) == 1, counts
    projects = _nodes(plan, LogicalPlanStepType.Project)
    assert not getattr(projects[0], "hoisted_columns", None)


def test_trivial_rename_chain_fusion_preserves_results():
    fused = _rows(
        "SELECT c AS z FROM (SELECT b AS c FROM (SELECT id AS b FROM $planets)) AS x "
        "ORDER BY z"
    )
    flat = _rows("SELECT id AS z FROM $planets ORDER BY z")
    assert fused == flat


# ---------------------------------------------------------------------------
# Bail case: a Project feeding more than one consumer must not be fused away
# ---------------------------------------------------------------------------
#
# A full SQL round-trip isn't a reliable way to exercise this: earlier stages
# (e.g. projection pushdown collapsing a CTE's plain-column SELECT straight
# onto the Scan) can make the Project nodes disappear before fan-out is even
# relevant, for reasons unrelated to ProjectFusionStrategy. Testing the
# strategy directly against a hand-built plan pins down the fan-out guard
# itself instead of an incidental plan shape from unrelated strategies.


from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext
from opteryx.planner.optimizer.strategies.project_fusion import ProjectFusionStrategy


def test_fanned_out_project_is_not_fused():
    # lower -> {upper_a, upper_b}: lower has two consumers, so fusing it into
    # either would duplicate lower's work for the other. Columns are left empty
    # since the fan-out check (len(outgoing edges) == 1) must short-circuit
    # before any column/identity resolution is attempted.
    plan = LogicalPlan()
    for nid in ("lower", "upper_a", "upper_b"):
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        node.columns = []
        plan.add_node(nid, node)
    plan.add_edge("lower", "upper_a")
    plan.add_edge("lower", "upper_b")

    strategy = ProjectFusionStrategy(QueryTelemetry())
    context = OptimizerContext(plan)
    context.optimized_plan = plan.copy()
    context.node_id = "lower"

    strategy.visit(plan["lower"], context)

    assert set(nid for nid, _ in context.optimized_plan.nodes(True)) == {
        "lower",
        "upper_a",
        "upper_b",
    }


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
