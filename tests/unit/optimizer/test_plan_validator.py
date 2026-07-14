# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-3: debug-mode plan invariant checker.

Unit tests for ``validate_plan`` over hand-built plans, plus an end-to-end check
that a real optimized plan passes every invariant (so the checker won't false-
positive when the ``VALIDATE_OPTIMIZER_PLANS`` flag is enabled in CI).
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.exceptions import InvalidInternalStateError
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.plan_validator import validate_plan


def _node(step_type):
    n = LogicalPlanNode(node_type=step_type)
    n.columns = []
    return n


def _valid_plan():
    plan = LogicalPlan()
    plan.add_node("scan", _node(LogicalPlanStepType.Scan))
    plan.add_node("exit", _node(LogicalPlanStepType.Exit))
    plan.add_edge("scan", "exit")
    return plan


# --- positive ---------------------------------------------------------------


def test_valid_plan_passes():
    validate_plan(_valid_plan())  # must not raise


def test_single_node_plan_passes():
    plan = LogicalPlan()
    plan.add_node("only", _node(LogicalPlanStepType.Exit))
    validate_plan(plan)


# --- structural violations --------------------------------------------------


def test_dangling_edge_target_raises():
    plan = _valid_plan()
    plan.add_edge("exit", "ghost")  # 'ghost' is not a node
    with pytest.raises(InvalidInternalStateError) as exc:
        validate_plan(plan)
    assert "ghost" in str(exc.value)


def test_two_roots_raises():
    # scan feeds two sink nodes: two exit points.
    plan = LogicalPlan()
    plan.add_node("scan", _node(LogicalPlanStepType.Scan))
    plan.add_node("b", _node(LogicalPlanStepType.Exit))
    plan.add_node("c", _node(LogicalPlanStepType.Exit))
    plan.add_edge("scan", "b")
    plan.add_edge("scan", "c")
    with pytest.raises(InvalidInternalStateError) as exc:
        validate_plan(plan)
    assert "exit point" in str(exc.value)


def test_orphan_node_raises():
    # A disconnected node is invisible to get_exit_points but is still corruption.
    plan = _valid_plan()
    plan.add_node("orphan", _node(LogicalPlanStepType.Project))
    with pytest.raises(InvalidInternalStateError) as exc:
        validate_plan(plan)
    assert "orphan" in str(exc.value)


def test_where_label_is_included_in_message():
    plan = _valid_plan()
    plan.add_edge("exit", "ghost")
    with pytest.raises(InvalidInternalStateError) as exc:
        validate_plan(plan, where="SomeStrategy")
    assert "SomeStrategy" in str(exc.value)


# --- end-to-end: real plan satisfies every invariant ------------------------


def test_real_optimized_plan_is_valid():
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    queries = [
        "SELECT COUNT(*) FROM testdata.tpch_001.lineitem",
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1",
        "SELECT p.name FROM testdata.planets p "
        "LEFT JOIN testdata.satellites s ON p.id = s.planetId LIMIT 5",
        "SELECT name FROM $planets ORDER BY id DESC LIMIT 3",
    ]
    for sql in queries:
        telemetry = QueryTelemetry()
        ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
        ast = do_ast_rewriter(
            sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[]
        )[0]
        plan, _, ctes = do_logical_planning_phase(ast)
        plan = do_resolve_relations(plan, ctes, telemetry)
        plan = do_plan_rewrite(plan, telemetry)
        bound = do_bind_phase(
            plan,
            execution_context=ctx,
            query_id=str(uuid.uuid4()),
            telemetry=telemetry,
        )
        optimized = do_optimizer(bound, telemetry)
        validate_plan(optimized, where="end-to-end")  # must not raise


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
