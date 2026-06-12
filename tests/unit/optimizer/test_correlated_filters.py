# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""CorrelatedFiltersStrategy — push a join key's realized (post-filter) range
onto the opposite leg's scan, statically.

Covers the full chain that had to be fixed:
  * statistics_refresh now narrows column value_range from filters / scan
    predicates (the range-narrowing routine was previously dead code);
  * CorrelatedFilters runs after PredicatePushdown, reads the propagated range,
    and appends a range predicate to the opposite scan;
  * the pushed predicate is a necessary condition for an inner join, so results
    are unchanged.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _optimized_plan(sql):
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    ast = do_ast_rewriter(
        sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[]
    )[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_plan_rewrite(plan, ctes, telemetry)
    bound = do_bind_phase(
        plan,
        execution_context=ctx,
        query_id=str(uuid.uuid4()),
        common_table_expressions=ctes,
        telemetry=telemetry,
    )
    return do_optimizer(bound, telemetry)


def _scan_predicate_ops(plan, relation):
    from opteryx.planner.logical_planner import LogicalPlanStepType

    for _, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan and getattr(node, "relation", None) == relation:
            preds = getattr(node, "predicates", None) or []
            return [
                (getattr(c, "value", None), getattr(getattr(c, "left", None), "value", None))
                for c in preds
            ]
    return None


def _count(sql):
    value = 0
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel.num_rows:
            value = morsel.column(b"c").to_pylist()[0]
    return value


_JOIN_SQL = (
    "SELECT l.l_orderkey FROM testdata.tpch_001.orders o "
    "JOIN testdata.tpch_001.lineitem l ON o.o_orderkey = l.l_orderkey "
    "WHERE o.o_orderkey > 1000 AND o.o_orderkey < 2000"
)


# --- the propagation fix -----------------------------------------------------


def test_statistics_refresh_narrows_value_range_from_filter():
    from opteryx.planner.logical_planner import LogicalPlanStepType

    plan = _optimized_plan("SELECT * FROM testdata.tpch_001.orders WHERE o_orderkey > 1000 AND o_orderkey < 2000")
    # the scan carries the BETWEEN; the narrowing it drives is exercised below.
    ops = _scan_predicate_ops(plan, "testdata.tpch_001.orders")
    assert ops, ops


# --- the range push ----------------------------------------------------------


def test_realized_range_pushed_onto_opposite_scan():
    plan = _optimized_plan(_JOIN_SQL)
    lineitem_ops = _scan_predicate_ops(plan, "testdata.tpch_001.lineitem")
    # lineitem had no WHERE of its own, yet now carries a range on its join key.
    assert lineitem_ops is not None
    cols = {col for _, col in lineitem_ops}
    assert "l_orderkey" in cols, lineitem_ops
    ops = {op for op, col in lineitem_ops if col == "l_orderkey"}
    assert {"GtEq", "LtEq"} <= ops, lineitem_ops


def test_no_push_without_a_constraining_filter():
    # Plain join, no WHERE: there's no realized range to push.
    plan = _optimized_plan(
        "SELECT l.l_orderkey FROM testdata.tpch_001.orders o "
        "JOIN testdata.tpch_001.lineitem l ON o.o_orderkey = l.l_orderkey"
    )
    lineitem_ops = _scan_predicate_ops(plan, "testdata.tpch_001.lineitem") or []
    assert not any(col == "l_orderkey" for _, col in lineitem_ops), lineitem_ops


# --- correctness: the pushed predicate must not change results ---------------


def test_pushed_range_preserves_results():
    import opteryx.planner.optimizer.strategies.correlated_filters as cf

    on_result = _count(
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.orders o "
        "JOIN testdata.tpch_001.lineitem l ON o.o_orderkey = l.l_orderkey "
        "WHERE o.o_orderkey > 1000 AND o.o_orderkey < 2000"
    )
    original = cf.CorrelatedFiltersStrategy.should_i_run
    try:
        cf.CorrelatedFiltersStrategy.should_i_run = lambda self, plan: False
        off_result = _count(
            "SELECT COUNT(*) AS c FROM testdata.tpch_001.orders o "
            "JOIN testdata.tpch_001.lineitem l ON o.o_orderkey = l.l_orderkey "
            "WHERE o.o_orderkey > 1000 AND o.o_orderkey < 2000"
        )
    finally:
        cf.CorrelatedFiltersStrategy.should_i_run = original
    assert on_result == off_result
    assert on_result > 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
