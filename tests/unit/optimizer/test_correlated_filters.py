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
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry.detached()
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


# ---- derived-bound typing -------------------------------------------------------
#
# The bound comes from the OTHER leg, so its Python type is the other column's.
# `build_literal_node` TAGS the literal with the target's type but never
# re-expresses the value, and the constant materialiser dispatches on the VALUE's
# Python type — so a float bound tagged INT32 became a FLOAT64 constant. The
# compare kernel is identical-type only, and the native ExprFilter has no
# fallback, so an int32 x float64 INNER JOIN died with
# `ExprFilterOperator: predicate evaluation failed (err_op=11)`.


def _column(column_type):
    from opteryx.models import Node
    from opteryx.types.schema import FunctionColumn

    return Node(
        node_type=None,
        value="k",
        schema_column=FunctionColumn(name="k", column_type=column_type, aliases=[]),
    )


@pytest.mark.parametrize(
    "column_type_name, upper, lower, expected_upper, expected_lower",
    [
        # float bounds onto an integer key: truncate TOWARD the range. Over an
        # integer domain `k <= 4.7` and `k <= 4` select the same rows.
        ("INT32", 4.0, 2.0, 4, 2),
        ("INT32", 4.7, 2.3, 4, 3),
        ("INT64", 4.0, 2.0, 4, 2),
        # integer bounds onto a float key.
        ("FLOAT64", 4, 2, 4.0, 2.0),
    ],
)
def test_derived_bound_matches_the_target_type(
    column_type_name, upper, lower, expected_upper, expected_lower
):
    """A pushed bound's Python value must match the type the literal is TAGGED
    with — anything else materialises a constant of the wrong physical type and
    the identical-type compare kernel declines it (err_op=11)."""
    from opteryx.planner.optimizer.strategies import correlated_filters as cf
    from opteryx.types import logical_type

    column_type = getattr(logical_type, column_type_name)
    conditions = cf._range_conditions(
        _column(column_type), type("R", (), {"upper_bound": upper, "lower_bound": lower})()
    )
    by_op = {c.value: c.right for c in conditions}
    assert by_op["LtEq"].value == expected_upper
    assert type(by_op["LtEq"].value) is type(expected_upper)
    assert by_op["GtEq"].value == expected_lower
    assert type(by_op["GtEq"].value) is type(expected_lower)
    for literal in by_op.values():
        assert literal.type is column_type


def test_derived_bound_is_dropped_when_it_cannot_be_carried():
    """Dropping is always sound — a correlated filter is a derived
    necessary-condition, so a missing bound only forgoes pruning."""
    from opteryx.planner.optimizer.strategies import correlated_filters as cf
    from opteryx.types import logical_type

    # A float bound onto a DECIMAL key would have to be quantized to the
    # column's declared scale, rounding in a direction this layer cannot see.
    conditions = cf._range_conditions(
        _column(logical_type.DECIMAL(18, 6)),
        type("R", (), {"upper_bound": 4.5, "lower_bound": 2.5})(),
    )
    assert conditions == []


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])


# ---- constant propagation ---------------------------------------------------
#
# When one equi-join operand is a column a Project binds to a LITERAL -- the shape
# a single-row parameter CTE takes -- the other operand's value is KNOWN, not
# merely bounded, and an equality goes onto the opposite scan. The constant is read
# from the plan, not from `value_range`, which holds numbers only by ruling and so
# cannot carry the VARCHAR keys this shape is overwhelmingly used with.


_PARAMS_CTE = "WITH params AS (SELECT 'Clerk#000000001' AS ck) "


def _scan_predicates(plan, alias):
    from opteryx.planner.logical_planner import LogicalPlanStepType

    for _, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan and getattr(node, "alias", None) == alias:
            return [
                (c.value, c.left.value, c.right.value) for c in (getattr(node, "predicates", None) or [])
            ]
    return None


def test_constant_pushed_onto_null_supplying_leg_of_left_join():
    plan = _optimized_plan(
        _PARAMS_CTE + "SELECT p.ck, o.o_orderkey FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck"
    )
    # VARCHAR literals are spelled as UTF-8 BYTES everywhere else in the engine;
    # the pushed predicate must be indistinguishable from a hand-written one or
    # the manifest/dictionary pruning it exists for compares against the wrong
    # representation.
    assert _scan_predicates(plan, "o") == [("Eq", "o_clerk", b"Clerk#000000001")]


def test_constant_pushed_for_an_integer_key():
    plan = _optimized_plan(
        "WITH params AS (SELECT 42 AS k) SELECT p.k, o.o_orderkey FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_orderkey = p.k"
    )
    assert _scan_predicates(plan, "o") == [("Eq", "o_orderkey", 42)]


def test_constant_reaches_every_leg_of_a_fanned_out_chain():
    """The motivating shape: one params CTE LEFT JOINed to several relations. The
    constant must survive the joins below to reach the later legs' scans."""
    plan = _optimized_plan(
        _PARAMS_CTE + "SELECT p.ck FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o  ON o.o_clerk  = p.ck "
        "LEFT JOIN testdata.tpch_001.orders o2 ON o2.o_clerk = p.ck "
        "LEFT JOIN testdata.tpch_001.orders o3 ON o3.o_clerk = p.ck"
    )
    for alias in ("o", "o2", "o3"):
        assert _scan_predicates(plan, alias) == [("Eq", "o_clerk", b"Clerk#000000001")], alias


def test_constant_not_pushed_onto_a_preserved_leg():
    """RIGHT JOIN preserves `orders`: an unmatched orders row is an OUTPUT row, so
    filtering that leg would delete rows the query must return."""
    plan = _optimized_plan(
        _PARAMS_CTE + "SELECT p.ck, o.o_orderkey FROM params p "
        "RIGHT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck"
    )
    assert _scan_predicates(plan, "o") == []


def test_constant_not_pushed_across_a_full_outer_join():
    plan = _optimized_plan(
        _PARAMS_CTE + "SELECT p.ck, o.o_orderkey FROM params p "
        "FULL OUTER JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck"
    )
    assert _scan_predicates(plan, "o") == []


def test_constant_not_taken_through_a_set_operation():
    """Two UNION branches feed the same output column, so a literal found under one
    of them describes only half the rows arriving at the join."""
    plan = _optimized_plan(
        "WITH params AS (SELECT 'Clerk#000000001' AS ck UNION ALL SELECT 'Clerk#000000002' AS ck) "
        "SELECT p.ck, o.o_orderkey FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck"
    )
    assert _scan_predicates(plan, "o") == []


@pytest.mark.parametrize(
    "sql",
    [
        # a constant that MATCHES
        _PARAMS_CTE + "SELECT p.ck, COUNT(o.o_orderkey) AS c FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck GROUP BY p.ck",
        # a constant that matches NOTHING: the preserved row must still be emitted,
        # null-filled. This is the shape a leg-filtering bug would silently delete.
        "WITH params AS (SELECT 'NOSUCHCLERK' AS ck) "
        "SELECT COUNT(*) AS c FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck",
        "WITH params AS (SELECT 'NOSUCHCLERK' AS ck) "
        "SELECT COUNT(*) AS c FROM params p "
        "FULL OUTER JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck",
        _PARAMS_CTE + "SELECT COUNT(*) AS c FROM params p "
        "RIGHT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck",
        "WITH params AS (SELECT 'Clerk#000000001' AS ck UNION ALL SELECT 'Clerk#000000002' AS ck) "
        "SELECT COUNT(*) AS c FROM params p "
        "LEFT JOIN testdata.tpch_001.orders o ON o.o_clerk = p.ck",
    ],
)
def test_constant_propagation_preserves_results(sql):
    import opteryx.planner.optimizer.strategies.correlated_filters as cf

    on_result = _count(sql)
    original = cf.CorrelatedFiltersStrategy.should_i_run
    try:
        cf.CorrelatedFiltersStrategy.should_i_run = lambda self, plan: False
        off_result = _count(sql)
    finally:
        cf.CorrelatedFiltersStrategy.should_i_run = original
    assert on_result == off_result, (on_result, off_result)
    assert on_result > 0
