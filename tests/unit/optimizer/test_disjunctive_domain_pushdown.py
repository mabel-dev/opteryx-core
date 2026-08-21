# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""DisjunctiveDomainPushdownStrategy — derive implied, weaker per-column domain
predicates (IN-list / range) from an OR-of-AND filter whose branches share no
identical predicate (so DisjunctionSimplificationStrategy's common-factoring
can't touch it), and push them to each branch's own scan.

Covers:
  * TPC-H Q7's bilateral-trade shape: `(n1.x=A AND n2.y=B) OR (n1.x=B AND n2.y=A)`
    derives `n1.x IN (A,B)` and `n2.x IN (A,B)`, one per scan (self-join, so the
    two `n_name` columns are the SAME name but different bound columns).
  * A branch that leaves a column unconstrained blocks derivation for it.
  * Range branches derive the convex hull (not the tighter exact union).
  * A branch mixing point and range leaves on the same column blocks
    derivation for that column (out of scope, not a correctness bug — the
    original OR still runs post-join regardless).
  * The derived predicates never change query results.
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

    # A unique query_id per call: QueryTelemetry is a process-wide singleton keyed
    # by query_id (opteryx/models/query_telemetry.py), so reusing the default ""
    # key would accumulate counters across calls instead of isolating each query.
    qid = str(uuid.uuid4())
    telemetry = QueryTelemetry(qid)
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    ast = do_ast_rewriter(
        sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[]
    )[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=qid, telemetry=telemetry)
    return do_optimizer(bound, telemetry), telemetry


def _scan_predicates(plan, alias):
    """Predicate nodes pushed onto the scan with this alias, or None if the
    scan has no alias match (distinguishes two aliases of a self-joined
    relation, which share `.relation` but not `.alias`)."""
    from opteryx.planner.logical_planner import LogicalPlanStepType

    for _, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan and getattr(node, "alias", None) == alias:
            return getattr(node, "predicates", None) or []
    return None


def _count(sql):
    value = 0
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel.num_rows:
            value = morsel.column(b"c").to_pylist()[0]
    return value


_Q7_SHAPE_SQL = (
    "SELECT n1.n_name, n2.n_name FROM testdata.tpch_001.nation AS n1, testdata.tpch_001.nation AS n2 "
    "WHERE (n1.n_name = 'KENYA' AND n2.n_name = 'PERU') "
    "OR (n1.n_name = 'PERU' AND n2.n_name = 'KENYA')"
)


def test_derives_in_list_for_bilateral_filter():
    plan, telemetry = _optimized_plan(_Q7_SHAPE_SQL)
    assert telemetry.optimization_disjunctive_domain_pushdown == 2

    for alias in ("n1", "n2"):
        preds = _scan_predicates(plan, alias)
        assert preds, (alias, preds)
        assert any(p.value in ("Eq", "InList") for p in preds), (alias, preds)


def test_original_disjunction_survives_untouched():
    from opteryx.expression.formatter import format_expression
    from opteryx.planner.logical_planner import LogicalPlanStepType

    plan, _ = _optimized_plan(_Q7_SHAPE_SQL)
    or_filters = [
        format_expression(node.condition)
        for _, node in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Filter
        and " OR " in format_expression(node.condition)
    ]
    assert len(or_filters) == 1, or_filters
    assert "KENYA" in or_filters[0] and "PERU" in or_filters[0]


def test_no_derivation_when_branch_leaves_column_unconstrained():
    sql = (
        "SELECT n1.n_name, n2.n_name FROM testdata.tpch_001.nation AS n1, testdata.tpch_001.nation AS n2 "
        "WHERE (n1.n_name = 'KENYA' AND n2.n_name = 'PERU') OR (n1.n_name = 'PERU')"
    )
    plan, telemetry = _optimized_plan(sql)
    # n1.n_name is constrained in both branches -> derives one predicate;
    # n2.n_name is unconstrained in the second branch -> must NOT derive for n2.
    assert telemetry.optimization_disjunctive_domain_pushdown == 1
    assert _scan_predicates(plan, "n1")
    assert not _scan_predicates(plan, "n2")


def test_range_hull_derived_across_branches():
    sql = (
        "SELECT n1.n_nationkey, n2.n_nationkey "
        "FROM testdata.tpch_001.nation AS n1, testdata.tpch_001.nation AS n2 "
        "WHERE (n1.n_nationkey > 5 AND n1.n_nationkey < 10 AND n2.n_nationkey > 20 AND n2.n_nationkey < 25) "
        "OR (n1.n_nationkey > 1 AND n1.n_nationkey < 3 AND n2.n_nationkey > 15 AND n2.n_nationkey < 18)"
    )
    plan, telemetry = _optimized_plan(sql)
    assert telemetry.optimization_disjunctive_domain_pushdown == 4  # 2 columns x (lo, hi)

    # PredicateCompactionStrategy (later in the pipeline) recombines the lo+hi
    # pair this strategy derives into one BETWEEN, so the hull shows up as a
    # single predicate carrying both literal bounds, not two comparisons.
    n1_preds = _scan_predicates(plan, "n1")
    n2_preds = _scan_predicates(plan, "n2")
    assert n1_preds and n2_preds

    def _bounds(preds):
        for p in preds:
            if p.left.source_column == "n_nationkey":
                return sorted((p.right.value, p.centre.value))
        return None

    # hull of (5,10) and (1,3) is the loosest cover: lo=min(5,1)=1, hi=max(10,3)=10
    assert _bounds(n1_preds) == [1, 10], n1_preds
    # hull of (20,25) and (15,18) is lo=min(20,15)=15, hi=max(25,18)=25
    assert _bounds(n2_preds) == [15, 25], n2_preds


def test_mixed_point_and_range_on_same_column_blocks_derivation():
    # n1.n_nationkey is compared with Eq in one branch and a range in the other
    # -> out of scope, must derive nothing for it (not a correctness bug: the
    # original OR still runs post-join regardless of what got pushed early).
    sql = (
        "SELECT n1.n_nationkey, n2.n_name "
        "FROM testdata.tpch_001.nation AS n1, testdata.tpch_001.nation AS n2 "
        "WHERE (n1.n_nationkey = 5 AND n2.n_name = 'PERU') "
        "OR (n1.n_nationkey > 10 AND n2.n_name = 'KENYA')"
    )
    plan, telemetry = _optimized_plan(sql)
    # n2.n_name still qualifies (Eq in both branches); n1.n_nationkey does not.
    assert telemetry.optimization_disjunctive_domain_pushdown == 1
    n1_preds = _scan_predicates(plan, "n1") or []
    assert not any(p.left.source_column == "n_nationkey" for p in n1_preds), n1_preds


def test_derived_predicates_preserve_results():
    import opteryx.planner.optimizer.strategies.disjunctive_domain_pushdown as ddp

    count_sql = _Q7_SHAPE_SQL.replace("SELECT n1.n_name, n2.n_name", "SELECT COUNT(*) AS c")
    on_result = _count(count_sql)

    original = ddp.DisjunctiveDomainPushdownStrategy.should_i_run
    try:
        ddp.DisjunctiveDomainPushdownStrategy.should_i_run = lambda self, plan: False
        off_result = _count(count_sql)
    finally:
        ddp.DisjunctiveDomainPushdownStrategy.should_i_run = original

    assert on_result == off_result
    assert on_result > 0


def test_derived_temporal_predicate_is_fully_bound():
    """A derived predicate's synthesized nodes must carry `schema_column`, not
    just `.type`.

    The bytecode compiler reads operand types exclusively off
    `schema_column.column_type`, so a literal stamped with only `.type` is a
    half-bound node. On a TEMPORAL column that is not a slow path but a hard
    failure: `_validate_temporal_at_bind` saw `right_type is None`, decided the
    literal was un-cast, and refused the whole query with "literals must be
    explicitly cast to temporal types" — for a query whose literals were all
    explicitly cast. Any temporal column reachable by this strategy reproduces
    it; `Lauched_at` is a stored TIMESTAMP64.
    """
    sql = (
        "SELECT COUNT(*) AS c FROM testdata.missions "
        "WHERE Lauched_at = CAST('2020-08-07 05:12:00' AS TIMESTAMP) "
        "   OR Lauched_at = CAST('1957-10-04 19:28:00' AS TIMESTAMP)"
    )
    assert _count(sql) == 2

    plan, _ = _optimized_plan(sql)
    for predicates in (_scan_predicates(plan, "missions") or [],):
        for predicate in predicates:
            assert predicate.schema_column is not None, predicate
            assert predicate.right.schema_column is not None, predicate
            assert predicate.right.schema_column.column_type is not None, predicate


def test_feature_flag_disables_strategy():
    from opteryx import config

    on_plan, on_telemetry = _optimized_plan(_Q7_SHAPE_SQL)
    assert on_telemetry.optimization_disjunctive_domain_pushdown > 0

    original = config.features.disable_disjunctive_domain_pushdown
    try:
        config.features.disable_disjunctive_domain_pushdown = True
        off_plan, off_telemetry = _optimized_plan(_Q7_SHAPE_SQL)
        assert off_telemetry.optimization_disjunctive_domain_pushdown == 0
        assert not _scan_predicates(off_plan, "n1")
        assert not _scan_predicates(off_plan, "n2")
    finally:
        config.features.disable_disjunctive_domain_pushdown = original


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
