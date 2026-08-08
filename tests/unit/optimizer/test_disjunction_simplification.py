# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""DisjunctionSimplificationStrategy.

Regression coverage for a correctness bug found 2026-07-28: cross-clause
dedup/absorption/common-factoring keyed each OR-branch predicate by
`format_expression(pred)` — rendered TEXT, not the bound column identity.
`format_expression` renders an IDENTIFIER by name only, dropping the table
qualifier, so two DIFFERENT columns that merely share a name (e.g. two
aliases of a self-joined table) render identically. For a filter with no
other conjunct alongside the OR (so the whole Filter condition IS the OR),
that made

    (n1.n_name = 'KENYA' AND n2.n_name = 'PERU')
    OR (n1.n_name = 'PERU' AND n2.n_name = 'KENYA')

's two branches compare equal as string KEY SETS (`{"n_name=KENYA",
"n_name=PERU"}` both times) even though they bind to opposite columns, so
cross-clause dedup silently dropped the second branch — changing the query's
result (1 row instead of the correct 2). Fixed by folding each predicate's
referenced schema_column.identity into its dedup/factoring key.

Also covers the strategy's actual intended job (TPC-H Q19's shape: a join key
repeated identically in every branch gets factored out) to guard against a
regression of that in fixing the bug above.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _rows(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel.num_rows == 0:
            continue
        names = morsel.column_names
        columns = [morsel.column(name).to_pylist() for name in names]
        for values in zip(*columns):
            rows.append(dict(zip(names, values)))
    return rows


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


_SELF_JOIN_SWAPPED_SQL = (
    "SELECT n1.n_name, n2.n_name FROM testdata.tpch_001.nation AS n1, testdata.tpch_001.nation AS n2 "
    "WHERE (n1.n_name = 'KENYA' AND n2.n_name = 'PERU') "
    "OR (n1.n_name = 'PERU' AND n2.n_name = 'KENYA')"
)


def test_self_join_swapped_branches_are_not_deduped_away():
    # Both branches are real and distinct: dropping either changes the answer.
    rows = _rows(_SELF_JOIN_SWAPPED_SQL)
    pairs = {(r[b"n1.n_name"], r[b"n2.n_name"]) for r in rows}
    assert pairs == {("KENYA", "PERU"), ("PERU", "KENYA")}, pairs


def test_self_join_swapped_branches_preserved_with_strategy_forced_on():
    # Directly confirms the strategy itself no longer collapses the two
    # branches, independent of whatever else runs around it in the full pipeline.
    from opteryx.expression.formatter import format_expression
    from opteryx.planner.logical_planner import LogicalPlanStepType

    plan, _ = _optimized_plan(_SELF_JOIN_SWAPPED_SQL)
    conditions = [
        format_expression(node.condition)
        for _, node in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Filter
    ]
    survivors = [c for c in conditions if "KENYA" in c and "PERU" in c]
    assert survivors, conditions
    assert " OR " in survivors[0], survivors[0]


def test_common_join_key_still_factored_across_branches():
    # The strategy's actual intended case (TPC-H Q19's shape): the SAME bound
    # column repeated identically in every branch is still factored out.
    sql = (
        "SELECT SUM(l_extendedprice) AS revenue "
        "FROM testdata.tpch_001.lineitem, testdata.tpch_001.part "
        "WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12') "
        "OR (p_partkey = l_partkey AND p_brand = 'Brand#23')"
    )
    plan, telemetry = _optimized_plan(sql)
    assert telemetry.optimization_disjunction_simplification > 0

    from opteryx.expression.formatter import format_expression
    from opteryx.planner.logical_planner import LogicalPlanStepType

    remaining_or = [
        format_expression(node.condition)
        for _, node in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Filter
        and " OR " in format_expression(node.condition)
    ]
    # p_partkey = l_partkey was common to both branches and factored out, so
    # any surviving OR is over p_brand only, not the join key.
    for cond in remaining_or:
        assert "l_partkey" not in cond, cond


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
