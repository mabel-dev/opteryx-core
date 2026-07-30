# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Column-vs-column comparisons (`a.x = b.y`, no literal on either side).

Found during a stats-system review: `_selectivity_comparison` required a
LITERAL on one side and returned 1.0 (no reduction) whenever both sides were
columns. Most column-vs-column equalities are extracted as equi-join keys
before they ever reach this estimator, but two shapes aren't:
  * same-relation comparisons, e.g. `WHERE l_shipdate < l_commitdate`
  * residual cross-relation predicates from a non-equi/implicit join

Eq/NotEq now reuse the same NDV formula as equi-join key selectivity
(1 / max(ndv_left, ndv_right)); range comparisons fall to a textbook
constant (1/3) since two independent single-column stats say nothing about
the correlation between the columns.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Importing opteryx.planner.optimizer (the package) resolves the
# optimizer <-> cost_estimation.selectivity import cycle first: strategies
# under this package import `estimate_selectivity` from the selectivity
# module below, so that module must not be the first thing to touch it.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation.selectivity import estimate_selectivity
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics

_X = b"tes_x_00000001"
_Y = b"tes_y_00000002"


def _identifier(identity: bytes) -> Node:
    n = Node(node_type=NodeType.IDENTIFIER)
    n.schema_column = type("_S", (), {"identity": identity})()
    return n


def _cmp(op: str, left_identity: bytes, right_identity: bytes) -> Node:
    n = Node(node_type=NodeType.COMPARISON_OPERATOR)
    n.value = op
    n.left = _identifier(left_identity)
    n.right = _identifier(right_identity)
    return n


def _stats(x_ndv, y_ndv) -> RelationStatistics:
    columns = {}
    if x_ndv is not None:
        columns[_X] = ColumnStatistics(column_name="x", data_type="INTEGER", distinct_count=x_ndv)
    if y_ndv is not None:
        columns[_Y] = ColumnStatistics(column_name="y", data_type="INTEGER", distinct_count=y_ndv)
    return RelationStatistics(row_count=1000, columns=columns)


def test_eq_uses_ndv_formula_like_a_join_key():
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("Eq", _X, _Y), stats)
    assert s == pytest.approx(1.0 / 40, rel=0.01)


def test_not_eq_is_the_complement_of_eq():
    stats = _stats(x_ndv=10, y_ndv=40)
    eq = estimate_selectivity(_cmp("Eq", _X, _Y), stats)
    not_eq = estimate_selectivity(_cmp("NotEq", _X, _Y), stats)
    assert not_eq == pytest.approx(1.0 - eq, rel=1e-9)


def test_eq_with_unknown_ndv_on_either_side_uses_constant_fallback():
    stats = _stats(x_ndv=None, y_ndv=40)
    s = estimate_selectivity(_cmp("Eq", _X, _Y), stats)
    assert s == pytest.approx(0.1, rel=0.01)


def test_range_comparison_between_two_columns_uses_textbook_constant():
    # No NDV, even a histogram, can tell you the correlation between two
    # different columns -- this must not be 1.0 (the old "no reduction" bug).
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("Lt", _X, _Y), stats)
    assert s == pytest.approx(1.0 / 3.0, rel=0.01)
    assert s != 1.0


def test_self_comparison_eq_is_always_true_not_an_ndv_question():
    # `a.x = a.x`: same identity on both sides. The old NDV-based formula
    # (1/max(ndv,ndv) = 1/ndv) would UNDERestimate a tautology.
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("Eq", _X, _X), stats)
    assert s == 1.0


def test_self_comparison_not_eq_is_always_false():
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("NotEq", _X, _X), stats)
    assert s == 0.0


def test_self_comparison_lt_is_always_false():
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("Lt", _X, _X), stats)
    assert s == 0.0


def test_self_comparison_lteq_is_always_true():
    stats = _stats(x_ndv=10, y_ndv=40)
    s = estimate_selectivity(_cmp("LtEq", _X, _X), stats)
    assert s == 1.0


# ── integration: the real pipeline actually reaches this code ───────────────────
#
# lineitem's `l_shipdate < l_commitdate` is a same-relation column-vs-column
# predicate that the filesystem connector pushes onto Scan.predicates
# (can_push only checks each side's schema-column type, not whether the other
# side is a literal) -- so it reaches estimate_selectivity via _scan_stats,
# not a surviving Filter node. Before this fix it contributed zero reduction.


def _optimized_and_refreshed_scan_row_count(sql):
    import uuid

    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.logical_planner import LogicalPlanStepType
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
    refreshed = refresh_statistics(optimized)

    for _nid, node in refreshed.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            return node.statistics.row_count, bool(getattr(node, "predicates", None))
    return None, False


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_same_relation_range_comparison_reduces_the_estimate():
    unfiltered, _ = _optimized_and_refreshed_scan_row_count(
        "SELECT * FROM testdata.tpch_001.lineitem"
    )
    filtered, pushed = _optimized_and_refreshed_scan_row_count(
        "SELECT * FROM testdata.tpch_001.lineitem WHERE l_shipdate < l_commitdate"
    )
    assert pushed, (
        "expected l_shipdate < l_commitdate to be pushed onto Scan.predicates "
        "-- if this is no longer true, this test is not reproducing the gap"
    )
    assert filtered == pytest.approx(unfiltered / 3, rel=0.02), (
        f"expected the 1/3 textbook fallback to apply ({unfiltered} -> "
        f"~{unfiltered // 3}), got {filtered} -- did column-vs-column stop "
        f"reaching estimate_selectivity, or fall back to 1.0 (no reduction)?"
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_same_relation_equality_comparison_reduces_the_estimate():
    unfiltered, _ = _optimized_and_refreshed_scan_row_count(
        "SELECT * FROM testdata.tpch_001.lineitem"
    )
    filtered, pushed = _optimized_and_refreshed_scan_row_count(
        "SELECT * FROM testdata.tpch_001.lineitem WHERE l_shipdate = l_commitdate"
    )
    assert pushed
    assert filtered < unfiltered, (
        f"l_shipdate = l_commitdate did not reduce the estimate at all "
        f"({filtered} == {unfiltered}) -- column-vs-column Eq is not reaching "
        f"the NDV-based estimator"
    )


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
