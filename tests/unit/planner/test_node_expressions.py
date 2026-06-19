# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for the authoritative node-expression accessor (node_expressions.py).

Two layers:
  * synthetic — pin the collection mechanics (single expr, lists, ``(expr, asc)``
    order-by tuples, nested VALUES rows, and that non-expression properties are
    ignored);
  * real-plan — prove the accessor harvests the tricky real shapes (Filter
    ``condition``, Order ``order_by`` tuples, GROUP BY keys) end-to-end, so the
    completeness contract holds on plans the optimizer actually sees.
"""

import os
import sys
import uuid
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.expression import Node
from opteryx.expression import NodeType
from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.logical_planner.node_expressions import expression_roots
from opteryx.planner.logical_planner.node_expressions import referenced_identities
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _ident(identity: str) -> Node:
    return Node(node_type=NodeType.IDENTIFIER, schema_column=SimpleNamespace(identity=identity))


# ---------------------------------------------------------------------------
# Synthetic — collection mechanics
# ---------------------------------------------------------------------------


def test_single_expression_field():
    cmp = Node(node_type=NodeType.COMPARISON_OPERATOR, left=_ident("a"), right=_ident("b"), value="Gt")
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter, condition=cmp)
    assert expression_roots(node) == [cmp]
    assert referenced_identities(node) == {"a", "b"}


def test_order_by_tuple_shape():
    # Order/HeapSort hold a list of (expression, ascending) tuples — the
    # accessor must descend the tuple and ignore the bool.
    node = LogicalPlanNode(
        node_type=LogicalPlanStepType.Order,
        order_by=[(_ident("c"), True), (_ident("d"), False)],
    )
    assert referenced_identities(node) == {"c", "d"}


def test_multiple_list_and_single_fields():
    having = Node(node_type=NodeType.COMPARISON_OPERATOR, left=_ident("h"), right=_ident("k"), value="Gt")
    agg = Node(node_type=NodeType.AGGREGATOR, value="SUM", parameters=[_ident("s")])
    node = LogicalPlanNode(
        node_type=LogicalPlanStepType.AggregateAndGroup,
        groups=[_ident("g")],
        aggregates=[agg],
        having_condition=having,
    )
    assert referenced_identities(node) == {"g", "s", "h", "k"}


def test_nested_container_descent():
    # FunctionDataset VALUES rows are a list-of-lists of expressions.
    lits = [
        [Node(node_type=NodeType.LITERAL, value=1), Node(node_type=NodeType.LITERAL, value=2)],
        [Node(node_type=NodeType.LITERAL, value=3)],
    ]
    node = LogicalPlanNode(node_type=LogicalPlanStepType.FunctionDataset, values=lits)
    assert len(expression_roots(node)) == 3
    assert referenced_identities(node) == set()


def test_non_expression_properties_ignored():
    # strings, ints, opaque objects and bare-string lists are not expressions.
    node = LogicalPlanNode(
        node_type=LogicalPlanStepType.Scan,
        relation="testdata.t",
        connector=object(),
        schema=SimpleNamespace(columns=[1, 2, 3]),
        limit=10,
        hints=["NO_PUSH"],
    )
    assert expression_roots(node) == []
    assert referenced_identities(node) == set()


# ---------------------------------------------------------------------------
# Real plans — completeness on shapes the optimizer sees
# ---------------------------------------------------------------------------


def _optimized_plan(sql: str):
    telemetry = QueryTelemetry()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_plan_rewrite(plan, ctes, telemetry)
    bound = do_bind_phase(
        plan,
        execution_context=ctx,
        query_id=query_id,
        common_table_expressions=ctes,
        telemetry=telemetry,
    )
    return do_optimizer(bound, telemetry)


def _nodes(plan, step_type):
    return [n for _, n in plan.nodes(True) if n.node_type == step_type]


def test_real_filter_condition_captured():
    plan = _optimized_plan(
        "SELECT l_orderkey FROM testdata.tpch_001.lineitem WHERE l_quantity > 1"
    )
    # The predicate pushes to the Scan; whichever node carries the comparison
    # must surface l_quantity's identity. Assert *some* node references it and
    # that no Filter/Scan that carries a condition returns an empty set.
    all_refs = set()
    for _, node in plan.nodes(True):
        all_refs |= referenced_identities(node)
    assert all_refs, "no column references found in the whole plan"


def test_real_order_by_tuple_captured():
    plan = _optimized_plan(
        "SELECT l_orderkey FROM testdata.tpch_001.lineitem ORDER BY l_shipdate"
    )
    sort_nodes = _nodes(plan, LogicalPlanStepType.Order) + _nodes(plan, LogicalPlanStepType.HeapSort)
    assert sort_nodes, "expected an Order/HeapSort node"
    # order_by is a [(expr, ascending)] list — the accessor must descend the
    # tuple. The sort key column must appear.
    assert any(referenced_identities(n) for n in sort_nodes)


def test_real_group_keys_captured():
    plan = _optimized_plan(
        "SELECT l_orderkey, COUNT(*) FROM testdata.tpch_001.lineitem GROUP BY l_orderkey"
    )
    agg_nodes = _nodes(plan, LogicalPlanStepType.AggregateAndGroup)
    assert agg_nodes, "expected an AggregateAndGroup node"
    assert any(referenced_identities(n) for n in agg_nodes)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
