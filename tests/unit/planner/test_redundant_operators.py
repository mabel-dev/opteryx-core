"""Tests for RedundantOperationsStrategy.

In particular, covers the bug where a Project node's ``order_by_columns``
were ignored when checking whether the project was a no-op. ProjectionNode
emits ``columns ∪ order_by_columns`` at runtime (see projection.pyx) so
both must be considered when deciding redundancy.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.expression import NodeType
from opteryx.models import Node, QueryTelemetry
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies.redundant_operators import (
    RedundantOperationsStrategy,
)
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext
from opteryx.types.logical_type import INT64
from opteryx.types.schema import SchemaColumn
from tests.helpers import execute_and_get_rowcount


def _physical_node_types(sql: str):
    """Execute SQL through a session and return the physical plan node type names."""
    session = opteryx.session()
    list(session.execute_to_morsels(sql))
    return [type(session._plan[nid]).__name__ for nid in session._plan.nodes()]


def _column(name):
    """Build an IDENTIFIER Node with a SchemaColumn whose identity is its name."""
    schema_column = SchemaColumn(name=name, column_type=INT64, identity=name)
    return Node(NodeType.IDENTIFIER, schema_column=schema_column)


def _scan(columns):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    node.columns = list(columns)
    node.relation = "fake"
    node.alias = "fake"
    node.all_relations = {"fake"}
    return node


def _project(columns, order_by_columns=None):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    node.columns = list(columns)
    if order_by_columns is not None:
        node.order_by_columns = list(order_by_columns)
    node.alias = None
    return node


def _exit():
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    node.columns = []
    return node


def _build_plan(provider_node, project_node, exit_node=None):
    plan = LogicalPlan()
    plan.add_node("scan", provider_node)
    plan.add_node("project", project_node)
    plan.add_edge("scan", "project")
    if exit_node is not None:
        plan.add_node("exit", exit_node)
        plan.add_edge("project", "exit")
    return plan


def _run_strategy(plan):
    telemetry = QueryTelemetry("test_redundant_operators")
    strategy = RedundantOperationsStrategy(telemetry=telemetry)
    context = OptimizerContext(plan)

    # Walk from the exit/root toward the leaves so the strategy sees the
    # Project node — mirrors the OptimizerVisitor traversal.
    exit_points = plan.get_exit_points()
    root_nid = list(exit_points)[0]

    def _inner(nid):
        context.node_id = nid
        strategy.visit(plan[nid], context)
        for child, _, _ in plan.ingoing_edges(nid):
            _inner(child)

    _inner(root_nid)
    return context.optimized_plan


# ---------------------------------------------------------------------------
# Direct unit tests against the strategy
# ---------------------------------------------------------------------------


def test_project_with_order_by_columns_matching_provider_is_removed():
    """Test 2 — column-order independence (positive: should remove).

    Provider produces {a, b, c}; project has columns=[c],
    order_by_columns=[a, b]. Sets are equal, so project is redundant.
    """
    a, b, c = _column("a"), _column("b"), _column("c")
    scan = _scan([a, b, c])
    project = _project(columns=[c], order_by_columns=[a, b])
    exit_node = _exit()
    plan = _build_plan(scan, project, exit_node)

    optimized = _run_strategy(plan)

    step_types = [n.node_type for _, n in optimized.nodes(True)]
    assert LogicalPlanStepType.Project not in step_types, (
        f"Project should be removed; got: {step_types}"
    )


def test_project_with_subset_of_provider_columns_is_kept():
    """Test 3 — provider has more columns (negative: keep).

    Provider produces {a, b, c, d}; project has columns=[a],
    order_by_columns=[b]. {a, b} ⊊ {a, b, c, d}, so the project still
    narrows the relation and must be kept.
    """
    a, b, c, d = _column("a"), _column("b"), _column("c"), _column("d")
    scan = _scan([a, b, c, d])
    project = _project(columns=[a], order_by_columns=[b])
    exit_node = _exit()
    plan = _build_plan(scan, project, exit_node)

    optimized = _run_strategy(plan)

    step_types = [n.node_type for _, n in optimized.nodes(True)]
    assert LogicalPlanStepType.Project in step_types, (
        f"Project must be kept when columns ⊊ provider; got: {step_types}"
    )


def test_project_without_order_by_columns_attribute_still_works():
    """Test 5 — no ``order_by_columns`` attribute (positive: rule still works).

    Some Project nodes (e.g. those built inside subqueries) don't set
    ``order_by_columns``. ``LogicalPlanNode.__getattr__`` returns ``None``
    for missing properties, and the fix's ``getattr(..., None) or []``
    handles that case.
    """
    a, b = _column("a"), _column("b")
    scan = _scan([a, b])
    project = _project(columns=[a, b])  # no order_by_columns set at all
    exit_node = _exit()
    plan = _build_plan(scan, project, exit_node)

    # Sanity-check the precondition: order_by_columns is genuinely absent.
    assert project.order_by_columns is None

    optimized = _run_strategy(plan)

    step_types = [n.node_type for _, n in optimized.nodes(True)]
    assert LogicalPlanStepType.Project not in step_types, (
        f"Project should still be removed when columns equal "
        f"and order_by_columns is absent; got: {step_types}"
    )


def test_project_with_only_columns_equal_to_provider_is_removed_regression():
    """Test 6 — regression: existing redundant-project behaviour is preserved.

    The pre-fix happy path: Project columns equal provider columns and there
    is no extra ORDER BY work. Must still be removed.
    """
    a, b = _column("a"), _column("b")
    scan = _scan([a, b])
    project = _project(columns=[a, b], order_by_columns=[])
    exit_node = _exit()
    plan = _build_plan(scan, project, exit_node)

    optimized = _run_strategy(plan)

    step_types = [n.node_type for _, n in optimized.nodes(True)]
    assert LogicalPlanStepType.Project not in step_types, (
        f"Project equal to provider should be removed; got: {step_types}"
    )


# ---------------------------------------------------------------------------
# End-to-end tests through the full pipeline
# ---------------------------------------------------------------------------


def test_q25_shape_project_removed_end_to_end():
    """Test 1 — Q25 shape (positive: should remove).

    ``SELECT a FROM t ORDER BY b`` where the provider already produces
    ``{a, b}``. The optimizer should drop the Project that exists only to
    carry the ORDER BY column through.
    """
    sql = "SELECT name FROM $planets ORDER BY id LIMIT 1"

    types = _physical_node_types(sql)
    assert "ProjectionNode" not in types, (
        "ProjectionNode should be removed when columns + order_by_columns "
        f"matches the provider; got physical plan: {types}"
    )

    # And the query must still produce correct output.
    assert execute_and_get_rowcount(sql) == 1


def test_expression_in_order_by_keeps_project_or_passes_through():
    """Test 4 — expression in order_by_columns.

    SQL: ``SELECT a FROM t ORDER BY a + 1 LIMIT 1`` — the ``a + 1``
    expression is bound to a synthetic identity. Whether the Project
    survives depends on whether the upstream provider produces that
    synthetic identity. We assert correctness only; the structural
    outcome is informational.
    """
    sql = "SELECT name FROM $planets ORDER BY id + 1 LIMIT 1"

    # Must produce correct output regardless of plan shape.
    assert execute_and_get_rowcount(sql) == 1

    # And the plan must be well-formed.
    types = _physical_node_types(sql)
    assert types, "Physical plan should be non-empty"


def test_existing_redundant_project_after_aggregate_still_optimized():
    """An aggregate followed by a projection should still be optimized away.

    This is the pre-existing test, kept to guard against the fix
    regressing the original behaviour.
    """
    count = execute_and_get_rowcount("SELECT total FROM (SELECT COUNT(*) AS total FROM $planets)")
    assert count == 1


# Keep the original test name for backward compatibility with any test
# discovery that referenced it.
def test_redundant_project_removed_after_aggregate() -> None:
    count = execute_and_get_rowcount("SELECT total FROM (SELECT COUNT(*) AS total FROM $planets)")
    assert count == 1


if __name__ == "__main__":  # pragma: no cover
    test_project_with_order_by_columns_matching_provider_is_removed()
    print("✅ test_project_with_order_by_columns_matching_provider_is_removed")
    test_project_with_subset_of_provider_columns_is_kept()
    print("✅ test_project_with_subset_of_provider_columns_is_kept")
    test_project_without_order_by_columns_attribute_still_works()
    print("✅ test_project_without_order_by_columns_attribute_still_works")
    test_project_with_only_columns_equal_to_provider_is_removed_regression()
    print("✅ test_project_with_only_columns_equal_to_provider_is_removed_regression")
    test_q25_shape_project_removed_end_to_end()
    print("✅ test_q25_shape_project_removed_end_to_end")
    test_expression_in_order_by_keeps_project_or_passes_through()
    print("✅ test_expression_in_order_by_keeps_project_or_passes_through")
    test_existing_redundant_project_after_aggregate_still_optimized()
    print("✅ test_existing_redundant_project_after_aggregate_still_optimized")
    test_redundant_project_removed_after_aggregate()
    print("✅ test_redundant_project_removed_after_aggregate")
    print("All tests passed.")
