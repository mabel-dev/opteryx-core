# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Unit tests for StatisticsOnlyResponseStrategy

These tests verify the strategy rewrites a simple COUNT(*) logical plan into a
projection of a literal count over the `$one_row` virtual relation, and that
it leaves non-eligible plans unchanged.
"""

import types

from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.expression import NodeType
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    StatisticsOnlyResponseStrategy,
)
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    get_count_from_manifest,
)
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    is_simple_aggregate,
)
from opteryx.compiled.structures.expressions import Aggregator
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.expressions import Wildcard
from opteryx.compiled.structures.plan_steps import AggregateStep
from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext
from opteryx.planner.plan_context import PlanContext
from opteryx.types.schema import FunctionColumn
from opteryx.types.schema import RelationSchema
from tests.manifests import FileSpec
from tests.manifests import build_manifest

# One query context for the expressions this module builds outside a plan.
_TEST_CONTEXT = PlanContext()
_TEST_ARENA = _TEST_CONTEXT.expressions

def _telemetry():
    return types.SimpleNamespace(optimization_statistics_only_response=0)


def _manifest(count):
    """A one-file manifest of `count` rows, written by a commit (authoritative):
    the only kind the strategy may answer from."""
    return build_manifest(
        RelationSchema(name="planets"), [FileSpec("planets.parquet", record_count=count)]
    )


def _count_star(plan_context):
    """COUNT(*) — the aggregate shape the strategy answers from the manifest."""
    return Aggregator(
        value="COUNT",
        parameters=[Wildcard(arena=plan_context.expressions)],
        schema_column=plan_context.columns.computed(FunctionColumn, "COUNT(*)"),
        arena=plan_context.expressions,
    )


def _count_distinct():
    return Aggregator(
        value="COUNT",
        parameters=[LogicalColumn(NodeType.IDENTIFIER, "x", arena=_TEST_ARENA)],
        duplicate_treatment="Distinct",
        schema_column=_TEST_CONTEXT.columns.computed(FunctionColumn, "COUNT(*)"),
        arena=_TEST_ARENA,
    )


def make_simple_count_plan(plan_context, count=9, alias="my_count"):
    plan = LogicalPlan(plan_context)

    # Scan node
    scan = ScanStep()
    scan.relation = "planets"
    scan.alias = "planets"
    scan.manifest = _manifest(count)

    # Aggregate node representing `SELECT COUNT(*) AS alias` over the scan
    agg = AggregateStep()
    aggregator = _count_star(plan_context)
    agg.aggregates = [aggregator]

    # Exit node to hold column alias. The strategy pairs Exit columns to aggregates
    # by schema IDENTITY (Exit order is not guaranteed to match aggregate order), so
    # the column has to carry the aggregate's identity for its alias to be found.
    exit_node = ExitStep()
    exit_node.columns = [
        LogicalColumn(
            NodeType.IDENTIFIER,
            None,
            alias=alias,
            schema_column=aggregator.schema_column,
            arena=plan_context.expressions,
        )
    ]

    scan_nid = plan.add_node(scan)
    agg_nid = plan.add_node(agg)
    exit_nid = plan.add_node(exit_node)

    plan.add_edge(scan_nid, agg_nid)
    plan.add_edge(agg_nid, exit_nid)

    return plan


def test_strategy_rewrites_count_star_plan():
    plan_context = PlanContext()
    plan = make_simple_count_plan(plan_context, count=9, alias="total_count")
    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())

    # Run the strategy's complete phase which performs the rewrite
    rewritten = strategy.complete(plan, OptimizerContext(plan, plan_context))

    # Assert the same plan object is returned
    assert rewritten is plan

    # The aggregate node should now be a Project with a literal column
    agg_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Project)
    assert hasattr(agg_node, "columns") and len(agg_node.columns) == 1
    literal = agg_node.columns[0]
    assert literal.value == 9
    assert literal.alias == "total_count"

    # The scan node should now point to $one_row and use the virtual connector
    scan_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    assert scan_node.relation == "$one_row"
    # If the strategy could replace the connector, it should be the virtual one.
    conn_type = scan_node.connector and scan_node.connector.__type__
    if conn_type is not None:
        assert conn_type == "VIRTUAL"
    # Schema may or may not be present in synthetic unit tests; accept either
    # the virtual schema or None (integration tests will validate end-to-end)
    schema_name = None if scan_node.schema is None else scan_node.schema.name
    assert schema_name in (None, "$one_row")

    # The exit node should reference the same literal column
    exit_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Exit)
    assert exit_node.columns[0].alias == "total_count"

    # All projection/exit columns should be replaced with the literal and should
    # share the same schema identity
    literal_id = None
    for nid, n in plan.nodes(data=True):
        if n.node_type == LogicalPlanStepType.Project:
            cols = n.columns or []
            for c in cols:
                if c.node_type is not None:
                    literal_id = c.schema_column.identity
    assert literal_id is not None
    # Exit must reference same identity
    exit_col = exit_node.columns[0]
    assert exit_col.schema_column and exit_col.schema_column.identity == literal_id


def test_strategy_prunes_manifest():
    plan_context = PlanContext()
    plan = make_simple_count_plan(plan_context, count=9, alias="total_count")
    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())

    # ensure manifest initially present
    scan_node = next(n for _, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    assert hasattr(scan_node, "manifest") and scan_node.manifest is not None

    strategy.complete(plan, OptimizerContext(plan, plan_context))

    # After the rewrite the scan is repointed at the `$one_row` virtual relation and
    # its manifest is dropped entirely — the strategy clears it so a file-based reader
    # can't supply a file list for a plan that must read nothing.
    assert scan_node.manifest is None
    assert scan_node.relation == "$one_row"


def test_strategy_no_manifest_leaves_plan_unchanged():
    plan_context = PlanContext()
    plan = make_simple_count_plan(plan_context, count=9, alias="total_count")
    # Remove manifest to simulate absence of statistics
    scan_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    scan_node.manifest = None

    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())
    rewritten = strategy.complete(plan, OptimizerContext(plan, plan_context))

    # Plan should be unchanged (still has Aggregate node)
    agg_nodes = [n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Aggregate]
    assert len(agg_nodes) == 1


def test_get_count_from_manifest():
    m = _manifest(123)
    assert get_count_from_manifest(m) == 123
    # A missing manifest is UNKNOWN, not 0. This number is handed straight back
    # as the answer to COUNT(*) with the scan deleted, so reporting 0 for "nobody
    # counted" is a silent wrong answer - the caller must abandon the rewrite.
    assert get_count_from_manifest(None) is None


def test_is_simple_aggregate_rejects_count_distinct():
    aggregate_node = types.SimpleNamespace(aggregates=[_count_distinct()])
    assert not is_simple_aggregate(aggregate_node)
