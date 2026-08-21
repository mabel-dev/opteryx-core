# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Unit tests for StatisticsOnlyResponseStrategy

These tests verify the strategy rewrites a simple COUNT(*) logical plan into a
projection of a literal count over the `$no_table` virtual relation, and that
it leaves non-eligible plans unchanged.
"""

import types

from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
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

def _telemetry():
    return types.SimpleNamespace(optimization_statistics_only_response=0)


class MockManifest:
    def __init__(self, count):
        # mimic Manifest.files list of FileEntry-like objects
        self.files = [__import__("types").SimpleNamespace(record_count=count, file_size_in_bytes=0)]

    def get_record_count(self):
        return sum(f.record_count for f in self.files)

    def get_file_count(self):
        return len(self.files)

    def subset(self, positions):
        # Mirror Manifest.subset's copy-on-write contract: a NEW manifest over
        # the selected files, the original untouched.
        clone = MockManifest.__new__(MockManifest)
        clone.files = [self.files[p] for p in positions]
        return clone


class MockAggregator:
    def __init__(self):
        # Minimal aggregator shape expected by the strategy
        self.node_type = NodeType.AGGREGATOR
        self.value = "COUNT"
        # Parameters: first parameter is wildcard for COUNT(*)
        self.parameters = [types.SimpleNamespace(node_type=NodeType.WILDCARD)]
        # Keep schema_column identity shape to match other code paths where needed
        self.schema_column = types.SimpleNamespace(identity="$COUNT(*)", column_type=None)
        self.duplicate_treatment = None
        self.condition = None


class MockDistinctCountAggregator(MockAggregator):
    def __init__(self):
        super().__init__()
        self.parameters = [types.SimpleNamespace(node_type=NodeType.IDENTIFIER)]
        self.duplicate_treatment = "Distinct"


def make_simple_count_plan(count=9, alias="my_count"):
    plan = LogicalPlan()

    # Scan node
    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = "planets"
    scan.alias = "planets"
    scan.manifest = MockManifest(count)

    # Aggregate node representing `SELECT COUNT(*) AS alias` over the scan
    agg = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
    aggregator = MockAggregator()
    agg.aggregates = [aggregator]

    # Exit node to hold column alias. The strategy pairs Exit columns to aggregates
    # by schema IDENTITY (Exit order is not guaranteed to match aggregate order), so
    # the column has to carry the aggregate's identity for its alias to be found.
    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = [
        types.SimpleNamespace(
            alias=alias,
            source_column=None,
            schema_column=types.SimpleNamespace(identity=aggregator.schema_column.identity),
        )
    ]

    plan.add_node("scan", scan)
    plan.add_node("agg", agg)
    plan.add_node("exit", exit_node)

    plan.add_edge("scan", "agg")
    plan.add_edge("agg", "exit")

    return plan


def test_strategy_rewrites_count_star_plan():
    plan = make_simple_count_plan(count=9, alias="total_count")
    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())

    # Run the strategy's complete phase which performs the rewrite
    rewritten = strategy.complete(plan, None)

    # Assert the same plan object is returned
    assert rewritten is plan

    # The aggregate node should now be a Project with a literal column
    agg_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Project)
    assert hasattr(agg_node, "columns") and len(agg_node.columns) == 1
    literal = agg_node.columns[0]
    assert getattr(literal, "value", None) == 9
    assert getattr(literal, "alias", None) == "total_count"

    # The scan node should now point to $no_table and use the virtual connector
    scan_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    assert scan_node.relation == "$no_table"
    # If the strategy could replace the connector, it should be the virtual one.
    conn_type = getattr(scan_node, 'connector', None) and getattr(scan_node.connector, '__type__', None)
    if conn_type is not None:
        assert conn_type == "VIRTUAL"
    # Schema may or may not be present in synthetic unit tests; accept either
    # the virtual schema or None (integration tests will validate end-to-end)
    schema_name = getattr(scan_node.schema, "name", None)
    assert schema_name in (None, "$no_table")

    # The exit node should reference the same literal column
    exit_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Exit)
    assert exit_node.columns[0].alias == "total_count"

    # All projection/exit columns should be replaced with the literal and should
    # share the same schema identity
    literal_id = None
    for nid, n in plan.nodes(data=True):
        if n.node_type == LogicalPlanStepType.Project:
            cols = getattr(n, 'columns', []) or []
            for c in cols:
                if getattr(c, 'node_type', None) is not None:
                    literal_id = getattr(c.schema_column, 'identity', None)
    assert literal_id is not None
    # Exit must reference same identity
    exit_col = exit_node.columns[0]
    assert getattr(exit_col, 'schema_column', None) and getattr(exit_col.schema_column, 'identity', None) == literal_id


def test_strategy_prunes_manifest():
    plan = make_simple_count_plan(count=9, alias="total_count")
    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())

    # ensure manifest initially present
    scan_node = next(n for _, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    assert hasattr(scan_node, "manifest") and scan_node.manifest is not None

    strategy.complete(plan, None)

    # After the rewrite the scan is repointed at the `$no_table` virtual relation and
    # its manifest is dropped entirely — the strategy clears it so a file-based reader
    # can't supply a file list for a plan that must read nothing.
    assert getattr(scan_node, "manifest", None) is None
    assert scan_node.relation == "$no_table"


def test_strategy_no_manifest_leaves_plan_unchanged():
    plan = make_simple_count_plan(count=9, alias="total_count")
    # Remove manifest to simulate absence of statistics
    scan_node = next(n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan)
    scan_node.manifest = None

    strategy = StatisticsOnlyResponseStrategy(telemetry=_telemetry())
    rewritten = strategy.complete(plan, None)

    # Plan should be unchanged (still has Aggregate node)
    agg_nodes = [n for nid, n in plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Aggregate]
    assert len(agg_nodes) == 1


def test_get_count_from_manifest():
    m = MockManifest(123)
    assert get_count_from_manifest(m) == 123
    # A missing manifest is UNKNOWN, not 0. This number is handed straight back
    # as the answer to COUNT(*) with the scan deleted, so reporting 0 for "nobody
    # counted" is a silent wrong answer - the caller must abandon the rewrite.
    assert get_count_from_manifest(None) is None


def test_is_simple_aggregate_rejects_count_distinct():
    aggregate_node = types.SimpleNamespace(aggregates=[MockDistinctCountAggregator()])
    assert not is_simple_aggregate(aggregate_node)
