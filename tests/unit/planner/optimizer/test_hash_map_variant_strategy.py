# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit tests for HashMapVariantStrategy optimizer.

Verifies that:
1. Small estimated group cardinality triggers parvi selection.
2. Large or unknown cardinality falls back to carchar.
3. The strategy correctly resolves group columns to manifest statistics.
"""

import pytest

from opteryx.models import QueryProperties, QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.optimizer.strategies.hash_map_variant import HashMapVariantStrategy
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext


def _make_scan_node_with_manifest(relation_name: str, record_count: int) -> LogicalPlanNode:
    """Create a mock Scan node with a minimal manifest."""
    class MockManifest:
        def get_record_count(self) -> int:
            return record_count

        def estimate_cardinality(self, col_name: str) -> int:
            # Simplistic: return a fixed estimate per column.
            # For testing, assume all columns have ~5 distinct values.
            return 5

    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = relation_name
    scan.manifest = MockManifest()
    return scan


def _make_identifier_node(name: str):
    """Create a mock IDENTIFIER expression node."""
    from opteryx.expression import NodeType

    class MockSchemaColumn:
        def __init__(self, col_name):
            self.name = col_name
            self.source_column = col_name

    node = LogicalPlanNode(node_type=NodeType.IDENTIFIER)
    node.schema_column = MockSchemaColumn(name)
    return node


def test_parvi_selected_on_small_ndv_product():
    """Parvi is selected when NDV product of group columns is <= the gate (40)."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    # Create a scan with manifest.
    scan = _make_scan_node_with_manifest("test_table", 1000)

    # Create a logical plan.
    plan = LogicalPlan()
    scan_nid = "scan_1"
    plan.add_node(scan_nid, scan)

    # Create an AggregateAndGroup node with GROUP BY col1, col2.
    # Assume each column has NDV=5 from the manifest, so product = 25.
    scan.manifest.estimate_cardinality = lambda col: 3  # 3 * 3 = 9 <= 40

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1"), _make_identifier_node("col2")]
    agg_node.nid = "agg_1"

    plan.add_node("agg_1", agg_node)
    plan.add_edge(scan_nid, "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan

    context = strategy.visit(agg_node, context)

    # After visit, the node should be tagged with group_map_variant.
    assert hasattr(agg_node, "group_map_variant")
    assert agg_node.group_map_variant == "parvi", f"Expected 'parvi' but got {agg_node.group_map_variant}"


def test_carchar_selected_on_large_ndv_product():
    """Carchar is selected when NDV product exceeds the gate (40)."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    scan = _make_scan_node_with_manifest("test_table", 1000)
    scan.manifest.estimate_cardinality = lambda col: 10  # 10 * 10 = 100 > 40

    plan = LogicalPlan()
    scan_nid = "scan_1"
    plan.add_node(scan_nid, scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1"), _make_identifier_node("col2")]
    agg_node.nid = "agg_1"

    plan.add_node("agg_1", agg_node)
    plan.add_edge(scan_nid, "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan

    context = strategy.visit(agg_node, context)

    assert agg_node.group_map_variant == "carchar"


def test_parvi_selected_on_small_record_count():
    """Parvi is selected when total record count is <= the gate (signal 2 priority)."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    # Create a scan with very small record count.
    scan = _make_scan_node_with_manifest("test_table", 10)  # <= 16
    scan.manifest.estimate_cardinality = lambda col: 100  # High NDV, but total rows <= 16

    plan = LogicalPlan()
    scan_nid = "scan_1"
    plan.add_node(scan_nid, scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1"), _make_identifier_node("col2")]
    agg_node.nid = "agg_1"

    plan.add_node("agg_1", agg_node)
    plan.add_edge(scan_nid, "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan

    context = strategy.visit(agg_node, context)

    # Even with high NDV, the total input rows <= 16, so parvi is safe.
    assert agg_node.group_map_variant == "parvi"


def test_carchar_default_on_missing_manifest():
    """Carchar is the default when scan manifest is None."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = "test_table"
    scan.manifest = None  # No manifest

    plan = LogicalPlan()
    scan_nid = "scan_1"
    plan.add_node(scan_nid, scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1")]
    agg_node.nid = "agg_1"

    plan.add_node("agg_1", agg_node)
    plan.add_edge(scan_nid, "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan

    context = strategy.visit(agg_node, context)

    assert agg_node.group_map_variant == "carchar"


def test_empty_group_by_is_parvi_eligible():
    """GROUP BY with no columns (scalar aggregate) is trivially parvi-eligible."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    scan = _make_scan_node_with_manifest("test_table", 1000)

    plan = LogicalPlan()
    scan_nid = "scan_1"
    plan.add_node(scan_nid, scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = []  # No group-by columns → produces 1 row
    agg_node.nid = "agg_1"

    plan.add_node("agg_1", agg_node)
    plan.add_edge(scan_nid, "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan

    context = strategy.visit(agg_node, context)

    assert agg_node.group_map_variant == "parvi"


def test_groupby_ndv_estimate_stashed_in_native_gate_band():
    """The raw group-count estimate is stored on the node for the native sink's
    parvi gate (kGBParviGateNDV=64) even when it exceeds the Cython
    PARVI_ELIGIBILITY_GATE (40)."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    scan = _make_scan_node_with_manifest("test_table", 1000)
    scan.manifest.estimate_cardinality = lambda col: 7  # 7 * 7 = 49: > 40, <= 64

    plan = LogicalPlan()
    plan.add_node("scan_1", scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1"), _make_identifier_node("col2")]
    agg_node.nid = "agg_1"
    plan.add_node("agg_1", agg_node)
    plan.add_edge("scan_1", "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan
    strategy.visit(agg_node, context)

    # 49 groups: above the Cython eligibility gate (40), in-band for the
    # native gate (64) — the estimate must still be stashed on the node.
    assert agg_node.group_map_variant == "carchar"
    assert agg_node.groupby_ndv_estimate == 49


def test_groupby_ndv_estimate_none_without_stats():
    """No manifest → no estimate → the native gate stays off (fail-safe)."""
    telemetry = QueryTelemetry()
    strategy = HashMapVariantStrategy(telemetry)

    scan = _make_scan_node_with_manifest("test_table", 1000)
    scan.manifest = None

    plan = LogicalPlan()
    plan.add_node("scan_1", scan)

    agg_node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
    agg_node.groups = [_make_identifier_node("col1")]
    agg_node.nid = "agg_1"
    plan.add_node("agg_1", agg_node)
    plan.add_edge("scan_1", "agg_1")

    context = OptimizerContext(plan)
    context.node_id = "agg_1"
    context.pre_optimized_tree = plan
    strategy.visit(agg_node, context)

    assert agg_node.group_map_variant == "carchar"
    assert agg_node.groupby_ndv_estimate is None
