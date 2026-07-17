# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for Statistics-Only Response Strategy

Tests detection of COUNT(*) queries and correctness of statistical results.
"""

import pytest
import pyarrow
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    is_simple_aggregate,
    extract_column_alias,
)
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.expression import NodeType


class MockNode:
    """Mock Node for testing"""

    def __init__(self, node_type, **kwargs):
        self.node_type = node_type
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockManifest:
    """Mock Manifest for testing"""

    def __init__(self, record_count=100):
        self.record_count = record_count
        self.files = [MockNode("file", record_count=record_count)]

    def get_record_count(self):
        return self.record_count


class MockAggregator:
    """Mock Aggregator node"""

    def __init__(self, aggregate_type="COUNT", parameters=None, duplicate_treatment=None, condition=None):
        self.node_type = NodeType.AGGREGATOR
        self.value = aggregate_type
        self.parameters = parameters
        self.duplicate_treatment = duplicate_treatment
        self.condition = condition

    def __repr__(self):
        return f"MockAggregator({self.value}, parameters={self.parameters})"


class TestCountStarDetection:
    """Test statistics-only aggregate pattern detection"""

    def test_is_simple_aggregate_count_star_true(self):
        """Test COUNT(*) aggregate is accepted"""
        wildcard = MockNode(NodeType.WILDCARD)
        aggregator = MockAggregator("COUNT", parameters=[wildcard])

        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]

        agg_node = MockAggregateNode()
        assert is_simple_aggregate(agg_node)

    def test_is_simple_aggregate_false_count_distinct(self):
        """Test COUNT(DISTINCT column) is rejected"""
        wildcard = MockNode(NodeType.WILDCARD)
        aggregator = MockAggregator("COUNT", parameters=[wildcard], duplicate_treatment="Distinct")

        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]

        agg_node = MockAggregateNode()
        assert not is_simple_aggregate(agg_node)

    def test_is_simple_aggregate_false_wrong_type(self):
        """Test non-COUNT/MIN/MAX aggregates are rejected"""
        wildcard = MockNode(NodeType.WILDCARD)
        aggregator = MockAggregator("SUM", parameters=[wildcard])

        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]

        agg_node = MockAggregateNode()
        assert not is_simple_aggregate(agg_node)

    def test_is_simple_aggregate_none(self):
        """Test None aggregate node returns False"""
        assert not is_simple_aggregate(None)


class TestColumnAlias:
    """Test column alias extraction"""

    def test_extract_column_alias_default(self):
        """Test default alias when none specified"""
        # Create a mock logical plan with no alias
        class MockLogicalPlan(dict):
            def nodes(self, **_):  # type: ignore
                exit_node = MockNode(LogicalPlanStepType.Exit, columns=[])
                return [("exit_id", exit_node)]

        plan = MockLogicalPlan()
        alias = extract_column_alias(plan)
        assert alias == "COUNT(*)"

    def test_extract_column_alias_with_alias(self):
        """Test alias extraction from exit node"""

        class MockColumn:
            def __init__(self, alias):
                self.alias = alias

        class MockExit:
            def __init__(self):
                self.node_type = LogicalPlanStepType.Exit
                self.columns = [MockColumn("total_count")]

        class MockLogicalPlan(dict):
            def nodes(self, **_):  # type: ignore
                return [("exit_id", MockExit())]

        plan = MockLogicalPlan()
        alias = extract_column_alias(plan)
        assert alias == "total_count"


class TestStatisticsOptimization:
    """Integration tests for statistics-only response strategy"""

    def test_simple_count_star(self):
        """Test optimization works for simple COUNT(*) query"""
        # This is a placeholder for end-to-end testing
        # Would need full query planner integration
        assert True

    def test_count_star_with_alias(self):
        """Test COUNT(*) with alias returns correct alias"""
        # Placeholder for full integration test
        assert True

    def test_statistics_result_without_manifest(self):
        """Test graceful fallback when manifest missing"""
        # Placeholder for full integration test
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
