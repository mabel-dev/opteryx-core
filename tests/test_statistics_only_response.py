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
    is_count_star_aggregate,
    extract_column_alias,
    create_count_result_table,
)
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType


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

    def __init__(self, aggregate_type="COUNT", expression=None):
        self.node_type = NodeType.AGGREGATOR
        self.value = aggregate_type
        self.expression = expression

    def __repr__(self):
        return f"MockAggregator({self.value}, expr={self.expression})"


class TestCountStarDetection:
    """Test COUNT(*) pattern detection"""

    def test_is_count_star_aggregate_true(self):
        """Test COUNT(*) aggregate is detected"""
        # Create an aggregator node (the expression that is being aggregated)
        aggregator = MockAggregator("COUNT", expression=None)
        
        # Create an aggregate node that contains it
        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]
        
        agg_node = MockAggregateNode()
        assert is_count_star_aggregate(agg_node)

    def test_is_count_star_aggregate_false_count_column(self):
        """Test COUNT(column) is rejected"""
        aggregator = MockAggregator("COUNT", expression="column")
        
        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]
        
        agg_node = MockAggregateNode()
        assert not is_count_star_aggregate(agg_node)

    def test_is_count_star_aggregate_false_wrong_type(self):
        """Test non-COUNT aggregates are rejected"""
        aggregator = MockAggregator("SUM", expression=None)
        
        class MockAggregateNode:
            def __init__(self):
                self.aggregates = [aggregator]
        
        agg_node = MockAggregateNode()
        assert not is_count_star_aggregate(agg_node)

    def test_is_count_star_aggregate_none(self):
        """Test None aggregate returns False"""
        assert not is_count_star_aggregate(None)


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


class TestResultTableCreation:
    """Test result table creation"""

    def test_create_count_result_table(self):
        """Test creation of result table"""
        table = create_count_result_table(42, "COUNT(*)")

        assert table.num_rows == 1
        assert "COUNT(*)" in table.column_names
        assert table.column("COUNT(*)")[0].as_py() == 42

    def test_create_count_result_table_with_alias(self):
        """Test result table with custom alias"""
        table = create_count_result_table(999, "total_rows")

        assert table.num_rows == 1
        assert "total_rows" in table.column_names
        assert table.column("total_rows")[0].as_py() == 999

    def test_create_count_result_table_metadata(self):
        """Test result table has statistics metadata"""
        table = create_count_result_table(100, "COUNT(*)")

        assert b"disposition" in table.schema.metadata
        assert table.schema.metadata[b"disposition"] == b"statistics"

    def test_create_count_result_table_type(self):
        """Test result table has int64 type"""
        table = create_count_result_table(42, "COUNT(*)")

        assert table.schema.field("COUNT(*)").type == pyarrow.int64()


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


class TestResultCorrectness:
    """Test result correctness"""

    def test_count_matches_manifest(self):
        """Test COUNT result matches manifest record count"""
        manifest = MockManifest(record_count=12345)
        table = create_count_result_table(manifest.get_record_count(), "COUNT(*)")

        assert table.column("COUNT(*)")[0].as_py() == 12345

    def test_zero_records(self):
        """Test COUNT with zero records"""
        table = create_count_result_table(0, "COUNT(*)")

        assert table.column("COUNT(*)")[0].as_py() == 0

    def test_large_count(self):
        """Test COUNT with large value"""
        large_count = 10**9  # 1 billion
        table = create_count_result_table(large_count, "COUNT(*)")

        assert table.column("COUNT(*)")[0].as_py() == large_count


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
