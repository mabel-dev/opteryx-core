# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.physical_planner import create_physical_plan
import opteryx.planner.physical_planner as physical_planner


class _Aggregate:
    def __init__(self, value: str, duplicate_treatment=None):
        self.value = value
        self.duplicate_treatment = duplicate_treatment


class _LogicalNode:
    def __init__(self, node_type, properties):
        self.node_type = node_type
        self.properties = properties
        self.manifest = {}


class _LogicalPlan:
    def __init__(self, node):
        self._node = node

    def nodes(self, data=True):
        if data:
            return [(1, self._node)]
        return [1]

    def edges(self):
        return []


class _BaseDummyNode:
    def __init__(self, query_properties, **parameters):
        self.query_properties = query_properties
        self.parameters = parameters


class _DummyDrakenAggregateAndGroupNode(_BaseDummyNode):
    @staticmethod
    def supports(aggregates, groups=None):
        _ = aggregates, groups
        return True


class _DummyDrakenAggregateNode(_BaseDummyNode):
    @staticmethod
    def supports(aggregates, groups=None):
        _ = aggregates, groups
        return True


class _DummyDrakenInnerJoinNode(_BaseDummyNode):
    @staticmethod
    def supports(**parameters):
        _ = parameters
        return True

def test_physical_planner_uses_draken_aggregate_and_group_when_flag_enabled(monkeypatch):
    # group-by column details (type/nullable) are not inspected by our dummy
    # node, so this test still works for nullability coverage.
    node = _LogicalNode(
        LogicalPlanStepType.AggregateAndGroup,
        properties={
            "aggregates": [_Aggregate("COUNT")],
            "groups": [],
            "projection": [],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _DummyDrakenAggregateAndGroupNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenAggregateAndGroupNode)


def test_physical_planner_uses_draken_aggregate_when_flag_enabled(monkeypatch):
    node = _LogicalNode(
        LogicalPlanStepType.Aggregate,
        properties={
            "aggregates": [_Aggregate("COUNT_DISTINCT")],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateNode",
        _DummyDrakenAggregateNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenAggregateNode)


def test_supports_accepts_nullable_group_column():
    """DrakenAggregateAndGroupNode.supports() should still return True when the
    grouping column is nullable; null-key handling is implemented in the kernel."""

    from opteryx.operators.draken_aggregate_and_group_node import DrakenAggregateAndGroupNode
    from opteryx.expression import NodeType
    from orso.types import OrsoTypes

    class FieldParam:
        node_type = NodeType.WILDCARD

    class Agg:
        value = "COUNT"
        duplicate_treatment = None
        parameters = [FieldParam()]
        class schema_column:
            identity = b'col'

    class Group:
        node_type = NodeType.IDENTIFIER
        class schema_column:
            type = OrsoTypes.INTEGER
            nullable = True
            identity = b'key'
    # We don't need to patch anything; nullable keys should pass.
    assert DrakenAggregateAndGroupNode.supports([Agg()], [Group()])


def test_draken_supports_max_in_fast_path():
    """The draken planner should accept ``MAX`` aggregates when grouping."""

    from opteryx.operators.draken_aggregate_and_group_node import DrakenAggregateAndGroupNode
    from opteryx.expression import NodeType

    class FieldParam:
        node_type = NodeType.WILDCARD

    class Agg:
        value = "MAX"
        duplicate_treatment = None
        parameters = [FieldParam()]
        class schema_column:
            identity = b'val'

    class Group:
        node_type = NodeType.IDENTIFIER
        class schema_column:
            identity = b'key'

    # sanity check constant
    assert "MAX" in DrakenAggregateAndGroupNode.FAST_PATH_AGGREGATES
    assert DrakenAggregateAndGroupNode.supports([Agg()], [Group()])


def test_physical_planner_errors_when_draken_not_supported(monkeypatch):
    """Unsupported grouped aggregate shapes should fail cleanly."""

    class _UnsupportedDrakenNode(_DummyDrakenAggregateAndGroupNode):
        @staticmethod
        def supports(aggregates, groups=None):
            _ = aggregates, groups
            return False

    node = _LogicalNode(
        LogicalPlanStepType.AggregateAndGroup,
        properties={
            "aggregates": [_Aggregate("COUNT")],
            "groups": [],
            "projection": [],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _UnsupportedDrakenNode,
    )

    with pytest.raises(UnsupportedSyntaxError):
        create_physical_plan(
            _LogicalPlan(node),
            QueryProperties(query_id="test-qid", variables={}),
        )


def test_physical_planner_errors_for_aggregate_when_draken_not_supported(monkeypatch):
    class _UnsupportedDrakenNode(_DummyDrakenAggregateNode):
        @staticmethod
        def supports(aggregates, groups=None):
            _ = aggregates, groups
            return False

    node = _LogicalNode(
        LogicalPlanStepType.Aggregate,
        properties={
            "aggregates": [_Aggregate("HISTOGRAM")],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateNode",
        _UnsupportedDrakenNode,
    )

    with pytest.raises(UnsupportedSyntaxError):
        create_physical_plan(
            _LogicalPlan(node),
            QueryProperties(query_id="test-qid", variables={}),
        )


def test_physical_planner_uses_draken_aggregate_and_group(monkeypatch):
    node = _LogicalNode(
        LogicalPlanStepType.AggregateAndGroup,
        properties={
            "aggregates": [_Aggregate("SUM")],
            "groups": [],
            "projection": [],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _DummyDrakenAggregateAndGroupNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenAggregateAndGroupNode)


def test_physical_planner_uses_draken_aggregate(monkeypatch):
    node = _LogicalNode(
        LogicalPlanStepType.Aggregate,
        properties={
            "aggregates": [_Aggregate("COUNT")],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateNode",
        _DummyDrakenAggregateNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenAggregateNode)


def test_physical_planner_uses_draken_inner_join(monkeypatch):
    node = _LogicalNode(
        LogicalPlanStepType.Join,
        properties={
            "type": "inner",
            "left_columns": [],
            "right_columns": [],
            "left_relation_names": [],
            "right_relation_names": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenInnerJoinNode",
        _DummyDrakenInnerJoinNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenInnerJoinNode)


def test_physical_planner_errors_when_draken_inner_join_not_supported(monkeypatch):
    class _UnsupportedDrakenInnerJoinNode(_DummyDrakenInnerJoinNode):
        @staticmethod
        def supports(**parameters):
            _ = parameters
            return False

    node = _LogicalNode(
        LogicalPlanStepType.Join,
        properties={
            "type": "inner",
            "left_columns": [],
            "right_columns": [],
            "left_relation_names": [],
            "right_relation_names": [],
        },
    )

    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenInnerJoinNode",
        _UnsupportedDrakenInnerJoinNode,
    )

    with pytest.raises(UnsupportedSyntaxError):
        create_physical_plan(
            _LogicalPlan(node),
            QueryProperties(query_id="test-qid", variables={}),
        )
