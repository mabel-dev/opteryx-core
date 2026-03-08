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


class _DummySimpleAggregateAndGroupNode(_BaseDummyNode):
    SIMPLE_AGGREGATES = {"COUNT"}


class _DummyArrowAggregateAndGroupNode(_BaseDummyNode):
    pass


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

    monkeypatch.setattr(physical_planner, "USE_DRAKEN_AGGREGATOR", True)
    monkeypatch.setattr(physical_planner, "ENABLE_NATIVE_AGGREGATOR", True)
    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _DummyDrakenAggregateAndGroupNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "SimpleAggregateAndGroupNode",
        _DummySimpleAggregateAndGroupNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "AggregateAndGroupNode",
        _DummyArrowAggregateAndGroupNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyDrakenAggregateAndGroupNode)


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


def test_physical_planner_errors_when_draken_not_supported(monkeypatch):
    """When the DRAKEN flag is enabled the planner must *not* fall back to any
    Python-based aggregate implementation.  If Draken.supports() returns False the
    planner should raise an UnsupportedSyntaxError so callers see a clear, clean
    failure rather than silently routing through the legacy path."""

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

    monkeypatch.setattr(physical_planner, "USE_DRAKEN_AGGREGATOR", True)
    monkeypatch.setattr(physical_planner, "ENABLE_NATIVE_AGGREGATOR", True)
    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _UnsupportedDrakenNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "SimpleAggregateAndGroupNode",
        _DummySimpleAggregateAndGroupNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "AggregateAndGroupNode",
        _DummyArrowAggregateAndGroupNode,
    )

    with pytest.raises(UnsupportedSyntaxError):
        create_physical_plan(
            _LogicalPlan(node),
            QueryProperties(query_id="test-qid", variables={}),
        )


def test_physical_planner_uses_arrow_aggregate_and_group_when_flag_disabled(monkeypatch):
    node = _LogicalNode(
        LogicalPlanStepType.AggregateAndGroup,
        properties={
            "aggregates": [_Aggregate("SUM")],
            "groups": [],
            "projection": [],
            "all_relations": [],
        },
    )

    monkeypatch.setattr(physical_planner, "USE_DRAKEN_AGGREGATOR", False)
    monkeypatch.setattr(physical_planner, "ENABLE_NATIVE_AGGREGATOR", False)
    monkeypatch.setattr(
        physical_planner.operators,
        "DrakenAggregateAndGroupNode",
        _DummyDrakenAggregateAndGroupNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "SimpleAggregateAndGroupNode",
        _DummySimpleAggregateAndGroupNode,
    )
    monkeypatch.setattr(
        physical_planner.operators,
        "AggregateAndGroupNode",
        _DummyArrowAggregateAndGroupNode,
    )

    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
    )
    assert isinstance(plan[1], _DummyArrowAggregateAndGroupNode)
