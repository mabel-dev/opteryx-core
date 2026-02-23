# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from opteryx.models import QueryProperties
from opteryx.exceptions import UnsupportedSyntaxError
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


def test_physical_planner_fails_when_draken_not_supported(monkeypatch):
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

    try:
        create_physical_plan(
            _LogicalPlan(node),
            QueryProperties(query_id="test-qid", variables={}),
        )
        assert False, "Expected UnsupportedSyntaxError when Draken mode cannot support aggregate shape"
    except UnsupportedSyntaxError:
        pass


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
