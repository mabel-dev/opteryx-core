# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The physical planner builds an INNER join only when `DrakenInnerJoinNode.supports`
accepts the join's shape, and refuses it (NotSupportedError) otherwise."""

import pytest

from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.physical_planner import create_physical_plan
from opteryx.planner.plan_context import PlanContext
import opteryx.planner.physical_planner as physical_planner


class _LogicalPlan:
    def __init__(self, node):
        self._node = node

    def nodes(self, data=True):
        if data:
            return [(1, self._node)]
        return [1]

    def edges(self):
        return []


class _CreatedNode:
    def __init__(self, name, query_properties, **parameters):
        self.name = name
        self.query_properties = query_properties
        self.parameters = parameters


class _RecordingRegistry:
    def create(self, name, query_properties, **parameters):
        return _CreatedNode(name, query_properties, **parameters)


def _inner_join_node():
    return LogicalPlanNode(
        node_type=LogicalPlanStepType.Join,
        type="inner",
        left_columns=[],
        right_columns=[],
        left_relation_names=[],
        right_relation_names=[],
    )


def _join_gate(supported: bool):
    class _Gate:
        @staticmethod
        def supports(**parameters):
            _ = parameters
            return supported

    return _Gate


def test_physical_planner_uses_draken_inner_join(monkeypatch):
    monkeypatch.setattr(physical_planner, "DrakenInnerJoinNode", _join_gate(True))
    monkeypatch.setattr(physical_planner, "get_registry", _RecordingRegistry)

    node = _inner_join_node()
    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
        PlanContext(),
    )
    assert plan[1].name == "Inner Join"
    # The node's own properties, plus the output-rows estimate the physical planner
    # computes (None: nothing in this PlanContext estimated the join).
    assert plan[1].parameters == {**node.properties, "join_output_rows_estimate": None}


def test_physical_planner_errors_when_draken_inner_join_not_supported(monkeypatch):
    monkeypatch.setattr(physical_planner, "DrakenInnerJoinNode", _join_gate(False))
    monkeypatch.setattr(physical_planner, "get_registry", _RecordingRegistry)

    with pytest.raises(NotSupportedError):
        create_physical_plan(
            _LogicalPlan(_inner_join_node()),
            QueryProperties(query_id="test-qid", variables={}),
            PlanContext(),
        )
