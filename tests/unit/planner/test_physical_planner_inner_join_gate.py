# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The physical planner builds an INNER join only when `_inner_join_supported`
accepts the join's shape, and refuses it (NotSupportedError) otherwise."""

import pytest

from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties
from opteryx.planner.physical_planner import create_physical_plan
from opteryx.planner.plan_context import PlanContext
import opteryx.planner.physical_planner as physical_planner
from opteryx.compiled.structures.plan_steps import JoinStep


class _LogicalPlan:
    def __init__(self, node):
        self._node = node

    def nodes(self, data=True):
        if data:
            return [(1, self._node)]
        return [1]

    def edges(self):
        return []


def _inner_join_node():
    return JoinStep(
        type="inner",
        left_columns=[],
        right_columns=[],
        left_relation_names=[],
        right_relation_names=[],
    )


def test_physical_planner_uses_draken_inner_join(monkeypatch):
    monkeypatch.setattr(physical_planner, "_inner_join_supported", lambda join: True)

    node = _inner_join_node()
    plan = create_physical_plan(
        _LogicalPlan(node),
        QueryProperties(query_id="test-qid", variables={}),
        PlanContext(),
    )
    assert plan[1].kind == "DrakenInnerJoinNode"
    assert plan[1].join_type == "inner"
    # The typed logical step itself, plus the output-rows estimate the physical
    # planner computes (None: nothing in this PlanContext estimated the join).
    assert plan[1].step is node
    assert plan[1].join_output_rows_estimate is None


def test_physical_planner_errors_when_draken_inner_join_not_supported(monkeypatch):
    monkeypatch.setattr(physical_planner, "_inner_join_supported", lambda join: False)

    with pytest.raises(NotSupportedError):
        create_physical_plan(
            _LogicalPlan(_inner_join_node()),
            QueryProperties(query_id="test-qid", variables={}),
            PlanContext(),
        )
