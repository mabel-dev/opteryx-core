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
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext
import opteryx.planner.physical_planner as physical_planner
from opteryx.compiled.structures.plan_steps import JoinStep


def _one_node_plan(node):
    """(logical plan, its PlanContext, the node's id): a plan holding just `node`."""
    plan_context = PlanContext()
    plan = LogicalPlan(plan_context)
    return plan, plan_context, plan.add_node(node)


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
    logical, plan_context, nid = _one_node_plan(node)
    plan = create_physical_plan(
        logical,
        QueryProperties(query_id="test-qid", variables={}),
        plan_context,
    )
    assert plan[nid].kind == "DrakenInnerJoinNode"
    assert plan[nid].join_type == "inner"
    # The typed logical step itself, plus the output-rows estimate the physical
    # planner computes (None: nothing in this PlanContext estimated the join).
    assert plan[nid].step is node
    assert plan[nid].join_output_rows_estimate is None


def test_physical_planner_errors_when_draken_inner_join_not_supported(monkeypatch):
    monkeypatch.setattr(physical_planner, "_inner_join_supported", lambda join: False)

    logical, plan_context, _nid = _one_node_plan(_inner_join_node())
    with pytest.raises(NotSupportedError):
        create_physical_plan(
            logical,
            QueryProperties(query_id="test-qid", variables={}),
            plan_context,
        )
