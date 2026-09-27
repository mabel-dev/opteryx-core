"""
Test basic functionality of the execution tree
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.physical_plan import PhysicalPlan
from opteryx.planner.plan_context import PlanContext


def test_execution_tree():
    et = PhysicalPlan(PlanContext())
    a = et.add_node(None)
    b = et.add_node(None)
    et.add_edge(a, b)

    assert et.is_acyclic()
    assert et.get_exit_points() == [b]

    c = et.add_node(None)
    et.add_edge(b, c)
    et.add_edge(c, a)

    assert not et.is_acyclic()
    assert et.get_exit_points() == []


def test_edge_roles_are_left_right_or_none():
    et = PhysicalPlan(PlanContext())
    a = et.add_node(None)
    b = et.add_node(None)
    import pytest

    from opteryx.exceptions import InvalidInternalStateError

    with pytest.raises(InvalidInternalStateError):
        et.add_edge(a, b, "forwards")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
