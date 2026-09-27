"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models import PhysicalPlan
from opteryx.planner.plan_context import PlanContext


def test_linear_execution_tree():
    """
    Test an execution tree where each item has no more than one incoming edge
    """
    tree = PhysicalPlan(PlanContext())
    p = tree.add_node(print)
    m = tree.add_node(max)
    tree.add_edge(p, m)

    assert len(tree.nodes()) == 2
    assert tree.nodes() == [p, m]
    assert tree[p] == print
    assert len(tree.edges()) == 1
    assert tree.get_exit_points() == [m]
    assert tree.is_acyclic()
    assert tree.outgoing_edges(p) == [(p, m, None)], tree.outgoing_edges(p)
    assert tree.ingoing_edges(m) == [(p, m, None)], tree.ingoing_edges(m)

    n = tree.add_node(min)
    tree.add_edge(m, n)

    assert len(tree.nodes()) == 3
    assert tree.nodes() == [p, m, n]
    assert len(tree.edges()) == 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
