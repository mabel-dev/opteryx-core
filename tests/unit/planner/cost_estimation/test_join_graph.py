"""Unit tests for opteryx.planner.cost_estimation.join_graph."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.cost_estimation import JoinEdge
from opteryx.planner.cost_estimation import JoinGraph
from opteryx.planner.cost_estimation import JoinVertex


def _v(i, name=None, rows=100):
    return JoinVertex(id=i, name=name or f"v{i}", row_count=rows)


def _e(l, r):
    return JoinEdge(left=l, right=r)


def test_rejects_empty_graph():
    with pytest.raises(ValueError):
        JoinGraph(vertices=[], edges=[])


def test_rejects_non_dense_vertex_ids():
    with pytest.raises(ValueError):
        JoinGraph(vertices=[_v(0), _v(2)], edges=[])


def test_rejects_self_loop():
    with pytest.raises(ValueError):
        JoinGraph(vertices=[_v(0), _v(1)], edges=[_e(0, 0)])


def test_neighbors_chain():
    # 0 - 1 - 2 - 3
    g = JoinGraph(
        vertices=[_v(i) for i in range(4)],
        edges=[_e(0, 1), _e(1, 2), _e(2, 3)],
    )
    # Neighbours of {0} = {1}
    assert g.neighbors(0b0001) == 0b0010
    # Neighbours of {0, 2} = {1, 3} (note 1 is a neighbour of 0, not in subset)
    assert g.neighbors(0b0101) == 0b1010
    # Neighbours of full set is empty
    assert g.neighbors(0b1111) == 0


def test_edges_between_chain():
    g = JoinGraph(
        vertices=[_v(i) for i in range(4)],
        edges=[_e(0, 1), _e(1, 2), _e(2, 3)],
    )
    # {0,1} vs {2,3} — only edge 1-2 crosses
    crossing = g.edges_between(0b0011, 0b1100)
    assert len(crossing) == 1
    assert {crossing[0].left, crossing[0].right} == {1, 2}


def test_edges_between_rejects_overlap():
    g = JoinGraph(vertices=[_v(0), _v(1)], edges=[_e(0, 1)])
    with pytest.raises(ValueError):
        g.edges_between(0b11, 0b01)


def test_is_connected_true_and_false():
    # 0 - 1   2 - 3   (two components)
    g = JoinGraph(
        vertices=[_v(i) for i in range(4)],
        edges=[_e(0, 1), _e(2, 3)],
    )
    assert g.is_connected(0b0011)
    assert g.is_connected(0b1100)
    assert not g.is_connected(0b1111)
    assert not g.is_connected(0b0101)


def test_connected_components():
    g = JoinGraph(
        vertices=[_v(i) for i in range(4)],
        edges=[_e(0, 1), _e(2, 3)],
    )
    assert g.connected_components(0b1111) == [0b0011, 0b1100]
