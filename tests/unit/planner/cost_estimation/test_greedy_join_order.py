"""Unit tests for opteryx.planner.cost_estimation.greedy_join_order."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.cost_estimation import JoinEdge
from opteryx.planner.cost_estimation import JoinGraph
from opteryx.planner.cost_estimation import JoinTreeLeaf
from opteryx.planner.cost_estimation import JoinTreeNode
from opteryx.planner.cost_estimation import JoinVertex
from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import greedy_join_order


def _v(i, rows, name=None):
    return JoinVertex(id=i, name=name or f"v{i}", row_count=rows)


def _key_edge(l, r, l_ndv, r_ndv):
    return JoinEdge(
        left=l,
        right=r,
        equi_keys=((KeyStats(ndv=l_ndv, null_fraction=0.0), KeyStats(ndv=r_ndv, null_fraction=0.0)),),
    )


def _all_subtrees(tree):
    yield tree
    if isinstance(tree, JoinTreeNode):
        yield from _all_subtrees(tree.left)
        yield from _all_subtrees(tree.right)


def _vertex_set(tree):
    if isinstance(tree, JoinTreeLeaf):
        return frozenset([tree.vertex_id])
    return _vertex_set(tree.left) | _vertex_set(tree.right)


def _validate_tree(tree, n):
    leaves = [n_ for n_ in _all_subtrees(tree) if isinstance(n_, JoinTreeLeaf)]
    assert {l.vertex_id for l in leaves} == set(range(n))
    for node in _all_subtrees(tree):
        if isinstance(node, JoinTreeNode):
            assert len(node.edges) >= 1
            assert _vertex_set(node.left).isdisjoint(_vertex_set(node.right))


def test_greedy_two_chain():
    g = JoinGraph(
        vertices=[_v(0, 1000), _v(1, 10)],
        edges=[_key_edge(0, 1, 1000, 10)],
    )
    tree = greedy_join_order(g)
    _validate_tree(tree, 2)


def test_greedy_chain_produces_valid_tree():
    g = JoinGraph(
        vertices=[_v(i, 100) for i in range(6)],
        edges=[_key_edge(i, i + 1, 100, 100) for i in range(5)],
    )
    tree = greedy_join_order(g)
    _validate_tree(tree, 6)


def test_greedy_picks_cheapest_seed_pair():
    """A tiny pair connected by a high-NDV edge should seed the tree."""
    # 0-1 with NDV=10 → produces 100 rows
    # 2-3 with NDV=1000 → produces 1 row
    # bridge 1-2
    g = JoinGraph(
        vertices=[_v(0, 100, "A"), _v(1, 100, "B"), _v(2, 100, "C"), _v(3, 100, "D")],
        edges=[
            _key_edge(0, 1, 10, 10),
            _key_edge(1, 2, 100, 100),
            _key_edge(2, 3, 1000, 1000),
        ],
    )
    tree = greedy_join_order(g)
    _validate_tree(tree, 4)
    # Find the deepest pair-of-leaves node.
    pair = None
    for node in _all_subtrees(tree):
        if (
            isinstance(node, JoinTreeNode)
            and isinstance(node.left, JoinTreeLeaf)
            and isinstance(node.right, JoinTreeLeaf)
        ):
            pair = node
            break
    assert pair is not None
    assert _vertex_set(pair) == frozenset({2, 3})


def test_greedy_disconnected_falls_back_to_cartesian():
    g = JoinGraph(
        vertices=[_v(0, 10), _v(1, 10), _v(2, 10), _v(3, 10)],
        edges=[_key_edge(0, 1, 10, 10), _key_edge(2, 3, 10, 10)],
    )
    tree = greedy_join_order(g)
    _validate_tree(tree, 4)


def test_greedy_deterministic_on_ties():
    g = JoinGraph(
        vertices=[_v(i, 100) for i in range(5)],
        edges=[_key_edge(i, i + 1, 100, 100) for i in range(4)],
    )
    a = greedy_join_order(g)
    b = greedy_join_order(g)
    assert a.estimated_cost == b.estimated_cost
    assert _vertex_set(a.left) == _vertex_set(b.left)
    assert _vertex_set(a.right) == _vertex_set(b.right)
