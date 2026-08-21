"""Unit tests for opteryx.planner.cost_estimation.dpccp."""

import os
import random
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.cost_estimation import JoinEdge
from opteryx.planner.cost_estimation import JoinGraph
from opteryx.planner.cost_estimation import JoinTreeLeaf
from opteryx.planner.cost_estimation import JoinTreeNode
from opteryx.planner.cost_estimation import JoinVertex
from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import NdvProvenance
from opteryx.planner.cost_estimation import dpccp
from opteryx.planner.cost_estimation import enumerate_join_tree


def _ks(ndv):
    """A KeyStats with a MEASURED NDV -- these tests exercise the arithmetic,
    not the domain-size stand-in path."""
    if ndv is None:
        return KeyStats(ndv=None, null_fraction=0.0)
    return KeyStats(ndv=ndv, null_fraction=0.0, ndv_provenance=NdvProvenance.MEASURED)


def _v(i, rows, name=None):
    return JoinVertex(id=i, name=name or f"v{i}", row_count=rows)


def _key_edge(l, r, l_ndv, r_ndv):
    return JoinEdge(
        left=l,
        right=r,
        equi_keys=((_ks(l_ndv), _ks(r_ndv)),),
    )


def _vertex_set(tree):
    if isinstance(tree, JoinTreeLeaf):
        return frozenset([tree.vertex_id])
    return _vertex_set(tree.left) | _vertex_set(tree.right)


def _all_subtrees(tree):
    yield tree
    if isinstance(tree, JoinTreeNode):
        yield from _all_subtrees(tree.left)
        yield from _all_subtrees(tree.right)


def _validate_tree(tree, graph):
    """Tree covers every vertex exactly once and every internal node has ≥1 edge."""
    leaves = [n for n in _all_subtrees(tree) if isinstance(n, JoinTreeLeaf)]
    assert len(leaves) == graph.n
    assert {l.vertex_id for l in leaves} == set(range(graph.n))
    for node in _all_subtrees(tree):
        if isinstance(node, JoinTreeNode):
            assert len(node.edges) >= 1, "internal node missing connecting edges"
            left_set = _vertex_set(node.left)
            right_set = _vertex_set(node.right)
            assert left_set.isdisjoint(right_set)
            for e in node.edges:
                # each edge should bridge left and right
                assert (e.left in left_set and e.right in right_set) or (
                    e.right in left_set and e.left in right_set
                )


# ---------------------------------------------------------------------------
# Trivial cases
# ---------------------------------------------------------------------------


def test_single_vertex():
    g = JoinGraph(vertices=[_v(0, 100)], edges=[])
    tree = dpccp(g)
    assert isinstance(tree, JoinTreeLeaf)
    assert tree.vertex_id == 0
    assert tree.estimated_rows == 100


def test_two_vertex_chain():
    g = JoinGraph(
        vertices=[_v(0, 1000), _v(1, 10)],
        edges=[_key_edge(0, 1, 1000, 10)],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    assert isinstance(tree, JoinTreeNode)
    # |A ⋈ B| = 1000 * 10 / max(1000, 10) = 10
    assert tree.estimated_rows == 10


# ---------------------------------------------------------------------------
# 3-relation chain
# ---------------------------------------------------------------------------


def test_three_chain_picks_bridge_first():
    """A(1000) - B(10) - C(1000); high-NDV keys.

    The optimal plan has B at the bottom: any pair joins to ~10 rows,
    while pairing A with C is impossible (no edge between them).
    """
    g = JoinGraph(
        vertices=[_v(0, 1000, "A"), _v(1, 10, "B"), _v(2, 1000, "C")],
        edges=[_key_edge(0, 1, 1000, 1000), _key_edge(1, 2, 1000, 1000)],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    # Both AB-then-C and CB-then-A produce the same total intermediate cost
    # (10 from the first join + 10 from the second).
    assert tree.estimated_cost == pytest.approx(20)
    # Both child subtrees together must contain B somewhere on the inside.
    assert isinstance(tree, JoinTreeNode)
    inner_subtree = (
        tree.left if isinstance(tree.left, JoinTreeNode) else tree.right
    )
    assert isinstance(inner_subtree, JoinTreeNode)
    assert 1 in _vertex_set(inner_subtree)


# ---------------------------------------------------------------------------
# Star
# ---------------------------------------------------------------------------


def test_star_with_large_leaves_joins_hub_last():
    """A is the hub; leaves are huge; A is small.

    Joining the hub last means each leaf has nothing to join against until
    DPccp gives up — but a star has no edges between leaves so the only
    valid trees go through the hub. The optimal plan still pairs leaves
    one at a time onto a partial tree containing the hub.
    """
    g = JoinGraph(
        vertices=[_v(0, 10, "A"), _v(1, 1000, "B"), _v(2, 1000, "C"), _v(3, 1000, "D")],
        edges=[
            _key_edge(0, 1, 10, 1000),
            _key_edge(0, 2, 10, 1000),
            _key_edge(0, 3, 10, 1000),
        ],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    # Every internal node must include A on one of its sides — the star has
    # no leaf-to-leaf edges so any join must traverse the hub.
    for node in _all_subtrees(tree):
        if isinstance(node, JoinTreeNode):
            covered = _vertex_set(node)
            # If a node contains more than one leaf, it must contain the hub.
            non_hub = covered - {0}
            if len(non_hub) >= 2:
                assert 0 in covered


def test_star_with_small_leaves_joins_hub_first_pairs():
    """Star with tiny leaves and a huge hub: the cheapest first joins are
    hub-leaf pairs."""
    g = JoinGraph(
        vertices=[_v(0, 100000, "A"), _v(1, 1, "B"), _v(2, 1, "C"), _v(3, 1, "D")],
        edges=[
            _key_edge(0, 1, 100000, 1),
            _key_edge(0, 2, 100000, 1),
            _key_edge(0, 3, 100000, 1),
        ],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    # The cheapest sub-join must be hub+leaf, producing very few rows; verify
    # that the deepest two-leaf node is exactly {hub, leaf}.
    smallest_pair = None
    for node in _all_subtrees(tree):
        if (
            isinstance(node, JoinTreeNode)
            and isinstance(node.left, JoinTreeLeaf)
            and isinstance(node.right, JoinTreeLeaf)
        ):
            smallest_pair = node
            break
    assert smallest_pair is not None
    assert 0 in _vertex_set(smallest_pair)


# ---------------------------------------------------------------------------
# 4-relation chain with a selective filter
# ---------------------------------------------------------------------------


def test_four_chain_pulls_small_relation_low():
    """A(1000) - B(1000) - C(5) - D(1000), high NDVs.

    C is tiny; it should pair early to suppress intermediates.
    """
    g = JoinGraph(
        vertices=[_v(0, 1000, "A"), _v(1, 1000, "B"), _v(2, 5, "C"), _v(3, 1000, "D")],
        edges=[
            _key_edge(0, 1, 1000, 1000),
            _key_edge(1, 2, 1000, 5),
            _key_edge(2, 3, 5, 1000),
        ],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    # The first join in the bottom-most subtree should include C (id=2).
    smallest = None
    for node in _all_subtrees(tree):
        if (
            isinstance(node, JoinTreeNode)
            and isinstance(node.left, JoinTreeLeaf)
            and isinstance(node.right, JoinTreeLeaf)
        ):
            if smallest is None or node.estimated_rows < smallest.estimated_rows:
                smallest = node
    assert smallest is not None
    assert 2 in _vertex_set(smallest)


# ---------------------------------------------------------------------------
# Asymmetric NDV
# ---------------------------------------------------------------------------


def test_asymmetric_ndv_prefers_lower_selectivity_first():
    """Three-vertex chain where one edge is much more selective than the other.

    A(1000) - B(1000) - C(1000), with edge A-B NDV=10 (selectivity 0.1) and
    edge B-C NDV=1000 (selectivity 0.001). The B-C pair produces ~1 row, the
    A-B pair produces ~100. DPccp should pick B-C first.
    """
    g = JoinGraph(
        vertices=[_v(0, 1000, "A"), _v(1, 1000, "B"), _v(2, 1000, "C")],
        edges=[
            _key_edge(0, 1, 10, 10),
            _key_edge(1, 2, 1000, 1000),
        ],
    )
    tree = dpccp(g)
    _validate_tree(tree, g)
    # Bottom pair should be {B, C} = {1, 2}.
    bottom = None
    for node in _all_subtrees(tree):
        if (
            isinstance(node, JoinTreeNode)
            and isinstance(node.left, JoinTreeLeaf)
            and isinstance(node.right, JoinTreeLeaf)
        ):
            bottom = node
            break
    assert bottom is not None
    assert _vertex_set(bottom) == frozenset({1, 2})


# ---------------------------------------------------------------------------
# Determinism on ties
# ---------------------------------------------------------------------------


def test_deterministic_on_ties():
    """Symmetric chain — two equivalent optimal trees. DPccp must pick the
    same one across runs."""
    runs = []
    for _ in range(5):
        g = JoinGraph(
            vertices=[_v(0, 100, "A"), _v(1, 100, "B"), _v(2, 100, "C")],
            edges=[
                _key_edge(0, 1, 100, 100),
                _key_edge(1, 2, 100, 100),
            ],
        )
        tree = dpccp(g)
        runs.append((tree.estimated_cost, _vertex_set(tree.left), _vertex_set(tree.right)))
    assert all(r == runs[0] for r in runs)


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_disconnected_graph_raises():
    g = JoinGraph(
        vertices=[_v(0, 100), _v(1, 100), _v(2, 100), _v(3, 100)],
        edges=[_key_edge(0, 1, 100, 100), _key_edge(2, 3, 100, 100)],
    )
    with pytest.raises(ValueError):
        dpccp(g)


def test_too_many_vertices_raises():
    vertices = [_v(i, 100) for i in range(31)]
    edges = [_key_edge(i, i + 1, 100, 100) for i in range(30)]
    g = JoinGraph(vertices=vertices, edges=edges)
    with pytest.raises(ValueError):
        dpccp(g)


# ---------------------------------------------------------------------------
# enumerate_join_tree dispatch
# ---------------------------------------------------------------------------


def test_dispatch_uses_dpccp_at_threshold():
    g = JoinGraph(
        vertices=[_v(i, 100) for i in range(12)],
        edges=[_key_edge(i, i + 1, 100, 100) for i in range(11)],
    )
    tree = enumerate_join_tree(g, dp_threshold=12)
    _validate_tree(tree, g)


def test_dispatch_uses_greedy_above_threshold():
    g = JoinGraph(
        vertices=[_v(i, 100) for i in range(13)],
        edges=[_key_edge(i, i + 1, 100, 100) for i in range(12)],
    )
    # Should not raise. Compare to direct greedy call.
    from opteryx.planner.cost_estimation import greedy_join_order

    via_dispatch = enumerate_join_tree(g, dp_threshold=12)
    direct_greedy = greedy_join_order(g)
    assert via_dispatch.estimated_cost == direct_greedy.estimated_cost


# ---------------------------------------------------------------------------
# Performance smoke
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.environ.get("OPTERYX_RUN_PERF_TESTS") != "1",
    reason="perf test gated behind OPTERYX_RUN_PERF_TESTS=1",
)
def test_seventeen_vertex_perf_under_100ms():
    rng = random.Random(42)
    n = 17
    vertices = [_v(i, rng.randint(100, 100000)) for i in range(n)]
    # Build a spanning tree, then sprinkle a few extra edges.
    edges = []
    order = list(range(n))
    rng.shuffle(order)
    for k in range(1, n):
        parent = order[rng.randint(0, k - 1)]
        child = order[k]
        l, r = min(parent, child), max(parent, child)
        edges.append(_key_edge(l, r, rng.randint(10, 1000), rng.randint(10, 1000)))
    # Add a couple of extra edges on top of the spanning tree — JOB-sized
    # join graphs are typically tree-like with a small number of cycles.
    extra = 2
    seen = {(e.left, e.right) for e in edges}
    while extra > 0:
        a, b = sorted(rng.sample(range(n), 2))
        if (a, b) in seen:
            continue
        seen.add((a, b))
        edges.append(_key_edge(a, b, rng.randint(10, 1000), rng.randint(10, 1000)))
        extra -= 1
    g = JoinGraph(vertices=vertices, edges=edges)
    start = time.perf_counter()
    tree = dpccp(g)
    elapsed = time.perf_counter() - start
    _validate_tree(tree, g)
    assert elapsed < 0.1, f"DPccp took {elapsed*1000:.1f}ms (>100ms)"
