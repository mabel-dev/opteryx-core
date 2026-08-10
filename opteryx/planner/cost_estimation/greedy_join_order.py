"""Greedy join enumerator — fallback for graphs above the DPccp threshold.

The algorithm is the classical greedy operator-tree builder:

1. Pick the cheapest connected pair of vertices to seed the tree.
2. Repeatedly extend the partial tree with the neighbouring vertex whose
   join produces the smallest intermediate cardinality.
3. Tie-break on the lower vertex id so the output is deterministic.

If the input graph is disconnected (which the planner shouldn't produce but
which can occur in tests) the remaining components are appended via
synthetic Cartesian-product joins in vertex-id order.
"""

from typing import List
from typing import Optional
from typing import Tuple

from opteryx.planner.cost_estimation.dpccp import JoinTree
from opteryx.planner.cost_estimation.dpccp import JoinTreeLeaf
from opteryx.planner.cost_estimation.dpccp import JoinTreeNode
from opteryx.planner.cost_estimation.dpccp import _combine
from opteryx.planner.cost_estimation.dpccp import _tree_subset
from opteryx.planner.cost_estimation.join_graph import JoinEdge
from opteryx.planner.cost_estimation.join_graph import JoinGraph
from opteryx.planner.cost_estimation.join_graph import _bits


def _leaf(graph: JoinGraph, vertex_id: int) -> JoinTreeLeaf:
    v = graph.vertices[vertex_id]
    return JoinTreeLeaf(
        vertex_id=vertex_id,
        estimated_rows=v.row_count,
        domain_rows=v.domain_row_count,
    )


def _best_join_into(
    graph: JoinGraph,
    current: JoinTree,
    current_subset: int,
    candidates: int,
) -> Optional[Tuple[JoinTreeNode, int]]:
    """Return the cheapest single-vertex extension of ``current``.

    ``candidates`` is the bitset of still-unassigned vertices. Only vertices
    connected by at least one edge to ``current`` are considered.
    """
    best: Optional[Tuple[JoinTreeNode, int]] = None
    for v_id in _bits(candidates):
        v_bit = 1 << v_id
        edges = tuple(graph.edges_between(current_subset, v_bit))
        if not edges:
            continue
        leaf = _leaf(graph, v_id)
        cand_a = _combine(current, leaf, edges)
        cand_b = _combine(leaf, current, edges)
        cand = cand_a if cand_a.estimated_cost <= cand_b.estimated_cost else cand_b
        if best is None:
            best = (cand, v_id)
        else:
            best_node, best_id = best
            if cand.estimated_cost < best_node.estimated_cost or (
                cand.estimated_cost == best_node.estimated_cost and v_id < best_id
            ):
                best = (cand, v_id)
    return best


def _greedy_component(graph: JoinGraph, component: int) -> JoinTree:
    """Greedy join order for the vertices in a single connected component."""
    vertex_ids = list(_bits(component))
    if len(vertex_ids) == 1:
        return _leaf(graph, vertex_ids[0])

    # Step 1: seed with the cheapest connected pair.
    best_pair: Optional[Tuple[JoinTreeNode, int, int]] = None
    for i_idx, i in enumerate(vertex_ids):
        for j in vertex_ids[i_idx + 1 :]:
            edges = tuple(graph.edges_between(1 << i, 1 << j))
            if not edges:
                continue
            left = _leaf(graph, i)
            right = _leaf(graph, j)
            cand_a = _combine(left, right, edges)
            cand_b = _combine(right, left, edges)
            cand = cand_a if cand_a.estimated_cost <= cand_b.estimated_cost else cand_b
            if best_pair is None or cand.estimated_cost < best_pair[0].estimated_cost or (
                cand.estimated_cost == best_pair[0].estimated_cost
                and (i, j) < (best_pair[1], best_pair[2])
            ):
                best_pair = (cand, i, j)

    if best_pair is None:
        # Component of size > 1 with no edges — caller should have split it.
        raise RuntimeError("greedy fallback hit an unexpectedly disconnected component")

    tree: JoinTree = best_pair[0]
    used = (1 << best_pair[1]) | (1 << best_pair[2])
    remaining = component & ~used

    # Step 2: extend one vertex at a time.
    while remaining:
        next_step = _best_join_into(graph, tree, used, remaining)
        if next_step is None:
            # Shouldn't happen for a connected component, but keep the loop
            # honest if upstream invariants are violated.
            raise RuntimeError("greedy fallback could not extend a connected component")
        tree, v_id = next_step
        used |= 1 << v_id
        remaining &= ~(1 << v_id)
    return tree


def greedy_join_order(graph: JoinGraph) -> JoinTree:
    """Greedy enumerator. Always returns a valid tree covering every vertex."""
    if graph.n == 0:
        raise ValueError("greedy_join_order requires at least one vertex")
    components = graph.connected_components(graph.full_mask)
    component_trees: List[JoinTree] = [_greedy_component(graph, c) for c in components]

    # Stitch components together with synthetic Cartesian joins, in vertex-id
    # order, so disconnected inputs (test scaffolding only) still produce a
    # complete tree.
    tree = component_trees[0]
    for extra in component_trees[1:]:
        cartesian_edge = JoinEdge(
            left=_tree_subset(tree).bit_length() - 1,
            right=_tree_subset(extra).bit_length() - 1,
            equi_keys=(),
            extra_selectivity=1.0,
        )
        tree = _combine(tree, extra, (cartesian_edge,))
    return tree
