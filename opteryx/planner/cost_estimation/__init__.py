"""Cost estimation primitives for the planner.

Pure, side-effect-free functions used by cost-based optimization strategies
(DPccp join planner and friends). Plan-walking and stat resolution live
elsewhere; modules here operate on pre-resolved statistics.
"""

from opteryx.planner.cost_estimation.dpccp import MAX_DPCCP_VERTICES
from opteryx.planner.cost_estimation.dpccp import JoinTree
from opteryx.planner.cost_estimation.dpccp import JoinTreeLeaf
from opteryx.planner.cost_estimation.dpccp import JoinTreeNode
from opteryx.planner.cost_estimation.dpccp import dpccp
from opteryx.planner.cost_estimation.greedy_join_order import greedy_join_order
from opteryx.planner.cost_estimation.join_cardinality import KeyStats
from opteryx.planner.cost_estimation.join_cardinality import estimate_after_filter
from opteryx.planner.cost_estimation.join_cardinality import estimate_group_by_cardinality
from opteryx.planner.cost_estimation.join_cardinality import estimate_join_cardinality
from opteryx.planner.cost_estimation.join_graph import JoinEdge
from opteryx.planner.cost_estimation.join_graph import JoinGraph
from opteryx.planner.cost_estimation.join_graph import JoinVertex
from opteryx.planner.cost_estimation.predicate_ordering import PredicateStats
from opteryx.planner.cost_estimation.predicate_ordering import order_predicates


def enumerate_join_tree(
    graph: JoinGraph, *, dp_threshold: int = 12, edge_threshold: int = 20
) -> JoinTree:
    """Pick DPccp or the greedy fallback based on vertex and edge counts.

    DPccp runs when *both* ``graph.n <= dp_threshold`` and
    ``len(graph.edges) <= edge_threshold``; otherwise the greedy enumerator
    is used. The edge threshold guards against pathologically dense schemas
    where DPccp's exponential enumeration becomes too slow (17v/24e ≈ 600ms).
    """
    if dp_threshold < 1:
        raise ValueError(f"dp_threshold must be >= 1 (got {dp_threshold})")
    if edge_threshold < 0:
        raise ValueError(f"edge_threshold must be >= 0 (got {edge_threshold})")
    if graph.n <= dp_threshold and len(graph.edges) <= edge_threshold:
        return dpccp(graph)
    return greedy_join_order(graph)


__all__ = [
    "KeyStats",
    "estimate_after_filter",
    "estimate_group_by_cardinality",
    "estimate_join_cardinality",
    "JoinVertex",
    "JoinEdge",
    "JoinGraph",
    "JoinTree",
    "JoinTreeLeaf",
    "JoinTreeNode",
    "MAX_DPCCP_VERTICES",
    "dpccp",
    "greedy_join_order",
    "enumerate_join_tree",
    "PredicateStats",
    "order_predicates",
]
