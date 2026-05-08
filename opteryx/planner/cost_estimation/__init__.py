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
from opteryx.planner.cost_estimation.join_cardinality import estimate_join_cardinality
from opteryx.planner.cost_estimation.join_graph import JoinEdge
from opteryx.planner.cost_estimation.join_graph import JoinGraph
from opteryx.planner.cost_estimation.join_graph import JoinVertex
from opteryx.planner.cost_estimation.predicate_ordering import PredicateStats
from opteryx.planner.cost_estimation.predicate_ordering import order_predicates


def enumerate_join_tree(graph: JoinGraph, *, dp_threshold: int = 12) -> JoinTree:
    """Pick DPccp or the greedy fallback based on the vertex count.

    ``dp_threshold`` is the largest vertex count that still uses DPccp; above
    it we drop to the greedy enumerator. Default 12 keeps DPccp's runtime
    well under 100ms on realistic join graphs.
    """
    if dp_threshold < 1:
        raise ValueError(f"dp_threshold must be >= 1 (got {dp_threshold})")
    if graph.n <= dp_threshold:
        return dpccp(graph)
    return greedy_join_order(graph)


__all__ = [
    "KeyStats",
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
