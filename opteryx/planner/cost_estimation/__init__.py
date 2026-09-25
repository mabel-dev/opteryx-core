"""Cost estimation primitives for the planner.

Pure, side-effect-free functions used by cost-based optimization strategies
(DPccp join planner and friends). Plan-walking and stat resolution live
elsewhere; modules here operate on pre-resolved statistics.

Join cardinality and join-order enumeration (DPccp / greedy) are native — see
opteryx/compiled/planner/join_estimator.pyx and
src/cpp/planner/join_estimator.hpp.
"""

from typing import Union

from opteryx.compiled.planner.join_estimator import MAX_DPCCP_VERTICES
from opteryx.compiled.planner.join_estimator import JoinEdge
from opteryx.compiled.planner.join_estimator import JoinGraph
from opteryx.compiled.planner.join_estimator import JoinTreeLeaf
from opteryx.compiled.planner.join_estimator import JoinTreeNode
from opteryx.compiled.planner.join_estimator import JoinVertex
from opteryx.compiled.planner.join_estimator import KeyStats
from opteryx.compiled.planner.join_estimator import NdvProvenance
from opteryx.compiled.planner.join_estimator import apply_occupancy_bound
from opteryx.compiled.planner.join_estimator import composite_key_ndv
from opteryx.compiled.planner.join_estimator import dpccp
from opteryx.compiled.planner.join_estimator import enumerate_join_tree
from opteryx.compiled.planner.join_estimator import estimate_after_filter
from opteryx.compiled.planner.join_estimator import estimate_group_by_cardinality
from opteryx.compiled.planner.join_estimator import estimate_join_cardinality
from opteryx.compiled.planner.join_estimator import greedy_join_order
from opteryx.compiled.planner.join_estimator import surviving_distinct_count
from opteryx.planner.cost_estimation.predicate_ordering import PredicateStats
from opteryx.planner.cost_estimation.predicate_ordering import order_predicates

JoinTree = Union[JoinTreeLeaf, JoinTreeNode]

__all__ = [
    "KeyStats",
    "NdvProvenance",
    "apply_occupancy_bound",
    "composite_key_ndv",
    "estimate_after_filter",
    "surviving_distinct_count",
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
