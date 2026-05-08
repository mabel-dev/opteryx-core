"""DPccp join enumerator.

Implements the CSG-CMP-pair enumeration of Moerkotte & Neumann
(VLDB 2006, "Analysis of Two Existing and One New Dynamic Programming
Algorithm for the Generation of Optimal Bushy Join Trees without Cross
Products"), §4. Cost function: sum of intermediate cardinalities, with the
output cardinality of each pairwise join computed by
``estimate_join_cardinality``.

Vertex ids are bitset positions; every subset is an ``int``. The enumerator
relies on the paper's vertex-ordering trick (lowest id first) to visit each
connected (S1, S2) pair exactly once and to guarantee that ``DP[S1]`` and
``DP[S2]`` are populated before the pair is processed.
"""

from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from opteryx.planner.cost_estimation.join_cardinality import _inner_estimate
from opteryx.planner.cost_estimation.join_cardinality import estimate_join_cardinality  # noqa: F401
from opteryx.planner.cost_estimation.join_graph import JoinEdge
from opteryx.planner.cost_estimation.join_graph import JoinGraph
from opteryx.planner.cost_estimation.join_graph import _bits

MAX_DPCCP_VERTICES = 30


@dataclass(frozen=True)
class JoinTreeLeaf:
    vertex_id: int
    estimated_rows: int


@dataclass(frozen=True)
class JoinTreeNode:
    left: "JoinTree"
    right: "JoinTree"
    edges: Tuple[JoinEdge, ...]
    estimated_rows: int
    estimated_cost: float


JoinTree = Union[JoinTreeLeaf, JoinTreeNode]


def _tree_cost(tree: JoinTree) -> float:
    if isinstance(tree, JoinTreeLeaf):
        return 0.0
    return tree.estimated_cost


def _tree_subset(tree: JoinTree) -> int:
    """Bitset of vertex ids covered by ``tree`` (used for tie-breaking only)."""
    if isinstance(tree, JoinTreeLeaf):
        return 1 << tree.vertex_id
    return _tree_subset(tree.left) | _tree_subset(tree.right)


def _combine(
    left: JoinTree,
    right: JoinTree,
    edges: Tuple[JoinEdge, ...],
) -> JoinTreeNode:
    equi_keys = []
    extra_sel = 1.0
    for e in edges:
        equi_keys.extend(e.equi_keys)
        extra_sel *= e.extra_selectivity
    # Skip the public wrapper's validation — inputs originate inside the
    # enumerator and are well-formed by construction.
    raw = _inner_estimate(
        left.estimated_rows,
        right.estimated_rows,
        equi_keys,
        extra_sel,
    )
    rows = max(1, int(raw))
    left_cost = left.estimated_cost if isinstance(left, JoinTreeNode) else 0.0
    right_cost = right.estimated_cost if isinstance(right, JoinTreeNode) else 0.0
    return JoinTreeNode(
        left=left,
        right=right,
        edges=edges,
        estimated_rows=rows,
        estimated_cost=left_cost + right_cost + float(rows),
    )


def dpccp(graph: JoinGraph) -> JoinTree:
    """Enumerate all CSG-CMP pairs and return the cheapest join tree.

    Raises ``ValueError`` for empty, disconnected, or oversized graphs.
    Callers above ``MAX_DPCCP_VERTICES`` should select the greedy fallback
    (``enumerate_join_tree`` does this automatically).
    """
    n = graph.n
    if n == 0:
        raise ValueError("DPccp requires at least one vertex")
    if n > MAX_DPCCP_VERTICES:
        raise ValueError(
            f"DPccp refuses graphs with more than {MAX_DPCCP_VERTICES} vertices "
            f"(got {n}); use the greedy fallback instead"
        )
    full = graph.full_mask
    if not graph.is_connected(full):
        raise ValueError("DPccp requires a connected join graph")

    DP: Dict[int, JoinTree] = {}
    for v in graph.vertices:
        DP[1 << v.id] = JoinTreeLeaf(vertex_id=v.id, estimated_rows=v.row_count)

    if n == 1:
        return DP[1]

    def update(s1: int, s2: int) -> None:
        # Both DP[s1] and DP[s2] are guaranteed populated by the paper's
        # enumeration order. The connectivity invariant (S1 & S2 in N(S1))
        # also guarantees at least one edge between them.
        left = DP[s1]
        right = DP[s2]
        edges = tuple(graph.edges_between(s1, s2))
        # Sum-of-intermediates cost is symmetric in (left, right). Step 6
        # will revisit build-side selection with an asymmetric cost
        # function; until then we build one candidate per pair and pick
        # a deterministic orientation (lower-bitset side on the left) so
        # the output is stable across runs.
        if s1 <= s2:
            best = _combine(left, right, edges)
        else:
            best = _combine(right, left, edges)
        union = s1 | s2
        existing = DP.get(union)
        if existing is None or best.estimated_cost < (
            existing.estimated_cost if isinstance(existing, JoinTreeNode) else 0.0
        ):
            DP[union] = best

    def enumerate_csg_rec(S: int, X: int, cmp_for: Optional[int]) -> None:
        N = graph.neighbors(S) & ~X
        if N == 0:
            return
        # Snapshot subsets so we can iterate twice (emit + recurse) without
        # rerunning the subset trick.
        subsets: List[int] = []
        sub = N
        while sub:
            subsets.append(sub)
            sub = (sub - 1) & N
        if cmp_for is None:
            # Emit csgs of smaller size first so DP entries are populated
            # before any larger csg is processed by enumerate_cmp.
            subsets.sort(key=lambda s: s.bit_count())
            for sub in subsets:
                enumerate_cmp(S | sub)
        else:
            # Order is irrelevant — DP[cmp_for] is already filled, and
            # DP[S|sub] for these csgs was filled in an earlier outer
            # iteration (smaller min vertex of S2).
            for sub in subsets:
                update(cmp_for, S | sub)
        new_X = X | N
        for sub in subsets:
            enumerate_csg_rec(S | sub, new_X, cmp_for)

    def enumerate_cmp(S1: int) -> None:
        # min(S1) is the lowest set bit in S1.
        min_v = (S1 & -S1).bit_length() - 1
        # X = B_min(S1) ∪ S1 where B_v = {v_j : j ≤ v}.
        X = ((1 << (min_v + 1)) - 1) | S1
        N = graph.neighbors(S1) & ~X
        # Iterate v in N in descending vertex order — matches the paper and
        # makes output ordering deterministic.
        bits_in_n = list(_bits(N))
        for v_pos in reversed(bits_in_n):
            v_bit = 1 << v_pos
            update(S1, v_bit)
            # Forbidden set for the recursive grow: prevent enumerating any
            # complement that doesn't have v as its minimum vertex.
            B_v = (1 << (v_pos + 1)) - 1
            enumerate_csg_rec(v_bit, X | (N & B_v), cmp_for=S1)

    # Driver: seed with each singleton in descending vertex order. The seed
    # ordering is what makes EnumerateCmp's "min(S1) < min(S2)" invariant
    # hold and ensures DP[S1]/DP[S2] are populated in time.
    for i in range(n - 1, -1, -1):
        v_bit = 1 << i
        # Singleton seed needs both: process it as an S1 (find pairs with it),
        # and grow it to find larger S1's rooted at v_i.
        enumerate_cmp(v_bit)
        X = (1 << (i + 1)) - 1  # B_i = {v_0, ..., v_i}
        enumerate_csg_rec(v_bit, X, cmp_for=None)

    result = DP.get(full)
    if result is None:
        # Should be impossible for a connected graph; treat as a logic bug.
        raise RuntimeError("DPccp failed to compute a tree for the full vertex set")
    return result
