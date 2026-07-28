"""Join graph data structure used by the cost-based join enumerator.

The graph is intentionally small and framework-free: vertices carry an opaque
``payload`` so the caller can attach a relation handle / scan node id without
this module having to know about plans. Vertex ids are dense 0-indexed
integers used directly as bitset positions — DPccp's hot loop is bitset
arithmetic, not set ops.
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.planner.cost_estimation.join_cardinality import KeyStats


@dataclass(frozen=True)
class JoinVertex:
    id: int
    name: str
    row_count: int
    payload: Any = None


@dataclass(frozen=True)
class JoinEdge:
    left: int
    right: int
    equi_keys: Tuple[Tuple[KeyStats, KeyStats], ...] = ()
    extra_selectivity: float = 1.0
    payload: Any = None
    # Equivalence-class id for this edge's join key (see
    # plan_adapter._group_equivalence_classes). Two edges sharing a class_id
    # restate the same transitive key identity — e.g. JOB's
    # `t.id=mi.movie_id AND t.id=mk.movie_id AND mk.movie_id=mi.movie_id`
    # triangle. ``None`` means "no known class" — never deduped against
    # another edge, matching the historical (pre-dedup) behaviour.
    class_id: Optional[int] = None


def _bits(mask: int):
    """Yield the bit positions set in ``mask``, ascending."""
    while mask:
        low = mask & -mask
        yield low.bit_length() - 1
        mask ^= low


def _popcount(mask: int) -> int:
    return bin(mask).count("1")


@dataclass
class JoinGraph:
    vertices: List[JoinVertex]
    edges: List[JoinEdge]

    _adj_mask: List[int] = field(init=False, repr=False)
    _edges_by_pair: dict = field(init=False, repr=False)

    def __post_init__(self) -> None:
        n = len(self.vertices)
        if n == 0:
            raise ValueError("JoinGraph requires at least one vertex")
        for i, v in enumerate(self.vertices):
            if v.id != i:
                raise ValueError(
                    f"vertex ids must be dense 0..n-1 in order; got id={v.id} at index {i}"
                )
            if v.row_count < 0:
                raise ValueError(f"vertex {v.name!r} has negative row_count {v.row_count}")
        adj_mask = [0] * n
        edges_by_pair: dict = {}
        for e in self.edges:
            if not (0 <= e.left < n and 0 <= e.right < n):
                raise ValueError(f"edge endpoints out of range: {e.left}, {e.right}")
            if e.left == e.right:
                raise ValueError(f"self-loop on vertex {e.left}")
            adj_mask[e.left] |= 1 << e.right
            adj_mask[e.right] |= 1 << e.left
            key = (e.left, e.right) if e.left < e.right else (e.right, e.left)
            edges_by_pair.setdefault(key, []).append(e)
        self._adj_mask = adj_mask
        self._edges_by_pair = edges_by_pair

    @property
    def n(self) -> int:
        return len(self.vertices)

    @property
    def full_mask(self) -> int:
        return (1 << self.n) - 1

    def neighbors(self, subset: int) -> int:
        """Bitset of vertices adjacent to ``subset`` and not in ``subset``."""
        result = 0
        for v in _bits(subset):
            result |= self._adj_mask[v]
        return result & ~subset

    def edges_between(self, lhs: int, rhs: int) -> List[JoinEdge]:
        """Edges with one endpoint in ``lhs`` and the other in ``rhs``.

        Result order is deterministic: ascending by (min_vertex, max_vertex).
        """
        if lhs & rhs:
            raise ValueError("edges_between requires disjoint subsets")
        out: List[JoinEdge] = []
        adj = self._adj_mask
        pairs = self._edges_by_pair
        s = lhs
        while s:
            low = s & -s
            v = low.bit_length() - 1
            cross = adj[v] & rhs
            while cross:
                low2 = cross & -cross
                w = low2.bit_length() - 1
                key = (v, w) if v < w else (w, v)
                bucket = pairs.get(key)
                if bucket:
                    out.extend(bucket)
                cross ^= low2
            s ^= low
        return out

    def is_connected(self, subset: int) -> bool:
        if subset == 0:
            return False
        # BFS over bit positions inside ``subset``.
        start = subset & -subset
        visited = start
        frontier = start
        while frontier:
            new_neighbours = 0
            for v in _bits(frontier):
                new_neighbours |= self._adj_mask[v]
            new_neighbours &= subset & ~visited
            visited |= new_neighbours
            frontier = new_neighbours
        return visited == subset

    def connected_components(self, subset: int) -> List[int]:
        """Bitsets of the connected components of ``subset``.

        Components are returned in ascending order of their lowest vertex id.
        """
        components: List[int] = []
        remaining = subset
        while remaining:
            start = remaining & -remaining
            visited = start
            frontier = start
            while frontier:
                new_neighbours = 0
                for v in _bits(frontier):
                    new_neighbours |= self._adj_mask[v]
                new_neighbours &= remaining & ~visited
                visited |= new_neighbours
                frontier = new_neighbours
            components.append(visited)
            remaining &= ~visited
        return components
