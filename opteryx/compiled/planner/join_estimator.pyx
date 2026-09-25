# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""Join cardinality estimation and join-order enumeration (DPccp + greedy).

The planner-facing surface of src/cpp/planner/join_estimator.hpp. Every
estimate and every enumeration step runs in that header; this module only
converts the planner's objects into its structs once per call (and the chosen
tree back), releasing the GIL while the enumerator runs.

The Python-facing API — KeyStats, NdvProvenance, JoinVertex, JoinEdge,
JoinGraph, JoinTreeLeaf, JoinTreeNode and the estimator functions — is the one
the Python implementation exposed, names, signatures and error messages
included, so every consumer and test reaches the native code unchanged.

`payload` and `name` on vertices and edges are the caller's handles, carried
for the caller and never read here: they are the only untyped attributes, and
nothing native sees them.
"""

from enum import Enum

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector


cdef extern from "planner/join_estimator.hpp" nogil:
    const double kEqUnknownNdvFallback "opteryx::planner::kEqUnknownNdvFallback"
    const int kMaxDpccpVertices "opteryx::planner::kMaxDpccpVertices"
    const int kMaxGraphVertices "opteryx::planner::kMaxGraphVertices"

    cdef enum CJoinType "opteryx::planner::JoinType":
        JT_INNER "opteryx::planner::JT_INNER"
        JT_LEFT_OUTER "opteryx::planner::JT_LEFT_OUTER"
        JT_RIGHT_OUTER "opteryx::planner::JT_RIGHT_OUTER"
        JT_FULL_OUTER "opteryx::planner::JT_FULL_OUTER"
        JT_CROSS "opteryx::planner::JT_CROSS"
        JT_SEMI "opteryx::planner::JT_SEMI"
        JT_ANTI "opteryx::planner::JT_ANTI"
        JT_SEMI_NOT_DISTINCT "opteryx::planner::JT_SEMI_NOT_DISTINCT"
        JT_ANTI_NOT_DISTINCT "opteryx::planner::JT_ANTI_NOT_DISTINCT"
        JT_ANTI_NULL_AWARE "opteryx::planner::JT_ANTI_NULL_AWARE"

    cdef struct CKeyStats "opteryx::planner::KeyStats":
        int64_t ndv
        int64_t live_ndv
        double null_fraction
        cbool has_ndv
        cbool has_live_ndv
        cbool has_null_fraction
        uint8_t provenance

    cdef struct CKeyPair "opteryx::planner::KeyPair":
        CKeyStats left
        CKeyStats right

    cdef struct CVertex "opteryx::planner::Vertex":
        int64_t row_count
        int64_t domain_rows

    cdef struct CEdge "opteryx::planner::Edge":
        int32_t left
        int32_t right
        int32_t class_id
        cbool has_class
        double extra_selectivity
        uint32_t key_begin
        uint32_t key_count

    cdef cppclass CGraph "opteryx::planner::Graph":
        vector[CVertex] vertices
        vector[CEdge] edges
        vector[CKeyPair] keys
        void finalize() except +
        int n()
        uint64_t full_mask()
        uint64_t neighbors(uint64_t subset)
        void edges_between(uint64_t lhs, uint64_t rhs, vector[uint32_t]& out) except +
        cbool is_connected(uint64_t subset)
        vector[uint64_t] connected_components(uint64_t subset)

    cdef struct CTreeNode "opteryx::planner::TreeNode":
        int32_t left
        int32_t right
        int32_t vertex
        int64_t rows
        double cost
        int64_t domain_rows
        uint32_t edge_begin
        uint32_t edge_count

    cdef cppclass CTree "opteryx::planner::Tree":
        vector[CTreeNode] nodes
        vector[int32_t] edge_refs
        vector[CEdge] synthetic
        int32_t root

    int64_t c_estimate_join_cardinality "opteryx::planner::estimate_join_cardinality"(
        int64_t left_rows, int64_t right_rows, CJoinType join_type,
        const CKeyPair* keys, size_t n, double extra_selectivity)
    cbool c_apply_occupancy_bound "opteryx::planner::apply_occupancy_bound"(
        const CKeyPair* keys, size_t n, int64_t left_domain_rows, int64_t right_domain_rows,
        CKeyPair* collapsed) except +
    int64_t c_estimate_after_filter "opteryx::planner::estimate_after_filter"(
        int64_t input_rows, double selectivity)
    int64_t c_surviving_distinct_count "opteryx::planner::surviving_distinct_count"(
        int64_t distinct_count, int64_t input_rows, double selectivity)
    int64_t c_estimate_group_by_cardinality "opteryx::planner::estimate_group_by_cardinality"(
        int64_t input_rows, const int64_t* ndvs, const uint8_t* has, size_t n)
    CTree c_dpccp "opteryx::planner::dpccp"(const CGraph& g) except +
    CTree c_greedy_join_order "opteryx::planner::greedy_join_order"(const CGraph& g) except +
    CTree c_enumerate_join_tree "opteryx::planner::enumerate_join_tree"(
        const CGraph& g, int dp_threshold, int edge_threshold) except +


EQ_UNKNOWN_NDV_FALLBACK = kEqUnknownNdvFallback
MAX_DPCCP_VERTICES = kMaxDpccpVertices
MAX_GRAPH_VERTICES = kMaxGraphVertices


class NdvProvenance(Enum):
    """Where a ``KeyStats.ndv`` came from.

    MEASURED is a distinct count somebody counted. DOMAIN_STANDIN is an UPPER
    BOUND on the NDV -- a pre-filter relation size, or a value-range span --
    substituted because no distinct count existed. UNKNOWN is no NDV at all,
    and pairs with ``ndv is None``. Consumers that ACT on the number being a
    real distinct count (the occupancy bound) must check the provenance.
    """

    MEASURED = "measured"
    DOMAIN_STANDIN = "domain_standin"
    UNKNOWN = "unknown"


# Provenance codes, in the header's NdvProvenance order.
_PROVENANCE_BY_CODE = (NdvProvenance.UNKNOWN, NdvProvenance.MEASURED, NdvProvenance.DOMAIN_STANDIN)

_JOIN_TYPES = {
    "inner": JT_INNER,
    "left outer": JT_LEFT_OUTER,
    "right outer": JT_RIGHT_OUTER,
    "full outer": JT_FULL_OUTER,
    "cross": JT_CROSS,
    "semi": JT_SEMI,
    "anti": JT_ANTI,
    "semi not-distinct": JT_SEMI_NOT_DISTINCT,
    "anti not-distinct": JT_ANTI_NOT_DISTINCT,
    "anti null-aware": JT_ANTI_NULL_AWARE,
}


cdef class KeyStats:
    """Statistics for one side of one equi-key class.

    ``ndv`` is the key's DOMAIN size (measured before any filter); ``live_ndv``
    the distinct values the relation holds after filters — only semi/anti
    reads it, and None there means semi/anti declines to estimate.
    """

    cdef CKeyStats c

    def __init__(self, ndv, null_fraction, ndv_provenance=NdvProvenance.UNKNOWN, live_ndv=None):
        # No default provenance for a present NDV: a construction site that
        # does not say where its number came from must fail here.
        if (ndv is None) != (ndv_provenance is NdvProvenance.UNKNOWN):
            raise ValueError(
                "ndv and ndv_provenance must agree: ndv=None iff provenance is UNKNOWN "
                f"(got ndv={ndv!r}, ndv_provenance={ndv_provenance!r})"
            )
        self.c.has_ndv = ndv is not None
        self.c.ndv = ndv if ndv is not None else 0
        self.c.has_null_fraction = null_fraction is not None
        self.c.null_fraction = null_fraction if null_fraction is not None else 0.0
        self.c.has_live_ndv = live_ndv is not None
        self.c.live_ndv = live_ndv if live_ndv is not None else 0
        if ndv_provenance is NdvProvenance.MEASURED:
            self.c.provenance = 1
        elif ndv_provenance is NdvProvenance.DOMAIN_STANDIN:
            self.c.provenance = 2
        elif ndv_provenance is NdvProvenance.UNKNOWN:
            self.c.provenance = 0
        else:
            raise ValueError(f"ndv_provenance must be an NdvProvenance (got {ndv_provenance!r})")

    @property
    def ndv(self):
        return self.c.ndv if self.c.has_ndv else None

    @property
    def null_fraction(self):
        return self.c.null_fraction if self.c.has_null_fraction else None

    @property
    def ndv_provenance(self):
        return _PROVENANCE_BY_CODE[self.c.provenance]

    @property
    def live_ndv(self):
        return self.c.live_ndv if self.c.has_live_ndv else None

    @property
    def ndv_is_measured(self) -> bool:
        """True only for a counted distinct value, never for a stand-in."""
        return self.c.provenance == 1

    cdef tuple _key(self):
        return (self.ndv, self.null_fraction, self.c.provenance, self.live_ndv)

    def __eq__(self, other):
        if type(other) is not KeyStats:
            return NotImplemented
        return self._key() == (<KeyStats>other)._key()

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return (
            f"KeyStats(ndv={self.ndv!r}, null_fraction={self.null_fraction!r}, "
            f"ndv_provenance={self.ndv_provenance!r}, live_ndv={self.live_ndv!r})"
        )


cdef inline KeyStats _wrap_key(CKeyStats c):
    cdef KeyStats out = KeyStats.__new__(KeyStats)
    out.c = c
    return out


cdef void _fill_pairs(equi_keys, vector[CKeyPair]& out) except *:
    cdef CKeyPair pair
    out.clear()
    for left_stat, right_stat in equi_keys:
        pair.left = (<KeyStats?>left_stat).c
        pair.right = (<KeyStats?>right_stat).c
        out.push_back(pair)


cdef class JoinVertex:
    """A relation in the join graph. ``base_row_count`` is its PRE-filter row
    count (None = same as ``row_count``) — read it through ``domain_row_count``."""

    cdef readonly int32_t id
    cdef readonly str name
    cdef readonly int64_t row_count
    cdef readonly object payload
    cdef int64_t _base_row_count
    cdef cbool _has_base

    def __init__(self, int32_t id, str name, int64_t row_count, payload=None, base_row_count=None):
        self.id = id
        self.name = name
        self.row_count = row_count
        self.payload = payload
        self._has_base = base_row_count is not None
        self._base_row_count = base_row_count if base_row_count is not None else 0

    @property
    def base_row_count(self):
        return self._base_row_count if self._has_base else None

    @property
    def domain_row_count(self) -> int:
        """Base (pre-filter) row count, falling back to the live row count."""
        return self._base_row_count if self._has_base else self.row_count

    def __repr__(self):
        return (
            f"JoinVertex(id={self.id}, name={self.name!r}, row_count={self.row_count}, "
            f"base_row_count={self.base_row_count!r})"
        )


cdef class JoinEdge:
    """One equality predicate between two vertices. ``class_id`` names the key
    equivalence class it restates (None = never deduplicated)."""

    cdef readonly int32_t left
    cdef readonly int32_t right
    cdef readonly tuple equi_keys
    cdef readonly double extra_selectivity
    cdef readonly object payload
    cdef int32_t _class_id
    cdef cbool _has_class

    def __init__(
        self,
        int32_t left,
        int32_t right,
        equi_keys=(),
        double extra_selectivity=1.0,
        payload=None,
        class_id=None,
    ):
        self.left = left
        self.right = right
        self.equi_keys = tuple(equi_keys)
        self.extra_selectivity = extra_selectivity
        self.payload = payload
        self._has_class = class_id is not None
        self._class_id = class_id if class_id is not None else 0

    @property
    def class_id(self):
        return self._class_id if self._has_class else None

    def __repr__(self):
        return (
            f"JoinEdge(left={self.left}, right={self.right}, equi_keys={self.equi_keys!r}, "
            f"extra_selectivity={self.extra_selectivity!r}, class_id={self.class_id!r})"
        )


cdef class JoinGraph:
    """Vertices (dense ids 0..n-1, used as bitset positions) and equality edges."""

    cdef CGraph g
    cdef readonly list vertices
    cdef readonly list edges

    def __init__(self, list vertices, list edges):
        cdef JoinVertex v
        cdef JoinEdge e
        cdef CVertex cv
        cdef CEdge ce
        cdef vector[CKeyPair] pairs
        cdef Py_ssize_t i
        if len(vertices) == 0:
            raise ValueError("JoinGraph requires at least one vertex")
        for i, vertex in enumerate(vertices):
            v = <JoinVertex?>vertex
            if v.id != i:
                raise ValueError(
                    f"vertex ids must be dense 0..n-1 in order; got id={v.id} at index {i}"
                )
            if v.row_count < 0:
                raise ValueError(f"vertex {v.name!r} has negative row_count {v.row_count}")
            cv.row_count = v.row_count
            cv.domain_rows = v._base_row_count if v._has_base else v.row_count
            self.g.vertices.push_back(cv)
        for edge in edges:
            e = <JoinEdge?>edge
            _fill_pairs(e.equi_keys, pairs)
            ce.left = e.left
            ce.right = e.right
            ce.class_id = e._class_id
            ce.has_class = e._has_class
            ce.extra_selectivity = e.extra_selectivity
            ce.key_begin = <uint32_t>self.g.keys.size()
            ce.key_count = <uint32_t>pairs.size()
            for i in range(<Py_ssize_t>pairs.size()):
                self.g.keys.push_back(pairs[i])
            self.g.edges.push_back(ce)
        self.g.finalize()
        self.vertices = list(vertices)
        self.edges = list(edges)

    @property
    def n(self) -> int:
        return self.g.n()

    @property
    def full_mask(self) -> int:
        return self.g.full_mask()

    def neighbors(self, uint64_t subset) -> int:
        """Bitset of vertices adjacent to ``subset`` and not in ``subset``."""
        return self.g.neighbors(subset)

    def edges_between(self, uint64_t lhs, uint64_t rhs) -> list:
        """Edges with one endpoint in ``lhs`` and the other in ``rhs``."""
        cdef vector[uint32_t] out
        self.g.edges_between(lhs, rhs, out)
        return [self.edges[out[i]] for i in range(<Py_ssize_t>out.size())]

    def is_connected(self, uint64_t subset) -> bool:
        return self.g.is_connected(subset)

    def connected_components(self, uint64_t subset) -> list:
        """Bitsets of the connected components of ``subset``, ascending by
        their lowest vertex id."""
        return list(self.g.connected_components(subset))


cdef class JoinTreeLeaf:
    cdef readonly int32_t vertex_id
    cdef readonly int64_t estimated_rows
    cdef readonly object domain_rows

    def __init__(self, int32_t vertex_id, int64_t estimated_rows, domain_rows=None):
        self.vertex_id = vertex_id
        self.estimated_rows = estimated_rows
        self.domain_rows = domain_rows

    def __repr__(self):
        return (
            f"JoinTreeLeaf(vertex_id={self.vertex_id}, estimated_rows={self.estimated_rows}, "
            f"domain_rows={self.domain_rows!r})"
        )


cdef class JoinTreeNode:
    cdef readonly object left
    cdef readonly object right
    cdef readonly tuple edges
    cdef readonly int64_t estimated_rows
    cdef readonly double estimated_cost
    cdef readonly object domain_rows

    def __init__(self, left, right, tuple edges, int64_t estimated_rows,
                 double estimated_cost, domain_rows=None):
        self.left = left
        self.right = right
        self.edges = edges
        self.estimated_rows = estimated_rows
        self.estimated_cost = estimated_cost
        self.domain_rows = domain_rows

    def __repr__(self):
        return (
            f"JoinTreeNode(left={self.left!r}, right={self.right!r}, "
            f"estimated_rows={self.estimated_rows}, estimated_cost={self.estimated_cost!r}, "
            f"domain_rows={self.domain_rows!r})"
        )


cdef object _tree_to_python(CTree& tree, JoinGraph graph, list synthetic, int32_t idx):
    cdef CTreeNode node = tree.nodes[idx]
    cdef int32_t ref
    cdef uint32_t k
    if node.vertex >= 0:
        return JoinTreeLeaf(node.vertex, node.rows, node.domain_rows)
    edges = []
    for k in range(node.edge_count):
        ref = tree.edge_refs[node.edge_begin + k]
        edges.append(graph.edges[ref] if ref >= 0 else synthetic[-1 - ref])
    return JoinTreeNode(
        _tree_to_python(tree, graph, synthetic, node.left),
        _tree_to_python(tree, graph, synthetic, node.right),
        tuple(edges),
        node.rows,
        node.cost,
        node.domain_rows,
    )


cdef object _to_python(CTree& tree, JoinGraph graph):
    cdef list synthetic = []
    cdef Py_ssize_t i
    for i in range(<Py_ssize_t>tree.synthetic.size()):
        synthetic.append(
            JoinEdge(tree.synthetic[i].left, tree.synthetic[i].right, (), 1.0)
        )
    return _tree_to_python(tree, graph, synthetic, tree.root)


def dpccp(JoinGraph graph):
    """Enumerate all CSG-CMP pairs and return the cheapest join tree.

    Raises ``ValueError`` for empty, disconnected, or oversized graphs.
    """
    cdef CTree tree
    with nogil:
        tree = c_dpccp(graph.g)
    return _to_python(tree, graph)


def greedy_join_order(JoinGraph graph):
    """Greedy enumerator. Always returns a valid tree covering every vertex."""
    cdef CTree tree
    with nogil:
        tree = c_greedy_join_order(graph.g)
    return _to_python(tree, graph)


def enumerate_join_tree(JoinGraph graph, *, int dp_threshold=12, int edge_threshold=20):
    """DPccp when BOTH ``graph.n <= dp_threshold`` and
    ``len(graph.edges) <= edge_threshold``, otherwise the greedy enumerator —
    the edge threshold guards against pathologically dense schemas where
    DPccp's enumeration explodes (17v/24e ≈ 600ms in the Python enumerator)."""
    cdef CTree tree
    with nogil:
        tree = c_enumerate_join_tree(graph.g, dp_threshold, edge_threshold)
    return _to_python(tree, graph)


def estimate_join_cardinality(
    int64_t left_rows,
    int64_t right_rows,
    str join_type,
    equi_keys,
    double extra_predicates_selectivity=1.0,
) -> int:
    """Estimate the row count of a join result, floored at 1."""
    cdef vector[CKeyPair] pairs
    if left_rows < 0 or right_rows < 0:
        raise ValueError(
            f"row counts must be non-negative (got left={left_rows}, right={right_rows})"
        )
    code = _JOIN_TYPES.get(join_type)
    if code is None:
        raise ValueError(f"unknown join_type: {join_type!r}")
    if extra_predicates_selectivity < 0.0:
        raise ValueError(
            "extra_predicates_selectivity must be non-negative "
            f"(got {extra_predicates_selectivity})"
        )
    _fill_pairs(equi_keys, pairs)
    return c_estimate_join_cardinality(
        left_rows, right_rows, <CJoinType>(<int>code), pairs.data(), pairs.size(),
        extra_predicates_selectivity,
    )


def apply_occupancy_bound(list equi_keys, int64_t left_domain_rows, int64_t right_domain_rows):
    """Bound a COMPOSITE key's domain by the rows available to hold it.

    Returns ``equi_keys`` itself when the bound does not bind, else a single
    collapsed (DOMAIN_STANDIN) pair. Callers must note the PRE-bound class
    count in telemetry: a collapsed list no longer shows the key was composite.
    """
    cdef vector[CKeyPair] pairs
    cdef CKeyPair collapsed
    _fill_pairs(equi_keys, pairs)
    # std::domain_error (isqrt of a negative NDV product) surfaces as ValueError,
    # as Python's math.isqrt raised.
    if not c_apply_occupancy_bound(
        pairs.data(), pairs.size(), left_domain_rows, right_domain_rows, &collapsed
    ):
        return equi_keys
    return [(_wrap_key(collapsed.left), _wrap_key(collapsed.right))]


def composite_key_ndv(ndvs):
    """Compose one side's per-column key NDVs: ``max`` of the known ones
    (architect ruling 2026-08-21), None when none is known."""
    cdef int64_t best = 0
    cdef cbool found = False
    cdef int64_t value
    for ndv in ndvs:
        if ndv is None:
            continue
        value = ndv
        if not found or value > best:
            best = value
            found = True
    return best if found else None


def estimate_after_filter(int64_t input_rows, double selectivity) -> int:
    """Row count after a filter of the given selectivity, floored at 1."""
    if input_rows < 0:
        raise ValueError(f"input_rows must be non-negative (got {input_rows})")
    if selectivity < 0.0:
        raise ValueError(f"selectivity must be non-negative (got {selectivity})")
    return c_estimate_after_filter(input_rows, selectivity)


def surviving_distinct_count(distinct_count, int64_t input_rows, double selectivity):
    """Distinct values expected to survive a filter; None in, None out.

    Scaling only — the NDV <= rows cap belongs to the caller, after this
    (architect ruling 2026-09-14).
    """
    if distinct_count is None:
        return None
    return c_surviving_distinct_count(distinct_count, input_rows, selectivity)


def estimate_group_by_cardinality(int64_t input_rows, group_key_ndvs) -> int:
    """min(input rows, product of group-key NDVs); an unknown or non-positive
    NDV makes it the input row count."""
    cdef vector[int64_t] ndvs
    cdef vector[uint8_t] has
    for ndv in group_key_ndvs:
        has.push_back(ndv is not None)
        ndvs.push_back(ndv if ndv is not None else 0)
    return c_estimate_group_by_cardinality(input_rows, ndvs.data(), has.data(), ndvs.size())
