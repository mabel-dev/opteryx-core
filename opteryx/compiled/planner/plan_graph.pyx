# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""The plan graph: the native structure (src/cpp/planner/plan_graph.hpp) behind
every logical and physical plan, and its Python interface.

Native plan graph P2 (architect rulings 2026-09-27):
  - node ids are per-query integers minted by the query's PlanContext
    (`PlanContext.node_ids`). The plan allocates them: `add_node(step)` and the
    `insert_node_*` methods mint the node's id and return it; `nid=` re-adds a
    step under an id this query already minted (a node a rebuild puts back);
  - a node's ingoing edges come back LEFT, RIGHT, unlabelled, creation order within;
  - reading a node that is not in the plan raises; so does merging plans that share
    a node, adding an edge to a node the plan does not hold, or removing an edge or
    node that is not there.

Edge roles cross this interface as `EdgeRole.LEFT` / `EdgeRole.RIGHT`, or None for
an unlabelled edge (architect ruling 2026-09-27: an enum, not strings).
"""

from enum import Enum

from libc.stdint cimport int32_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libcpp cimport bool as cbool
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from cpython.ref cimport PyObject

from opteryx.exceptions import InvalidInternalStateError


cdef extern from "planner/plan_graph.hpp":
    cdef enum CEdgeRole "opteryx::planner::EdgeRole":
        EDGE_NONE "opteryx::planner::EDGE_NONE"
        EDGE_LEFT "opteryx::planner::EDGE_LEFT"
        EDGE_RIGHT "opteryx::planner::EDGE_RIGHT"

    cdef cppclass CEdge "opteryx::planner::Edge":
        uint32_t other
        CEdgeRole role

    cdef cppclass CPlanNode "opteryx::planner::PlanNode":
        uint32_t id
        PyObject* step
        vector[CEdge] out
        vector[CEdge] ingoing "in"

    cdef cppclass CPlanGraph "opteryx::planner::PlanGraph":
        CPlanGraph() except +
        CPlanGraph(const CPlanGraph&) except +
        size_t size()
        size_t edge_count()
        uint64_t epoch()
        cbool contains(uint32_t id)
        const CPlanNode& node_at(size_t position)
        const CPlanNode& node(uint32_t id) except +
        PyObject* step(uint32_t id) except +
        void add(uint32_t id, PyObject* step) except +
        void replace(uint32_t id, PyObject* step) except +
        void add_edge(uint32_t source, uint32_t target, CEdgeRole role) except +
        cbool remove_edge(uint32_t source, uint32_t target, CEdgeRole role) except +
        cbool relationship(uint32_t source, uint32_t target, CEdgeRole* role) except +
        cbool legs(uint32_t id, uint32_t* left, uint32_t* right) except +
        void remove(uint32_t id, cbool heal) except +
        void insert_before(uint32_t id, PyObject* step, uint32_t before) except +
        void insert_after(uint32_t id, PyObject* step, uint32_t after) except +
        void absorb(const CPlanGraph& other) except +
        CPlanGraph* remapped(const vector[uint32_t]& ids, const vector[PyObject*]& steps) except +
        vector[pair[uint32_t, uint32_t]] visit_order(uint32_t root) except +
        vector[uint32_t] exit_points()


class EdgeRole(Enum):
    """The place an edge takes at its CONSUMER: the LEFT or RIGHT leg of a join or
    set operation. An edge with no role (a node's single input) is None."""

    LEFT = 1
    RIGHT = 2


cdef object _LEFT = EdgeRole.LEFT
cdef object _RIGHT = EdgeRole.RIGHT


cdef inline CEdgeRole _role_code(object relationship) except *:
    if relationship is None:
        return EDGE_NONE
    if relationship is _LEFT:
        return EDGE_LEFT
    if relationship is _RIGHT:
        return EDGE_RIGHT
    raise InvalidInternalStateError(
        f"A plan edge role is EdgeRole.LEFT, EdgeRole.RIGHT or None; got {relationship!r}."
    )


cdef inline object _role_name(CEdgeRole role):
    if role == EDGE_LEFT:
        return _LEFT
    if role == EDGE_RIGHT:
        return _RIGHT
    return None


cdef class NodeIds:
    """One query's node ids: minted in order from 1, never reused.

    Never 0: the planner tests an optional node id for presence by truthiness
    (`if context.parent_nid:`), and a 0 id would read as "no node"."""

    cdef uint32_t _next

    def __cinit__(self):
        self._next = 1

    cpdef uint32_t mint(self):
        cdef uint32_t nid = self._next
        self._next += 1
        return nid

    cdef inline bint minted(self, object nid):
        return type(nid) is int and 0 < nid < self._next


cdef class PlanGraph:
    """A plan: steps under per-query node ids, joined by producer -> consumer edges.

    Subclassed by LogicalPlan and PhysicalPlan; both are built with the query's
    PlanContext, which mints their node ids."""

    cdef CPlanGraph* _graph
    cdef readonly object plan_context
    cdef NodeIds _node_ids
    # A copy-on-write view (`cow_view`) borrows its source's native graph until
    # the first mutator, which takes the view its own structural copy. `_source`
    # keeps the borrowed graph alive and is what `unwrap` hands back untouched.
    cdef bint _borrowed
    cdef object _source

    def __cinit__(self, *args, **kwargs):
        self._graph = new CPlanGraph()

    def __dealloc__(self):
        if not self._borrowed:
            del self._graph

    def __init__(self, plan_context):
        self._bind(plan_context)

    cdef int _bind(self, object plan_context) except -1:
        self.plan_context = plan_context
        self._node_ids = plan_context.node_ids
        return 0

    cdef PlanGraph _empty_like(self):
        """A new, empty graph of this graph's own class over the same query."""
        cdef PlanGraph graph = type(self).__new__(type(self))
        graph._bind(self.plan_context)
        return graph

    cdef inline int _own(self) except -1:
        """Before a mutation: a borrowed view takes its own copy of the structure
        (same nodes and edges, SHARED steps, epoch 0) and lets go of its source."""
        if self._borrowed:
            self._graph = new CPlanGraph(self._graph[0])
            self._borrowed = False
            self._source = None
        return 0

    # -- copy on write ------------------------------------------------------------
    def cow_view(self):
        """A working view of this plan for a pass that may or may not change it.

        Reads go straight to this plan's structure - no copy, no wrapper. The first
        mutator (node replace, add/remove of nodes or edges, merge) takes the view a
        `shallow_copy` of the structure and every later read and write is on that.
        `unwrap()` then says which happened. The steps are shared either way, so an
        in-place step edit lands identically before or after the copy is taken."""
        cdef PlanGraph view = self._empty_like()
        del view._graph
        view._graph = self._graph
        view._borrowed = True
        view._source = self
        return view

    def unwrap(self):
        """The plan a pass hands onward: this plan's source, untouched, when no
        mutation happened; this plan itself (the materialized copy) otherwise. A
        plan that is not a view is its own answer."""
        if self._borrowed:
            return self._source
        return self

    cdef inline uint32_t _present(self, object nid) except? 0:
        if type(nid) is not int or nid < 0 or not self._graph.contains(<uint32_t>nid):
            raise InvalidInternalStateError(f"Plan node {nid!r} is not in the plan.")
        return <uint32_t>nid

    cdef inline uint32_t _unplaced(self, object nid) except? 0:
        if not self._node_ids.minted(nid):
            raise InvalidInternalStateError(
                f"Plan node id {nid!r} was not minted by this query's PlanContext."
            )
        if self._graph.contains(<uint32_t>nid):
            raise InvalidInternalStateError(f"Plan node {nid!r} is already in the plan.")
        return <uint32_t>nid

    # -- nodes ------------------------------------------------------------------
    cdef uint32_t _allocate(self, object nid) except? 0:
        """A new id, or `nid` - an id this query minted that is not in the plan."""
        if nid is None:
            return self._node_ids.mint()
        return self._unplaced(nid)

    def add_node(self, step, nid=None) -> int:
        """Add `step` and return its id: a newly minted one, or `nid` to re-add a
        step under an id this query already minted (architect ruling 2026-09-27:
        the plan allocates node ids)."""
        cdef uint32_t placed = self._allocate(nid)
        self._own()
        self._graph.add(placed, <PyObject*>step)
        return placed

    def __getitem__(self, nid):
        return <object>self._graph.step(self._present(nid))

    def __setitem__(self, nid, step):
        """Replace the step behind `nid`, which must be in the plan."""
        cdef uint32_t placed = self._present(nid)
        self._own()
        self._graph.replace(placed, <PyObject*>step)

    def __contains__(self, nid) -> bool:
        return type(nid) is int and nid >= 0 and self._graph.contains(<uint32_t>nid)

    def __len__(self) -> int:
        return self._graph.size()

    def __bool__(self) -> bool:
        return self._graph.size() != 0

    def __repr__(self):
        return f"{type(self).__name__} - {self._graph.size()} nodes, {self._graph.edge_count()} edges"

    def nodes(self, data=False) -> list:
        """The node ids in insertion order, or `(id, step)` pairs with `data`."""
        cdef size_t i
        cdef size_t n = self._graph.size()
        out = []
        if data:
            for i in range(n):
                out.append((self._graph.node_at(i).id, <object>self._graph.node_at(i).step))
        else:
            for i in range(n):
                out.append(self._graph.node_at(i).id)
        return out

    def remove_node(self, nid, heal: bool = False):
        """Remove a node and its edges; with `heal`, wire each of its producers to
        each of its consumers."""
        cdef uint32_t placed = self._present(nid)
        self._own()
        self._graph.remove(placed, heal)

    def insert_node_before(self, step, before_nid, *, nid=None) -> int:
        """Put `step` between `before_nid` and everything feeding it; its id (new,
        or `nid` for a re-add) is returned."""
        cdef uint32_t before = self._present(before_nid)
        cdef uint32_t placed = self._allocate(nid)
        self._own()
        self._graph.insert_before(placed, <PyObject*>step, before)
        return placed

    def insert_node_after(self, step, after_nid, *, nid=None) -> int:
        """Put `step` between `after_nid` and everything it feeds; its id (new, or
        `nid` for a re-add) is returned."""
        cdef uint32_t after = self._present(after_nid)
        cdef uint32_t placed = self._allocate(nid)
        self._own()
        self._graph.insert_after(placed, <PyObject*>step, after)
        return placed

    # -- edges ------------------------------------------------------------------
    def add_edge(self, source, target, relationship=None):
        """Add the edge source -> target, or set the role of the one already there."""
        cdef uint32_t producer = self._present(source)
        cdef uint32_t consumer = self._present(target)
        cdef CEdgeRole role = _role_code(relationship)
        self._own()
        self._graph.add_edge(producer, consumer, role)

    def remove_edge(self, source, target, relationship):
        cdef uint32_t producer = self._present(source)
        cdef uint32_t consumer = self._present(target)
        cdef CEdgeRole role = _role_code(relationship)
        self._own()
        if not self._graph.remove_edge(producer, consumer, role):
            raise InvalidInternalStateError(
                f"Plan has no edge {source!r} -> {target!r} ({relationship!r}) to remove."
            )

    def relationship(self, source, target):
        """The role on the edge source -> target; None when unlabelled or absent."""
        cdef CEdgeRole role = EDGE_NONE
        if not self._graph.relationship(self._present(source), self._present(target), &role):
            return None
        return _role_name(role)

    def legs(self, nid) -> tuple:
        """`(left, right)`: the producers of a two-input node - a join or a set
        operation - by edge role. Legs are labelled where they are made and nothing
        reads a leg from edge order (architect ruling 2026-09-27), so anything but
        exactly one LEFT and one RIGHT edge into `nid` is refused."""
        cdef uint32_t left = 0
        cdef uint32_t right = 0
        if not self._graph.legs(self._present(nid), &left, &right):
            raise InvalidInternalStateError(
                f"Node {nid!r} does not have exactly one LEFT and one RIGHT input: "
                f"{[(source, role) for source, _t, role in self.ingoing_edges(nid)]}."
            )
        return left, right

    def ingoing_edges(self, target) -> list:
        """`(producer, target, role)` for each edge into `target`: LEFT, RIGHT,
        unlabelled, creation order within."""
        cdef uint32_t nid = self._present(target)
        cdef const CPlanNode* node = &self._graph.node(nid)
        cdef size_t i
        out = []
        for i in range(node.ingoing.size()):
            out.append((node.ingoing[i].other, target, _role_name(node.ingoing[i].role)))
        return out

    def outgoing_edges(self, source) -> list:
        """`(source, consumer, role)` for each edge out of `source`, in insertion order."""
        cdef uint32_t nid = self._present(source)
        cdef const CPlanNode* node = &self._graph.node(nid)
        cdef size_t i
        out = []
        for i in range(node.out.size()):
            out.append((source, node.out[i].other, _role_name(node.out[i].role)))
        return out

    def edges(self) -> list:
        """Every edge as `(producer, consumer, role)`, by producer in node order."""
        cdef size_t i, j
        cdef const CPlanNode* node
        out = []
        for i in range(self._graph.size()):
            node = &self._graph.node_at(i)
            for j in range(node.out.size()):
                out.append((node.id, node.out[j].other, _role_name(node.out[j].role)))
        return out

    def edge_count(self) -> int:
        """How many edges the plan has - `len(edges())` without building them."""
        return self._graph.edge_count()

    def visit_order(self, root) -> list:
        """`(nid, consumer)` for every visit a top-down walk from `root` makes:
        each node before its producers, producers in ingoing order, a node with
        two consumers visited once per consumer. The root's consumer is None.
        Raises when the plan has a cycle."""
        cdef uint32_t start = self._present(root)
        cdef vector[pair[uint32_t, uint32_t]] visits = self._graph.visit_order(start)
        cdef size_t i
        out = [(start, None)]
        for i in range(1, visits.size()):
            out.append((visits[i].first, visits[i].second))
        return out

    def get_exit_points(self) -> list:
        """The plan's heads: nodes that consume something and feed nothing,
        ascending by id (a one-node plan's node)."""
        return list(self._graph.exit_points())

    def exit_point(self):
        """The plan's single head. A plan has exactly one (architect ruling
        2026-09-27: callers that need THE head say so, and anything else is
        refused rather than one head being picked)."""
        cdef vector[uint32_t] heads = self._graph.exit_points()
        if heads.size() != 1:
            raise InvalidInternalStateError(
                f"Expected a plan with exactly one exit point, found {heads.size()}: {list(heads)}."
            )
        return heads[0]

    @property
    def mutation_epoch(self) -> int:
        """Bumped by every node or edge change. A copy starts at 0, so a copy with
        epoch > 0 was changed after it was taken."""
        return self._graph.epoch()

    # -- walks --------------------------------------------------------------------
    def trace_to_root(self, nid) -> list:
        """The ids from `nid`'s first consumer up to the plan's head, following the
        first out-edge at each step."""
        route = []
        cdef uint32_t current = self._present(nid)
        cdef const CPlanNode* node
        while True:
            node = &self._graph.node(current)
            if node.out.size() == 0:
                return route
            current = node.out[0].other
            route.append(current)

    def breadth_first_search(self, source, int depth=100, bint reverse=False) -> list:
        """Every edge met walking breadth-first from `source` — toward the producers
        with `reverse`, the consumers otherwise — up to `depth` steps."""
        from collections import deque

        visited = {source}
        queue = deque([(source, 0)])
        traversed = []
        while queue:
            current, current_depth = queue.popleft()
            if current_depth < depth:
                edges = self.ingoing_edges(current) if reverse else self.outgoing_edges(current)
                for edge in edges:
                    reached = edge[0] if reverse else edge[1]
                    traversed.append(edge)
                    if reached not in visited:
                        visited.add(reached)
                        queue.append((reached, current_depth + 1))
        return traversed

    def depth_first_order(self, root, reversed_nodes=frozenset()) -> list:
        """The ids reachable from `root` toward its producers, depth-first, each
        node before its producers and a node's producers in ingoing order — except
        at the ids in `reversed_nodes`, whose producers are taken in reverse."""
        out = []
        visited = set()
        self._depth_first(self._present(root), reversed_nodes, visited, out)
        return out

    cdef int _depth_first(self, uint32_t nid, object reversed_nodes, set visited, list out) except -1:
        cdef const CPlanNode* node = &self._graph.node(nid)
        cdef size_t i
        visited.add(nid)
        out.append(nid)
        producers = []
        for i in range(node.ingoing.size()):
            producers.append(node.ingoing[i].other)
        if nid in reversed_nodes:
            producers.reverse()
        for producer in producers:
            if producer not in visited:
                self._depth_first(producer, reversed_nodes, visited, out)
        return 0

    def is_acyclic(self) -> bool:
        """Whether the plan has no cycle (Kahn's algorithm over the edges)."""
        cdef size_t i, j
        cdef const CPlanNode* node
        indegree = {}
        for i in range(self._graph.size()):
            node = &self._graph.node_at(i)
            indegree[node.id] = node.ingoing.size()
        ready = [nid for nid, count in indegree.items() if count == 0]
        visited = 0
        while ready:
            current = ready.pop()
            visited += 1
            node = &self._graph.node(current)
            for j in range(node.out.size()):
                indegree[node.out[j].other] -= 1
                if indegree[node.out[j].other] == 0:
                    ready.append(node.out[j].other)
        return visited == len(indegree)

    def draw(self) -> str:
        """An indented tree of the plan from its first head, for debugging."""
        lines = []
        heads = self.get_exit_points()
        if heads:
            self._draw(heads[0], "", set(), lines)
        return "\n".join(lines)

    cdef int _draw(self, object nid, str indent, set visited, list lines) except -1:
        visited.add(nid)
        lines.append(f"{indent}{nid}: {self[nid]}")
        for producer, _, role in self.ingoing_edges(nid):
            if producer not in visited:
                self._draw(producer, indent + ("  " if role is None else f"  [{role}] "), visited, lines)
        return 0

    # -- copies and merges ----------------------------------------------------------
    def shallow_copy(self):
        """The same nodes and edges in new structure, sharing the step objects."""
        cdef PlanGraph graph = self._empty_like()
        del graph._graph
        graph._graph = new CPlanGraph(self._graph[0])
        return graph

    def copy(self, bint fresh_ids=False):
        """A deep copy: every step copied through one memo, so a value two steps
        share is copied once. With `fresh_ids`, every node gets a new id and the
        result is `(copy, id_map)`, mapping each original id to its new one."""
        memo = {}
        cdef size_t i
        cdef size_t n = self._graph.size()
        cdef vector[uint32_t] ids
        cdef vector[PyObject*] steps
        copied = []
        id_map = {}
        for i in range(n):
            old_id = self._graph.node_at(i).id
            new_id = self._node_ids.mint() if fresh_ids else old_id
            id_map[old_id] = new_id
            ids.push_back(new_id)
            step_copy = (<object>self._graph.node_at(i).step).copy(memo)
            copied.append(step_copy)
            steps.push_back(<PyObject*>step_copy)
        cdef PlanGraph graph = self._empty_like()
        del graph._graph
        graph._graph = self._graph.remapped(ids, steps)
        if fresh_ids:
            return graph, id_map
        return graph

    def absorb(self, PlanGraph other):
        """Merge `other`'s nodes and edges into this plan. The two must share no
        node — copy a sub-plan with `copy(fresh_ids=True)` before merging it twice."""
        cdef size_t i
        if other.plan_context is not self.plan_context:
            raise InvalidInternalStateError("Cannot merge plans from two different queries.")
        for i in range(other._graph.size()):
            if self._graph.contains(other._graph.node_at(i).id):
                raise InvalidInternalStateError(
                    f"Cannot merge plans that share node {other._graph.node_at(i).id}."
                )
        self._own()
        self._graph.absorb(other._graph[0])
