# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.formatter import format_expression
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType


# A Filter's `.columns` is documented elsewhere (set_ops.py, projection_pushdown.py)
# as "the predicate's referenced identifiers" — but a HAVING clause that could not
# fuse onto its own aggregate (predicate_pushdown normally folds HAVING directly
# onto the aggregate node; this is the one shape that can't — e.g. it also needs a
# column from a decorrelated subquery's join, so it stays a standalone Filter ABOVE
# that join) still has its AGGREGATOR nodes (`SUM(x)`) sitting in `.condition`. The
# compiler treats AGGREGATOR exactly like IDENTIFIER — an atomic, already-resolved
# "load this identity from the stream" reference, never "recompute from its operand"
# (see compiler.py's array-hoist gate: IDENTIFIER/EVALUATED/AGGREGATOR all "already
# lower to BC_LOAD_COL"). Walking a condition for IDENTIFIER alone descends PAST the
# aggregate into its pre-aggregation operand (`mass` under `SUM(mass)`) instead of
# stopping at the aggregate's own identity — the identity the filter actually reads
# and the one every rebuild of `.columns` from `.condition` must therefore keep.
FILTER_REFERENCED_NODE_TYPES = (NodeType.IDENTIFIER, NodeType.AGGREGATOR)
"""Node types a Filter's `.columns` must preserve when rebuilt from `.condition` —
see `filter_referenced_columns`. Exported so a strategy filtering an ALREADY-BUILT
`.columns` list (rather than re-walking `.condition`) keys on the same set instead
of hand-rolling `column.node_type == NodeType.IDENTIFIER`."""


def filter_referenced_columns(condition: Node) -> list:
    """The atomic, already-resolved column references a Filter's condition reads.

    THE ONE DEFINITION for rebuilding a Filter node's `.columns` from its
    `.condition` — every optimizer strategy that does this (splitting conjuncts,
    pushing/inlining/compacting predicates, decorrelating a subquery) must use this,
    not a bare `get_all_nodes_of_type(condition, (NodeType.IDENTIFIER,))`, or it
    silently drops a standalone HAVING's own aggregate identity and a later stage
    prunes the column the filter needs: "expression references column ... which the
    stream does not carry".
    """
    return get_all_nodes_of_type(condition, FILTER_REFERENCED_NODE_TYPES)


def predicate_key(pred: Node) -> str:
    """Canonical dedup/factoring key for one predicate.

    This is the only sanctioned way to ask "are these two predicates the same
    predicate?" inside the optimizer.

    Do NOT use `Node.uuid` for this. A uuid identifies a node *instance*, not a
    value: the binder collapses expressions that render alike and hands out
    COPIES that preserve the original's uuid (see the binder's use of
    format_expression as expression identity), and the rewrite strategies mutate
    nodes in place without refreshing it. Two conjuncts can therefore share a
    uuid while saying opposite things — `b_value = TRUE` and the `b_value !=
    TRUE` that BooleanSimplificationStrategy inverted out of `NOT (b_value =
    TRUE)` are the same uuid, and deduping them against each other deletes a
    live conjunct and changes the answer.

    format_expression() renders by NAME, not by bound identity — so two
    predicates on DIFFERENT columns that merely share a name (e.g. two
    self-join aliases: n1.n_name vs n2.n_name) render IDENTICALLY. Keying
    purely on that text made cross-clause dedup/absorption/factoring in
    DisjunctionSimplificationStrategy collapse
    `(n1.n_name=A AND n2.n_name=B) OR (n1.n_name=B AND n2.n_name=A)`
    into a single branch — both branches render to the same *set* of strings
    even though they bind to opposite columns — silently dropping the second
    branch and changing the query's result. Appending each referenced column's
    schema_column.identity disambiguates them: two predicates only share a key
    if they are actually the same predicate on the same bound column, not
    merely same-looking text.
    """
    identities = sorted(
        str(node.schema_column.identity)
        for node in get_all_nodes_of_type(pred, (NodeType.IDENTIFIER,))
        if node.schema_column is not None
    )
    return format_expression(pred) + "‖" + ",".join(identities)


def get_nodes_of_type_from_logical_plan(plan: LogicalPlan, types: Tuple[LogicalPlanStepType]):
    """Utility to get all nodes of a given type from a logical plan"""
    matches = []
    for node in plan.nodes(True):
        if node[1].node_type in types:
            matches.append(node)
    return matches


def flip_join_leg_labels(plan: LogicalPlan, join_nid: str) -> None:
    """Exchange the 'left'/'right' labels on a join's ingoing edges.

    A strategy that swaps a join's left/right node attributes (readers, columns,
    relation names) MUST also call this. The edge labels are what the physical
    plan reads to decide which leg it builds the hash table from; swapping the
    attributes alone leaves the two disagreeing, and the build side silently
    reverts to the pre-swap leg.

    Edges an optimizer rewrite left unlabelled are skipped — the physical plan
    infers those from the (already swapped) reader UUIDs.
    """
    opposite = {"left": "right", "right": "left"}
    # Materialise before mutating: add_edge invalidates the edge caches.
    for provider, _target, relation in list(plan.ingoing_edges(join_nid)):
        flipped = opposite.get(relation)
        if flipped is not None:
            plan.add_edge(provider, join_nid, flipped)


class CopyOnWritePlan:
    """The working plan a strategy visits, with the copy deferred until the
    first mutation.

    Every strategy used to open its visit with `optimized_plan =
    pre_optimized_tree.copy()` — a full deep copy of the plan, paid whether or
    not the strategy went on to change anything (~19 copies per query, most of
    them for passes that did nothing). This stand-in delegates every READ to
    the pristine input plan and takes the copy only when a strategy performs a
    graph MUTATION (node replace, add/remove of nodes or edges). A pass that
    never mutates never copies, and `unwrap()` hands the untouched input plan
    back to the optimizer — which also keeps the plan's statistics valid, so
    the next cost-based strategy skips its refresh.

    The materialized copy is `shallow_copy()`: fresh structure, SHARED node
    objects. The input plan is discarded the moment the pass completes, so
    the structure is the only thing the walk needs protected; sharing the
    nodes means an in-place node edit lands identically whether it happens
    before or after the copy is taken.

    Reads and writes of plain attributes (e.g. `statistics_are_stale`) pass
    through to the underlying plan — they are metadata, not plan mutations,
    and do not trigger the copy.
    """

    __slots__ = ("_source", "_materialized")

    def __init__(self, source: LogicalPlan):
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "_materialized", None)

    # -- materialization ---------------------------------------------------
    def _plan(self) -> LogicalPlan:
        materialized = self._materialized
        return self._source if materialized is None else materialized

    def _mutable(self) -> LogicalPlan:
        if self._materialized is None:
            object.__setattr__(self, "_materialized", self._source.shallow_copy())
        return self._materialized

    def unwrap(self, unchanged: LogicalPlan) -> LogicalPlan:
        """The real plan to hand onward: the materialized copy when a mutation
        happened, otherwise `unchanged` (the pass's input plan)."""
        materialized = self._materialized
        return unchanged if materialized is None else materialized

    # -- reads: delegate to whichever plan is current ------------------------
    def __getattr__(self, name):
        return getattr(self._plan(), name)

    def __setattr__(self, name, value):
        setattr(self._plan(), name, value)

    def __getitem__(self, nid):
        return self._plan()[nid]

    def __len__(self):
        return len(self._plan())

    def __bool__(self):
        return bool(self._plan())

    def __contains__(self, nid):
        return nid in self._plan()

    def __repr__(self):  # pragma: no cover
        return f"CopyOnWrite({self._plan()!r})"

    # -- mutations: take the copy first --------------------------------------
    def __setitem__(self, nid, node):
        self._mutable()[nid] = node

    def __add__(self, other):
        return self._mutable() + other

    def add_node(self, nid, node):
        return self._mutable().add_node(nid, node)

    def add_edge(self, source, target, relationship=None):
        return self._mutable().add_edge(source, target, relationship)

    def remove_node(self, nid, heal: bool = False):
        return self._mutable().remove_node(nid, heal)

    def remove_edge(self, source, target, relationship):
        return self._mutable().remove_edge(source, target, relationship)

    def insert_node_before(self, nid, node, before_nid):
        return self._mutable().insert_node_before(nid, node, before_nid)

    def insert_node_after(self, nid, node, after_nid):
        return self._mutable().insert_node_after(nid, node, after_nid)


class OptimizerContext:
    """Context object to carry state"""

    def __init__(self, tree: LogicalPlan):
        self.node_id = None
        self.parent_nid = None
        self.last_nid = None
        self.pre_optimized_tree = tree
        self.optimized_plan = CopyOnWritePlan(tree)

        self.seen_projections: int = 0
        self.seen_unions: int = 0
        self.seen_distincts: int = 0
        self.seen_projects_since_distinct: int = 0

        self.false_filters: list = []
        """We collect FILTER(FALSE) nodes for later rewriting"""

        self.collected_predicates: list = []
        """We collect predicates we should be able to push to reads and joins"""

        self.collected_decorrelations: list = []
        """Filter nodes holding a scalar subquery, decorrelated in complete()"""

        self.collected_identities: set = set()
        """We collect column identities so we can push column selection as close to the read as possible, including off to remote systems"""

        self.collected_distincts: list = []
        """We collect distincts to try to eliminate rows earlier"""

        self.collected_limits: list = []
        """We collect limits to to to eliminate rows earlier"""

        self.collected_joins = []
        """We collect joins to try to rewrite to inner (or filter joins)"""

        self.distincted_indentities: set = set()
        """The columns that implicitly exist in the plan because of a distinct"""

        self.bag = {}


class OptimizationStrategy:
    """Base class for one optimizer pass.

    THE MUTATION CONTRACT: every change a strategy makes must go through a
    graph operation on `context.optimized_plan` — replacing a node
    (`plan[nid] = node`, the idiomatic write-back after an in-place edit),
    or adding/removing nodes and edges. The working plan is copy-on-write
    (see CopyOnWritePlan) and the optimizer detects "did this pass change
    anything?" purely from those operations: an in-place node edit with no
    write-back neither materializes the working copy nor marks the plan's
    statistics stale, so it is a defect, not a shortcut.
    """

    rebuilds_plan: bool = False
    """True for strategies whose visit() REBUILDS the whole plan — re-adding
    every node and edge into an initially EMPTY working plan, deleting and
    rewiring by construction (what they don't re-add doesn't exist). These get
    a fresh empty LogicalPlan as `context.optimized_plan` instead of the
    copy-on-write view: handing them a populated plan leaves the ORIGINAL
    edges alive next to the rebuilt ones, silently corrupting the plan shape
    (a node with two consumers where the query has one)."""

    optimization_technique: str = "heuristic"
    """Strategies that consult plan statistics to make decisions set this to "cost".
    The optimizer refreshes statistics before running a "cost" strategy when the
    plan's ``statistics_are_stale`` flag is set."""

    provides: Tuple[str, ...] = ()
    """Capability tokens this strategy establishes for strategies ordered after it.
    Consumed by :func:`_validate_strategy_order` to assert pipeline ordering at
    construction time — purely declarative, it changes no runtime behaviour."""

    requires: Tuple[str, ...] = ()
    """Capability tokens that must be ``provides``-ed by an earlier strategy in the
    pipeline. A violation is a loud construction-time error, not a silent misorder."""

    def __init__(self, telemetry):
        self.telemetry = telemetry

    def record_decision(self, label: str, detail: str) -> None:
        """Record one costed plan choice for EXPLAIN's OPTIMIZATIONS block.

        THE CONTRACT for strategies that compare concrete plan alternatives
        (costed pairs): the comparison must be visible. ``detail`` states the
        outcome WITH the numbers it was decided on — "pushed: input est 601M >
        leg base 150M", "declined: 1.6M < 380M" — never a bare "applied". A
        cost function here may read only trusted statistics (manifest base
        counts, or ``node.statistics`` estimates the strategy documents as safe
        for its decision), and any missing statistic means "keep today's plan".
        """
        self.telemetry.add_decision(label, detail)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Visit a node in the logical plan
        """
        raise NotImplementedError(
            "Visit method must be implemented in OptimizationStrategy classes."
        )

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Complete the optimization process and return the optimized logical plan.
        """
        raise NotImplementedError(
            "Complete method must be implemented in OptimizationStrategy classes."
        )

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """
        Determine if the optimization strategy should run on the given plan.
        """
        return True
