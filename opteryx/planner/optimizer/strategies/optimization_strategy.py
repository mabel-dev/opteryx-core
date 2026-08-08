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


class OptimizerContext:
    """Context object to carry state"""

    def __init__(self, tree: LogicalPlan):
        self.node_id = None
        self.parent_nid = None
        self.last_nid = None
        self.pre_optimized_tree = tree
        self.optimized_plan = LogicalPlan()

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
