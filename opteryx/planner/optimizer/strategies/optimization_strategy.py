# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType


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
