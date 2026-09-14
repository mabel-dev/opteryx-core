# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Redundant Sort Elimination

Type: Heuristic
Goal: Reduce Work

Drops a sort whose output is fully re-sorted further up the plan. The common
shape is a view (or an inline subquery) that carries its own ORDER BY, read by
a query that orders by the same thing:

    SELECT ... FROM a_view_with_an_order_by ORDER BY yr DESC

    Order(yr DESC)            <- the query's
    └─ Project
       └─ Order(yr DESC)      <- the view's, entirely wasted
          └─ Reader

The lower sort's ordering is discarded the moment the upper sort runs, so the
lower one is removed and the upper one kept.

Two things this rule deliberately does NOT do:

1. It does not compare sort keys. A sort whose result is re-sorted is wasted
   work whatever it sorted by — matching keys is a special case, not the
   condition.

2. It never removes the UPPER sort. Doing that would require proving every
   operator in between preserves row order, which in a parallel engine is not
   something to assume; the lower sort's removal needs no such proof, because
   nothing between the two can observe an ordering that the upper sort is
   about to overwrite.

The operators permitted between the two sorts are an allow-list, deny by
default. Only Project, Filter and Subquery qualify: each maps rows one-to-one
or drops them, and none reads the ordering of its input. Everything else makes
the lower sort load-bearing and must block the rewrite —

  Limit / HeapSort     select WHICH rows survive, from the incoming order
  Aggregate/AndGroup   ordered aggregates (ARRAY_AGG, first/last-style)
  Distinct             decides which duplicate survives
  Window               frames are evaluated over the incoming order
  Join / Union / ...    not order-preserving, and not enumerable in advance

A HeapSort is never removed as the lower node either: it is a fused
Order+Limit, so dropping it would drop the LIMIT with it.
"""

from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Node types that may sit between a sort and the sort that supersedes it. Row
# mapping is one-to-one or narrowing, and none of them consumes the ordering of
# its input. Deny-by-default: anything absent from this set stops the descent.
_ORDER_TRANSPARENT_TYPES = {
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Subquery,
}


def _dominated_sort(plan, order_nid):
    """The nid of the sort `order_nid` makes redundant, or None.

    Descends from `order_nid` toward the sources (edges run child -> parent, so
    `ingoing_edges` walks downward) through order-transparent nodes only, and
    stops at the first `Order` it reaches.

    Every node on the path — including the sort found — must have exactly one
    provider and exactly one consumer. A branch point means some other consumer
    may still be reading the lower sort's ordering, and this rule has proven
    nothing about that consumer.
    """
    current = order_nid
    while True:
        providers = plan.ingoing_edges(current)
        if len(providers) != 1:
            return None
        child = providers[0][0]
        if len(plan.outgoing_edges(child)) != 1:
            # a second consumer may depend on this ordering
            return None
        child_type = plan[child].node_type
        if child_type == LogicalPlanStepType.Order:
            return child
        if child_type not in _ORDER_TRANSPARENT_TYPES:
            return None
        current = child


class RedundantSortEliminationStrategy(OptimizationStrategy):
    """Drop a sort whose ordering a later sort overwrites."""

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        # Decided globally in `complete` — the rewrite is a walk between pairs
        # of nodes, not a per-node rewrite, and mutating mid-traversal is
        # unsafe while the walk is still descending.
        return context

    def should_i_run(self, plan) -> bool:
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Order,))) > 1

    def complete(self, plan, context: OptimizerContext) -> object:
        # Candidates are collected against the untouched plan, then removed, so
        # a chain of three sorts resolves to the topmost one in a single pass:
        # the middle sort is found from the top, the bottom from the middle.
        redundant = set()
        for nid, _ in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Order,)):
            dominated = _dominated_sort(plan, nid)
            if dominated is not None:
                redundant.add(dominated)

        for nid in redundant:
            plan.remove_node(nid, heal=True)
            if self.telemetry is not None:
                self.telemetry.optimization_remove_redundant_sort += 1

        return plan
