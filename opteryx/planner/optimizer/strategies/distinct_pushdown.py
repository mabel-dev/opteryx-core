# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Distinct Pushdown

Type: Heuristic
Goal: Reduce Rows

This is a very specific rule, on a CROSS JOIN UNNEST, if the result
is the only column in a DISTINCT clause, we push the DISTINCT into
the JOIN.

We've written as a Optimization rule rather than in the JOIN code
as it is expected other instances of pushing DISTINCT may be found.

Order:
    This plan must run after the Projection Pushdown
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

"""
Aggregations we can push the DISTINCT past
"""


class DistinctPushdownStrategy(OptimizationStrategy):
    requires = ("projection-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if (node.node_type == LogicalPlanStepType.Distinct) and node.on is None:
            node.nid = context.node_id
            context.collected_distincts.append(node)
            return context

        # NOTE: DISTINCT-into-CROSS JOIN UNNEST folding is intentionally disabled.
        # The native UnnestOperator (src/cpp/engine/native_unnest.hpp) does plain
        # expansion only — it does not fold a `node.distinct` dedup. Leaving the
        # DISTINCT as a standalone node lets it compile to the proven native
        # DistinctSink after the unnest (correct; it dedups the fully-expanded
        # stream instead of the intermediate — a perf, not correctness, difference).
        # Re-enable once the native operator folds distinct. The Unnest barrier
        # below still clears collected DISTINCTs so they are not pushed past it.

        if node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Limit,
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.Subquery,
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Unnest,
        ):
            # we don't push past here
            context.collected_distincts.clear()

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are DISTINCT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Distinct,))
        return len(candidates) > 0
