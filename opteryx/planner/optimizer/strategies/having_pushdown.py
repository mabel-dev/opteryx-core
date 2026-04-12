# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Having Pushdown

Type: Heuristic
Goal: Reduce Rows

This rule pushes HAVING clauses down the query plan when possible,
reducing the number of rows that need to be processed by upstream operations.

A HAVING clause filters the results of GROUP BY aggregations. In some cases,
conditions in the HAVING clause can be evaluated earlier in the plan.

Order:
    This plan should run after aggregation planning but before final optimization.
"""

from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode

from .optimization_strategy import OptimizationStrategy, OptimizerContext


class HavingPushdownStrategy(OptimizationStrategy):
    """
    Optimization strategy for pushing down HAVING clauses.

    Currently implemented as a no-op placeholder. HAVING clauses are
    typically not candidates for pushdown since they reference aggregation
    results. This strategy is maintained for future enhancement and
    for cases where partial pushdown might be beneficial.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Visit a node in the logical plan during traversal.

        Args:
            node: The logical plan node to visit
            context: Optimizer context with state

        Returns:
            The updated optimizer context
        """
        # HAVING clauses filter post-aggregation results and cannot generally
        # be pushed down before aggregation. This is a no-op for now.
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Complete the optimization and return the final plan.

        Args:
            plan: The logical plan being optimized
            context: Optimizer context with state

        Returns:
            The optimized logical plan (unchanged in this implementation)
        """
        # No transformations performed - return plan unchanged
        return plan
