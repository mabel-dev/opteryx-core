# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Function Rewriter

Type: Heuristic
Goal: Rewrite functions in projections and aggregations to cheaper equivalents

Applies PredicateRewrite's expression rewrites (the shared `_rewrite_predicate`,
which handles both comparison forms and function nodes) to the non-Filter nodes:
Project columns, and an Aggregate's groups, aggregates and projection.
PredicateRewrite covers Filter conditions; this covers everything else, so a
SELECT-list `SUBSTRING(x, 1, n)` is rewritten to `LEFT(x, n)` exactly as a
WHERE-clause one is.

Order:
    After:  ConstantFolding and RedundantCastElimination (expressions are in their
            folded form before they are rewritten); PredicateRewrite (same rewrite
            table, kept adjacent so the two cannot drift).
    Before: GroupKeyReduction (reads the final spelling of GROUP BY expressions),
            ProjectionPushdown.

Rewrite:
    SELECT SUBSTRING(name, 1, 3), COALESCE(a, b) FROM t
    ->
    SELECT LEFT(name, 3), IFNULL(a, b) FROM t
"""

from opteryx.planner.logical_planner import LogicalPlan, PlanStep, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext
from .predicate_rewriter import _rewrite_predicate


class FunctionRewriteStrategy(OptimizationStrategy):
    def _rewrite_expression_list(self, expressions, *, plan_context):
        if not expressions:
            return expressions
        return [
            _rewrite_predicate(expr, self.telemetry, plan_context=plan_context)
            for expr in expressions
        ]

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = self._rewrite_expression_list(
                node.columns, plan_context=context.plan_context
            )
            context.optimized_plan[context.node_id] = node

        if node.node_type in {LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup}:
            if node.groups:
                node.groups = self._rewrite_expression_list(
                    node.groups, plan_context=context.plan_context
                )
            if node.aggregates:
                node.aggregates = self._rewrite_expression_list(
                    node.aggregates, plan_context=context.plan_context
                )
            if node.projection:
                node.projection = self._rewrite_expression_list(
                    node.projection, plan_context=context.plan_context
                )
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
