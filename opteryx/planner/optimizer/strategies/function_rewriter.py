# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Function rewriter

Type: Heuristic
Goal: Rewrite functions to more efficient equivalents in projection and aggregation contexts.

Applies expression-level rewrites to non-filter plan nodes (Project, Aggregate,
AggregateAndGroup). The rewrite logic is shared with PredicateRewriteStrategy via
_rewrite_predicate, which handles both comparison forms and function nodes.
"""

from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext
from .predicate_rewriter import _rewrite_predicate


class FunctionRewriteStrategy(OptimizationStrategy):
    def _rewrite_expression_list(self, expressions):
        if not expressions:
            return expressions
        return [_rewrite_predicate(expr, self.telemetry) for expr in expressions]

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Project:
            node.columns = self._rewrite_expression_list(node.columns)
            context.optimized_plan[context.node_id] = node

        if node.node_type in {LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup}:
            if getattr(node, "groups", None):
                node.groups = self._rewrite_expression_list(node.groups)
            if getattr(node, "aggregates", None):
                node.aggregates = self._rewrite_expression_list(node.aggregates)
            if getattr(node, "projection", None):
                node.projection = self._rewrite_expression_list(node.projection)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
