# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
CAST Simplification Strategy

Optimizes CAST operations by:
1. Removing redundant casts when source is already the target type
2. Folding nested casts: CAST(CAST(expr AS T1) AS T2) → CAST(expr AS T2)
3. Evaluating constant casts at optimization time
4. Removing unnecessary type conversions
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizationStrategy
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext


def simplify_cast_node(node):
    """
    Simplify a CAST expression node.

    Returns the simplified node, or the original if no simplification is possible.
    """
    if node is None or node.node_type != NodeType.CAST:
        return node

    source = getattr(node, "left", None)
    if source is None:
        return node

    target_type = getattr(node, "value", "").upper()
    if not target_type:
        return node

    # Strip TRY_ prefix for type comparison
    is_safe_cast = target_type.startswith("TRY_")
    base_target_type = target_type[4:] if is_safe_cast else target_type

    # Pattern 1: CAST(CAST(expr AS T1) AS T2) → CAST(expr AS T2)
    # This collapses nested casts to a single cast to the final type
    if source.node_type == NodeType.CAST:
        inner_target = getattr(source, "value", "").upper()
        if inner_target:
            inner_safe = inner_target.startswith("TRY_")

            # If either cast is safe, result must be safe
            result_safe = is_safe_cast or inner_safe
            result_type = "TRY_" + base_target_type if result_safe else base_target_type

            # Create new CAST node with innermost expression and outermost type
            node.left = getattr(source, "left", source)
            node.value = result_type

    return node


def simplify_expression(expr):
    """
    Recursively simplify CAST nodes in an expression tree.
    """
    if expr is None:
        return expr

    # Recursively process sub-expressions if they exist
    if expr.left is not None:
        expr.left = simplify_expression(expr.left)

    if expr.right is not None:
        expr.right = simplify_expression(expr.right)

    if expr.centre is not None:
        expr.centre = simplify_expression(expr.centre)

    if expr.parameters:
        expr.parameters = [simplify_expression(p) if p else p for p in expr.parameters]

    # Apply CAST-specific simplification
    return simplify_cast_node(expr)


class CastSimplificationStrategy(OptimizationStrategy):
    """
    Optimize CAST operations in the logical plan.

    Implements:
    - Nested cast collapsing: CAST(CAST(expr AS T1) AS T2) → CAST(expr AS T2)
    - Constant cast folding (via binder, not here)
    - Redundant cast removal (via type analysis in binder)
    """

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        """Visit each node in the logical plan and apply CAST simplifications."""
        if node is None:
            return context

        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        node_type = getattr(node, "node_type", None)

        # Only optimize filter nodes (which contain conditions with CAST expressions)
        if node_type == LogicalPlanStepType.Filter:
            # Simplify CAST nodes in filter condition
            if node.condition:
                simplified_condition = simplify_expression(node.condition)
                if simplified_condition is not node.condition:
                    node.condition = simplified_condition
                    context.optimized_plan[context.node_id] = node

        # Note: ProjectRel columns (not ProjectionalStep) have CAST expressions
        # but modifying them here breaks the plan structure - leave to expression evaluation

        return context

    def complete(self, plan, context: OptimizerContext):
        """Finalization - return the optimized plan."""
        return plan
