# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Filter-Implied Group Key Reduction

Type: Heuristic
Goal: Strip GROUP BY keys that are provably single-valued because a Filter
      below the aggregate constrains them with a col = literal equality.

A query like:
    SELECT name, COUNT(*) FROM t WHERE name = 'bob' GROUP BY name
has `name` forced constant by the filter. Grouping on it adds no information
to the partition. This rule strips such keys from the aggregate and
reconstructs them as literals in a Project inserted immediately above.

Walk rule: descend through Filter, Project, Limit, Order, Distinct, and
similar pass-through nodes. Stop at Join, DependentJoin, Union, Intersect,
Except, Subquery, Aggregate, or another AggregateAndGroup — any of those
can invalidate the equality guarantee.

Safety: if stripping would leave zero group keys, keep one to preserve the
AggregateAndGroup semantics (empty input → zero output rows, not one).
"""

from opteryx.compiled.structures.expressions import Expression
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan, PlanStep, LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy, OptimizerContext
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.plan_steps import ProjectStep

_STOP_TYPES = frozenset(
    {
        LogicalPlanStepType.Join,
        LogicalPlanStepType.DependentJoin,
        LogicalPlanStepType.Union,
        LogicalPlanStepType.Intersect,
        LogicalPlanStepType.Except,
        LogicalPlanStepType.Subquery,
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
    }
)


def _extract_equalities(expr, equalities: dict) -> None:
    """Collect col=literal equalities from an AND-tree.

    Descends through AND, NESTED, and DNF nodes only. OR, NOT, and function
    calls terminate the walk on that branch (matching filter.pyx behaviour).
    """
    if expr is None:
        return
    stack = [expr]
    while stack:
        n = stack.pop()
        nt = n.node_type
        if nt == NodeType.NESTED:
            if n.centre is not None:
                stack.append(n.centre)
            continue
        if nt == NodeType.AND:
            if n.left is not None:
                stack.append(n.left)
            if n.right is not None:
                stack.append(n.right)
            continue
        if nt == NodeType.DNF:
            for sub in n.parameters or []:
                if sub is not None:
                    stack.append(sub)
            continue
        if nt != NodeType.COMPARISON_OPERATOR or n.value != "Eq":
            continue
        left, right = n.left, n.right
        if left is None or right is None:
            continue
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident, lit = left, right
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident, lit = right, left
        else:
            continue
        qn = ident.qualified_name
        if qn is None:
            continue
        val = lit.value
        if val is None:
            continue
        equalities[qn] = val


def _collect_equality_predicates(plan: LogicalPlan, start_nid) -> dict:
    """Walk downward from start_nid collecting col=literal equalities from Filters."""
    equalities: dict = {}
    stack = list(plan.ingoing_edges(start_nid))
    while stack:
        child_id, _, _ = stack.pop()
        node = plan[child_id]
        if node.node_type in _STOP_TYPES:
            continue
        if node.node_type == LogicalPlanStepType.Filter:
            _extract_equalities(node.condition, equalities)
        stack.extend(plan.ingoing_edges(child_id))
    return equalities


def _make_passthrough(original: Expression) -> Expression:
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column=(
            original.schema_column.name if original.schema_column else getattr(original, "value", None)
        ),
        schema_column=original.schema_column,
    )


def _make_constant_literal(original: Expression, value) -> Expression:
    """Create a LITERAL node that emits value under the original column's schema identity."""
    lit = Literal()
    lit.value = value
    lit.schema_column = original.schema_column
    return lit


class FilterImpliedGroupKeyReductionStrategy(OptimizationStrategy):
    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.AggregateAndGroup:
            return context

        groups = node.groups
        if not groups:
            return context

        bare_keys = [g for g in groups if g.node_type == NodeType.IDENTIFIER]
        if not bare_keys:
            return context

        equalities = _collect_equality_predicates(context.pre_optimized_tree, context.node_id)
        if not equalities:
            return context

        implied = [g for g in bare_keys if g.qualified_name in equalities]
        if not implied:
            return context

        # Validate schema_column is present on every node we'll reference.
        for g in groups:
            if g.schema_column is None:
                return context
        aggregates = node.aggregates or []
        for agg in aggregates:
            if agg.schema_column is None:
                return context

        remaining = [g for g in groups if g not in implied]
        if not remaining:
            # Stripping all keys would change the node to Aggregate semantics.
            # Keep the first implied key as the surviving group key.
            keep_one = implied[0]
            implied = implied[1:]
            if not implied:
                return context  # Only one group key; nothing to strip.
            remaining = [keep_one]

        node.groups = remaining
        context.optimized_plan[context.node_id] = node

        project_columns = []
        for g in remaining:
            project_columns.append(_make_passthrough(g))
        for g in implied:
            project_columns.append(_make_constant_literal(g, equalities[g.qualified_name]))
        for agg in aggregates:
            project_columns.append(_make_passthrough(agg))

        project_node = ProjectStep()
        project_node.columns = project_columns
        project_node.passthrough_columns = []

        context.optimized_plan.insert_node_after(
            random_string(), project_node, context.node_id
        )
        self.telemetry.optimization_filter_implied_group_key_reduction = (
            getattr(self.telemetry, "optimization_filter_implied_group_key_reduction", 0)
            + len(implied)
        )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
