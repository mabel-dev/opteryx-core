# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Filter-Implied Group Key Reduction

Type: Heuristic
Goal: Strip GROUP BY keys a Filter below the aggregate pins to a single literal

A key constrained by a `col = literal` equality below the aggregate adds nothing to
the partition. It is removed from the aggregate's keys and re-emitted as a literal
by a Project inserted directly above. The equalities are read from the Filter nodes
in the chain AND from the predicates the Scan has absorbed — after pushdown the
WHERE equality lives on the scan for every connector that accepts predicates, and
a pushed predicate is a filter the reader applies, so it gives the same guarantee.

Order:
    After:  PredicatePushdown (the equality it reads is in `scan.predicates` or a
            declined Filter node, both in their final place), GroupKeyReduction
            (strips derived expressions first, so this sees bare keys).
    Before: ProjectionPushdown and ProjectFusion (the inserted Project is pruned and
            fused like any other).

Rewrite:
    SELECT name, city, COUNT(*) FROM t WHERE name = 'bob' GROUP BY name, city
    ->
    SELECT 'bob' AS name, city, COUNT(*) FROM t WHERE name = 'bob' GROUP BY city

Walk rule: descend through Filter, Project, Limit, Order, Distinct, and
similar pass-through nodes, reading Filter conditions and a Scan's absorbed
predicates. Stop at Join, Union, Intersect, Except,
Subquery, Aggregate, or another AggregateAndGroup — any of those
can invalidate the equality guarantee.

Safety: if stripping would leave zero group keys, keep one to preserve the
AggregateAndGroup semantics (empty input → zero output rows, not one).
"""

from opteryx.compiled.structures.expressions import Expression
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan, PlanStep, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext, get_nodes_of_type_from_logical_plan
from opteryx.compiled.structures.expressions import Literal
from opteryx.types.literal_values import native_literal_value
from opteryx.compiled.structures.plan_steps import ProjectStep

_STOP_TYPES = frozenset(
    {
        LogicalPlanStepType.Join,
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
    """Walk downward from start_nid collecting col=literal equalities.

    Two sources, both filters on the rows that reach the aggregate:
      - Filter nodes in the chain (predicates a connector declined, or that no
        connector was offered);
      - the predicates a Scan has ABSORBED (`scan.predicates`). This strategy runs
        after PredicatePushdown, so on every connector that accepts pushdown
        (parquet, skene, SQL) the WHERE equality is here and nowhere else; the
        reader applies it, so the scan emits only rows satisfying it — exactly
        the guarantee a Filter node gives.
    """
    equalities: dict = {}
    stack = list(plan.ingoing_edges(start_nid))
    while stack:
        child_id, _, _ = stack.pop()
        node = plan[child_id]
        if node.node_type in _STOP_TYPES:
            continue
        if node.node_type == LogicalPlanStepType.Filter:
            _extract_equalities(node.condition, equalities)
        elif node.node_type == LogicalPlanStepType.Scan:
            for predicate in node.predicates or ():
                _extract_equalities(predicate, equalities)
        stack.extend(plan.ingoing_edges(child_id))
    return equalities


def _make_passthrough(original: Expression) -> Expression:
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column=original.schema_column.name,
        schema_column=original.schema_column,
        arena=original.arena,
    )


def _make_constant_literal(original: Expression, value) -> Expression:
    """Create a LITERAL node that emits value under the original column's schema
    identity, typed as that column (a literal is never untyped)."""
    column_type = original.schema_column.column_type
    return Literal(
        value=native_literal_value(value, column_type),
        type=column_type,
        schema_column=original.schema_column,
        arena=original.arena,
    )


class FilterImpliedGroupKeyReductionStrategy(OptimizationStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        # visit() acts on AggregateAndGroup nodes only.
        return (
            len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.AggregateAndGroup,)))
            > 0
        )

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
            project_node, context.node_id
        )
        self.telemetry.optimization_filter_implied_group_key_reduction += len(implied)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
