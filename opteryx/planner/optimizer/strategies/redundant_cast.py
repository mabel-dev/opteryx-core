# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Redundant Cast Elimination

Type: Heuristic
Goal: Evaluate Once (don't evaluate at all)

A CAST whose operand already has the cast's target type is impotent: it produces
a value indistinguishable from the operand. We never promised to *perform* a cast,
only to return a value of the requested type — so when the operand already carries
that type we decide, at plan time, to populate the column from the operand directly
with no cast kernel.

The rewrite is context-aware, because the cast column's *identity* matters in some places
and not others:

* Identity context — something downstream references the cast column's identity, so the
  identity must survive. We rewrite the CAST node into a transparent `NESTED` wrapper around
  the operand, carrying the cast's schema_column. `NESTED` lowers to just its operand at
  compile time (no instruction emitted), so the evaluator aliases the cast column's identity
  straight onto the operand's buffer — no per-row cast kernel.
  This is the SELECT list: the result (EXIT) schema names `CAST(x AS INTEGER)`.

* Value context — the node is consumed for its *value*; nothing downstream references the
  cast's identity. We replace the CAST node with the bare operand `x` (its own identity).
  Three places:

  - Filter conditions, so every predicate strategy that keys on a raw `IDENTIFIER` (reader
    pushdown, parquet row-group pruning, LIKE/Eq/range rewrites) treats it as the raw column
    it is, rather than the opaque result of a runtime calculation. Wrapping it in `NESTED`
    here would hide `x`'s identity behind the cast's and forfeit all of that.
  - Aggregate operands, where the AGGREGATOR node carries its own schema_column and the
    operand is read only to be folded. A bare operand also skips the compiler's
    `_project_agg_operands` hoist entirely (it short-circuits on `IDENTIFIER`), so the
    aggregate reads the scanned column directly instead of a projected copy of it.
  - GROUP BY keys — value context here is forced, not chosen. The Project above the group
    has had the same CAST folded to a `NESTED` reading the *operand*, so the group must emit
    the operand for that read to resolve; a `NESTED` key would emit `x::DOUBLE` and strand
    the projection looking for `x`. `GroupedAggregateHashedNode` re-derives
    `group_by_columns` from the key nodes after optimization, so the key list follows the
    substitution rather than being stranded on the cast's identity.

Note that only a *top-level* node's identity is ever referenced, so a CAST nested inside a
larger expression is always value context. The projection rule threads identity context all
the way down rather than switching after the first level; that is conservative, not wrong —
the surplus `NESTED` wrappers lower to nothing.

Type equality is the unified `ColumnType` `==` (frozen dataclass): physical tag plus the
logical descriptor (DECIMAL precision/scale, TIMESTAMP unit) and ARRAY element. Only a
true no-op folds — `DECIMAL(10,2) -> DECIMAL(12,2)`, `VARCHAR -> NVARCHAR`, etc. are kept.

This mirrors the plan-time rewrites in Constant Folding (e.g. `x * 1 -> x`), which also
replace an expression with a transparent `NESTED` wrapper around its operand.
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _eliminate_redundant_casts(node, telemetry, value_context=False):
    """Depth-first rewrite of no-op CAST nodes anywhere in an expression tree.

    ``value_context`` is threaded down unchanged: once we are inside a Filter
    condition or an aggregate operand, every CAST below it is consumed for its
    value, so it folds to the bare operand rather than a NESTED wrapper (see
    module docstring).
    """
    if node is None:
        return node

    # Recurse into children first so nested casts collapse bottom-up.
    if node.left is not None:
        node.left = _eliminate_redundant_casts(node.left, telemetry, value_context)
    if node.right is not None:
        node.right = _eliminate_redundant_casts(node.right, telemetry, value_context)
    if node.centre is not None:
        node.centre = _eliminate_redundant_casts(node.centre, telemetry, value_context)
    if node.parameters:
        node.parameters = [
            _eliminate_redundant_casts(p, telemetry, value_context) for p in node.parameters
        ]
    if node.node_type == NodeType.CASE:
        if node.conditions:
            node.conditions = [
                _eliminate_redundant_casts(c, telemetry, value_context) for c in node.conditions
            ]
        if node.results:
            node.results = [
                _eliminate_redundant_casts(r, telemetry, value_context) for r in node.results
            ]
        if node.else_result is not None:
            node.else_result = _eliminate_redundant_casts(
                node.else_result, telemetry, value_context
            )

    if node.node_type != NodeType.CAST:
        return node

    operand = node.left
    if operand is None or operand.schema_column is None or node.schema_column is None:
        return node

    # A FORMAT-bearing CAST is never a no-op even when source/target ColumnType
    # match — the FORMAT still drives a real parse/format kernel (or, for an
    # unsupported pairing, a deliberate fail-loud error at compile time). Eliding
    # the node here would silently swallow both.
    if getattr(node, "format", None) is not None:
        return node

    source_type = operand.schema_column.column_type
    target_type = node.schema_column.column_type
    if source_type is None or target_type is None or source_type != target_type:
        return node

    telemetry.optimization_remove_redundant_cast += 1

    # Value context: the cast's identity is never referenced downstream — the predicate or
    # the aggregate consumes the value — so substitute the bare operand. Pushdown /
    # row-group pruning / the IDENTIFIER-keyed predicate rewrites then see the raw column,
    # not an opaque node.
    if value_context:
        return operand

    # Identity context: the EXIT schema or the group's key list references the cast column's
    # identity, so a transparent NESTED wrapper carries the cast's schema_column (keeping its
    # identity and name) while lowering to just its centre operand — the evaluator aliases
    # the operand's buffer onto the cast column's identity, with no cast kernel.
    nested = Node(node_type=NodeType.NESTED)
    nested.centre = operand
    nested.schema_column = node.schema_column
    nested.query_column = node.query_column
    nested.alias = node.alias
    return nested


class RedundantCastEliminationStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Project:
            node.columns = [_eliminate_redundant_casts(c, self.telemetry) for c in node.columns]
            context.optimized_plan[context.node_id] = node

        elif node.node_type == LogicalPlanStepType.Filter:
            node.condition = _eliminate_redundant_casts(
                node.condition, self.telemetry, value_context=True
            )
            context.optimized_plan[context.node_id] = node

        elif node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
        ):
            # `groups` and `projection` are None on an AggregateAndGroup that
            # decorrelate_subquery promoted from a bare Aggregate, so each list is guarded
            # rather than assumed — an unset list is nothing to rewrite, not an error.
            # Aggregate operands are folded — nothing reads the operand's identity, so the
            # bare column substitutes and the compiler's agg-operand hoist never fires.
            if node.aggregates is not None:
                node.aggregates = [
                    _eliminate_redundant_casts(a, self.telemetry, value_context=True)
                    for a in node.aggregates
                ]
            # GROUP BY keys are value context too, and must be: the Project above the group
            # has had the same CAST folded to a NESTED wrapper reading the *operand*, so the
            # group has to emit the operand for that read to resolve. Substituting the bare
            # column keeps both sides naming `x`; GroupedAggregateHashedNode re-derives
            # `group_by_columns` from these nodes after optimization, so the key list
            # follows the substitution rather than being stranded on the cast's identity.
            if node.node_type == LogicalPlanStepType.AggregateAndGroup:
                if node.groups is not None:
                    node.groups = [
                        _eliminate_redundant_casts(g, self.telemetry, value_context=True)
                        for g in node.groups
                    ]
                # `projection` is what a positional `GROUP BY 1` resolves against at
                # physical-node construction — rewrite it the same way the explicit keys
                # are, or the two spellings of the same GROUP BY would diverge.
                if node.projection is not None:
                    node.projection = [
                        _eliminate_redundant_casts(c, self.telemetry, value_context=True)
                        for c in node.projection
                    ]
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
