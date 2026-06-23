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

The rewrite is context-aware, because the cast column's *identity* matters in one place
and not the other:

* Projection context — the result (EXIT) schema references the cast column's identity
  (e.g. `CAST(x AS INTEGER)`), so the identity must survive. We rewrite the CAST node into
  a transparent `NESTED` wrapper around the operand, carrying the cast's schema_column.
  `NESTED` lowers to just its operand at compile time (no instruction emitted), so the
  evaluator aliases the cast column's identity straight onto the operand's buffer — no
  per-row cast kernel, no copy.

* Predicate context — a CAST inside a Filter condition is consumed for its *value*; nothing
  downstream references the cast's identity. We replace the CAST node with the bare operand
  `x` (its own identity), so every predicate strategy that keys on a raw `IDENTIFIER` —
  reader pushdown, parquet row-group pruning, LIKE/Eq/range rewrites — treats it as the raw
  column it is, rather than the opaque result of a runtime calculation. Wrapping it in
  `NESTED` here would hide `x`'s identity behind the cast's and forfeit all of that.

Type equality is the unified `ColumnType` `==` (frozen dataclass): physical tag plus the
logical descriptor (DECIMAL precision/scale, TIMESTAMP unit) and ARRAY element. Only a
true no-op folds — `DECIMAL(10,2) -> DECIMAL(12,2)`, `VARCHAR -> NVARCHAR`, etc. are kept.

This mirrors the plan-time rewrites in Constant Folding (e.g. `x * 1 -> x`), which also
replace an expression with a `_PASSTHRU` of its operand.
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _eliminate_redundant_casts(node, telemetry, in_predicate=False):
    """Depth-first rewrite of no-op CAST nodes anywhere in an expression tree.

    ``in_predicate`` is threaded down unchanged: a CAST anywhere inside a Filter
    condition is in predicate context, so it folds to the bare operand rather than
    a NESTED wrapper (see module docstring).
    """
    if node is None:
        return node

    # Recurse into children first so nested casts collapse bottom-up.
    if node.left is not None:
        node.left = _eliminate_redundant_casts(node.left, telemetry, in_predicate)
    if node.right is not None:
        node.right = _eliminate_redundant_casts(node.right, telemetry, in_predicate)
    if node.centre is not None:
        node.centre = _eliminate_redundant_casts(node.centre, telemetry, in_predicate)
    if node.parameters:
        node.parameters = [
            _eliminate_redundant_casts(p, telemetry, in_predicate) for p in node.parameters
        ]
    if node.node_type == NodeType.CASE:
        if node.conditions:
            node.conditions = [
                _eliminate_redundant_casts(c, telemetry, in_predicate) for c in node.conditions
            ]
        if node.results:
            node.results = [
                _eliminate_redundant_casts(r, telemetry, in_predicate) for r in node.results
            ]
        if node.else_result is not None:
            node.else_result = _eliminate_redundant_casts(
                node.else_result, telemetry, in_predicate
            )

    if node.node_type != NodeType.CAST:
        return node

    operand = node.left
    if operand is None or operand.schema_column is None or node.schema_column is None:
        return node

    source_type = operand.schema_column.column_type
    target_type = node.schema_column.column_type
    if source_type is None or target_type is None or source_type != target_type:
        return node

    telemetry.optimization_remove_redundant_cast += 1

    # Predicate context: the cast's identity is never referenced downstream — the predicate
    # consumes the value — so substitute the bare operand. Pushdown / row-group pruning /
    # the IDENTIFIER-keyed predicate rewrites then see the raw column, not an opaque node.
    if in_predicate:
        return operand

    # Projection context: the EXIT schema references the cast column's identity, so a
    # transparent NESTED wrapper carries the cast's schema_column (keeping its identity and
    # name) while lowering to just its centre operand — the evaluator aliases the operand's
    # buffer onto the cast column's identity, with no cast kernel and no copy.
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
                node.condition, self.telemetry, in_predicate=True
            )
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
