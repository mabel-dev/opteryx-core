# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Project Fusion

Type: Heuristic
Goal: Fewer physical passes over the row

Two consecutive Project nodes (an inner SELECT/view feeding an outer SELECT) each
compile to their own physical ProjectionNode pass. When the lower Project's sole
consumer is the upper Project, they can be fused into one: the upper's expressions
are rewritten in place of the lower's, so one physical pass does the work of two.

Fusion never duplicates computation. A lower column that is a bare rename
(IDENTIFIER) is always inlined — renaming costs nothing extra to repeat. A lower
column that is a genuine computed expression is inlined only when the upper
Project references it exactly once; when referenced two or more times, it is kept
as its own entry in the fused node's ``hoisted_columns`` list instead of being
duplicated into every reference site.

``hoisted_columns`` is computed by the fused ProjectionNode but never appears in
its output row (see projection.pyx / compiler.py) — the native execution engine
already supports one program's output being loaded by a later program in the same
pass (``ExprMultiProjectOperator``, the same mechanism ``_hoist_array_operands``
in compiler.py uses for ARRAY operands), so this is a planning-time-only
representation; no new native/Cython evaluation machinery is required.

Fusion is all-or-nothing per Project pair: if the lower Project feeds more than
one consumer, or any identity referenced by the upper Project can't be resolved
against the lower Project's output, the pair is left exactly as it was. No
partial/best-effort fusion.
"""

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _identity_of(node):
    sc = getattr(node, "schema_column", None)
    return sc.identity if sc is not None else None


def _substitute_tree(node, inline_map):
    """Bottom-up rewrite of embedded IDENTIFIER references: an IDENTIFIER whose
    identity is in ``inline_map`` is replaced by a deep copy of the defining
    expression. Mutates in place (the tree already belongs to a fresh copy owned
    by this strategy invocation) — same idiom as
    ``redundant_cast._eliminate_redundant_casts``."""
    if node is None:
        return None

    if node.node_type == NodeType.IDENTIFIER:
        ident = _identity_of(node)
        if ident in inline_map:
            return inline_map[ident].copy()
        return node

    if node.left is not None:
        node.left = _substitute_tree(node.left, inline_map)
    if node.right is not None:
        node.right = _substitute_tree(node.right, inline_map)
    if node.centre is not None:
        node.centre = _substitute_tree(node.centre, inline_map)
    if node.parameters:
        node.parameters = [_substitute_tree(p, inline_map) for p in node.parameters]
    if node.node_type == NodeType.CASE:
        if node.conditions:
            node.conditions = [_substitute_tree(c, inline_map) for c in node.conditions]
        if node.results:
            node.results = [_substitute_tree(r, inline_map) for r in node.results]
        if node.else_result is not None:
            node.else_result = _substitute_tree(node.else_result, inline_map)
    return node


def _substitute_column(col, inline_map):
    """Rewrite one top-level Project column expression. The column's own
    schema_column/alias/query_column is always preserved, even when the column
    IS itself a bare reference into ``inline_map`` — in that case the defining
    expression is wrapped in a transparent NESTED node carrying the original
    identity, the same pattern ``redundant_cast`` uses for a no-op CAST."""
    if col.node_type == NodeType.IDENTIFIER:
        ident = _identity_of(col)
        if ident in inline_map:
            nested = Node(node_type=NodeType.NESTED)
            nested.centre = inline_map[ident].copy()
            nested.schema_column = col.schema_column
            nested.alias = col.alias
            nested.query_column = getattr(col, "query_column", None)
            return nested
        return col
    return _substitute_tree(col, inline_map)


class ProjectFusionStrategy(OptimizationStrategy):
    provides = ("project-fused",)
    requires = ("projection-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Project:
            edges = context.optimized_plan.outgoing_edges(context.node_id)
            if len(edges) == 1:
                upper_id = edges[0][1]
                upper = context.optimized_plan[upper_id]
                if upper.node_type == LogicalPlanStepType.Project:
                    fused = self._try_fuse(node, upper)
                    if fused is not None:
                        context.optimized_plan[upper_id] = fused
                        context.optimized_plan.remove_node(context.node_id, heal=True)
                        self.telemetry.optimization_fuse_operators_project += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    @staticmethod
    def _try_fuse(lower: LogicalPlanNode, upper: LogicalPlanNode):
        """Build a fused Project node, or return None if the pair can't be fused
        safely (see module docstring — no partial fusion)."""
        lower_cols = list(lower.columns or []) + list(getattr(lower, "passthrough_columns", None) or [])
        lower_map = {}
        for col in lower_cols:
            if col.node_type == NodeType.WILDCARD:
                return None  # unresolved wildcard — should not survive to bind time, bail defensively
            ident = _identity_of(col)
            if ident is None:
                return None  # unbound — don't guess
            lower_map[ident] = col

        upper_cols = list(upper.columns or [])
        upper_order = list(getattr(upper, "passthrough_columns", None) or [])
        upper_exprs = upper_cols + upper_order
        if not upper_exprs:
            return None

        # `upper` may itself already be a fused node from an earlier pair in the
        # same chain (A<-B<-C fuses B into A first, then C is checked against the
        # already-fused A). Its own hoisted_columns are self-produced — a
        # reference to one of those identities is satisfied by `upper` itself, not
        # by `lower`, and must not be treated as an unresolved reference.
        upper_hoisted_identities = {
            _identity_of(c) for c in getattr(upper, "hoisted_columns", None) or []
        }

        # Count references upper makes into lower's identity space; also validate
        # every IDENTIFIER upper carries is bound (schema_column present).
        counts = {}
        for expr in upper_exprs:
            if expr is None:
                return None
            for ident_node in get_all_nodes_of_type(expr, (NodeType.IDENTIFIER,)):
                sc = getattr(ident_node, "schema_column", None)
                if sc is None or sc.identity is None:
                    return None  # unbound — don't guess
                if sc.identity in upper_hoisted_identities:
                    continue  # satisfied by upper's own hoisted list, not by lower
                if sc.identity not in lower_map:
                    return None  # upper references something lower doesn't emit — bail, don't guess
                counts[sc.identity] = counts.get(sc.identity, 0) + 1

        inline_map = {}
        hoisted = []
        for ident, count in counts.items():
            lower_expr = lower_map[ident]
            if lower_expr.node_type == NodeType.IDENTIFIER or count == 1:
                inline_map[ident] = lower_expr
            else:
                hoisted.append(lower_expr.copy())

        fused = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        fused.columns = [_substitute_column(c, inline_map) for c in upper_cols]
        fused.passthrough_columns = [_substitute_column(c, inline_map) for c in upper_order]
        # Carry forward any hoisted columns `upper` already had (a prior fusion
        # earlier in the same chain) alongside the ones this fusion just hoisted.
        fused.hoisted_columns = list(getattr(upper, "hoisted_columns", None) or []) + hoisted
        fused.except_columns = getattr(upper, "except_columns", None)
        return fused
