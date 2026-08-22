# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Group Key Reduction

Type: Heuristic
Goal: Remove GROUP BY expressions that are deterministic scalar functions of a bare
      column already present in the GROUP BY, reducing the grouping key set.

If GROUP BY contains (x, f(x), g(x)), the partition is determined entirely by x.
The derived expressions are removed from the aggregate and recomputed in a Project
node inserted immediately above it.

Handles both arithmetic (x + 1) and function (f(x)) forms, provided all identifier
leaves within the expression resolve to a single base column already in the GROUP BY
as a bare IDENTIFIER.
"""

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.expression_traits import has_volatile_function
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _collect_identifier_names(expr) -> set:
    return {
        node.qualified_name
        for node in get_all_nodes_of_type(expr, (NodeType.IDENTIFIER,))
        if node.qualified_name is not None
    }


def _is_reducible(expr, bare_key_names: set) -> bool:
    """Return True if expr is a deterministic expression that adds nothing to the
    partition: either a pure constant, or one whose only identifier leaves are a
    subset of the bare group key names."""
    if expr.node_type == NodeType.IDENTIFIER:
        return False
    if has_volatile_function(expr):
        return False
    identifiers = _collect_identifier_names(expr)
    if not identifiers:
        return True  # pure constant — partitions nothing, recompute in projection
    return identifiers.issubset(bare_key_names)


def _make_passthrough(original: Node) -> Node:
    """Create an IDENTIFIER node that passes through an already-computed column."""
    ref = Node(node_type=NodeType.IDENTIFIER)
    ref.schema_column = original.schema_column
    ref.value = original.schema_column.name if original.schema_column else original.value
    ref.qualified_name = original.qualified_name
    return ref


class GroupKeyReductionStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.AggregateAndGroup:
            return context

        groups = getattr(node, "groups", None)
        if not groups or len(groups) < 2:
            return context

        bare_keys = [g for g in groups if g.node_type == NodeType.IDENTIFIER]
        bare_key_names = {g.qualified_name for g in bare_keys if g.qualified_name is not None}
        if not bare_key_names:
            return context

        reducible = [g for g in groups if _is_reducible(g, bare_key_names)]
        if not reducible:
            return context

        # Validate schema_column is present on all nodes we'll reference; abort if not.
        for g in bare_keys + reducible:
            if g.schema_column is None:
                return context
        aggregates = getattr(node, "aggregates", None) or []
        for agg in aggregates:
            if agg.schema_column is None:
                return context

        node.groups = [g for g in groups if g not in reducible]
        context.optimized_plan[context.node_id] = node

        # Build the Project columns: pass-through surviving keys, recompute derived
        # expressions, pass-through aggregate outputs.
        project_columns = []
        for g in node.groups:
            project_columns.append(_make_passthrough(g))
        for g in reducible:
            project_columns.append(g)
        for agg in aggregates:
            project_columns.append(_make_passthrough(agg))

        project_node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        project_node.columns = project_columns
        project_node.passthrough_columns = []

        context.optimized_plan.insert_node_after(
            random_string(), project_node, context.node_id
        )
        self.telemetry.optimization_group_key_reduction = (
            getattr(self.telemetry, "optimization_group_key_reduction", 0) + len(reducible)
        )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
