# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Remove Redundant Operators

Type: Heuristic
Goal: Remove steps which don't affect the result

This optimization runs toward the end of the set, it removes operators which
were useful during planning and optimization.

- Some projections are redundant (reselecting down to the columns which the
  providing operation has already limited down to).
- SubQuery nodes are useful for planning and optimization, but don't do
  anything during execution, we can remove them here.

Both of these operations are cheap to execute, the benefit for this
optimization isn't expected to be realized until we implement multiprocessing
and there is work associated with IPC which we are avoiding by removing
impotent steps.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import PlanStep
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# Steps that emit exactly their input's columns: a Project over one of these is
# judged against whatever fixes the width beneath it.
_PASS_THROUGH = (
    LogicalPlanStepType.Filter,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.Limit,
)


def _output_identities(plan: LogicalPlan, nid) -> set | None:
    """The identities the step at `nid` emits, or None when that is not known.

    Deny by default. A Join's `.columns` is what is needed ABOVE it
    (projection_pushdown), not what it emits: the compiler narrows some join emits
    to that set but not all (a swapped semi/anti emits its full build leg, a
    residual keeps the payload full width). A Filter's or Order's `.columns` is what
    it READS; they emit their whole input, so the answer is their input's.
    """
    node = plan[nid]
    while node.node_type in _PASS_THROUGH:
        nid = plan.ingoing_edges(nid)[0][0]
        node = plan[nid]
    if node.node_type == LogicalPlanStepType.Scan:
        return {c.schema_column.identity for c in node.columns}
    if node.node_type == LogicalPlanStepType.Project:
        # A Project at runtime emits `columns ∪ passthrough_columns` (see
        # projection.pyx).
        return {
            c.schema_column.identity
            for c in list(node.columns) + list(node.passthrough_columns or [])
        }
    if node.node_type in (
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
    ):
        # Aggregate logical nodes' `.columns` conflates outputs with input
        # identifiers referenced by the aggregates (binder uses it for upstream
        # schema pruning — see binder/aggregate.py). The outputs are the
        # aggregates and groups.
        return {
            c.schema_column.identity for c in (node.aggregates or []) + (node.groups or [])
        }
    return None


class RedundantOperationsStrategy(OptimizationStrategy):
    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        # If we're a project and the providing step has the same columns, we're
        # not doing anything so can be removed.
        if node.node_type == LogicalPlanStepType.Project:
            providers = context.pre_optimized_tree.ingoing_edges(context.node_id)
            consumer = context.pre_optimized_tree.outgoing_edges(context.node_id)
            if consumer:
                consumer_node = context.pre_optimized_tree[consumer[0][1]]
                if consumer_node.node_type == LogicalPlanStepType.Union:
                    # if the consumer is a union, we can't remove the project
                    return context

            if len(providers) == 1:
                provider_nid = providers[0][0]
                provider_node = context.pre_optimized_tree[provider_nid]
                # Only a provider whose output width is KNOWN can make this Project
                # a no-op. Trusting a Join's `.columns` deleted the Project over a
                # swapped anti join, leaving its key in the stream: a `SELECT
                # DISTINCT` above deduplicated (i_group, s_low) and returned 80 rows
                # where 16 was right.
                provider_columns = _output_identities(
                    context.pre_optimized_tree, provider_nid
                )
                if provider_columns is not None:
                    # A Project at runtime emits `columns ∪ passthrough_columns` (see
                    # projection.pyx). Both must be considered when deciding whether the
                    # upstream operator already produces the same set of columns.
                    my_columns = {
                        c.schema_column.identity
                        for c in list(node.columns)
                        + list(getattr(node, "passthrough_columns", None) or [])
                    }
                    if provider_columns == my_columns:
                        # we need to ensure we keep some of the context if not the step
                        source_node_alias = context.optimized_plan[context.node_id].alias
                        if provider_node.all_relations:
                            provider_node.all_relations.add(source_node_alias)
                        else:
                            provider_node.all_relations = {source_node_alias}
                        context.optimized_plan.add_node(provider_nid, provider_node)
                        # remove the node
                        context.optimized_plan.remove_node(context.node_id, heal=True)
                        self.telemetry.optimization_remove_redundant_operators_project += 1

        # Subqueries are useful for planning but not needed for execution
        # We need to ensure the alias of the subquery is pushed
        if node.node_type == LogicalPlanStepType.Subquery:
            alias = node.alias
            nid = context.optimized_plan.ingoing_edges(context.node_id)[0][0]
            updated_node = context.optimized_plan[nid]
            # if we have multiple layers of subqueries, ignore everything other than the outermost
            while updated_node.node_type == LogicalPlanStepType.Subquery:
                nid = context.optimized_plan.ingoing_edges(nid)[0][0]
                updated_node = context.optimized_plan[nid]
            updated_node.alias = alias
            if updated_node.all_relations:
                updated_node.all_relations.add(alias)
            else:
                updated_node.all_relations = {alias}
            context.optimized_plan.add_node(nid, updated_node)
            context.optimized_plan.remove_node(context.node_id, heal=True)
            self.telemetry.optimization_remove_redundant_operators_subquery += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
