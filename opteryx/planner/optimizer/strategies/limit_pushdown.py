# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Limit Pushdown

Type: Heuristic
Goal: Reduce Rows

We try to push the limit to the other side of PROJECTS
"""

from typing import Optional
from typing import Set

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class LimitPushdownStrategy(OptimizationStrategy):
    """Push LIMIT operators towards scans when it is safe to do so."""

    provides = ("limits-pushed",)

    _BARRIER_TYPES = {
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
        LogicalPlanStepType.Distinct,
        LogicalPlanStepType.Filter,
        LogicalPlanStepType.FunctionDataset,
        LogicalPlanStepType.HeapSort,
        LogicalPlanStepType.Limit,
        LogicalPlanStepType.Order,
        LogicalPlanStepType.Set,
        LogicalPlanStepType.Union,
        # CROSS JOIN UNNEST compiles to an Unnest node, not a Join - it still
        # multiplies rows (one row in can become N rows out), so pushing a
        # LIMIT below it onto the scan caps the wrong side of the expansion.
        # Same reasoning as the Join case below, just a different node type.
        LogicalPlanStepType.Unnest,
        # A window function is evaluated over the whole (partitioned) input, and SQL
        # evaluates it BEFORE the LIMIT. Pushing a LIMIT below it changes what the
        # window sees: a ranking window then numbers an arbitrary N-row subset
        # (`ROW_NUMBER() OVER (ORDER BY x DESC) ... LIMIT 3` returned the numbering of
        # the first three rows read, not the top three), and an aggregate window
        # computes its per-partition value over a truncated partition. Both are wrong
        # answers, silently, so the LIMIT stays above the Window.
        LogicalPlanStepType.Window,
        # Same reasoning, framed aggregate windows (SUM/COUNT/AVG/MIN/MAX OVER
        # (... ROWS/RANGE BETWEEN ...)) — a running total over a truncated
        # partition is a different (wrong) running total.
        LogicalPlanStepType.FramedWindow,
    }

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Limit:
            if node.offset is not None or node.limit in (None, 0):
                return context
            node.nid = context.node_id
            if getattr(node, "pushdown_targets", None) is None:
                node.pushdown_targets = set(node.all_relations or [])
            context.collected_limits.append(node)
            return context

        remaining_limits = []
        for limit_node in context.collected_limits:
            if self._should_skip_branch(limit_node, node):
                remaining_limits.append(limit_node)
                continue

            if node.node_type == LogicalPlanStepType.Scan:
                outcome = self._apply_to_scan(limit_node, node, context)
                if outcome is True:
                    continue
                if outcome is None:
                    remaining_limits.append(limit_node)
                    continue
                self._place_before_node(limit_node, node, context)
                continue

            if node.node_type == LogicalPlanStepType.Join:
                # A LIMIT must never be pushed below a join. Every join type can
                # multiply rows relative to a single input: cross joins produce
                # |left| * |right|, and outer/inner equi-joins match a preserved
                # row against N rows on the other side (1:N). Limiting (or
                # relocating the LIMIT onto) one input therefore does not cap the
                # join's output, so the LIMIT stays directly above the join.
                self._place_before_node(limit_node, node, context)
                continue

            if node.node_type in self._BARRIER_TYPES:
                self._place_before_node(limit_node, node, context)
                continue

            remaining_limits.append(limit_node)

        context.collected_limits = remaining_limits
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        context.collected_limits.clear()
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))
        return len(candidates) > 0

    @staticmethod
    def _collect_relations(node: LogicalPlanNode) -> Set[str]:
        relations = getattr(node, "all_relations", None)
        if relations:
            return set(relations)
        return set()

    def _should_skip_branch(self, limit_node: LogicalPlanNode, node: LogicalPlanNode) -> bool:
        targets: Set[str] = getattr(limit_node, "pushdown_targets", set())
        if not targets:
            return False
        node_relations = self._collect_relations(node)
        return bool(node_relations) and targets.isdisjoint(node_relations)

    def _apply_to_scan(
        self,
        limit_node: LogicalPlanNode,
        scan_node: LogicalPlanNode,
        context: OptimizerContext,
    ) -> Optional[bool]:
        targets: Set[str] = getattr(
            limit_node, "pushdown_targets", set(limit_node.all_relations or [])
        )
        relation_names = {scan_node.relation, getattr(scan_node, "alias", None)}
        if targets and targets.isdisjoint({name for name in relation_names if name}):
            return None

        if getattr(scan_node, "predicates", None):
            # A predicate has been pushed into this scan (predicate pushdown removes the
            # Filter node from the plan, so it no longer acts as a barrier here). Limit
            # pushdown must not apply on top of a predicate: the scan would cap rows read
            # from source before filtering, changing which rows survive the LIMIT.
            return False

        connector = getattr(scan_node, "connector", None)
        if connector and connector.supports_limit_pushdown:
            current_limit = getattr(scan_node, "limit", None)
            scan_node.limit = (
                limit_node.limit if current_limit is None else min(current_limit, limit_node.limit)
            )
            if limit_node.nid in context.optimized_plan:
                context.optimized_plan.remove_node(limit_node.nid, heal=True)
            context.optimized_plan[context.node_id] = scan_node
            self.telemetry.optimization_limit_pushdown += 1
            return True

        return False

    def _place_before_node(
        self, limit_node: LogicalPlanNode, _: LogicalPlanNode, context: OptimizerContext
    ) -> None:
        if limit_node.nid in context.optimized_plan:
            context.optimized_plan.remove_node(limit_node.nid, heal=True)
        context.optimized_plan.insert_node_after(limit_node.nid, limit_node, context.node_id)
        limit_node.columns = []
        limit_node.pushdown_targets = set(limit_node.all_relations or [])
        self.telemetry.optimization_limit_pushdown += 1
