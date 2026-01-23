# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Manifest-Based File Pruning

Type: Heuristic
Goal: Eliminate files from scan operations based on statistics

This strategy uses manifest data (file-level bounds, k-hashes, histograms) to
determine which data files can be safely skipped based on query predicates.
This happens at optimization time, making execution deterministic.

Unlike traditional predicate pushdown which happens per-file during execution,
this strategy makes pruning decisions once during optimization based on
file-level statistics from the catalog.

Architecture:
- Binder attaches Manifest to READ nodes (if connector supports it)
- This strategy calls manifest.prune_files(predicates) during optimization
- Pruned file list is stored in node properties
- Execution reads only the predetermined files

Key benefits:
- Earlier pruning (plan time vs execution time)
- More informed decisions (file-level stats vs table-level)
- Deterministic execution (same predicates = same files)
- Catalog-aware (leverages Iceberg/PyIceberg statistics)
"""

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class ManifestPruningStrategy(OptimizationStrategy):
    """
    Prunes files from SCAN operations using manifest statistics.

    This strategy:
    1. Finds SCAN nodes with manifests attached (by binder)
    2. Collects applicable predicates from parent FILTER nodes
    3. Calls manifest.prune_files(predicates) to get pruned file list
    4. Stores pruned files in node properties for execution
    """

    def __init__(self, telemetry):
        super().__init__(telemetry)
        self.collected_predicates = []

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Visit each node in the logical plan.

        - Collect predicates from FILTER nodes
        - Apply pruning when we reach SCAN nodes with manifests
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Scan:
            # Try to prune files using manifest
            if node.manifest is not None and node.predicates:
                # Apply manifest-based pruning
                original_count = node.manifest.get_file_count()

                node.manifest.prune_files(node.predicates)

                pruned_count = node.manifest.get_file_count()
                self.telemetry.files_pruned += original_count - pruned_count

            context.optimized_plan[context.node_id] = node

        return context

    def _is_prunable_predicate(self, condition: Node) -> bool:
        """
        Check if a predicate can be used for file pruning.

        Prunable predicates:
        - Simple comparisons on columns (=, <, >, <=, >=, !=)
        - AND/OR combinations of prunable predicates

        Not prunable:
        - Aggregations (MAX, MIN, etc.)
        - Subqueries
        - Complex expressions that can't be evaluated per-file
        """
        if condition.node_type == NodeType.COMPARISON_OPERATOR:
            # Simple comparison: column op literal
            has_column = (
                condition.left.node_type == NodeType.IDENTIFIER
                or condition.right.node_type == NodeType.IDENTIFIER
            )
            has_literal = (
                condition.left.node_type == NodeType.LITERAL
                or condition.right.node_type == NodeType.LITERAL
            )
            return has_column and has_literal

        elif condition.node_type in (NodeType.AND, NodeType.OR):
            # Logical combination: recurse on both sides
            left_ok = self._is_prunable_predicate(condition.left)
            right_ok = self._is_prunable_predicate(condition.right)
            return left_ok and right_ok

        return False

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        context.collected_limits.clear()
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """
        Determine if this strategy should run.

        Only run if there's at least one SCAN node with a manifest.
        """
        from opteryx import config

        if config.features.disable_manifest_pruning:
            return False

        for nid in plan.nodes():
            node = plan[nid]
            if node.node_type == LogicalPlanStepType.Scan and node.manifest is not None:
                return True
        return False
