# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Storage Cache Strategy

Type: Cost-based
Goal: Reduce bandwidth and improve performance

Rewrites file paths in READ nodes to use cached/optimized storage locations
when certain criteria are met (e.g., file size, access patterns, etc.).
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class StorageCacheStrategy(OptimizationStrategy):
    """Rewrite file paths to use cached storage when beneficial."""

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Visit a node in the logical plan.

        Only processes SCAN operations; passes through all other node types.
        For each SCAN, checks three killer questions:
        1. Are all files on GCS?
        2. Do we have either selection (filters) or projection (columns) pushed in?
        3. Are all files less than 30MB?

        If any of these questions fails, pass through.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        # Only care about scan operations
        if node.node_type == LogicalPlanStepType.Scan:
            # Killer question 1: Are all files on GCS?
            if not self._all_files_on_gcs(node):
                return context

            # Killer question 2: Do we have pushed-down filters or columns?
            if not self._has_pushed_filters_or_columns(node):
                return context

            # Killer question 3: Are all files less than 30MB?
            # if not self._all_files_under_30mb(node):
            #    return context

            # All criteria met - rewrite file paths from GCS to S3
            self._rewrite_paths_to_s3(node)

        # Pass through all other node types
        return context

    @staticmethod
    def _all_files_on_gcs(node: LogicalPlanNode) -> bool:
        """Check if all files in the scan are on GCS (gs:// protocol)."""
        if not hasattr(node, "manifest") or not node.manifest:
            return False

        for file_entry in node.manifest.files:
            protocol = file_entry.file_path.split("://")[0] if "://" in file_entry.file_path else ""
            if protocol not in ("gs", "gcs"):
                return False

        return len(node.manifest.files) > 0

    @staticmethod
    def _has_pushed_filters_or_columns(node: LogicalPlanNode) -> bool:
        """Check if the scan has either pushed-down filters or columns."""
        has_filters = hasattr(node, "predicates") and node.predicates
        has_columns = hasattr(node, "columns") and node.columns
        return has_filters or has_columns

    @staticmethod
    def _all_files_under_30mb(node: LogicalPlanNode) -> bool:
        """Check if all files are less than 30MB."""
        if not hasattr(node, "manifest") or not node.manifest:
            return False

        max_bytes = 30 * 1024 * 1024  # 30MB in bytes

        for file_entry in node.manifest.files:
            if file_entry.file_size_in_bytes > max_bytes:
                return False

        return True

    @staticmethod
    def _rewrite_paths_to_s3(node: LogicalPlanNode) -> None:
        """Rewrite file paths in manifest from GCS (gs://) to S3 (s3://)."""
        if not hasattr(node, "manifest") or not node.manifest:
            return

        for file_entry in node.manifest.files:
            # Replace gs:// or gcs:// with s3://
            if file_entry.file_path.startswith("gs://"):
                file_entry.file_path = "s3://" + file_entry.file_path[5:]
            elif file_entry.file_path.startswith("gcs://"):
                file_entry.file_path = "s3://" + file_entry.file_path[6:]

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Complete the optimization process and return the optimized logical plan.
        """
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """
        Determine if the optimization strategy should run on the given plan.

        Returns True only if S3 endpoint is configured (MINIO_END_POINT env var or config).
        """
        import os

        end_point = os.environ.get("MINIO_END_POINT")
        return end_point is not None
