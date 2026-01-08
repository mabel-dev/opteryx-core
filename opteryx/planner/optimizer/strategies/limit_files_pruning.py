# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - File Pruning for LIMIT Queries

Type: Heuristic
Goal: Reduce file I/O for SELECT queries with LIMIT and no filters

This strategy detects queries of the form:
    SELECT ... FROM table LIMIT n

Where there are no WHERE filters, and optimizes by:
1. Sorting files by size (largest first)
2. Selecting minimum files needed to satisfy LIMIT
3. Updating manifest to contain only selected files

This dramatically reduces I/O for LIMIT queries on partitioned data.

Example:
    SELECT * FROM events LIMIT 1000
    - If we have 10 files with 100K rows each
    - We only need to read 1 file instead of all 10

Expected Speedup: 2-10x (depending on data distribution)
"""

from opteryx.models import Node
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class LimitFilesPruningStrategy(OptimizationStrategy):
    """
    Prunes files for LIMIT queries when no filters are present.

    This strategy optimizes SELECT * FROM table LIMIT n by selecting
    only the largest files needed to satisfy the limit.
    """

    def __init__(self, telemetry: QueryTelemetry):
        """Initialize the strategy with telemetry."""
        super().__init__(telemetry=telemetry)

    def visit(self, node: Node, context: OptimizerContext) -> OptimizerContext:
        """Visitor method - process each node."""
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type == LogicalPlanStepType.Scan and node.limit is not None:
            if node.filters:
                # We only optimize when there are no filters
                return context

            limit_value = node.limit
            if limit_value is None or limit_value <= 0:
                return context

            # Sort files by size descending
            sorted_files = sorted(
                node.manifest.files,
                key=lambda f: f.record_count,
                reverse=True,
            )

            selected_files = []
            accumulated_rows = 0

            for file in sorted_files:
                selected_files.append(file)
                accumulated_rows += file.record_count
                if accumulated_rows >= limit_value:
                    break

            # Update manifest to only include selected files
            node.manifest.files = selected_files
            self.telemetry.optimization_limit_file_pruning += 1
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
