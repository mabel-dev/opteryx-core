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

from opteryx.compiled.structures.plan_steps import PlanStep
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

    requires = ("limits-pushed",)

    def __init__(self, telemetry: QueryTelemetry):
        """Initialize the strategy with telemetry."""
        super().__init__(telemetry=telemetry)

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        """Visitor method - process each node."""
        if node.node_type == LogicalPlanStepType.Scan and node.limit is not None:
            if node.predicates:
                # We only optimize when there are no filters. This guard is what
                # protects the file drop below: a LIMIT CAN now sit on a scan that
                # absorbed a predicate (supports_filtered_limit_pushdown), and the
                # record_count accumulation is unsound the moment any row of a
                # file may be filtered out. A DECLINED predicate cannot reach here
                # either: it stays as a Filter node, which LimitPushdownStrategy
                # refuses to push a LIMIT past, so `node.limit` is None whenever
                # one survives. Same "do not widen this guard" note as
                # TopNManifestPruningStrategy.
                return context

            limit_value = node.limit
            if limit_value is None or limit_value <= 0:
                return context

            # A limit-pushable reader with no file manifest (the PostgreSQL
            # connector: the LIMIT went into the statement) has no files to
            # prune. Reading `node.manifest.files` here would be an
            # AttributeError, not a no-op.
            if node.manifest is None:
                return context

            # LAW: this DROPS files, trusting each one's record_count to supply
            # the limit. A stale count returns fewer rows than the query asked
            # for, which no caller can detect.
            if not node.manifest.stats_are_authoritative:
                return context

            # Sort file POSITIONS by row count descending — positions, not the
            # FileEntry objects, so the surviving set can be handed to
            # Manifest.subset, which keeps the sketch-vector row mapping
            # aligned with the reordered/truncated file list.
            sorted_positions = sorted(
                range(len(node.manifest.files)),
                key=lambda p: node.manifest.files[p].record_count,
                reverse=True,
            )

            selected_positions = []
            accumulated_rows = 0

            for position in sorted_positions:
                selected_positions.append(position)
                accumulated_rows += node.manifest.files[position].record_count
                if accumulated_rows >= limit_value:
                    break

            if len(selected_positions) == len(node.manifest.files):
                # Nothing dropped — a pure reorder changes no answer, and
                # writing the node back would force a redundant statistics
                # refresh for a plan that didn't change.
                return context

            # Copy-on-write: subset returns a NEW Manifest so the optimizer's
            # id()-keyed scan statistics cache misses and recomputes over the
            # selected file set.
            node.manifest = node.manifest.subset(selected_positions)
            self.telemetry.optimization_limit_file_pruning += 1
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
