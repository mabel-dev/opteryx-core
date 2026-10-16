# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - File Pruning for LIMIT Queries

Type: Heuristic (from manifest record counts)
Goal: Reduce file I/O for `SELECT ... FROM table LIMIT n` with no filters

For a Scan carrying a pushed LIMIT and no predicate, sort its files by size (largest
first), keep the fewest whose record counts cover the LIMIT, and narrow the scan's
manifest to them. A scan with ANY predicate is left alone: the record-count
accumulation is unsound the moment any row of a file may be filtered out (same
guard, same reason, as TopNManifestPruning).

Order:
    After:  LimitPushdown (requires limits-pushed: it reads the LIMIT the pushdown
            stamped on the Scan; a declined predicate stays a Filter node that
            LimitPushdown refuses to push past, so `scan.limit` is None whenever one
            survives), ManifestPruning (pipeline position).
    Before: nothing depends on it.
    Declares: requires limits-pushed.

Rewrite:
    none in SQL. `SELECT * FROM events LIMIT 1000` is unchanged; with ten files of
    100K rows each, the manifest is narrowed to one file instead of ten.

Expected speedup: 2-10x, depending on data distribution.
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

    def should_i_run(self, plan: LogicalPlan) -> bool:
        # visit() acts only on a Scan carrying a pushed limit.
        return any(
            node.limit is not None
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Scan,))
        )

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
            # prune.
            if node.manifest is None:
                return context

            # LAW: this DROPS files, trusting each one's record_count to supply
            # the limit. A stale count returns fewer rows than the query asked
            # for, which no caller can detect.
            if not node.manifest.stats_are_authoritative:
                return context

            # A file whose row count is UNKNOWN (mabel) supplies no rows to
            # count on: nothing can be dropped against it.
            record_counts = node.manifest.record_counts()
            if any(count is None for count in record_counts):
                return context
            # record_count is PHYSICAL rows; a file's merge-on-read deleted rows
            # supply nothing to the limit, so count only its live rows.
            if node.manifest.has_deletes():
                deletes = node.manifest.delete_positions()
                record_counts = [
                    count - len(deletes.get(path, ()))
                    for count, path in zip(record_counts, node.manifest.get_file_paths())
                ]

            # Sort file POSITIONS by row count descending, so the surviving set
            # can be handed to Manifest.subset.
            sorted_positions = sorted(
                range(len(record_counts)),
                key=lambda p: record_counts[p],
                reverse=True,
            )

            selected_positions = []
            accumulated_rows = 0

            for position in sorted_positions:
                selected_positions.append(position)
                accumulated_rows += record_counts[position]
                if accumulated_rows >= limit_value:
                    break

            if len(selected_positions) == len(record_counts):
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
