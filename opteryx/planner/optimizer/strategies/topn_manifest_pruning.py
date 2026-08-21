# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Manifest-Based File Pruning for Top-N

Type: Heuristic
Goal: Skip whole files that cannot contain any of the top-N rows of a
single-column `ORDER BY <col> [ASC|DESC] LIMIT n` query, using the per-file
min/max bounds and record_count already carried in the manifest.

Runs strictly after TopNScanPushdownStrategy (WP-2) has stamped
topn_sort_name/topn_descending/topn_limit onto the Scan, and after
WHERE-predicate manifest pruning (ManifestPruningStrategy) has already
narrowed the file list - this strategy narrows it further, using the sort
key instead of a WHERE predicate.

Algorithm: see Manifest.prune_files_for_topn's docstring for the
threshold-accumulation itself.

v1 scope, deliberately narrow:
- Only fires when the Scan carries NO residual predicate. `node.predicates` is
  populated only when the connector ACCEPTED a predicate, which looks like it
  misses a DECLINED one (left as a Filter NODE above the scan, cutting just as
  many rows). It does not, and the reason is upstream, not here:
  TopNScanPushdownStrategy stamps `topn_sort_name` only when the HeapSort reads
  DIRECTLY from the Scan, so a surviving Filter node between them means this
  strategy is never armed at all. Checked 2026-08-21 against a declined
  disjunction on both parquet and skene - the spec is not stamped. Do not
  "fix" this guard to walk parent Filters: that walk can only decline a case
  that cannot arise, and it would read as though the adjacency requirement were
  not load-bearing. See test_declined_predicate_disables_the_optimization.
  The accumulation
  counts each file's `record_count`, which is its TOTAL row count; a filter
  applied at scan time means an unknown number of those rows never reach the
  sort, so the threshold is computed from rows that do not exist and files
  holding the only surviving rows get dropped. Measured, on a 3-file, 9-row
  dataset (tests/storage/test_temporal_domain_manifest_pruning.py):

      WHERE n IN (4,5,6) ORDER BY date_added ASC LIMIT 3  -> <empty>  want 4,5,6
      WHERE n <> 1       ORDER BY date_added ASC LIMIT 3  -> 2,3      want 2,3,4

  A predicate that manifest pruning DID use is no safer: bounds are ranges, so
  a surviving file's record_count still overstates how many of its rows pass.
  Making this filter-aware needs a per-file LOWER bound on surviving rows,
  which the manifest does not carry - a bound-derived estimate is an estimate,
  and an estimate here is a wrong answer, not a slower one. Same gate, same
  reason, as LimitFilesPruningStrategy.
- Only fires when the sort column has ZERO NULLs anywhere in the manifest
  (Manifest.get_total_null_count == 0). NULL/ASC ordering interacts with
  this kind of pruning in ways this codebase has already shipped one bug
  over (apply_topn_null_asc_bug fixed a NULLS-FIRST-under-ASC top-n cut);
  the honest v1 scope is "don't touch a column that has NULLs" rather than
  re-deriving that reasoning here under a new mechanism.
- Only fires for non-FLOAT sort columns. NaN ordering is separately tracked
  as divergent from the rest of the type system (draken_float_nan_semantics)
  and this rule must not paper over that with an unrelated pruning decision.
- This is file-level only. It does not feed the threshold into row-group
  (footer-stats) pruning - that is a natural follow-up, not bundled in here.

This pruning is purely a plan-time file-list decision (identical in spirit to
ManifestPruningStrategy): it never becomes a runtime row filter, so it cannot
interact with the native two-pass scan's own intra-file top-N row cut
(reduce_to_topn in native_latmat_scan_source.hpp) - the two mechanisms answer
different questions (which files to open vs which decoded rows survive) and
stay independent by construction.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class TopNManifestPruningStrategy(OptimizationStrategy):
    """Prune whole files for `ORDER BY <col> LIMIT n` using manifest min/max."""

    requires = ("predicates-pushed", "topn-scan-pushdown")

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.Scan:
            return context

        sort_name = getattr(node, "topn_sort_name", None)
        limit = getattr(node, "topn_limit", None)
        if node.manifest is None or not sort_name or not limit:
            return context

        if node.predicates:
            # A residual filter at the Scan makes record_count-based
            # accumulation unsound - see module docstring.
            return context

        column = next(
            (c for c in node.manifest.schema.columns if c.name == sort_name), None
        )
        if column is None or column.column_type.category == LogicalCategory.FLOAT:
            return context

        if node.manifest.get_total_null_count(sort_name) != 0:
            # Non-zero or unknown NULL count - out of v1 scope, see module docstring.
            return context

        descending = bool(getattr(node, "topn_descending", False))

        original_count = node.manifest.get_file_count()
        # Copy-on-write: a real prune hands back a NEW Manifest (same object
        # when nothing was pruned), so the id()-keyed scan statistics cache
        # misses and recomputes over the pruned file set.
        pruned_manifest = node.manifest.prune_files_for_topn(sort_name, descending, limit)
        if pruned_manifest is not node.manifest:
            node.manifest = pruned_manifest
            self.telemetry.files_pruned += original_count - pruned_manifest.get_file_count()
            self.telemetry.optimization_topn_manifest_pruning += 1
            # Only a real prune marks the plan mutated; an unconditional write
            # forces a redundant full statistics refresh after this strategy.
            context.optimized_plan[context.node_id] = node
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Scan,)):
            if getattr(node, "topn_limit", None) and node.manifest is not None:
                return True
        return False
