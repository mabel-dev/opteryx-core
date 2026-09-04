# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Compaction Planning

Type: Physical planning
Goal: Decide which data files an OPTIMIZE rewrites

`OPTIMIZE TABLE x` arrives here already desugared into an ordinary plan —
`SELECT * FROM x [ORDER BY <cluster cols>]` under a CompactionCommit sink. This
strategy makes the two decisions the SQL could not:

  1. WHICH FILES the pass rewrites. Selection reads manifest statistics and
     narrows the scan's manifest to what it chose. That narrowing is the whole
     mechanism — there is no separate "pinned scan", just a manifest with fewer
     entries in it.
  2. WHETHER THE SORT IS NEEDED. The ORDER BY is emitted unconditionally by the
     desugar because the rule is not known until the manifest has been read.
     A brute plan never sorts, so its Order node is removed here.

WHY THIS IS AN OPTIMIZER STRATEGY. Choosing which files to merge is reasoning
over statistics — sizes, record counts, key ranges, delete debt — which is
planning, and contract §1 puts planning with the planner. It used to live in
opteryx-catalog's `DatasetCompactor`, where it could see none of the statistics
machinery the planner has. See `docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md`.

⛔ THE SCAN MUST NOT BE NARROWED BY COLUMN. Compaction rewrites whole rows, so a
scan narrowed to the sort column silently drops every other column from the
rewritten files — and the row-count invariant does not catch it, because the
counts still match. Pushdown strategies decline a plan containing this sink
through their `should_i_run` gate (D-10).
"""

from typing import Optional

from opteryx.planner.compaction import CompactionPlan
from opteryx.planner.compaction import SelectionOutcome
from opteryx.planner.compaction import select_compaction_plan
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


def plan_has_compaction(plan) -> bool:
    """Whether this plan is an OPTIMIZE.

    The gate every pushdown strategy asks before running — see the module
    docstring for why narrowing a compaction scan is a data-loss bug rather
    than an optimization.
    """
    return (
        len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.CompactionCommit,)))
        > 0
    )


def _source_below(plan, nid, wanted):
    """The first node of type `wanted` reachable downward from `nid`.

    Edges run child -> parent, so `ingoing_edges` walks toward the sources.
    """
    stack = [source for source, _, _ in plan.ingoing_edges(nid)]
    seen = set()
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        if plan[current].node_type == wanted:
            return current, plan[current]
        stack.extend(source for source, _, _ in plan.ingoing_edges(current))
    return None, None


class CompactionPlanningStrategy(OptimizationStrategy):
    """Select the files an OPTIMIZE pass rewrites."""

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        # Decided globally in `complete`; nothing to accumulate on the way down.
        return context

    def should_i_run(self, plan) -> bool:
        return plan_has_compaction(plan)

    def complete(self, plan, context: OptimizerContext) -> object:
        for nid, sink in get_nodes_of_type_from_logical_plan(
            plan, (LogicalPlanStepType.CompactionCommit,)
        ):
            scan_id, scan = _source_below(plan, nid, LogicalPlanStepType.Scan)
            if scan is None or scan.manifest is None:
                # No manifest means nothing committed. Not an error — an empty
                # relation has nothing to compact — but it must be said, not
                # inferred from an empty file list later.
                self.record_decision(
                    "compaction", f"declined: {sink.relation_name} has no committed snapshot"
                )
                continue

            manifest = scan.manifest
            sort_column = self._sort_column(sink, scan)
            key_ranges = manifest.file_key_ranges(sort_column) if sort_column else None

            # ⛔ KNOWN GAP: the delete-debt threshold is per-dataset overridable
            # in the catalog (`maintenance_policy["delete-debt-threshold"]`) and
            # the engine has no reader for maintenance policy at all, so every
            # dataset gets the default. A dataset that set its own threshold is
            # planned against the wrong one.
            result = select_compaction_plan(
                manifest.files, sort_column=sort_column, key_ranges=key_ranges
            )

            if result.outcome is not SelectionOutcome.PLANNED:
                # Nothing to do. The sink retires no files and commits nothing,
                # which is a successful no-op rather than a failure.
                #
                # ⛔ THE SCAN IS NARROWED TO NOTHING, not left alone. A scan
                # still pointing at the full manifest would read the ENTIRE
                # relation to produce rows the sink then discards — so the
                # cheapest possible statement, "this dataset needs no work",
                # would be the most expensive one. This is what makes an
                # unnecessary OPTIMIZE free, and therefore what lets the
                # scheduled sweep submit without pre-checking.
                scan.manifest = manifest.subset([])
                plan[scan_id] = scan
                self._drop_order(plan, nid)
                self.record_decision(
                    "compaction",
                    f"no plan for {sink.relation_name}: {result.outcome.value} "
                    f"({result.detail})",
                )
                continue

            selected: CompactionPlan = result.plan
            chosen = {entry.file_path for entry in selected.files}
            positions = [
                index for index, entry in enumerate(manifest.files) if entry.file_path in chosen
            ]

            # THE PIN. `subset` keeps the sketch vectors positionally aligned
            # with the surviving file list, which is why the file set is narrowed
            # through it rather than by rebuilding a Manifest.
            scan.manifest = manifest.subset(positions)
            plan[scan_id] = scan

            sink.retired_files = sorted(chosen)
            sink.baseline_snapshot_id = getattr(scan.connector, "snapshot_id", None)
            plan[nid] = sink

            if selected.mode == "brute":
                # A brute plan concatenates and never sorts; sorting it would be
                # wasted work on files chosen precisely because sorting them is
                # not worth it yet.
                self._drop_order(plan, nid)

            self.record_decision(
                "compaction",
                f"{selected.strategy}/{selected.mode} on {sink.relation_name}: "
                f"{len(selected)} of {len(manifest.files)} files, "
                f"{selected.input_bytes >> 20} MB, -> {selected.expected_outputs} output(s), "
                f"reason {selected.reason}",
            )

        return plan

    def _sort_column(self, sink, scan) -> Optional[str]:
        """The relation's primary sort key, or None when it has none.

        Asked of the connector, which is the same call `plan_optimize_table`
        made to emit the ORDER BY, so the column the plan sorts by and the
        column selection reasons about cannot diverge.
        """
        columns = sink.connector.cluster_by_columns(sink.relation_name)
        return columns[0] if columns else None

    def _drop_order(self, plan, sink_nid) -> None:
        """Remove the Order node feeding this sink, if there is one."""
        order_id, _ = _source_below(plan, sink_nid, LogicalPlanStepType.Order)
        if order_id is not None:
            plan.remove_node(order_id, heal=True)
