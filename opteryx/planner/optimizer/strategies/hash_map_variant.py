# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule — Hash Map Variant Selection for GROUP BY and DISTINCT

Type: Cost-based
Goal: Pick the smallest viable hash map for AggregateAndGroup and Distinct nodes.

GROUP BY (AggregateAndGroup):
  When the estimated distinct group count is small enough
  (<= PARVI_ELIGIBILITY_GATE), we tag the node with
  `group_map_variant = "parvi"` so the operator uses the fixed-capacity
  64-slot inline map instead of heap-allocating a CarcharIndex. The operator
  migrates back to carchar automatically if the estimate turns out wrong
  (see GroupHashEngine._promote_parvi_to_carchar).

DISTINCT:
  When the estimated distinct-on cardinality is small enough
  (<= PARVI_ELIGIBILITY_GATE), we tag the node with `set_variant = "parvi"`
  so the operator uses the fixed-capacity 64-slot inline set instead of
  CarcharSetWrapper. The operator migrates to carchar on overflow.

Fail-safe default: carchar. Parvi is only chosen with positive evidence.
Missing or stale stats → carchar.

Two signals are used, in priority order:

1. **NDV-product bound** — for each GROUP BY/DISTINCT column, resolve to a source
   column in the upstream Scan manifest and read the KMV-based distinct-count
   estimate. If every column resolves and the product is <= the gate, pick parvi.

2. **Input-rows bound** — if the total record count across all upstream
   Scan manifests is <= the gate, the output cardinality cannot exceed that
   regardless of NDV, so pick parvi.

If neither signal fires, carchar.
"""

from typing import Optional

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Parvi eligibility gate for the Cython engine's single front map.
#
# Parvi is 64 slots (opteryx::parvi::kCapacity) arranged as 4 group-selected
# groups of 16; a key can only occupy its own group, so overflow fires on the
# first FULL GROUP, not at 64 keys. Measured effective capacity before first
# overflow is p5 = 40 (size-curve experiment, 2026-08-06; re-confirmed at
# min=33 / avg=51 over 1000 seeds). Gating eligibility at the raw 64 would
# put estimates in the 41–64 band on the promote-on-every-seed path, which
# measures 9–11% SLOWER than going straight to Carchar. Gate at the p5 so the
# promote rate stays near zero — this still ~2.5×es the eligible band over
# the old 16-slot map.
PARVI_ELIGIBILITY_GATE = 40

# The native GroupBySink gates its per-partition parvi front maps on the raw
# NDV estimate (kGBParviGateNDV in src/cpp/engine/native_group_sinks.hpp = 64).
# The NDV-product early-exit below must not truncate estimates the sink still
# wants to see, so it cuts off above the native gate, not at the (lower)
# PARVI_ELIGIBILITY_GATE.
NATIVE_GB_GATE = 64


class HashMapVariantStrategy(OptimizationStrategy):
    """Tag AggregateAndGroup nodes with their preferred hash-map variant."""

    # reads FileEntry.stats_by_name, which is stable only after projection pushdown
    requires = ("projection-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        # Handle both GROUP BY and DISTINCT nodes.
        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            # GROUP BY — use group_map_variant hint
            if getattr(node, "group_map_variant", None) is not None:
                return context
            variant, estimate = self._variant_and_estimate(node, context)
            node.group_map_variant = variant
            # Raw distinct-group-count estimate for the native sink's own gate
            # (int or None). Kept separate from the variant tag: the Cython
            # engine's single 16-slot map and the native sink's 64 partitioned
            # maps have different capacity envelopes.
            node.groupby_ndv_estimate = estimate
            context.optimized_plan[context.node_id] = node
            return context

        elif node.node_type == LogicalPlanStepType.Distinct:
            # DISTINCT — use set_variant hint
            if getattr(node, "set_variant", None) is not None:
                return context
            variant, estimate = self._variant_and_estimate(node, context)
            node.set_variant = variant
            # Raw distinct-count estimate for the native DistinctSink's parvi
            # gate (kDistinctParviGateNDV) — int or None.
            node.distinct_ndv_estimate = estimate
            context.optimized_plan[context.node_id] = node
            return context

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return bool(
            get_nodes_of_type_from_logical_plan(
                plan, (LogicalPlanStepType.AggregateAndGroup, LogicalPlanStepType.Distinct)
            )
        )

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------

    def _pick_variant(
        self, agg_node: LogicalPlanNode, context: OptimizerContext
    ) -> str:
        variant, _ = self._variant_and_estimate(agg_node, context)
        return variant

    def _variant_and_estimate(
        self, agg_node: LogicalPlanNode, context: OptimizerContext
    ) -> tuple:
        """(variant, group-count estimate). The estimate is the best available
        distinct-group-count bound (int) or None when neither signal resolves.
        Values above NATIVE_GB_GATE may be truncated lower bounds (the NDV
        product early-exits) — only <= NATIVE_GB_GATE comparisons are valid."""
        plan = context.pre_optimized_tree
        scan_nodes = self._upstream_scans(plan, agg_node)
        if not scan_nodes:
            return "carchar", None

        estimate = None
        total_rows = self._total_record_count(scan_nodes)
        if total_rows is not None and total_rows <= NATIVE_GB_GATE:
            # Output groups cannot exceed input rows — a provable bound.
            estimate = total_rows
        ndv_product = self._ndv_product(agg_node, scan_nodes)
        if ndv_product is not None:
            estimate = ndv_product if estimate is None else min(estimate, ndv_product)

        # Signal 2 first — cheapest and provably correct.
        if total_rows is not None and total_rows <= PARVI_ELIGIBILITY_GATE:
            return "parvi", estimate
        # Signal 1 — NDV product across group columns.
        if ndv_product is not None and ndv_product <= PARVI_ELIGIBILITY_GATE:
            return "parvi", estimate
        return "carchar", estimate

    def _upstream_scans(self, plan: LogicalPlan, agg_node: LogicalPlanNode) -> list:
        """All Scan nodes that feed into `agg_node`."""
        agg_nid = getattr(agg_node, "nid", None)
        if agg_nid is None:
            # Fall back to locating the node by identity.
            for nid, candidate in plan.nodes(True):
                if candidate is agg_node:
                    agg_nid = nid
                    break
        if agg_nid is None:
            return []

        scans = []
        # reverse=True walks from agg_node back toward the scans (ingoing edges).
        for source, _target, _rel in plan.breadth_first_search(agg_nid, reverse=True):
            candidate = plan[source]
            if candidate.node_type == LogicalPlanStepType.Scan:
                scans.append(candidate)
        return scans

    @staticmethod
    def _total_record_count(scan_nodes: list) -> Optional[int]:
        total = 0
        for scan in scan_nodes:
            manifest = getattr(scan, "manifest", None)
            if manifest is None:
                return None
            try:
                count = manifest.get_record_count()
            except (AttributeError, TypeError):
                # Malformed manifest file stats — fall back to the default join
                # variant rather than failing the query (perf signal only).
                return None
            if count is None:
                return None
            total += int(count)
        return total

    def _ndv_product(self, node: LogicalPlanNode, scan_nodes: list) -> Optional[int]:
        # Handle both GROUP BY (groups) and DISTINCT (on) columns.
        columns = getattr(node, "groups", None) or getattr(node, "on", None)
        if not columns:
            # GROUP BY () or DISTINCT with no specific columns — trivially parvi-eligible.
            return 1

        # Try to resolve each column to a manifest cardinality estimate.
        # Any expression column (e.g. GROUP BY UPPER(x)) defeats this signal and
        # we bail out to carchar — we'd need per-expression NDV propagation.
        product = 1
        for col_expr in columns:
            col_name = self._source_column_name(col_expr)
            if col_name is None:
                return None
            ndv = self._column_ndv(col_name, scan_nodes)
            if ndv is None:
                return None
            product *= max(1, ndv)
            if product > NATIVE_GB_GATE:
                # Early exit — already above every gate that reads this estimate
                # (Cython parvi at PARVI_ELIGIBILITY_GATE, native sink at
                # NATIVE_GB_GATE).
                return product
        return product

    @staticmethod
    def _source_column_name(group_expr) -> Optional[str]:
        """Return the scan-level column name for a simple IDENTIFIER group, else None."""
        from opteryx.expression import NodeType

        if getattr(group_expr, "node_type", None) != NodeType.IDENTIFIER:
            return None
        schema_column = getattr(group_expr, "schema_column", None)
        # schema_column.name is the user-facing column; source_column / identity
        # can vary by binder stage — try `name` first, fall back to `source_column`.
        if schema_column is None:
            return None
        for attr in ("name", "source_column"):
            value = getattr(schema_column, attr, None)
            if value:
                return str(value)
        return None

    @staticmethod
    def _column_ndv(col_name: str, scan_nodes: list) -> Optional[int]:
        """Sum of per-scan cardinality estimates. Any unknown → give up."""
        total = 0
        for scan in scan_nodes:
            manifest = getattr(scan, "manifest", None)
            if manifest is None:
                return None
            try:
                estimate = manifest.estimate_cardinality(col_name)
            except (AttributeError, TypeError, ZeroDivisionError):
                # Malformed/degenerate KMV stats — fall back to the default join
                # variant rather than failing the query (perf signal only).
                return None
            if estimate is None:
                return None
            total += int(estimate)
        return total if total > 0 else None
