# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule — Hash Map Variant Selection for GROUP BY

Type: Cost-based
Goal: Pick the smallest viable hash map for each AggregateAndGroup node.

When the estimated distinct group count is small enough (<= kParviCapacity),
we tag the node with `group_map_variant = "parvi"` so the operator uses the
fixed-capacity 16-slot inline map instead of heap-allocating a 256-slot
CarcharIndex. The operator migrates back to carchar automatically if the
estimate turns out wrong (see GroupHashEngine._promote_parvi_to_carchar).

Fail-safe default: carchar. Parvi is only chosen with positive evidence.
Missing or stale stats → carchar.

Two signals are used, in priority order:

1. **NDV-product bound** — for each GROUP BY column, resolve to a source
   column in the upstream Scan manifest and read the KMV-based distinct-count
   estimate. If every column resolves and the product is <= 16, pick parvi.

2. **Input-rows bound** — if the total record count across all upstream
   Scan manifests is <= 16, the group-by output cannot exceed that regardless
   of NDV, so pick parvi.

If neither signal fires, carchar.
"""

from typing import Optional

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Must match opteryx::parvi::kCapacity in third_party/mabel/parvi/parvi.hpp.
PARVI_CAPACITY = 16


class HashMapVariantStrategy(OptimizationStrategy):
    """Tag AggregateAndGroup nodes with their preferred hash-map variant."""

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type != LogicalPlanStepType.AggregateAndGroup:
            return context

        # Already tagged (e.g. by a later traversal) — leave alone.
        if getattr(node, "group_map_variant", None) is not None:
            return context

        variant = self._pick_variant(node, context)
        # Always stamp, so the physical planner sees an explicit choice and
        # we never silently depend on the operator default.
        node.group_map_variant = variant
        context.optimized_plan[context.node_id] = node
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return bool(
            get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.AggregateAndGroup,))
        )

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------

    def _pick_variant(
        self, agg_node: LogicalPlanNode, context: OptimizerContext
    ) -> str:
        plan = context.pre_optimized_tree
        scan_nodes = self._upstream_scans(plan, agg_node)
        if not scan_nodes:
            return "carchar"

        # Signal 2 first — cheapest and provably correct.
        total_rows = self._total_record_count(scan_nodes)
        if total_rows is not None and total_rows <= PARVI_CAPACITY:
            return "parvi"

        # Signal 1 — NDV product across group columns.
        ndv_product = self._ndv_product(agg_node, scan_nodes)
        if ndv_product is not None and ndv_product <= PARVI_CAPACITY:
            return "parvi"

        return "carchar"

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
            except Exception:
                return None
            if count is None:
                return None
            total += int(count)
        return total

    def _ndv_product(self, agg_node: LogicalPlanNode, scan_nodes: list) -> Optional[int]:
        groups = getattr(agg_node, "groups", None)
        if not groups:
            # GROUP BY () collapses to a single row — trivially parvi-eligible.
            return 1

        # Try to resolve each group column to a manifest cardinality estimate.
        # Any expression group (e.g. GROUP BY UPPER(x)) defeats this signal and
        # we bail out to carchar — we'd need per-expression NDV propagation.
        product = 1
        for group_expr in groups:
            col_name = self._source_column_name(group_expr)
            if col_name is None:
                return None
            ndv = self._column_ndv(col_name, scan_nodes)
            if ndv is None:
                return None
            product *= max(1, ndv)
            if product > PARVI_CAPACITY:
                # Early exit — we already know we'll pick carchar on this signal.
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
            except Exception:
                return None
            if estimate is None:
                return None
            total += int(estimate)
        return total if total > 0 else None
