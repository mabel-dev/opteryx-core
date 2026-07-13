# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - DISTINCT LIMIT Bound (MinHash exact set)

Type: Heuristic (statistics-driven)
Goal: Early-terminate the scan under a DISTINCT

When the consolidated KMV sketch for a single DISTINCT key column is COMPLETE
(fewer than K=32 distinct hashes), the column has an exactly known number of
distinct values. ``SELECT DISTINCT col`` can therefore emit at most that many
rows (a NULL counts as one distinct row, and the complete sketch already counts
it). Injecting a ``LIMIT`` of exactly that count lets the push-based engine stop
as soon as every distinct value has been seen: the LIMIT operator signals
``ctx.terminate()`` upstream, so files/morsels after the last new distinct value
are never read.

Soundness: the injected limit equals the EXACT distinct-row count (not an upper
bound), so DISTINCT emits precisely that many rows and the LIMIT never cuts a
real one — it only removes the "keep scanning to be sure" tail.

Scope (v1): plain ``SELECT DISTINCT`` (``node.on is None``) over a SINGLE key
column that is a direct column of one Scan with a manifest, and not already
capped by a LIMIT. Multi-column DISTINCT is skipped: the per-column product is a
loose upper bound (not the exact tuple count), so it would never trigger the
early stop.

Order: after LimitPushdown/LimitFilesPruning — the injected LIMIT sits above the
DISTINCT barrier purely to drive execution-time early termination and must not be
re-collected by the limit-movement strategies.
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class DistinctLimitBoundStrategy(OptimizationStrategy):
    """Inject an exact LIMIT above a single-column DISTINCT proven small by KMV."""

    requires = ("limits-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]
        return context

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Distinct,))) > 0

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        for nid, node in list(plan.nodes(data=True)):
            if node.node_type != LogicalPlanStepType.Distinct:
                continue
            # DISTINCT ON (...) dedups on a subset while carrying other columns —
            # the output row count is not bounded by the key columns' cardinality.
            if getattr(node, "on", None) is not None:
                continue
            # Already capped downstream by a LIMIT — nothing to add.
            if any(
                plan[target].node_type == LogicalPlanStepType.Limit
                for _, target, _ in plan.outgoing_edges(nid)
            ):
                continue

            resolved = self._resolve_single_key(plan, nid)
            if resolved is None:
                continue
            manifest, key_name = resolved

            # Exact SELECT DISTINCT row count (NULL counts as one row).
            bound = manifest.exact_distinct_count(key_name, exclude_nulls=False)
            if bound is None or bound <= 0:
                continue
            # No early-stop benefit if the bound is not below the rows available.
            if bound >= manifest.get_record_count():
                continue

            limit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
            limit_node.limit = int(bound)
            limit_node.offset = 0
            limit_node.columns = []
            plan.insert_node_after(random_string(), limit_node, nid)
            self.telemetry.optimization_limit_pushdown += 1

        return plan

    @staticmethod
    def _resolve_single_key(plan: LogicalPlan, distinct_nid):
        """Return (manifest, key_column_name) when the DISTINCT dedups a single
        plain column of one Scan; else None.

        Accepts DISTINCT directly over a Scan, or over a projection that selects
        exactly one plain column from a Scan.
        """
        ins = plan.ingoing_edges(distinct_nid)
        if len(ins) != 1:
            return None
        source = plan[ins[0][0]]

        key_name = None
        if source.node_type == LogicalPlanStepType.Project:
            columns = getattr(source, "columns", None) or []
            if len(columns) != 1:
                return None
            col = columns[0]
            if getattr(col, "node_type", None) != NodeType.IDENTIFIER:
                return None  # expression key — a column sketch cannot bound it
            key_name = getattr(col, "source_column", None)
            if not key_name:
                return None
            pins = plan.ingoing_edges(ins[0][0])
            if len(pins) != 1:
                return None
            scan_node = plan[pins[0][0]]
        elif source.node_type == LogicalPlanStepType.Scan:
            scan_node = source
        else:
            return None

        if scan_node.node_type != LogicalPlanStepType.Scan:
            return None
        manifest = getattr(scan_node, "manifest", None)
        if manifest is None:
            return None

        if key_name is None:
            schema = getattr(scan_node, "schema", None)
            columns = getattr(schema, "columns", None) if schema is not None else None
            if not columns or len(columns) != 1:
                return None
            key_name = columns[0].name
            if not key_name:
                return None

        return manifest, key_name
