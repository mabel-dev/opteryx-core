# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule — Build-Side Output Shape for Joins

Type: Cost-based
Goal: tell the native Join2BuildSink how many rows the join is expected to emit,
so it can decide whether consolidating its retained build payload into one block
(and emitting the build half of every output morsel as CODES over that block)
costs less than copying the payload once per output row.

Today the build sink retains VIEWS — it copies no values and builds no string
arena, because "the slots still live in the source vectors, which we hold alive"
(native_join2.hpp's sink()). The probe then materialises the build half with a
full row gather per output batch, which copies one physical value per OUTPUT row.
That is the right trade when a join emits FEWER rows than its build side holds,
and the wrong one as soon as it emits more: at fanout 16 the same 72-byte string
is copied sixteen times.

Consolidating flips it — one copy of the build payload, then 4 bytes per output
row — but it is only a win when the output is big enough to repay that one copy.
The comparison needs two numbers: the build payload's real size, and the join's
output row count. The sink measures the FIRST exactly (it holds the vectors); only
the SECOND has to be estimated, which is what this strategy supplies.

THE PAYLOAD: ``node.join_output_rows_estimate`` — int, or None for unknown.
Fail-safe default: unknown. The sink leaves the build side exactly as it is today
when it gets no estimate, so a plan with no statistics can never be moved onto the
consolidating path by a fabricated number. Same posture as
:mod:`~opteryx.planner.optimizer.strategies.hash_map_variant` ("Missing or stale
stats -> carchar").

Only join types with a build-side payload gather are tagged. SEMI/ANTI emit probe
rows collapsed to existence and deliberately drop the build payload (see
compiler.py's ``semi_no_payload``), so they have no build gather to improve and are
skipped — tagging them would be a number nothing reads.
"""

from typing import Optional

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Join types whose probe materialises a BUILD payload. Everything here routes
# through the one gather at native_join2.hpp's build-payload emit.
#
# These are the LOGICAL planner's ``node.type`` spellings, which are NOT the same
# strings as compiler.py's `modes` map: the logical side says "cross join" and
# "nested loop" (spaces), the compiler says "cross" and "nested_loop"
# (underscore), and the physical planner is where they are translated. Both
# spellings are listed so a rename on either side degrades to "no estimate"
# (today's behaviour) rather than to a wrong one.
#
# Cross and nested-loop are here deliberately: they compile to an INNER probe
# with a ZERO KEY (compiler.py's `zero_key`), so they are the same code path at
# maximum fan-out — the shape this rule exists for.
_BUILD_PAYLOAD_JOINS = frozenset(
    {
        "inner",
        "left outer",
        "right outer",
        "full outer",
        "cross",
        "cross join",
        "nested loop",
        "nested_loop",
        "asof",
    }
)


class JoinBuildShapeStrategy(OptimizationStrategy):
    optimization_technique = "cost"
    # node.statistics is attached by refresh_statistics, which the optimizer runs
    # before any "cost" strategy whose plan has stale statistics.
    requires = ("joins-planned",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type not in (
            LogicalPlanStepType.Join,
            LogicalPlanStepType.DependentJoin,
        ):
            return context

        join_type = getattr(node, "type", None)
        if join_type not in _BUILD_PAYLOAD_JOINS:
            return context

        node.join_output_rows_estimate = self._output_rows(node)
        context.optimized_plan[context.node_id] = node
        return context

    @staticmethod
    def _output_rows(node) -> Optional[int]:
        """The join's estimated output row count, or None when unknown.

        Read straight off the statistics the refresh pass already propagated —
        ``_join_stats`` is what computes it, and it is the same number
        JoinOrderingStrategy costs its trees with. Never fabricated: a node with
        no statistics, or a non-positive count, yields None so the sink keeps
        today's behaviour.
        """
        stats = getattr(node, "statistics", None)
        if stats is None:
            return None
        rows = getattr(stats, "row_count", None)
        if rows is None or rows <= 0:
            return None
        return int(rows)

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return bool(
            get_nodes_of_type_from_logical_plan(
                plan, (LogicalPlanStepType.Join, LogicalPlanStepType.DependentJoin)
            )
        )
