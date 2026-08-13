# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Distinct Pushdown

Type: Heuristic
Goal: Reduce Rows

This is a very specific rule, on a CROSS JOIN UNNEST, if the result
is the only column in a DISTINCT clause, we push the DISTINCT into
the JOIN.

We've written as a Optimization rule rather than in the JOIN code
as it is expected other instances of pushing DISTINCT may be found.

Order:
    This plan must run after the Projection Pushdown
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

"""
Aggregations we can push the DISTINCT past
"""


# Disarm the pushed DISTINCT only when the SOURCE ARRAY column is essentially unique
# per row — NDV within this fraction of the record count.
#
# The wrapper's NDV is a much poorer indicator than the element NDV we would like and
# cannot have (no statistic describes an array's element cardinality). It is not
# valueless though, and the threshold is set from measurement rather than taste:
#
#   measured WIN  (-3.6%)  200,000 rows, wrapper NDV 163,152 = 0.816
#   measured LOSS (+4.2%)  200,000 rows, wrapper NDV 200,000 = 1.000
#
# That 0.816..1.000 band is the entire discriminating range, so the only honest
# reading is the degenerate one: every array distinct means the elements have no
# duplication to harvest and the pre-reduction is pure cost. Anything below arms.
#
# This is deliberately biased toward ARMING. A user writes DISTINCT because they
# expect it to remove rows, so the fold is the expected case and the gate exists to
# catch the pathology, not to prove the win in advance. An UNKNOWN NDV therefore arms
# — and unknown is the common case: the estimate needs catalog KMV stats, so plain
# parquet-on-disk never produces one.
_DEGENERATE_NDV_RATIO = 0.95


class DistinctPushdownStrategy(OptimizationStrategy):
    requires = ("projection-pushed",)

    @staticmethod
    def _wrapper_ndv_is_degenerate(plan, node) -> bool:
        """True only on POSITIVE evidence that the source array is unique per row.

        Returns False when anything is unknown — no manifest, no KMV stats, no record
        count, an unresolvable column name. False means "arm the fold", which is the
        intended default; this gate can only ever veto on evidence."""
        source = getattr(node, "unnest_column", None)
        schema_column = getattr(source, "schema_column", None)
        if schema_column is None:
            return False
        column_name = getattr(schema_column, "name", None) or getattr(
            schema_column, "source_column", None
        )
        if not column_name:
            return False

        nid = getattr(node, "nid", None)
        if nid is None:
            for candidate_nid, candidate in plan.nodes(True):
                if candidate is node:
                    nid = candidate_nid
                    break
        if nid is None:
            return False

        scans = [
            plan[source_nid]
            for source_nid, _t, _r in plan.breadth_first_search(nid, reverse=True)
            if plan[source_nid].node_type == LogicalPlanStepType.Scan
        ]
        if not scans:
            return False

        rows = 0
        ndv = 0
        for scan in scans:
            manifest = getattr(scan, "manifest", None)
            if manifest is None:
                return False
            try:
                count = manifest.get_record_count()
                estimate = manifest.estimate_cardinality(column_name)
            except (AttributeError, TypeError, ZeroDivisionError):
                # Malformed/degenerate stats are a perf signal only — never a reason
                # to fail a query, and never evidence of degeneracy.
                return False
            if count is None or estimate is None:
                return False
            rows += int(count)
            ndv += int(estimate)

        if rows <= 0:
            return False
        return (ndv / rows) >= _DEGENERATE_NDV_RATIO

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if (node.node_type == LogicalPlanStepType.Distinct) and node.on is None:
            node.nid = context.node_id
            context.collected_distincts.append(node)
            return context

        # Mark a CROSS JOIN UNNEST whose target is the sole column a collected
        # DISTINCT dedups on. The native UnnestOperator then skips an element whose
        # value that worker has already emitted, so the duplicate is never
        # materialized (src/cpp/engine/native_unnest.hpp::apply_distinct).
        #
        # This is a PRE-REDUCTION and the Distinct node STAYS: operator state is
        # per-worker, so only the DistinctSink can dedup across workers. Nothing here
        # removes a node — this rule only sets a flag, and the compiler re-checks the
        # precondition against the actual output layout before honouring it.
        #
        # `unnest_function` gates out CIDR_UNNEST for the same reason the filter fold
        # does: its elements are generated, not stored, so there is no child vector to
        # dedup over before expanding.
        # The "target is the only column" test is NOT made here, because it cannot be:
        # a collected DISTINCT has `on is None`, and such a node dedups on every column
        # that REACHES it, which `node.columns` does not describe (see
        # projection_pushdown's note on the same trap). Only the compiler knows the
        # actual output layout, so it re-tests there and ignores this flag unless the
        # unnest genuinely emits the target alone. This rule states the INTENT; the
        # compiler holds the veto.
        #
        # What this rule is still responsible for is that the collected DISTINCT
        # really does consume this stream: the barrier list below clears the
        # collection at Aggregate/Join/Limit/Union/Subquery, so a DISTINCT sitting
        # above e.g. `COUNT(*)` — where dropping duplicates WOULD change the answer —
        # has already been cleared before this node is reached.
        if (
            node.node_type == LogicalPlanStepType.Unnest
            and context.collected_distincts
            and getattr(node, "unnest_function", "UNNEST") == "UNNEST"
            and node.unnest_target is not None
            and not self._wrapper_ndv_is_degenerate(
                context.optimized_plan, node
            )
        ):
            node.distinct_target = True
            # `visit` is handed a node from the tree being WALKED, while the plan that
            # gets compiled is `context.optimized_plan` — a copy taken above. Mutating
            # the node alone sets the flag on the wrong tree and the fold silently
            # never fires. Write it back, as predicate_pushdown's fold does.
            context.optimized_plan[context.node_id] = node

        if node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Limit,
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.Subquery,
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Unnest,
        ):
            # we don't push past here
            context.collected_distincts.clear()

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are DISTINCT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Distinct,))
        return len(candidates) > 0
