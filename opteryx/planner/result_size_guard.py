# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Plan-time rejection of queries whose result would exceed `sql_select_limit`.

Refuses the query BEFORE any data is read, so an accidental cross join costs
nothing rather than an hour of IO.

The check is deliberately CONDITIONAL: it only fires when every scanned relation
carries a real row count. `statistics_refresh._UNKNOWN_ROW_COUNT` substitutes
1,000,000 for a relation that cannot report its size, and that fabrication
multiplies through joins — before virtual datasets declared their counts, a
2-way self cross join of the 9-row `$planets` was estimated at 10**12 rows
against an actual 81. Gating on a number like that would refuse trivial queries,
so a single unknown input disables the plan-time check entirely and leaves
enforcement to the runtime counter (which measures rows instead of guessing).

This is the "too big to be worth starting" gate; it is not the only one. A query
whose estimate is UNDER the limit can still deliver more rows than predicted, so
the runtime counter in `query_session` remains the backstop.
"""

from typing import Optional

from opteryx.exceptions import ResultTooLargeError
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType


def _declared_row_count(node) -> Optional[int]:
    """The relation's REAL row count, or None when it cannot report one.

    Mirrors the precedence in statistics_refresh._scan_stats, minus its
    `_UNKNOWN_ROW_COUNT` fallback — the whole purpose here is to detect that the
    fallback WOULD have been used, which is information the statistics themselves
    no longer carry once it has been applied.
    """
    manifest = getattr(node, "manifest", None)
    if manifest is not None:
        count = manifest.get_record_count()
        if count is not None and count > 0:
            return count
    schema = getattr(node, "schema", None)
    if schema is not None:
        count = schema.row_count_metric or schema.row_count_estimate
        if count is not None and count > 0:
            return count
    return None


def every_input_has_row_counts(plan) -> bool:
    """True when no scanned relation would fall back to a fabricated row count."""
    saw_a_scan = False
    for nid in plan.nodes():
        node = plan[nid]
        if node.node_type != LogicalPlanStepType.Scan:
            continue
        saw_a_scan = True
        if _declared_row_count(node) is None:
            return False
    return saw_a_scan


def check_estimated_result_size(plan, limit: int, telemetry=None):
    """Raise ResultTooLargeError when the plan's estimated result exceeds `limit`.

    No-op (returns the plan unchanged) when the limit is not positive, when any
    input lacks real statistics, or when no estimate could be produced.
    """
    if not limit or limit <= 0:
        return plan
    if not every_input_has_row_counts(plan):
        return plan

    # Statistics are refreshed opportunistically during optimization (only when a
    # strategy asks for them), so a simple scan reaches here with none attached.
    # Refresh only now that the inputs are known to be trustworthy — this is the
    # one place that pays for it, and only for plans it can actually act on.
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

    if getattr(plan, "statistics_are_stale", True):
        plan = refresh_statistics(plan)

    estimate = None
    for nid in plan.nodes():
        node = plan[nid]
        if node.node_type == LogicalPlanStepType.Exit:
            estimate = getattr(getattr(node, "statistics", None), "row_count", None)
            break

    if estimate is not None and estimate > limit:
        if telemetry is not None:
            telemetry._reading["result_size_rejected_estimate"] = int(estimate)
        raise ResultTooLargeError(rows=int(estimate), limit=int(limit), estimated=True)

    return plan
