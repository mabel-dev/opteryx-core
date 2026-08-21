# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Plan-time rejection of queries whose result would exceed `sql_select_limit`.

Refuses the query BEFORE any data is read, so an accidental cross join costs
nothing rather than an hour of IO.

The check is deliberately CONDITIONAL, twice over:

  * It only fires when every scanned relation carries a real row count.
    `statistics_refresh._UNKNOWN_ROW_COUNT` substitutes 1,000,000 for a
    relation that cannot report its size, and that fabrication multiplies
    through joins — before virtual datasets declared their counts, a 2-way
    self cross join of the 9-row `$planets` was estimated at 10**12 rows
    against an actual 81. Gating on a number like that would refuse trivial
    queries, so a single unknown input disables the plan-time check entirely.

  * It only fires when the terminal count is a `row_count_metric` — a number
    the statistics claim to KNOW (exact arithmetic over real counts: the
    accidental plain cross join, the unfiltered too-large scan). A
    `row_count_estimate` — anything that passed through a selectivity or NDV
    heuristic (a filter, an equi-join, a grouped aggregate) — is a guess, and
    refusing a query on a guess is the same dishonesty as the fabricated
    unknown-count case: TPC-DS Q39 (a self-join of a grouped-aggregate CTE)
    was refused on an "estimated" 8.5 billion rows against a tiny actual
    result. Estimates defer to the runtime counter, which measures rows
    instead of guessing.

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


def check_estimated_result_size(plan, limit: int, telemetry=None, scan_stats_cache=None):
    """Raise ResultTooLargeError when the plan's estimated result exceeds `limit`.

    No-op (returns the plan unchanged) when the limit is not positive, when any
    input lacks real statistics, when no estimate could be produced, or when
    the terminal count is only a `row_count_estimate` rather than a metric
    (see the module docstring — the guard acts on numbers it can stand
    behind, never on heuristics).

    Also a no-op when the plan's actual output isn't a plain `Exit` -- e.g.
    `EXPLAIN SELECT ...` wraps the inner query's plan (`... -> Exit -> Explain`)
    but its own output is a plan description, not the inner query's row set.
    The inner `Exit` node still exists (and still carries the inner query's
    row-count estimate), but it is no longer the plan's terminal node once
    Explain sits above it -- checking the graph's real exit point catches this
    instead of a linear scan for any node of type Exit, which would find and
    enforce the limit against a row count EXPLAIN never streams.
    """
    if not limit or limit <= 0:
        return plan
    if not every_input_has_row_counts(plan):
        return plan

    # Statistics are refreshed opportunistically during optimization (only when a
    # strategy asks for them), so a simple scan reaches here with none attached.
    # Refresh only now that the inputs are known to be trustworthy — this is the
    # one place that pays for it, and only for plans it can actually act on. This
    # runs even for EXPLAIN (where enforcement below is skipped): EXPLAIN's own
    # `est_rows` column reads these same `.statistics` attachments.
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

    if getattr(plan, "statistics_are_stale", True):
        plan = refresh_statistics(plan, telemetry=telemetry, scan_stats_cache=scan_stats_cache)

    exit_points = plan.get_exit_points()
    if len(exit_points) != 1 or plan[exit_points[0]].node_type != LogicalPlanStepType.Exit:
        return plan

    # Enforce ONLY on a metric terminal count — `row_count_metric` is None
    # whenever the number is an estimate, so estimates fall through to the
    # runtime counter without a special case here.
    estimate = getattr(
        getattr(plan[exit_points[0]], "statistics", None), "row_count_metric", None
    )

    if estimate is not None and estimate > limit:
        if telemetry is not None:
            telemetry._reading["result_size_rejected_estimate"] = int(estimate)
        raise ResultTooLargeError(rows=int(estimate), limit=int(limit), estimated=True)

    return plan
