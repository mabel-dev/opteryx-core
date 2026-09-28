# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statistics refresh: the planner's entry to the native refresh (native plan graph P5).

`refresh_statistics` recomputes, in the query's StatisticsStore
(`PlanContext.statistics`), the estimated statistics of every node of a logical
plan - see src/cpp/planner/statistics_refresh.hpp for what each operator does to
them. Triggered by ``OptimizerVisitor.optimize`` before any cost-based strategy
while the plan's ``statistics_are_stale`` flag is set, and by the planner's
estimate consumers (the result-size guard, EXPLAIN ANALYZE, shared CTE bodies).

With `telemetry`, the refresh also records the planner's estimates for comparison
with what the query produced: each node's row count (with its provenance) and
byte total, each predicate's selectivity, cost and estimator tier, and each
join's cardinality inputs. The numbers are native; rendering a predicate's
condition as text is the one thing done here.
"""

from opteryx.compiled.structures.plan_steps import steps_with
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext


def refresh_statistics(plan: LogicalPlan, context: PlanContext, telemetry=None) -> LogicalPlan:
    """Recompute the statistics of every node in `plan` into `context`, and clear
    the plan's ``statistics_are_stale`` flag. With `telemetry`, record the
    estimates onto ``telemetry._reading`` (``estimated_row_counts`` /
    ``estimated_total_bytes`` / ``predicate_estimates`` / ``join_estimates``) -
    diagnostic detail, never consulted by planning."""
    notes = context.statistics.refresh(plan, telemetry=telemetry is not None)
    if telemetry is not None:
        predicates, joins = notes
        _record_telemetry(plan, context, telemetry, predicates, joins)
    plan.statistics_are_stale = False
    return plan


def _expressions_by_id(plan: LogicalPlan) -> dict:
    """Every expression in the plan's steps, by expr_id."""
    found = {}
    stack = []
    for _nid, step in plan.nodes(True):
        stack.extend(step.expressions(True))
    while stack:
        expression = stack.pop()
        if expression.expr_id in found:
            continue
        found[expression.expr_id] = expression
        stack.extend(expression.children())
    return found


def _record_telemetry(plan: LogicalPlan, context: PlanContext, telemetry, predicates, joins) -> None:
    from opteryx.expression import format_expression

    store = context.statistics
    relational = steps_with("relation")
    row_counts = []
    total_bytes_by_node = []
    for nid, node in plan.nodes(True):
        if not store.has(nid):
            continue
        relation = node.relation if node.node_type in relational else None
        row_counts.append(
            {
                "nid": nid,
                "node_type": node.node_type.name,
                "relation": relation,
                "row_count": store.row_count(nid),
                # "metric" is a number we claim to KNOW, "estimate" passed through
                # a selectivity or NDV heuristic: the estimate-vs-actual harness
                # scores only the numbers the estimators produced.
                "row_count_kind": "metric" if store.row_count_metric(nid) is not None else "estimate",
            }
        )
        # summed over the columns with a known size; None (not 0) when none has
        # one, so "unknown" and "known to be empty" stay distinct
        total_bytes_by_node.append(
            {
                "nid": nid,
                "node_type": node.node_type.name,
                "relation": relation,
                "total_bytes": store.total_bytes(nid),
            }
        )
    expressions = _expressions_by_id(plan)
    telemetry._reading["estimated_row_counts"] = row_counts
    telemetry._reading["estimated_total_bytes"] = total_bytes_by_node
    telemetry._reading["predicate_estimates"] = [
        {
            "nid": nid,
            "node_type": "Scan" if scan else "Filter",
            "relation": relation,
            "condition": format_expression(expressions[condition]),
            "selectivity": selectivity,
            "cost": cost,
            "estimator": estimator,
        }
        for nid, scan, relation, condition, selectivity, cost, estimator in predicates
    ]
    telemetry._reading["join_estimates"] = [
        {
            "nid": nid,
            "join_type": join_type,
            "left_row_count": left_rows,
            "right_row_count": right_rows,
            "row_count": out_rows,
            "key_count": key_count,
        }
        for nid, join_type, left_rows, right_rows, out_rows, key_count in joins
    ]
