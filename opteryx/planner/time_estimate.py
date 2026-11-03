# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A rough, plan-time estimate of how long a statement will take: one of
`<1s`, `1-5s`, `5-15s`, `>15s`, recorded as the `estimated_time_bin` reading.

WHAT IT IS. A small decision tree (planner/time_estimate_tree.py, generated)
fitted on production job telemetry: features available once planning has
finished, against the `time_total` those jobs went on to take. It is a BIN,
not a duration, because nothing the planner knows supports a duration: the
first fit (2026-11-03, 7,532 prod jobs, held-out last 30% by time) put 78% of
test jobs in the right bin and 98% within one, against 65.5% for always
answering `1-5s`. The weak bins are the slow ones: most `5-15s` and `>15s`
jobs are called one bin low, and a tail driven by IO and cache state is not
visible from the plan. Read it as "at least this slow", not "this slow".

WHY AT THE END OF PLANNING. The strongest single feature is how long planning
itself took. Planning is mostly catalog and manifest work, and a statement
that is slow to plan is slow to run for the same reasons (large manifests,
many files, a cold metadata path). So the estimate is taken once
`time_planning` is known, not inside the optimizer.

ONE FEATURE DEFINITION. `plan_features` is the only definition of the
features. The engine calls it with the live readings; the fitter
(tests/performance/estimation/fit_time_bins.py) calls the SAME function with
the readings a job doc stored. That is why it takes plain values rather than
a plan: the readings are the one thing both sides have, so the tree is scored
on exactly the numbers it will be served. This module imports nothing from
opteryx for the same reason - the fitter loads it standalone.

Every input is a plan-time reading: `billing_bytes` (planner/data_processed.py),
`estimated_row_counts` and `join_estimates` (statistics refresh on the final
plan), and `time_planning`. None is a runtime counter, so the estimate never
borrows from the execution it is predicting.

BATCHES. A semicolon-separated batch plans each statement in turn and these
readings accumulate across it, so the estimate recorded after the last
statement is for the batch - the same scope the training rows (one per job)
had.
"""

import math
import re
from typing import Iterable, Mapping, Optional

__all__ = ["BINS", "plan_features", "estimate_time_bin", "record_time_estimate"]

BINS = ("<1s", "1-5s", "5-15s", ">15s")

# Statement verbs with enough history to learn from. Anything else is OTHER;
# `WITH` is a query and is folded into SELECT.
_VERBS = ("SELECT", "EXECUTE", "OPTIMIZE", "REFRESH", "SET", "SHOW", "EXPLAIN", "INSERT")

_LEADING_COMMENTS = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|\Z)|/\*.*?\*/)*", re.DOTALL)


def statement_verb(sql: str) -> str:
    """The statement's first keyword, past any leading comments."""
    body = _LEADING_COMMENTS.sub("", sql or "", count=1)
    match = re.match(r"[A-Za-z]+", body)
    verb = match.group(0).upper() if match else ""
    if verb == "WITH":
        verb = "SELECT"
    return verb if verb in _VERBS else "OTHER"


def _log(value) -> float:
    return math.log10(1 + max(0, value or 0))


def plan_features(
    *,
    time_planning_s: float,
    billing_bytes: int,
    estimated_row_counts: Optional[Iterable[Mapping]],
    join_estimates: Optional[Iterable[Mapping]],
    sql: str,
) -> dict:
    """The features the time tree reads, from plan-time readings only.

    `estimated_row_counts` is empty when the optimizer never estimated - the
    row features are then zero and `has_estimates` says so, rather than the
    absence reading as "estimated zero rows".
    """
    estimates = list(estimated_row_counts or [])
    joins = list(join_estimates or [])
    rows = [e.get("row_count") or 0 for e in estimates]
    node_types = [e.get("node_type") for e in estimates]
    join_types = [j.get("join_type") for j in joins]

    def count(*names):
        return sum(t in names for t in node_types)

    features = {
        "time_planning": float(time_planning_s or 0),
        "log_billing_bytes": _log(billing_bytes),
        "has_estimates": int(bool(estimates)),
        "log_sum_est_rows": _log(sum(rows)),
        "log_max_est_rows": _log(max(rows, default=0)),
        "n_scan": count("Scan"),
        "n_join": count("Join"),
        "n_aggregate": count("Aggregate", "AggregateAndGroup"),
        "n_window": count("Window"),
        "n_sort": count("Order", "HeapSort"),
        "n_union": count("Union"),
        "n_insert": count("Insert"),
        "n_merge": count("Merge"),
        "n_compaction": count("CompactionCommit"),
        "n_nested_loop": sum(t == "nested loop" for t in join_types),
        "n_cross_join": sum(t == "cross join" for t in join_types),
        "log_max_join_rows": _log(max((j.get("row_count") or 0 for j in joins), default=0)),
        "log_max_join_build": _log(max((j.get("right_row_count") or 0 for j in joins), default=0)),
    }
    verb = statement_verb(sql)
    for name in _VERBS + ("OTHER",):
        features["verb_" + name] = int(verb == name)
    return features


def estimate_time_bin(features: Mapping, tree=None) -> str:
    """Walk the tree: a node is `(feature, threshold, if_le, if_gt)`, a leaf is
    a bin label."""
    if tree is None:
        from opteryx.planner.time_estimate_tree import TREE as tree
    node = tree
    while not isinstance(node, str):
        feature, threshold, if_le, if_gt = node
        node = if_le if features[feature] <= threshold else if_gt
    return node


def record_time_estimate(telemetry, sql: str) -> None:
    """Record `estimated_time_bin` from the session's readings. Call once
    `time_planning` includes this statement's planning."""
    readings = telemetry._reading
    features = plan_features(
        time_planning_s=readings["time_planning"] / 1e9,
        billing_bytes=readings["billing_bytes"],
        estimated_row_counts=readings.get("estimated_row_counts"),
        join_estimates=readings.get("join_estimates"),
        sql=sql,
    )
    readings["estimated_time_bin"] = estimate_time_bin(features)
