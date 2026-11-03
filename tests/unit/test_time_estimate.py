# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The plan-time `estimated_time_bin` reading.

See opteryx/planner/time_estimate.py. These pin the contract, not the fitted
tree: a refit changes which bin a query lands in, never these.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.planner.time_estimate import BINS
from opteryx.planner.time_estimate import estimate_time_bin
from opteryx.planner.time_estimate import plan_features
from opteryx.planner.time_estimate import statement_verb
from opteryx.planner.time_estimate_tree import TREE


def _features(**overrides):
    args = dict(time_planning_s=0.1, billing_bytes=0, estimated_row_counts=None, join_estimates=None, sql="SELECT 1")
    args.update(overrides)
    return plan_features(**args)


@pytest.mark.parametrize(
    "sql, verb",
    [
        ("SELECT 1", "SELECT"),
        ("  with q as (select 1) select * from q", "SELECT"),
        ("/* view as CTE */ WITH q AS (SELECT 1) SELECT * FROM q", "SELECT"),
        ("-- note\n-- another\nEXECUTE nightly", "EXECUTE"),
        ("OPTIMIZE TABLE t", "OPTIMIZE"),
        ("DROP TABLE t", "OTHER"),
        ("", "OTHER"),
    ],
)
def test_statement_verb(sql, verb):
    assert statement_verb(sql) == verb


def test_no_estimates_is_not_zero_rows():
    features = _features()
    assert features["has_estimates"] == 0
    assert features["log_sum_est_rows"] == 0


def test_features_read_the_plan_readings():
    features = _features(
        estimated_row_counts=[
            {"node_type": "Scan", "row_count": 979},
            {"node_type": "Join", "row_count": 10},
            {"node_type": "HeapSort", "row_count": 10},
        ],
        join_estimates=[{"join_type": "nested loop", "row_count": 9, "right_row_count": 99}],
    )
    assert features["has_estimates"] == 1
    assert (features["n_scan"], features["n_join"], features["n_sort"], features["n_nested_loop"]) == (1, 1, 1, 1)
    assert features["log_sum_est_rows"] == pytest.approx(3.0)
    assert features["log_max_join_build"] == pytest.approx(2.0)


def test_every_tree_feature_is_a_served_feature():
    """A refit naming a feature `plan_features` no longer makes would KeyError
    on the first query - catch it here instead."""
    served = set(_features())
    used, leaves = set(), set()

    def walk(node):
        if isinstance(node, str):
            leaves.add(node)
            return
        used.add(node[0])
        walk(node[2])
        walk(node[3])

    walk(TREE)
    assert used <= served
    assert leaves <= set(BINS)


def test_tree_walk():
    tree = ("time_planning", 1.0, "<1s", ("log_billing_bytes", 9.0, "1-5s", ">15s"))
    assert estimate_time_bin(_features(time_planning_s=0.5), tree) == "<1s"
    assert estimate_time_bin(_features(time_planning_s=2, billing_bytes=10**6), tree) == "1-5s"
    assert estimate_time_bin(_features(time_planning_s=2, billing_bytes=10**12), tree) == ">15s"


def test_executed_query_records_a_bin():
    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT name FROM $planets WHERE id > 3"):
        pass
    assert session.telemetry["estimated_time_bin"] in BINS
