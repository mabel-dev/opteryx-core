# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""JoinOrderingStrategy is a costed pair, so its choices must be visible.

`OptimizationStrategy.record_decision` states the contract: a strategy that compares
concrete plan alternatives must report the outcome WITH the numbers it decided on.
Join ordering was not meeting it — it incremented a bare counter, and only when it
SWAPPED. A build side left where it was, a swap skipped because a leg was synthetic,
and a swap the row-count guard overturned were all indistinguishable from a query
with no joins in it.

That matters because the choice is expensive and reversible-looking: TPC-H Q18 at
SF100 ran 13.6s because rule 3 moved the 600M-row side onto the build leg on NDV
alone, and 3.5s once the row-count guard stopped it. Neither run said which rule had
spoken, or on what numbers.

These tests pin the record for both decision points — the inner-join build side and
the SEMI/ANTI exchange — and specifically that each DECLINE is spelled apart from
the others, since "no statistics" (an estimator gap), "ratio below the margin" (the
rule working) and "a LIMIT could short-circuit" (a correctness gate) send a reader
to three different places.

Reporting only. None of these change a decision — the parallel assertions on
`optimization_*` counters and `swap_build_side` are here to prove that.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.planner.optimizer.strategies.join_ordering import _decide_swap_reasoned

TPCH = "testdata.tpch_1"


def _decisions(sql, label):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    readings = session.telemetry
    return [
        d["detail"]
        for d in readings.get("optimizer_decisions") or []
        if d["label"] == label
    ]


# --- the rules name themselves ---------------------------------------------


def test_every_rule_names_itself():
    """Each rule reports a DIFFERENT name — a record that called everything
    "cost-based" would carry no more information than the counter it replaces."""
    rules = {
        _decide_swap_reasoned(301, 100, None, None, None, None)[1],  # rule 1
        _decide_swap_reasoned(200, 200, 100, 100, None, None)[1],  # rule 2 (tie)
        _decide_swap_reasoned(200, 200, 150, 50, None, None)[1],  # rule 3
        _decide_swap_reasoned(286, 600, 53, 37, None, None)[1],  # rule 3 overruled
        _decide_swap_reasoned(100, 200, None, None, None, None)[1],  # no NDV
    }
    assert len(rules) == 5


def test_row_count_guard_is_named_not_folded_into_rule_three():
    """The guard that stopped Q18's 13.6s regression must be visible AS the guard.
    Reported as plain "rule 3" it would look like cardinality picked this side."""
    swap, rule = _decide_swap_reasoned(286, 600, 53, 37, None, None)
    assert swap is False
    assert "overruled" in rule


# --- end to end -------------------------------------------------------------


def test_inner_join_records_a_kept_decision_with_its_numbers():
    """A build side LEFT WHERE IT WAS is a decision. The old counter only spoke on
    a swap, so "the rule considered this and kept it" and "the rule never ran" were
    the same silence."""
    details = _decisions(
        f"""SELECT c.c_name, o.o_orderkey FROM {TPCH}.customer c
            INNER JOIN {TPCH}.orders o ON c.c_custkey = o.o_custkey""",
        "inner join build side",
    )
    assert len(details) == 1
    detail = details[0]
    assert detail.startswith("kept (")
    # Both sides' numbers, so the choice is checkable rather than assertable.
    assert "149,998 rows" in detail
    assert "1,500,000 rows" in detail


def test_inner_join_records_a_swap_and_still_counts_it():
    """The record is ADDED, not swapped in for the counter — EXPLAIN's OPTIMIZATIONS
    block renders both, and existing consumers of the counter keep working."""
    session = opteryx.session()
    for _ in session.execute_to_morsels(
        f"""SELECT c.c_name, n.n_name FROM {TPCH}.orders o
            INNER JOIN {TPCH}.customer c ON c.c_custkey = o.o_custkey
            INNER JOIN {TPCH}.nation n ON n.n_nationkey = c.c_nationkey"""
    ):
        pass
    readings = session.telemetry
    details = [
        d["detail"]
        for d in readings["optimizer_decisions"]
        if d["label"] == "inner join build side"
    ]
    assert len(details) == 2, "one record per join, not one per query"
    assert all(d.startswith("swapped (") for d in details)
    assert readings["optimization_inner_join_smallest_table_left"] == 2


def test_ndv_is_reported_as_unknown_when_the_rule_wanted_it():
    """"ndv unknown" and "ndv 1" send a reader to different places, so a consulted
    statistic that was missing is spelled, never omitted or defaulted."""
    details = _decisions(
        "SELECT a.name FROM $planets a INNER JOIN $planets b ON a.id = b.id",
        "inner join build side",
    )
    assert len(details) == 1
    assert "ndv unknown" in details[0]


def test_semi_join_exchange_declines_below_the_margin_with_the_ratio():
    """The ratio gate is the whole decision, so the ratio and the margin are the
    numbers that have to appear."""
    details = _decisions(
        f"""SELECT o_orderkey FROM {TPCH}.orders
            WHERE o_custkey IN (SELECT c_custkey FROM {TPCH}.customer)""",
        "left semi join exchange",
    )
    assert len(details) == 1
    detail = details[0]
    assert detail.startswith("declined, ratio ")
    assert "10x margin" in detail


def test_semi_join_exchange_records_the_exchange_it_made():
    """The accepted branch reports the ratio that cleared the margin — the counter
    it sits beside says only that SOMETHING was exchanged, not on what evidence."""
    details = _decisions(
        f"""SELECT n_name FROM {TPCH}.nation
            WHERE n_nationkey IN (SELECT c_nationkey FROM {TPCH}.customer)""",
        "left semi join exchange",
    )
    assert len(details) == 1
    assert details[0].startswith("exchanged, ratio ")
    assert "clears the 10x margin" in details[0]


def test_ratio_stays_readable_across_its_real_range():
    """Real ratios here run from 0.004 to several thousand. One format string
    renders one end "0.00x" and the other "6e+03x" — unreadable exactly where the
    number IS the argument."""
    small = _decisions(
        f"""SELECT o_orderkey FROM {TPCH}.orders
            WHERE o_custkey IN (SELECT c_custkey FROM {TPCH}.customer)""",
        "left semi join exchange",
    )[0]
    large = _decisions(
        f"""SELECT n_name FROM {TPCH}.nation
            WHERE n_nationkey IN (SELECT c_nationkey FROM {TPCH}.customer)""",
        "left semi join exchange",
    )[0]
    assert "ratio 0.1x" in small
    assert "ratio 6,000x" in large


def test_semi_join_exchange_does_not_report_statistics_it_never_read():
    """The exchange reads ROW COUNTS only. Printing "ndv unknown" there would
    implicate a missing statistic in a decision that never wanted one."""
    details = _decisions(
        f"""SELECT o_orderkey FROM {TPCH}.orders
            WHERE o_custkey IN (SELECT c_custkey FROM {TPCH}.customer)""",
        "left semi join exchange",
    )
    assert "ndv" not in details[0]


def test_no_join_records_no_join_decision():
    """No decision was made, so nothing is claimed. The record exists to remove
    silence where a choice happened, not to add noise where none did."""
    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT name FROM $planets"):
        pass
    labels = {d["label"] for d in session.telemetry["optimizer_decisions"]}
    assert "inner join build side" not in labels
    assert "left semi join exchange" not in labels


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
