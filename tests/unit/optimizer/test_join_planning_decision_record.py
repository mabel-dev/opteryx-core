# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""JoinPlanningStrategy is a costed strategy, so its declines must be visible.

The strategy walked away from a chain in six places and said nothing. All a
reader got was ``{'strategy': 'JoinPlanningStrategy', 'changed': False}`` in
``optimizer_trace`` — which reads identically whether DPccp compared plans and
kept this one, or never ran at all.

That is not academic: TPC-H Q21 at SF100 got NO join ordering whatsoever
(measured 2026-09-13), and establishing that required monkeypatching the
strategy's internals. The particular decline responsible — "no predicates above
the chain", which a decorrelated EXISTS over a comma-join chain triggers — is
pinned separately here, because it is masking a real planning bug and must not
read like "nothing to do".

Reporting only. These tests assert what is SAID, not what is planned.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx

T = "testdata.tpch_1."
LABEL_PREFIX = "cost-based join planning"


def _details(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    return [
        d["detail"]
        for d in (session.telemetry.get("optimizer_decisions") or [])
        if d["label"].startswith(LABEL_PREFIX)
    ]


def _only(sql):
    details = _details(sql)
    assert len(details) == 1, f"expected one decision, got {details}"
    return details[0]


def test_no_predicates_above_the_chain_is_named():
    """A pure cartesian chain: DPccp cannot run without predicates, and the
    record must say DPccp did not run rather than implying a comparison."""
    detail = _only(f"SELECT c.c_custkey, n.n_name FROM {T}customer c, {T}nation n")
    assert "no predicates above the chain" in detail
    assert "DPccp did not run" in detail


def test_decorrelated_exists_no_longer_blinds_the_enumerator():
    """The Q21 shape, and the regression guard for the fix.

    DecorrelateSubqueryStrategy lowers EXISTS into a semi Join that lands
    between the chain and the WHERE filters. _collect_predicates_above used to
    stop there and hand back nothing, so DPccp never ran on a chain whose join
    predicates were plainly present in the query. It now walks through Join
    parents, so the enumerator must reach a costed outcome instead of the
    no-predicates decline."""
    detail = _only(
        f"SELECT * FROM {T}customer c, {T}orders o"
        f" WHERE c.c_custkey = o.o_custkey"
        f" AND EXISTS (SELECT 1 FROM {T}lineitem l WHERE l.l_orderkey = o.o_orderkey)"
    )
    assert "no predicates above the chain" not in detail
    # The enumerator ran: either it kept the order or it reordered, but it costed.
    assert ("kept" in detail) or ("reordered" in detail), detail


def test_no_usable_join_graph_is_named_and_carries_counts():
    """Predicates exist but none is a cross-leaf equality, so build_join_graph
    refuses. Distinct from the no-predicates case, and the counts let a reader
    tell "no equi edge" from "missing statistics"."""
    detail = _only(
        f"SELECT c.c_custkey FROM {T}customer c, {T}nation n"
        f" WHERE c.c_nationkey > n.n_nationkey"
    )
    assert "no usable join graph" in detail
    assert "2 leaves" in detail
    assert "1 predicate(s)" in detail


def test_keeping_the_existing_order_is_recorded_as_a_decision():
    """The enumerator ran and chose the order already there. That is a costed
    outcome, not an absence of one, and it names the graph it decided on."""
    detail = _only(
        f"SELECT * FROM {T}nation n, {T}customer c, {T}orders o"
        f" WHERE n.n_nationkey = c.c_nationkey AND c.c_custkey = o.o_custkey"
    )
    assert "kept" in detail
    assert "3 vertices" in detail
    assert "2 edge(s)" in detail


def test_a_reorder_is_recorded_too():
    detail = _only(
        f"SELECT * FROM {T}lineitem l, {T}orders o, {T}customer c, {T}nation n"
        f" WHERE l.l_orderkey = o.o_orderkey AND o.o_custkey = c.c_custkey"
        f" AND c.c_nationkey = n.n_nationkey"
    )
    assert "reordered" in detail
    assert "4 vertices" in detail


def test_the_declines_are_spelled_apart():
    """Three declines that point at three different pieces of work must not
    share wording — a record that called them all "declined" would be no more
    use than the counter it replaces."""
    no_predicates = _only(f"SELECT c.c_custkey, n.n_name FROM {T}customer c, {T}nation n")
    no_graph = _only(
        f"SELECT c.c_custkey FROM {T}customer c, {T}nation n"
        f" WHERE c.c_nationkey > n.n_nationkey"
    )
    kept = _only(
        f"SELECT * FROM {T}nation n, {T}customer c, {T}orders o"
        f" WHERE n.n_nationkey = c.c_nationkey AND c.c_custkey = o.o_custkey"
    )
    assert len({no_predicates, no_graph, kept}) == 3


def test_the_enumerator_is_not_wrapped_in_a_try_except():
    """§9: control flow by exception is forbidden. build_join_graph already
    enforces every precondition the enumerator refuses on (>= 1 vertex,
    connected), so a raise here is an invariant violation that must surface,
    not a shape the strategy may silently decline."""
    import inspect

    from opteryx.planner.optimizer.strategies.join_planning import JoinPlanningStrategy

    source = inspect.getsource(JoinPlanningStrategy.complete)
    assert "except" not in source
    assert "enumerate_join_tree(graph)" in source


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
