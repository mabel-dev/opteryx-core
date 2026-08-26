# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The billing events record WHICH relations a statement read.

Nothing else records it. The engine is the only place that knows — the answer
comes off the bound plan, and recovering it downstream would mean a second
implementation of name resolution that can disagree with this one.

The load-bearing property here is not that the list is populated but that it
describes the SAME scans `billing_bytes` was measured over. Both come from
`iter_scan_nodes`, so a consumer can attribute one event's volume to one
event's tables without the two ever describing different queries.

See opteryx/planner/data_processed.py.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.managers.billing import BillingEventType

ASTRONAUTS = "testdata.astronauts"
MISSIONS = "testdata.flat.space_missions"


def _run(sql: str):
    """Execute `sql` to exhaustion, return its telemetry."""
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    return session._telemetry


def _relations(sql: str) -> list:
    return sorted(_run(sql).relations)


def test_a_scan_is_recorded():
    assert _relations(f"SELECT * FROM {ASTRONAUTS}") == [ASTRONAUTS]


def test_both_sides_of_a_join_are_recorded():
    relations = _relations(
        f"SELECT a.name FROM {ASTRONAUTS} AS a "
        f"INNER JOIN {MISSIONS} AS m ON a.name = m.Mission"
    )
    assert relations == [ASTRONAUTS, MISSIONS]


def test_a_self_join_records_one_relation():
    """A self-join reads one relation, twice.

    The number of scans is a plan property; what a consumer wants is the set of
    things touched, so this deduplicates. `billing_bytes` does NOT — it counts
    the scan twice, correctly, because the bytes really are read twice.
    """
    sql = (
        f"SELECT a.name FROM {ASTRONAUTS} AS a "
        f"INNER JOIN {ASTRONAUTS} AS b ON a.name = b.name"
    )
    assert _relations(sql) == [ASTRONAUTS]


def test_an_aliased_relation_is_recorded_under_its_real_name():
    """`FROM x AS p` reads x. The alias is a name for the statement, not a table."""
    assert _relations(f"SELECT p.name FROM {ASTRONAUTS} AS p") == [ASTRONAUTS]


def test_a_relation_inside_a_subquery_is_recorded():
    relations = _relations(
        f"SELECT name FROM {ASTRONAUTS} WHERE name IN (SELECT Mission FROM {MISSIONS})"
    )
    assert relations == [ASTRONAUTS, MISSIONS]


def test_a_materialized_cte_body_is_recorded():
    """A CTE referenced twice executes once, off to the side.

    Its scans are not in the main graph. The meter walks them explicitly or a
    CTE over a large table bills nothing; the relation list rides the same walk,
    so it cannot miss what the bill charges for.
    """
    sql = (
        f"WITH c AS (SELECT * FROM {MISSIONS}) "
        f"SELECT (SELECT COUNT(*) FROM c) + (SELECT COUNT(*) FROM c)"
    )
    assert _relations(sql) == [MISSIONS]


def test_a_union_records_both_arms():
    relations = _relations(
        f"SELECT name FROM {ASTRONAUTS} UNION ALL SELECT Mission FROM {MISSIONS}"
    )
    assert relations == [ASTRONAUTS, MISSIONS]


def test_a_statement_with_no_from_clause_records_nothing():
    """`$no_table` is a planner artifact, not a relation anyone named."""
    assert _relations("SELECT 1 + 1") == []


def test_explain_records_nothing():
    """EXPLAIN plans and describes. It reads nothing, bills nothing, touches nothing."""
    telemetry = _run(f"EXPLAIN SELECT * FROM {ASTRONAUTS}")
    assert sorted(telemetry.relations) == []
    assert telemetry.billing_bytes == 0


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT * FROM {ASTRONAUTS}",
        f"SELECT name FROM {ASTRONAUTS} WHERE name IN (SELECT Mission FROM {MISSIONS})",
        f"SELECT a.name FROM {ASTRONAUTS} AS a INNER JOIN {MISSIONS} AS m ON a.name = m.Mission",
        f"WITH c AS (SELECT * FROM {MISSIONS}) SELECT (SELECT COUNT(*) FROM c) + (SELECT COUNT(*) FROM c)",
        "SELECT 1 + 1",
        f"EXPLAIN SELECT * FROM {ASTRONAUTS}",
    ],
)
def test_bytes_and_relations_never_disagree(sql):
    """The invariant the shared scan walk exists to guarantee.

    Bytes without relations would attribute volume to nothing; relations
    without bytes would report a table as read when the bill says it was not.
    Either way the two halves of one event contradict each other and a consumer
    has no way to tell which half is wrong.
    """
    telemetry = _run(sql)
    assert bool(telemetry.relations) == bool(telemetry.billing_bytes), (
        f"relations={sorted(telemetry.relations)} but "
        f"billing_bytes={telemetry.billing_bytes} for: {sql}"
    )


def test_a_batch_accumulates_relations_across_its_statements():
    """One event is emitted per execute(), so the batch's relations must union.

    `billing_bytes` is increased rather than assigned for exactly this reason.
    Assigning the relations would report the last statement's tables alongside
    every statement's bytes.
    """
    telemetry = _run(f"SELECT * FROM {ASTRONAUTS}; SELECT * FROM {MISSIONS};")
    assert sorted(telemetry.relations) == [ASTRONAUTS, MISSIONS]


def test_a_batch_scopes_each_event_to_its_own_query_text(monkeypatch):
    """The two events have different scopes, and each must match its own `query`.

    QUERY_EXECUTION is emitted once PER STATEMENT and carries that statement's
    text, so it carries that statement's relations. DATA_PROCESSED_BYTES is
    emitted once per execute() and carries the whole batch's text and the whole
    batch's bytes, so it carries the union. Getting this wrong makes the second
    statement of `A; B;` report that it read A.
    """
    from opteryx import query_session

    captured = []
    monkeypatch.setattr(
        query_session, "write_billing_event", lambda **kwargs: captured.append(kwargs)
    )

    session = opteryx.session()
    for _ in session.execute_to_morsels(
        f"SELECT * FROM {ASTRONAUTS}; SELECT * FROM {MISSIONS};"
    ):
        pass

    per_statement = [
        event["event_details"]
        for event in captured
        if event["billing_event"] == BillingEventType.QUERY_EXECUTION
    ]
    assert [d["relations"] for d in per_statement] == [[ASTRONAUTS], [MISSIONS]]

    batch = [
        event["event_details"]
        for event in captured
        if event["billing_event"] == BillingEventType.DATA_PROCESSED_BYTES
    ]
    assert len(batch) == 1, "one DATA_PROCESSED_BYTES event per execute()"
    assert batch[0]["relations"] == [ASTRONAUTS, MISSIONS]


def test_relations_are_not_in_the_public_telemetry_dict():
    """`as_dict()` is materialised into API responses; a set does not belong there.

    The billing event reads the relations off the telemetry object directly.
    """
    session = opteryx.session()
    for _ in session.execute_to_morsels(f"SELECT * FROM {ASTRONAUTS}"):
        pass
    # Through `Session.telemetry`, which closes the timing window - calling
    # `as_dict()` on the readings directly is refused.
    assert "relations" not in session.telemetry


def test_the_event_carries_the_relations(monkeypatch):
    """End to end: what actually lands on the billing event.

    Patched at `query_session`'s import site rather than in the billing module,
    because that is the name the session calls.
    """
    from opteryx import query_session

    captured = []
    monkeypatch.setattr(
        query_session, "write_billing_event", lambda **kwargs: captured.append(kwargs)
    )

    session = opteryx.session()
    for _ in session.execute_to_morsels(f"SELECT * FROM {ASTRONAUTS}"):
        pass

    assert captured, "no billing event was emitted"
    for event in captured:
        details = event["event_details"]
        assert details["relations"] == [ASTRONAUTS], details
        # Sorted and JSON-serialisable: the payload is compared and grouped
        # downstream, and a set would neither serialise nor order stably.
        assert isinstance(details["relations"], list)
