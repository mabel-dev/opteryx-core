# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""What a statement read, recorded for a copy of its RESULT made afterwards.

worker.opteryx's SAVE RESULTS copies a finished SELECT's result into a dataset.
That copy is the CTAS of the SELECT with the rows taken from storage instead of
a re-run, so it has to be refused exactly when the CTAS would be and receipted
exactly as the CTAS would be. The engine is the only thing that knows what a
statement read, so it records it (`QuerySession.reads`) and exposes the
binder's own filter and wording (`egress_sources`, `egress_refusal_message`).

The load-bearing test here is the parity one: the egress hook sees the SAME
call from "SELECT, then judge its recorded reads" as from the CTAS.
"""

import pytest

from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors.capabilities.writable import EgressRefusal
from opteryx.exceptions import EgressRestrictedError
from opteryx.planner.binder.relation import egress_refusal_message
from opteryx.planner.binder.relation import egress_sources

_OWNER = [{"pattern": "*", "role": "owner"}]


@pytest.fixture
def workspaces(tmp_path, monkeypatch):
    """Two local-store workspaces whose egress hook records what it is asked."""
    import opteryx
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    calls = []

    def record(self, target_relation, source_relations, secured=None):
        calls.append((target_relation, list(source_relations), secured))
        return []

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", record)
    register_workspace("src_ws", LocalStoreConnector, store_root=str(tmp_path / "a"))
    register_workspace("dst_ws", LocalStoreConnector, store_root=str(tmp_path / "b"))

    session = opteryx.session(user="alice", access_policies=_OWNER)
    list(session.execute_to_morsels("CREATE TABLE src_ws.src (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO src_ws.src VALUES (1), (2), (3)"))
    list(session.execute_to_morsels("CREATE VIEW src_ws.v AS SELECT a FROM src_ws.src WHERE a > 1"))
    calls.clear()
    return calls


def _reads(sql):
    import opteryx

    session = opteryx.session(user="alice", access_policies=_OWNER)
    list(session.execute_to_morsels(sql))
    return session, session.reads


def _judge_as_a_save(target, reads):
    """What worker.opteryx does with a source job's recorded reads."""
    sources = egress_sources(reads["scanned_relations"])
    if not sources:
        return None
    gateway = connector_factory(target, telemetry=None)
    return gateway.egress_verdict(target, sources, secured=None)


# --- what is recorded ------------------------------------------------------


def test_a_plain_select_records_its_relation(workspaces):
    _session, reads = _reads("SELECT a FROM src_ws.src")

    assert reads["scanned_relations"] == ["src_ws.src"]
    assert reads["read_sources"] == []  # the local store is not a catalog relation


def test_a_view_records_its_base_table_not_its_name(workspaces):
    """The egress question is about where the ROWS live. A view is SQL text;
    its base table is what was copied."""
    _session, reads = _reads("SELECT a FROM src_ws.v")

    assert reads["scanned_relations"] == ["src_ws.src"]


def test_a_count_answered_from_statistics_still_records_its_relation(workspaces):
    """The optimizer answers this through `$one_row`, so the billing
    `relations` reading is empty. The count is still a copy of the source's
    data, and the CTAS of it is refused - so the reads come off the bound plan."""
    session, reads = _reads("SELECT COUNT(*) FROM src_ws.src")

    assert reads["scanned_relations"] == ["src_ws.src"]
    assert "src_ws.src" not in session._telemetry.relations


def test_a_subquery_relation_is_recorded(workspaces):
    _session, reads = _reads("SELECT id FROM $planets WHERE id IN (SELECT a FROM src_ws.v)")

    assert reads["scanned_relations"] == ["$planets", "src_ws.src"]


def test_a_batch_accumulates_what_every_statement_read(workspaces):
    """One execute(), one result, one answer about what fed it. Over-reporting
    an earlier statement's read errs toward refusing a copy, never allowing one."""
    _session, reads = _reads("SELECT * FROM $planets; SELECT a FROM src_ws.src")

    assert reads["scanned_relations"] == ["$planets", "src_ws.src"]


def test_the_reads_are_not_in_the_api_telemetry(workspaces):
    session, _reads_ = _reads("SELECT a FROM src_ws.src")

    as_dict = session.telemetry
    assert "scanned_relations" not in as_dict
    assert "read_sources" not in as_dict


def test_the_reads_are_copies(workspaces):
    session, reads = _reads("SELECT a FROM src_ws.src")

    reads["scanned_relations"].append("dst_ws.forged")

    assert session.reads["scanned_relations"] == ["src_ws.src"]


# --- the egress filter -----------------------------------------------------


def test_non_catalog_sources_are_filtered_out(workspaces):
    """$planets, information_schema and the planner's `$one_row` belong to no
    workspace, so they cannot leave one."""
    assert egress_sources(["$planets", "$one_row", "src_ws.information_schema.tables"]) == []
    assert egress_sources(["$planets", "src_ws.src"]) == ["src_ws.src"]


# --- parity with the CTAS --------------------------------------------------


@pytest.mark.parametrize(
    "select",
    [
        "SELECT a FROM src_ws.src",
        "SELECT a FROM src_ws.v",
        "SELECT COUNT(*) FROM src_ws.src",
        "SELECT id FROM $planets WHERE id IN (SELECT a FROM src_ws.src)",
        "SELECT id FROM $planets",
    ],
)
def test_judging_the_recorded_reads_asks_what_the_ctas_asks(workspaces, select):
    """THE contract SAVE RESULTS depends on. The hook sees the same
    (target, sources, secured) from a save of the SELECT's result as from the
    CTAS of that SELECT - including not being asked at all when the SELECT
    read nothing that belongs to a workspace."""
    import opteryx

    _session, reads = _reads(select)
    _judge_as_a_save("dst_ws.saved", reads)
    from_save = list(workspaces)
    workspaces.clear()

    session = opteryx.session(user="alice", access_policies=_OWNER)
    list(session.execute_to_morsels(f"CREATE TABLE dst_ws.saved AS {select}"))
    from_ctas = list(workspaces)

    assert from_save == from_ctas


def test_a_save_is_refused_in_the_ctas_own_words(workspaces, monkeypatch):
    import opteryx
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    def refuse(self, target_relation, source_relations, secured=None):
        return [
            EgressRefusal(workspace="alpha", remediation="ALTER WORKSPACE alpha ...", message="a"),
            EgressRefusal(workspace="beta", remediation="ALTER WORKSPACE beta ...", message="b"),
        ]

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", refuse)

    _session, reads = _reads("SELECT a FROM src_ws.src")
    from_save = egress_refusal_message("dst_ws.saved", _judge_as_a_save("dst_ws.saved", reads))

    session = opteryx.session(user="alice", access_policies=_OWNER)
    with pytest.raises(EgressRestrictedError) as exc:
        list(session.execute_to_morsels("CREATE TABLE dst_ws.saved AS SELECT a FROM src_ws.src"))

    assert from_save == str(exc.value)


def test_one_refusal_reads_as_the_store_wrote_it():
    class CatalogRefusal:
        """opteryx_catalog's own refusal: no `.message`, its text is `str()`."""

        workspace = "landing"
        remediation = "clear it"

        def __str__(self):
            return "Cannot write mine.x.y: as the catalog words it"

    assert egress_refusal_message("mine.x.y", [CatalogRefusal()]) == (
        "Cannot write mine.x.y: as the catalog words it"
    )
    assert (
        egress_refusal_message(
            "mine.x.y", [EgressRefusal(workspace="w", remediation="r", message="ONLY THIS")]
        )
        == "ONLY THIS"
    )
