# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""REFRESH MATERIALIZED VIEW, and the refusal of every table modifier on a view.

A materialized view is not a table: its contents are derived from its defining
SELECT, so nothing writes to one directly. REFRESH is the statement that
rebuilds it, and it is the only write allowed to land on one besides the CREATE
that made it.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _setup(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session(user="alice", access_policies=_OWNER_POLICY)


def _rows(morsels):
    out = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        n = len(next(iter(pydict.values()))) if pydict else 0
        out.extend({k: vs[i] for k, vs in pydict.items()} for i in range(n))
    return out


def _seed_view(session):
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (1), (2)"))
    list(
        session.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 0"
        )
    )


# --- REFRESH MATERIALIZED VIEW ------------------------------------------


def test_refresh_rebuilds_the_view_from_its_definition(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)
    assert len(_rows(session.execute_to_morsels("SELECT * FROM ws.mv"))) == 2

    # New rows in the source do not appear until the view is refreshed.
    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (3), (4), (-9)"))
    assert len(_rows(session.execute_to_morsels("SELECT * FROM ws.mv"))) == 2

    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))

    rows = _rows(session.execute_to_morsels("SELECT * FROM ws.mv"))
    # The definition's WHERE is re-applied, so -9 is still excluded.
    assert sorted(r["a"] for r in rows) == [1, 2, 3, 4]


def test_refresh_stamps_its_own_success(tmp_path):
    """A manual refresh is the documented recovery path after a failed one, so
    it has to record that it ran - the worker only ever stamps the
    trigger-fired refreshes, and a failed refresh cannot stamp itself."""
    import json
    import os

    session = _setup(tmp_path)
    _seed_view(session)
    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))

    with open(os.path.join(str(tmp_path), "ws", "mv", "materialized_view.json")) as f:
        record = json.load(f)
    assert record["last-refresh-status"] == "succeeded"
    assert isinstance(record["last-refreshed-at-ms"], int)


def test_refresh_leaves_the_relation_a_materialized_view(tmp_path):
    """A refresh must not quietly demote the view to a plain table - if it did,
    the next refresh would have nothing to refresh from."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))

    from opteryx.connectors import connector_factory

    assert connector_factory("ws.mv", telemetry=None).is_materialized_view("ws.mv")
    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))


def test_refresh_runs_the_current_definition(tmp_path):
    """Redefining a view takes effect on its next refresh - the definition is
    read at plan time, not carried from whenever the refresh was requested."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 1"
        )
    )
    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))

    rows = _rows(session.execute_to_morsels("SELECT * FROM ws.mv"))
    assert sorted(r["a"] for r in rows) == [2]


def test_refresh_rejects_a_plain_table(tmp_path):
    session = _setup(tmp_path)
    list(session.execute_to_morsels("CREATE TABLE ws.plain (a BIGINT)"))

    with pytest.raises(UnsupportedSyntaxError, match="is not a materialized view"):
        list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.plain"))


def test_refresh_of_anything_else_is_named_not_a_parse_error(tmp_path):
    """REFRESH MATERIALIZED VIEW is the only REFRESH statement, and saying so
    beats a generic syntax error several layers from the offending word."""
    session = _setup(tmp_path)

    with pytest.raises(UnsupportedSyntaxError, match="REFRESH MATERIALIZED VIEW"):
        list(session.execute_to_morsels("REFRESH TABLE ws.src"))


class _RecordingStore(LocalStoreConnector):
    """A local store that records the commit message each write carries.

    The local store's snapshots hold neither author nor message (see `_commit`),
    so the message these statements compose can only be observed where it is
    handed over. The catalog connector is where it is actually recorded.
    """

    commits = []

    def insert(self, relation_name, file_entries, author=None, commit_message=None):
        _RecordingStore.commits.append((relation_name, commit_message))
        super().insert(
            relation_name, file_entries, author=author, commit_message=commit_message
        )

    def replace_relation(
        self, relation_name, schema, file_entries, author=None, commit_message=None
    ):
        _RecordingStore.commits.append((relation_name, commit_message))
        super().replace_relation(
            relation_name, schema, file_entries, author=author, commit_message=commit_message
        )


def _setup_recording(tmp_path):
    _RecordingStore.commits = []
    register_workspace("ws", _RecordingStore, store_root=str(tmp_path))
    return opteryx.session(user="alice", access_policies=_OWNER_POLICY)


def test_creating_a_view_names_its_first_write(tmp_path):
    """The snapshot that first fills a view is where its history starts, so it
    says so rather than reading as an anonymous append."""
    session = _setup_recording(tmp_path)
    _seed_view(session)

    assert _RecordingStore.commits[-1] == ("ws.mv", "initial population of materialized view")


def test_a_refresh_names_itself_in_the_commit_message(tmp_path):
    """A view's snapshot history should say a refresh happened, rather than
    reading as a series of anonymous overwrites indistinguishable from someone
    replacing the backing table by hand."""
    session = _setup_recording(tmp_path)
    _seed_view(session)
    _RecordingStore.commits = []

    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW ws.mv"))

    assert _RecordingStore.commits == [("ws.mv", "materialized view refreshed")]


def test_ordinary_table_writes_leave_the_message_to_the_store(tmp_path):
    """Only the statements that own a view have something to add - an INSERT
    and a CTAS pass None so the store describes them however it describes any
    append or replace, rather than every write claiming to be a view's."""
    session = _setup_recording(tmp_path)
    _seed_view(session)
    _RecordingStore.commits = []

    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (5)"))
    list(session.execute_to_morsels("CREATE TABLE ws.plain (a BIGINT)"))
    list(session.execute_to_morsels("CREATE OR REPLACE TABLE ws.plain AS SELECT a FROM ws.src"))

    assert _RecordingStore.commits == [("ws.src", None), ("ws.plain", None)]


# --- a materialized view is not a table ---------------------------------


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE OR REPLACE TABLE ws.mv AS SELECT a FROM ws.src",
        "INSERT INTO ws.mv SELECT a FROM ws.src",
        "INSERT INTO ws.mv VALUES (9)",
        "TRUNCATE TABLE ws.mv",
        "ALTER TABLE ws.mv RENAME TO ws.renamed",
        "ALTER TABLE ws.mv CLUSTER BY (a)",
    ],
    ids=["cortas", "insert-select", "insert-values", "truncate", "rename", "cluster-by"],
)
def test_table_modifiers_are_refused_on_a_materialized_view(tmp_path, statement):
    session = _setup(tmp_path)
    _seed_view(session)

    with pytest.raises(UnsupportedSyntaxError, match="is a materialized view, not a table"):
        list(session.execute_to_morsels(statement))


def test_the_refusal_names_the_statements_that_do_work(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)

    with pytest.raises(UnsupportedSyntaxError, match="REFRESH MATERIALIZED VIEW"):
        list(session.execute_to_morsels("TRUNCATE TABLE ws.mv"))


def test_a_refused_modifier_leaves_the_view_intact(tmp_path):
    """Refused at bind time, so nothing is written and the view still reads."""
    session = _setup(tmp_path)
    _seed_view(session)

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("TRUNCATE TABLE ws.mv"))

    assert len(_rows(session.execute_to_morsels("SELECT * FROM ws.mv"))) == 2


def test_plain_tables_are_unaffected(tmp_path):
    """The guard keys on the target being a view, not on the statement."""
    session = _setup(tmp_path)
    _seed_view(session)
    list(session.execute_to_morsels("CREATE TABLE ws.plain (a BIGINT)"))

    list(session.execute_to_morsels("INSERT INTO ws.plain VALUES (7)"))
    list(session.execute_to_morsels("TRUNCATE TABLE ws.plain"))
    list(session.execute_to_morsels("CREATE OR REPLACE TABLE ws.plain AS SELECT a FROM ws.src"))

    assert len(_rows(session.execute_to_morsels("SELECT * FROM ws.plain"))) == 2


def test_creating_a_view_over_an_existing_view_still_works(tmp_path):
    """CREATE OR REPLACE MATERIALIZED VIEW targets a view by design - it must
    not be caught by the guard that refuses CTAS."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 1"
        )
    )

    assert len(_rows(session.execute_to_morsels("SELECT * FROM ws.mv"))) == 1
