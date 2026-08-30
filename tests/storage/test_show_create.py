# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW CREATE for TABLE, VIEW, MATERIALIZED VIEW and TASK.

A view, a materialized view and a task each kept the statement that defined
them, so showing one is a read. A table did not, so its DDL is reconstructed
from the catalog - and the bar for that reconstruction is that replaying it
rebuilds the same table, which is what the round-trip test here asserts.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import DatasetNotFoundError, UnsupportedSyntaxError
from opteryx.models.create_statement import render_create_table
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity
from opteryx.types import logical_type as _lt


def _setup(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session()


def _run(session, sql):
    return list(session.execute_to_morsels(sql))


def _statement(session, sql):
    for morsel in _run(session, sql):
        return morsel.column("create_statement").to_pylist()[0]
    raise AssertionError(f"{sql} returned no rows")


def _seed(session):
    _run(session, "CREATE TABLE ws.customers (id BIGINT, label VARCHAR)")
    _run(
        session,
        "CREATE TABLE ws.events (id BIGINT NOT NULL, ref BIGINT, amount DECIMAL(10, 2), "
        "at TIMESTAMP, tags ARRAY<VARCHAR>, ip IPV4, small TINYINT, "
        "CONSTRAINT fk FOREIGN KEY (ref) REFERENCES ws.customers (id) NOT ENFORCED)",
    )


def test_show_create_table_renders_columns_constraints_and_types(tmp_path):
    """Every column definition, and every declared relationship.

    The type spelling is `str(ColumnType)` - draken's, and the same one a CAST
    target parses from - so DECIMAL keeps its precision, TIMESTAMP its unit and
    ARRAY its element type rather than all three degrading to a bare name.
    """
    session = _setup(tmp_path)
    _seed(session)

    statement = _statement(session, "SHOW CREATE TABLE ws.events")

    assert statement.startswith("CREATE TABLE ws.events (")
    assert "id INT64 NOT NULL" in statement
    assert "ref INT64," in statement
    assert "amount DECIMAL(10, 2)" in statement
    assert "at TIMESTAMP[us]" in statement
    assert "tags ARRAY<VARCHAR>" in statement
    assert "ip IPV4" in statement
    assert (
        "CONSTRAINT fk FOREIGN KEY (ref) REFERENCES ws.customers (id) NOT ENFORCED"
        in statement
    )
    # NOT NULL is rendered where it applies and nowhere else.
    assert statement.count("NOT NULL") == 1


def test_show_create_table_round_trips(tmp_path):
    """The bar for a reconstruction: replaying it rebuilds the same table.

    Dropped and recreated from its own rendered DDL, the table renders
    identically the second time - so nothing the CREATE can express was lost in
    either direction.
    """
    session = _setup(tmp_path)
    _seed(session)

    original = _statement(session, "SHOW CREATE TABLE ws.events")

    _run(session, "DROP TABLE ws.events")
    for part in (part for part in original.split(";") if part.strip()):
        _run(session, part)

    assert _statement(session, "SHOW CREATE TABLE ws.events") == original


def test_show_create_table_never_renders_a_default(tmp_path):
    """A DEFAULT is a backfill value, not stored state.

    `ADD COLUMN ... DEFAULT` writes into the rows that already existed and is
    consulted by nothing afterwards, so there is no default to show - and
    emitting one would assert a constraint the engine does not have.
    """
    session = _setup(tmp_path)
    _run(session, "CREATE TABLE ws.events (id BIGINT)")
    _run(session, "ALTER TABLE ws.events ADD COLUMN label VARCHAR DEFAULT 'x'")

    statement = _statement(session, "SHOW CREATE TABLE ws.events")

    assert "label VARCHAR" in statement
    assert "DEFAULT" not in statement


def test_show_create_table_renders_clustering_as_a_second_statement():
    """CREATE TABLE has no CLUSTER BY clause, so a clustered table is a script.

    Exercised on the renderer directly: clustering is stored by the catalog
    connector, and the local store cannot set one - `set_cluster_by` raises for
    it, which is exactly why `cluster_by_columns` may answer "none" there.
    """
    schema = RelationSchema(
        name="ws.events",
        columns=[
            SchemaColumn(
                name="id",
                column_type=_lt.INT64,
                identity=mint_column_identity("ws.events", "id"),
            ),
            SchemaColumn(
                name="label",
                column_type=_lt.VARCHAR,
                identity=mint_column_identity("ws.events", "label"),
            ),
        ],
    )

    statement = render_create_table("ws.events", schema, cluster_columns=["id", "label"])

    create, alter = statement.split(";")[0].strip(), statement.split(";")[1].strip()
    assert create.startswith("CREATE TABLE ws.events (")
    assert alter == "ALTER TABLE ws.events CLUSTER BY (id, label)"
    # And nothing is emitted when there is no clustering to emit.
    assert ";" not in render_create_table("ws.events", schema).rstrip(";")


def test_show_create_view_returns_the_stored_body(tmp_path):
    """A view really did keep its statement - this is a read, not a rebuild."""
    session = _setup(tmp_path)
    _seed(session)
    _run(session, "CREATE VIEW ws.v AS SELECT id FROM ws.events")

    statement = _statement(session, "SHOW CREATE VIEW ws.v")

    assert statement.startswith("CREATE VIEW ws.v AS")
    assert "SELECT id FROM ws.events" in statement


def test_show_create_materialized_view_returns_the_defining_query(tmp_path):
    """An MV's defining SELECT is registered so a refresh can re-run it.

    That is what makes this a read like a view's, and not a reconstruction like
    a table's, even though an MV's storage is an ordinary backing table.
    """
    session = _setup(tmp_path)
    _run(session, "CREATE TABLE ws.src (a BIGINT)")
    _run(session, "INSERT INTO ws.src VALUES (1), (2)")
    _run(session, "CREATE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src")

    statement = _statement(session, "SHOW CREATE MATERIALIZED VIEW ws.mv")

    assert statement.startswith("CREATE MATERIALIZED VIEW ws.mv AS")
    assert "ws.src" in statement


def test_show_create_table_refuses_a_materialized_view(tmp_path):
    """An MV's backing store is a relation in every other respect.

    Left alone, SHOW CREATE TABLE would describe one as a table and produce a
    CREATE that recreates the storage and loses the view.
    """
    session = _setup(tmp_path)
    _run(session, "CREATE TABLE ws.src (a BIGINT)")
    _run(session, "INSERT INTO ws.src VALUES (1), (2)")
    _run(session, "CREATE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src")

    with pytest.raises(UnsupportedSyntaxError):
        _run(session, "SHOW CREATE TABLE ws.mv")


def test_show_create_task_returns_the_statement_it_runs(tmp_path):
    """A task IS a statement, so showing one shows that statement.

    The ON clause is not rendered: it creates a trigger rather than belonging
    to the task, and triggers are shown by SHOW TRIGGERS FOR.
    """
    session = _setup(tmp_path)
    _seed(session)
    _run(session, "CREATE TASK ws.k ON ws.events AS SELECT id FROM ws.events")

    statement = _statement(session, "SHOW CREATE TASK ws.k")

    assert statement.startswith("CREATE TASK ws.k AS")
    assert "SELECT id FROM ws.events" in statement
    assert " ON " not in statement.split("AS")[0]


def test_show_create_refuses_a_name_of_the_wrong_kind(tmp_path):
    """One namespace, four object types: naming the wrong one is not found.

    Silently answering for whatever the name happens to hold would report a
    view's body as a task's, which is a wrong answer rather than a missing one.
    """
    session = _setup(tmp_path)
    _seed(session)
    _run(session, "CREATE VIEW ws.v AS SELECT id FROM ws.events")
    _run(session, "CREATE TASK ws.k AS SELECT 1 AS a")

    for sql in (
        "SHOW CREATE TABLE ws.v",
        "SHOW CREATE VIEW ws.events",
        "SHOW CREATE TASK ws.v",
        "SHOW CREATE MATERIALIZED VIEW ws.events",
        "SHOW CREATE TABLE ws.k",
    ):
        with pytest.raises(DatasetNotFoundError):
            _run(session, sql)


def test_show_create_rejects_object_types_with_no_definition(tmp_path):
    """sqlparser also parses TRIGGER, FUNCTION, PROCEDURE and EVENT."""
    session = _setup(tmp_path)
    _seed(session)

    for sql in (
        "SHOW CREATE TRIGGER ws.t",
        "SHOW CREATE FUNCTION ws.f",
        "SHOW CREATE PROCEDURE ws.p",
    ):
        with pytest.raises(UnsupportedSyntaxError):
            _run(session, sql)


def test_show_create_names_the_object_it_cannot_parse(tmp_path):
    """MATERIALIZED VIEW and TASK come through pre-parse, so a malformed one is
    refused there by name rather than reaching sqlparser, which knows neither."""
    session = _setup(tmp_path)

    for sql in ("SHOW CREATE TASK", "SHOW CREATE MATERIALIZED VIEW", "SHOW CREATE TASK a b"):
        with pytest.raises(UnsupportedSyntaxError):
            _run(session, sql)
