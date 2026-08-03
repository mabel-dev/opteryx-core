# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import pytest
import tempfile
from pathlib import Path

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import DatasetNotFoundError, ReadOnlyConnectorError, UnsupportedSyntaxError


def _setup_workspace(tmp_path):
    """Set up a temporary workspace for testing."""
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def test_create_table_basic(tmp_path):
    """CREATE TABLE with basic columns."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    result = list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    # Verify folder structure
    events_dir = tmp_path / "ws" / "events"
    assert events_dir.exists()
    assert (events_dir / "dataset.json").exists()

    # Verify schema in dataset.json
    with open(events_dir / "dataset.json") as f:
        dataset_info = json.load(f)

    assert len(dataset_info["schema"]["columns"]) == 2
    assert dataset_info["schema"]["columns"][0]["name"] == "id"
    assert dataset_info["schema"]["columns"][0]["type"] == "INTEGER"
    assert dataset_info["schema"]["columns"][1]["name"] == "name"
    assert dataset_info["schema"]["columns"][1]["type"] == "VARCHAR"


def test_create_table_nested_name(tmp_path):
    """CREATE TABLE with nested schema path."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    result = list(session.execute_to_morsels("CREATE TABLE ws.a.b.events (id BIGINT)"))

    # Verify nested folder structure
    events_dir = tmp_path / "ws" / "a" / "b" / "events"
    assert events_dir.exists()
    assert (events_dir / "dataset.json").exists()


def test_create_table_if_not_exists(tmp_path):
    """CREATE TABLE IF NOT EXISTS allows duplicate creation."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # First create should succeed
    list(session.execute_to_morsels("CREATE TABLE ws.users (id BIGINT, name VARCHAR)"))

    # Second create with IF NOT EXISTS should succeed silently
    result = list(session.execute_to_morsels("CREATE TABLE IF NOT EXISTS ws.users (id BIGINT, name VARCHAR)"))
    assert result is not None

    # Second create without IF NOT EXISTS should raise
    with pytest.raises(ValueError, match="relation already exists"):
        list(session.execute_to_morsels("CREATE TABLE ws.users (id BIGINT, name VARCHAR)"))


def test_create_table_zero_columns_rejected(tmp_path):
    """CREATE TABLE with no columns is rejected."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Depending on whether sqloxide accepts empty column lists,
    # we may get a parser error or our UnsupportedSyntaxError.
    # Accept any exception here.
    with pytest.raises(Exception):
        list(session.execute_to_morsels("CREATE TABLE ws.x ()"))


def test_create_table_unsupported_type(tmp_path):
    """CREATE TABLE with unsupported type is rejected."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="unsupported column type"):
        list(session.execute_to_morsels("CREATE TABLE ws.x (a JSON)"))


def test_create_table_readonly_connector_rejected(tmp_path):
    """CREATE TABLE on read-only connector raises ReadOnlyConnectorError."""
    # Do not register ws; use filesystem path which is read-only
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support CREATE TABLE"):
        list(session.execute_to_morsels("CREATE TABLE somefile.foo (a BIGINT)"))


def test_create_table_requires_writer_or_owner(tmp_path):
    """Plain CREATE TABLE (column-defs form) must be permission-checked, same
    as CTAS - a reader-only session cannot create a brand-new relation."""
    _setup_workspace(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])
    with pytest.raises(PermissionError, match="permission to create table"):
        list(reader.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))
    assert not (tmp_path / "ws" / "events").exists()


def test_create_table_allowed_for_writer(tmp_path):
    _setup_workspace(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])
    list(writer.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))
    assert (tmp_path / "ws" / "events" / "dataset.json").exists()


def test_drop_table_removes_folder(tmp_path):
    """DROP TABLE removes the table folder."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table
    list(session.execute_to_morsels("CREATE TABLE ws.products (id BIGINT, name VARCHAR)"))
    events_dir = tmp_path / "ws" / "products"
    assert events_dir.exists()

    # Drop table
    list(session.execute_to_morsels("DROP TABLE ws.products"))
    assert not events_dir.exists()


def test_drop_table_if_exists(tmp_path):
    """DROP TABLE IF EXISTS succeeds for non-existent table."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Drop non-existent table with IF EXISTS should succeed
    list(session.execute_to_morsels("DROP TABLE IF EXISTS ws.nonexistent"))

    # Drop non-existent table without IF EXISTS should raise
    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("DROP TABLE ws.nonexistent"))


def test_drop_table_multiple(tmp_path):
    """DROP TABLE can drop multiple tables."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create two tables
    list(session.execute_to_morsels("CREATE TABLE ws.a (id BIGINT)"))
    list(session.execute_to_morsels("CREATE TABLE ws.b (id BIGINT)"))

    assert (tmp_path / "ws" / "a").exists()
    assert (tmp_path / "ws" / "b").exists()

    # Drop both tables
    list(session.execute_to_morsels("DROP TABLE ws.a, ws.b"))

    assert not (tmp_path / "ws" / "a").exists()
    assert not (tmp_path / "ws" / "b").exists()


def test_drop_table_readonly_rejected(tmp_path):
    """DROP TABLE on read-only connector raises ReadOnlyConnectorError."""
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support DROP TABLE"):
        list(session.execute_to_morsels("DROP TABLE somefile.foo"))


def test_alter_table_cluster_by_not_implemented_on_local_store(tmp_path):
    """LocalStoreConnector has no catalog to persist a sort order in, so CLUSTER
    BY is rejected explicitly rather than silently doing nothing."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    with pytest.raises(NotImplementedError, match="does not support ALTER TABLE"):
        list(session.execute_to_morsels("ALTER TABLE ws.events CLUSTER BY (name)"))


def test_alter_table_cluster_by_missing_table(tmp_path):
    """ALTER TABLE CLUSTER BY on a non-existent table raises, IF EXISTS suppresses it."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("ALTER TABLE ws.nonexistent CLUSTER BY (name)"))

    # IF EXISTS on a missing table succeeds without reaching the connector
    # (so it never hits the NotImplementedError a real cluster attempt would).
    list(session.execute_to_morsels("ALTER TABLE IF EXISTS ws.nonexistent CLUSTER BY (name)"))


def test_alter_table_cluster_by_readonly_rejected(tmp_path):
    """ALTER TABLE ... CLUSTER BY on a read-only connector raises ReadOnlyConnectorError."""
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support ALTER TABLE"):
        list(session.execute_to_morsels("ALTER TABLE somefile.foo CLUSTER BY (name)"))


def test_alter_table_unsupported_operation_rejected(tmp_path):
    """Only CLUSTER BY and RENAME TO are supported; any other ALTER TABLE
    operation is rejected at plan time rather than silently mishandled."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("ALTER TABLE ws.events ADD COLUMN extra VARCHAR"))


def test_alter_table_rename_not_implemented_on_local_store(tmp_path):
    """LocalStoreConnector has no rename primitive, so RENAME TO is rejected
    explicitly rather than silently doing nothing."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    with pytest.raises(NotImplementedError):
        list(session.execute_to_morsels("ALTER TABLE ws.events RENAME TO ws.renamed"))


def test_alter_table_rename_missing_table(tmp_path):
    """RENAME TO on a non-existent table raises, IF EXISTS suppresses it."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("ALTER TABLE ws.nonexistent RENAME TO ws.renamed"))

    # IF EXISTS on a missing table succeeds without reaching the connector
    # (so it never hits the NotImplementedError a real rename would).
    list(session.execute_to_morsels("ALTER TABLE IF EXISTS ws.nonexistent RENAME TO ws.renamed"))


def test_alter_table_rename_onto_existing_rejected(tmp_path):
    """A rename must not absorb an existing relation - that would destroy the
    target's data with no DROP anywhere in the statement."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT)"))
    list(session.execute_to_morsels("CREATE TABLE ws.other (id BIGINT)"))

    with pytest.raises(ValueError, match="relation already exists"):
        list(session.execute_to_morsels("ALTER TABLE ws.events RENAME TO ws.other"))


def test_alter_table_rename_across_workspaces_rejected(tmp_path):
    """A rename may move a relation between collections but never between
    workspaces - that is a copy, not a rename."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="between workspaces"):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.collection.events RENAME TO other.collection.events"
            )
        )


def test_alter_table_rename_to_same_name_rejected(tmp_path):
    """Renaming to the current name is a no-op written as a mutation - rejected
    rather than reported as a successful rename that changed nothing."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="same as the source"):
        list(session.execute_to_morsels("ALTER TABLE ws.events RENAME TO ws.events"))


def test_alter_table_rename_readonly_rejected(tmp_path):
    """RENAME TO on a read-only connector raises ReadOnlyConnectorError."""
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support ALTER TABLE"):
        list(session.execute_to_morsels("ALTER TABLE somefile.foo RENAME TO somefile.bar"))


def test_alter_workspace_unknown_property_rejected(tmp_path):
    """An unrecognised property is rejected at plan time - a typo must not be
    written through to the catalog as a new, meaningless property."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="not a settable workspace property"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET delete_protecton TO OFF"))


def test_alter_workspace_non_boolean_value_rejected(tmp_path):
    """delete_protection is boolean; a non-boolean value is rejected rather than
    coerced into something arbitrary."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="is a boolean"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET delete_protection TO 7"))


def test_alter_workspace_rejects_qualified_name(tmp_path):
    """ALTER WORKSPACE names a workspace, not a relation inside one."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="not a relation within one"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws.collection SET delete_protection TO OFF"))


def test_alter_workspace_not_implemented_on_local_store(tmp_path):
    """LocalStoreConnector has no catalog to persist workspace properties in, so
    it is rejected explicitly rather than silently doing nothing."""
    _setup_workspace(tmp_path)
    session = opteryx.session(access_policies=[{"pattern": "ws", "role": "owner"}])

    with pytest.raises(NotImplementedError):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET delete_protection TO OFF"))


def test_create_view_basic(tmp_path):
    """CREATE VIEW stores the view definition."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))
    list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT * FROM ws.events"))

    view_path = tmp_path / "ws" / "events_view" / "view.json"
    assert view_path.exists()

    with open(view_path) as f:
        view_info = json.load(f)
    assert view_info["name"] == "ws.events_view"
    assert "events" in view_info["statement"]


def test_create_view_duplicate_rejected(tmp_path):
    """CREATE VIEW without OR REPLACE rejects an existing view name."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT)"))
    list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT * FROM ws.events"))

    with pytest.raises(ValueError, match="view already exists"):
        list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT * FROM ws.events"))


def test_create_or_replace_view(tmp_path):
    """CREATE OR REPLACE VIEW overwrites an existing view definition."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))
    list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT id FROM ws.events"))
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE VIEW ws.events_view AS SELECT id, name FROM ws.events"
        )
    )

    view_path = tmp_path / "ws" / "events_view" / "view.json"
    with open(view_path) as f:
        view_info = json.load(f)
    assert "name" in view_info["statement"]


def test_show_create_view(tmp_path):
    """SHOW CREATE VIEW returns the stored view statement."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT)"))
    list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT * FROM ws.events"))

    morsels = list(session.execute_to_morsels("SHOW CREATE VIEW ws.events_view"))
    assert len(morsels) == 1


def test_show_create_view_missing_raises(tmp_path):
    """SHOW CREATE VIEW on a non-existent view raises DatasetNotFoundError."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("SHOW CREATE VIEW ws.nonexistent"))


def test_drop_view_still_works(tmp_path):
    """DROP VIEW removes the view definition."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT)"))
    list(session.execute_to_morsels("CREATE VIEW ws.events_view AS SELECT * FROM ws.events"))

    view_dir = tmp_path / "ws" / "events_view"
    assert (view_dir / "view.json").exists()

    list(session.execute_to_morsels("DROP VIEW ws.events_view"))
    assert not view_dir.exists()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("DROP VIEW ws.events_view"))


def test_truncate_creates_empty_snapshot(tmp_path):
    """TRUNCATE TABLE clears the table."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create a table (no data, so we just verify structure)
    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    # Truncate should succeed
    result = list(session.execute_to_morsels("TRUNCATE TABLE ws.events"))

    # Verify folder still exists
    events_dir = tmp_path / "ws" / "events"
    assert events_dir.exists()
    assert (events_dir / "dataset.json").exists()

    # The current snapshot should have no files
    with open(events_dir / "dataset.json") as f:
        dataset_info = json.load(f)

    # Check that there's a snapshot entry (may be null)
    # The exact structure depends on LocalStoreConnector implementation
    # For now, we just verify the table still exists
    assert "relation_name" in dataset_info
    assert "ws.events" in dataset_info["relation_name"]


def test_truncate_missing_relation_raises(tmp_path):
    """TRUNCATE on missing table raises DatasetNotFoundError."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("TRUNCATE TABLE ws.nonexistent"))


def test_create_table_with_not_null(tmp_path):
    """CREATE TABLE respects NOT NULL constraints."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.strict (id BIGINT NOT NULL, name VARCHAR)"))

    # Verify schema
    events_dir = tmp_path / "ws" / "strict"
    with open(events_dir / "dataset.json") as f:
        dataset_info = json.load(f)

    # id should be NOT NULL
    id_col = next((c for c in dataset_info["schema"]["columns"] if c["name"] == "id"), None)
    assert id_col is not None
    assert id_col.get("nullable", True) == False

    # name should be nullable
    name_col = next((c for c in dataset_info["schema"]["columns"] if c["name"] == "name"), None)
    assert name_col is not None
    assert name_col.get("nullable", True) == True


def test_create_table_all_types(tmp_path):
    """CREATE TABLE supports all mapped types."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("""
        CREATE TABLE ws.all_types (
            int_col BIGINT,
            varchar_col VARCHAR,
            double_col DOUBLE,
            bool_col BOOLEAN,
            date_col DATE,
            timestamp_col TIMESTAMP,
            blob_col BLOB
        )
    """))

    # Verify all columns exist
    types_dir = tmp_path / "ws" / "all_types"
    with open(types_dir / "dataset.json") as f:
        dataset_info = json.load(f)

    expected_cols = {
        "int_col": "INTEGER",
        "varchar_col": "VARCHAR",
        "double_col": "FLOAT",
        "bool_col": "BOOLEAN",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "blob_col": "VARBINARY",
    }

    for col in dataset_info["schema"]["columns"]:
        assert col["name"] in expected_cols
        assert col["type"] == expected_cols[col["name"]]


_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
_WRITER_POLICY = [{"pattern": "*", "role": "writer"}]


def _seed_relations(tmp_path):
    """Create a table and a view as an owner, returning an owner session."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))
    list(owner.execute_to_morsels("CREATE VIEW ws.v AS SELECT * FROM ws.t"))
    return owner


def test_drop_table_requires_owner(tmp_path):
    """A writer may not DROP TABLE - only an owner may."""
    _seed_relations(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)

    with pytest.raises(PermissionError, match="permission to drop table"):
        list(writer.execute_to_morsels("DROP TABLE ws.t"))

    assert (tmp_path / "ws" / "t" / "dataset.json").exists()


def test_drop_view_requires_owner(tmp_path):
    """A writer may not DROP VIEW - only an owner may."""
    _seed_relations(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)

    with pytest.raises(PermissionError, match="permission to drop view"):
        list(writer.execute_to_morsels("DROP VIEW ws.v"))

    assert (tmp_path / "ws" / "v" / "view.json").exists()


def test_owner_can_drop_table_and_view(tmp_path):
    """An owner may DROP both tables and views."""
    owner = _seed_relations(tmp_path)

    list(owner.execute_to_morsels("DROP VIEW ws.v"))
    list(owner.execute_to_morsels("DROP TABLE ws.t"))

    assert not (tmp_path / "ws" / "v" / "view.json").exists()
    assert not (tmp_path / "ws" / "t").exists()


def test_truncate_requires_writer_or_owner(tmp_path):
    owner = _seed_relations(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])
    with pytest.raises(PermissionError, match="permission to truncate table"):
        list(reader.execute_to_morsels("TRUNCATE TABLE ws.t"))


def test_insert_into_existing_requires_writer_or_owner(tmp_path):
    owner = _seed_relations(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])
    with pytest.raises(PermissionError, match="permission to insert into"):
        list(reader.execute_to_morsels("INSERT INTO ws.t VALUES (1)"))


def test_writer_retains_non_drop_ddl(tmp_path):
    """Restricting DROP to owners does not restrict the WRITE tier."""
    _seed_relations(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)

    list(writer.execute_to_morsels("CREATE VIEW ws.v2 AS SELECT * FROM ws.t"))

    assert (tmp_path / "ws" / "v2" / "view.json").exists()


def test_view_owner_is_session_user(tmp_path):
    """A created view records the session user, not a fixed literal."""
    _setup_workspace(tmp_path)
    owner_alice = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(owner_alice.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))
    list(owner_alice.execute_to_morsels("CREATE VIEW ws.va AS SELECT * FROM ws.t"))

    owner_bob = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    list(owner_bob.execute_to_morsels("CREATE VIEW ws.vb AS SELECT * FROM ws.t"))

    with open(tmp_path / "ws" / "va" / "view.json") as f:
        assert json.load(f)["owner"] == "alice"
    with open(tmp_path / "ws" / "vb" / "view.json") as f:
        assert json.load(f)["owner"] == "bob"


def test_view_owner_none_when_unauthenticated(tmp_path):
    """No session user means no attribution - not an invented one."""
    _setup_workspace(tmp_path)
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))
    list(session.execute_to_morsels("CREATE VIEW ws.v AS SELECT * FROM ws.t"))

    with open(tmp_path / "ws" / "v" / "view.json") as f:
        assert json.load(f)["owner"] is None


# --- Statements that reached their operator with no permission check at all.
# Each of these node types had no binder visitor, and BinderVisitor.visit_node
# used to pass an unvisited node straight through. See NO_BINDER_REQUIRED.


def test_analyze_table_requires_owner(tmp_path):
    """A writer may not ANALYZE - it rewrites the metadata the optimizer plans
    from, the same tier as ALTER TABLE ... CLUSTER BY."""
    _seed_relations(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)

    with pytest.raises(PermissionError, match="permission to analyze table"):
        list(writer.execute_to_morsels("ANALYZE TABLE ws.t"))


def test_analyze_table_reader_rejected(tmp_path):
    _seed_relations(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])

    with pytest.raises(PermissionError, match="permission to analyze table"):
        list(reader.execute_to_morsels("ANALYZE TABLE ws.t"))


def test_drop_statistics_requires_owner(tmp_path):
    """DROP STATISTICS destroys what ANALYZE builds, so it is gated the same."""
    _seed_relations(tmp_path)
    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)

    with pytest.raises(PermissionError, match="permission to drop statistics on table"):
        list(writer.execute_to_morsels("DROP STATISTICS ON ws.t"))


def test_show_create_view_requires_read(tmp_path):
    """A view's body names the relations it reads; showing it is a read of the
    view, and a caller with no grant on it may not."""
    _seed_relations(tmp_path)
    outsider = opteryx.session(
        user="oscar", access_policies=[{"pattern": "other.*", "role": "owner"}]
    )

    with pytest.raises(PermissionError, match="permission to read view"):
        list(outsider.execute_to_morsels("SHOW CREATE VIEW ws.v"))


def test_show_create_view_allowed_for_reader(tmp_path):
    """READ is the tier - a reader can see the definition."""
    _seed_relations(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])

    result = list(reader.execute_to_morsels("SHOW CREATE VIEW ws.v"))
    assert len(result) == 1


# --- Syntax the parser accepted and the engine silently ignored.


def test_drop_cascade_rejected(tmp_path):
    """CASCADE was parsed and discarded, so a DROP COLLECTION ... CASCADE read
    as a successful recursive drop that never happened."""
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    for statement in (
        "DROP TABLE ws.t CASCADE",
        "DROP VIEW ws.v CASCADE",
        "DROP COLLECTION ws.c CASCADE",
    ):
        with pytest.raises(UnsupportedSyntaxError, match="CASCADE"):
            list(owner.execute_to_morsels(statement))


def test_drop_restrict_rejected(tmp_path):
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="RESTRICT"):
        list(owner.execute_to_morsels("DROP TABLE ws.t RESTRICT"))


def test_materialized_view_rejected(tmp_path):
    """Opteryx has no materialization; this used to create an ordinary view."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))

    with pytest.raises(UnsupportedSyntaxError, match="materialized views"):
        list(owner.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.t"))


def test_show_create_table_rejected_at_plan_time(tmp_path):
    """Rejected by name, rather than as 'Invalid SHOW statement' at execution."""
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW CREATE TABLE"):
        list(owner.execute_to_morsels("SHOW CREATE TABLE ws.t"))


def test_comment_on_column_rejected(tmp_path):
    """COMMENT ON COLUMN used to fail as a missing dataset named after the column."""
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="COMMENT ON COLUMN"):
        list(owner.execute_to_morsels("COMMENT ON COLUMN ws.t.id IS 'the id'"))


# --- CREATE COLLECTION. The counterpart DROP COLLECTION had none of.


def test_create_collection_not_implemented_on_local_store(tmp_path):
    """LocalStoreConnector has no catalog to register a collection in, so this is
    rejected explicitly rather than silently doing nothing."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(NotImplementedError, match="does not support CREATE COLLECTION"):
        list(owner.execute_to_morsels("CREATE COLLECTION ws.staging"))


def test_create_collection_requires_two_part_name(tmp_path):
    """A collection is always `<workspace>.<collection>`. A bare name would
    resolve to some default workspace and create it somewhere unnamed."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    for name in ("staging", "ws.a.b"):
        with pytest.raises(UnsupportedSyntaxError, match="<workspace>.<collection>"):
            list(owner.execute_to_morsels(f"CREATE COLLECTION {name}"))


def test_create_collection_rejected_for_reader(tmp_path):
    """Checked at bind time, before the connector is asked to do anything."""
    _setup_workspace(tmp_path)
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])

    with pytest.raises(PermissionError, match="permission to create collection"):
        list(reader.execute_to_morsels("CREATE COLLECTION ws.staging"))
