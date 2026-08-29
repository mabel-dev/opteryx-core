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
from opteryx.exceptions import (
    ColumnNotFoundError,
    DatasetNotFoundError,
    ReadOnlyConnectorError,
    UnsupportedSyntaxError,
)


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
    """Only CLUSTER BY, RENAME TO, the four column operations and an
    informational FOREIGN KEY are supported; any other ALTER TABLE operation is
    rejected at plan time rather than silently mishandled.

    PRIMARY KEY and UNIQUE stay rejected for the reason they always were: the
    engine enforces nothing, so accepting them would imply behaviour it does
    not have. NOT ENFORCED is the one form that escapes that argument, and it
    is tested separately below.
    """
    _setup_workspace(tmp_path)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, name VARCHAR)"))

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("ALTER TABLE ws.events ADD CONSTRAINT pk PRIMARY KEY (id)"))

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("ALTER TABLE ws.events ADD CONSTRAINT u UNIQUE (id)"))


def _setup_two_tables(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.events (id BIGINT, customer_ref BIGINT)"))
    list(session.execute_to_morsels("CREATE TABLE ws.customers (id BIGINT, label VARCHAR)"))
    return session


_INFORMATIONAL_FK = (
    "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
    "FOREIGN KEY (customer_ref) REFERENCES ws.customers (id) NOT ENFORCED"
)


def test_alter_table_add_constraint_not_enforced_is_recorded(tmp_path):
    """The one admitted constraint form is accepted and stored.

    Kept in the relation's own directory beside triggers.json, mirroring the
    catalog's relationships subcollection under the dataset document -- so
    "what relates to this dataset" is a keyed read.
    """
    session = _setup_two_tables(tmp_path)

    list(session.execute_to_morsels(_INFORMATIONAL_FK))

    store = tmp_path / "ws" / "events" / "relationships.jsonl"
    assert store.exists()
    with open(store) as f:
        rows = [json.loads(line) for line in f if line.strip()]

    assert len(rows) == 1
    row = rows[0]
    assert row["kind"] == "maps"
    assert row["constraint_name"] == "events_customer_fk"
    assert row["origin"] == "asserted"
    assert row["cardinality"] == "many_to_one"
    # Names are stored SPLIT, never as a dotted string.
    assert row["from_relation"] == ["ws", "events"]
    assert row["from_column"] == "customer_ref"
    assert row["to_relation"] == ["ws", "customers"]
    assert row["to_column"] == "id"
    # One row, not two: there is no mirrored copy and no direction column.
    assert "direction" not in row


def test_alter_table_add_constraint_rejects_a_duplicate_name(tmp_path):
    """The name is the only handle DROP CONSTRAINT has, so two of them on one
    relation would make a drop ambiguous."""
    session = _setup_two_tables(tmp_path)
    list(session.execute_to_morsels(_INFORMATIONAL_FK))

    with pytest.raises(ValueError, match="constraint already exists"):
        list(session.execute_to_morsels(_INFORMATIONAL_FK))


def test_the_relationship_store_is_not_a_relation(tmp_path):
    """The store is a file in the relation's directory, where a relation is a
    directory containing dataset.json -- so no scan can resolve it, and the
    visibility model does not rest on a name check."""
    session = _setup_two_tables(tmp_path)
    list(session.execute_to_morsels(_INFORMATIONAL_FK))

    with pytest.raises(Exception):
        list(session.execute_to_morsels("SELECT * FROM ws.events.relationships"))


def test_alter_table_add_constraint_requires_explicit_not_enforced(tmp_path):
    """NOT ENFORCED is never defaulted.

    A bare FOREIGN KEY is an enforcing one, and reading it as informational
    would be exactly the implied behaviour the engine refuses to imply. So is
    an explicit ENFORCED.
    """
    session = _setup_two_tables(tmp_path)

    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                "FOREIGN KEY (customer_ref) REFERENCES ws.customers (id)"
            )
        )

    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                "FOREIGN KEY (customer_ref) REFERENCES ws.customers (id) ENFORCED"
            )
        )


def test_alter_table_add_constraint_rejects_clauses_that_imply_checking(tmp_path):
    """A referential action, a check-time, or NOT VALID all describe enforcement."""
    session = _setup_two_tables(tmp_path)

    for tail in (
        "ON DELETE CASCADE NOT ENFORCED",
        "ON UPDATE CASCADE NOT ENFORCED",
        "NOT ENFORCED DEFERRABLE",
    ):
        with pytest.raises(UnsupportedSyntaxError):
            list(
                session.execute_to_morsels(
                    "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                    f"FOREIGN KEY (customer_ref) REFERENCES ws.customers (id) {tail}"
                )
            )


def test_alter_table_add_constraint_cannot_cross_workspaces(tmp_path):
    """A declared relationship is held in the workspace it is declared in.

    Both ends must live there, so a REFERENCES into another workspace is refused
    at plan time rather than producing a row describing a dataset the workspace
    does not contain. The same boundary RENAME TO enforces.
    """
    session = _setup_two_tables(tmp_path)

    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                "FOREIGN KEY (customer_ref) REFERENCES other.customers (id) NOT ENFORCED"
            )
        )


def test_alter_table_add_constraint_requires_a_name(tmp_path):
    """An unnamed constraint could never be dropped -- the name is the handle."""
    session = _setup_two_tables(tmp_path)

    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD FOREIGN KEY (customer_ref) "
                "REFERENCES ws.customers (id) NOT ENFORCED"
            )
        )


def test_alter_table_add_constraint_checks_both_columns_exist(tmp_path):
    """A declaration naming a column that is not there is not a relationship.

    This statement is the only validation point the store has, so it validates
    both ends, not just the one being altered.
    """
    session = _setup_two_tables(tmp_path)

    with pytest.raises(ColumnNotFoundError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                "FOREIGN KEY (nope) REFERENCES ws.customers (id) NOT ENFORCED"
            )
        )

    with pytest.raises(ColumnNotFoundError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events ADD CONSTRAINT events_customer_fk "
                "FOREIGN KEY (customer_ref) REFERENCES ws.customers (nope) NOT ENFORCED"
            )
        )


def test_alter_table_drop_constraint_removes_it_by_name(tmp_path):
    """DROP CONSTRAINT names the constraint, not the dataset it referenced."""
    session = _setup_two_tables(tmp_path)
    list(session.execute_to_morsels(_INFORMATIONAL_FK))

    list(session.execute_to_morsels("ALTER TABLE ws.events DROP CONSTRAINT events_customer_fk"))

    # The last row out takes the store file with it.
    assert not (tmp_path / "ws" / "events" / "relationships.jsonl").exists()


def test_alter_table_drop_constraint_missing_needs_if_exists(tmp_path):
    """A DROP that silently matched nothing would let a typo read as success."""
    session = _setup_two_tables(tmp_path)

    with pytest.raises(ValueError, match="no constraint"):
        list(session.execute_to_morsels("ALTER TABLE ws.events DROP CONSTRAINT no_such_fk"))

    list(session.execute_to_morsels("ALTER TABLE ws.events DROP CONSTRAINT IF EXISTS no_such_fk"))


def test_alter_table_constraint_if_exists_is_a_no_op(tmp_path):
    """IF EXISTS on the ALTER makes a missing TABLE a no-op, for constraints too.

    Not inherited from the column statements, which report the very absence the
    reader asked to tolerate. Nothing is declared, so neither end's columns are
    checked -- including a far dataset that does not exist either.
    """
    session = _setup_two_tables(tmp_path)

    list(
        session.execute_to_morsels(
            "ALTER TABLE IF EXISTS ws.missing ADD CONSTRAINT fk "
            "FOREIGN KEY (id) REFERENCES ws.customers (id) NOT ENFORCED"
        )
    )
    list(
        session.execute_to_morsels(
            "ALTER TABLE IF EXISTS ws.missing ADD CONSTRAINT fk "
            "FOREIGN KEY (id) REFERENCES ws.also_missing (id) NOT ENFORCED"
        )
    )
    list(session.execute_to_morsels("ALTER TABLE IF EXISTS ws.missing DROP CONSTRAINT fk"))

    # The guard is IF EXISTS, not the constraint statement: without it the
    # missing relation is still an error.
    with pytest.raises(Exception):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.missing ADD CONSTRAINT fk "
                "FOREIGN KEY (id) REFERENCES ws.customers (id) NOT ENFORCED"
            )
        )


def test_alter_table_drop_constraint_rejects_cascade(tmp_path):
    """Nothing references a declaration, so there is nothing for CASCADE to decide."""
    session = _setup_two_tables(tmp_path)

    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "ALTER TABLE ws.events DROP CONSTRAINT events_customer_fk CASCADE"
            )
        )


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
    """deletion_protection is boolean; a non-boolean value is rejected rather than
    coerced into something arbitrary."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="is a boolean"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET deletion_protection TO 7"))


@pytest.mark.parametrize("value", ["ON", "OFF", "TRUE", "FALSE"])
def test_alter_workspace_egress_protection_is_settable(tmp_path, value):
    """egress_protection is in WORKSPACE_PROPERTIES, so it plans exactly like
    deletion_protection. It defaults to ON in the catalog, which makes the OFF
    form the only way a workspace's owners can opt out of the restriction - it
    has to reach the planner at all.

    LocalStoreConnector has no catalog to store properties in, so reaching its
    NotImplementedError is what shows the statement planned and bound.
    """
    _setup_workspace(tmp_path)
    session = opteryx.session(access_policies=[{"pattern": "ws", "role": "owner"}])

    with pytest.raises(NotImplementedError):
        list(session.execute_to_morsels(f"ALTER WORKSPACE ws SET egress_protection TO {value}"))


def test_alter_workspace_egress_protection_non_boolean_rejected(tmp_path):
    """Same boolean discipline as deletion_protection."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="is a boolean"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET egress_protection TO 7"))


def test_alter_workspace_rejects_qualified_name(tmp_path):
    """ALTER WORKSPACE names a workspace, not a relation inside one."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(UnsupportedSyntaxError, match="not a relation within one"):
        list(session.execute_to_morsels("ALTER WORKSPACE ws.collection SET deletion_protection TO OFF"))


def test_alter_workspace_not_implemented_on_local_store(tmp_path):
    """LocalStoreConnector has no catalog to persist workspace properties in, so
    it is rejected explicitly rather than silently doing nothing."""
    _setup_workspace(tmp_path)
    session = opteryx.session(access_policies=[{"pattern": "ws", "role": "owner"}])

    with pytest.raises(NotImplementedError):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET deletion_protection TO OFF"))


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


def _seed_relations(tmp_path):
    """Create a table and a view as an owner, returning an owner session."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))
    list(owner.execute_to_morsels("CREATE VIEW ws.v AS SELECT * FROM ws.t"))
    return owner


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


# CREATE/DROP MATERIALIZED VIEW behaviour is covered by
# tests/storage/test_materialized_views.py - it is CTAS plus registration,
# no longer a rejection.


def test_show_create_table_rejected_at_plan_time(tmp_path):
    """Rejected by name, rather than as 'Invalid SHOW statement' at execution."""
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match=r"\*\*SHOW CREATE\*\* TABLE"):
        list(owner.execute_to_morsels("SHOW CREATE TABLE ws.t"))


def test_comment_on_column_rejected(tmp_path):
    """COMMENT ON COLUMN used to fail as a missing dataset named after the column."""
    _seed_relations(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match=r"\*\*COMMENT ON\*\* COLUMN"):
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


