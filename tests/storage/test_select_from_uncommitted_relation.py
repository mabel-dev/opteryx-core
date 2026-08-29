# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Reading a relation that exists but has never been committed to.

A dataset created by DDL (or by the catalog's `ensure_dataset`) has a
registered schema and ZERO snapshots. Every read path resolved a snapshot
before it read anything, so all of them refused:

    DatasetReadError: The dataset exists, but no data has been committed to it yet.

Ruled 2026-08-29: such a relation reads as the schema it declares, with no
rows. The state the error named is indistinguishable from a TRUNCATEd
relation, which the engine already answers that way - `truncate()` appends a
snapshot carrying an empty manifest, and the scan serves that as one empty
morsel. And zero snapshots can mean nothing else: nothing in the catalog ever
clears `current-snapshot-id`, and a relation that does not exist raises
DatasetNotFound before a reader is built.

Time travel is NOT relaxed. `VERSION AS OF`, a tag, and a point-in-time read
each ask for a version that a never-written relation does not have, and each
still says so.

The two obligations these tests hold:

  * the CHECK and the RUN agree. `context.schema_only` binding (the edit-time
    check) goes through `get_dataset_schema`, the run through
    `get_dataset_metadata`, and both resolve their snapshot in the same place.
    A run that returns rows while the check refuses - or the reverse, which is
    what `SHOW SNAPSHOTS` did before this change - is a false green.
  * `information_schema.columns` agrees with `SHOW COLUMNS`. It used to skip a
    snapshot-less dataset entirely, which said a readable relation has no
    columns.

The reproduction needs a real `OpteryxTable` over a snapshot-less catalog
dataset; the fixtures for that live in
`test_insert_into_uncommitted_relation`, which covers the write half.
"""

import datetime
from types import SimpleNamespace

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.exceptions import DatasetReadError
from opteryx.models.execution_context import ExecutionContext
from opteryx.models.query_telemetry import QueryTelemetry
from opteryx.planner.query_check import check_statement
from tests.storage.test_insert_into_uncommitted_relation import _COLUMNS
from tests.storage.test_insert_into_uncommitted_relation import _OWNER_POLICY
from tests.storage.test_insert_into_uncommitted_relation import _UncommittedCatalog
from tests.storage.test_insert_into_uncommitted_relation import _UncommittedConnector

RELATION = "cat.ops.compaction_log"


@pytest.fixture
def catalog_workspace():
    register_workspace("cat", _UncommittedConnector)


def _table():
    return OpteryxTable(
        dataset="ops.compaction_log",
        catalog=_UncommittedCatalog(),
        workspace="cat",
        telemetry=None,
    )


def _run(statement):
    """The rows the statement produces, as tuples."""
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    rows = []
    for morsel in session.execute_to_morsels(statement):
        rows.extend(tuple(row) for row in morsel)
    return rows


def _check(statement):
    return check_statement(
        statement,
        execution_context=ExecutionContext(
            query_id="check", user="olive", access_policies=_OWNER_POLICY
        ),
        query_id="check",
        telemetry=QueryTelemetry.detached(),
    )


# ---------------------------------------------------------------------------
# The connector, directly.
# ---------------------------------------------------------------------------


def test_dataset_schema_is_the_declared_schema():
    """The schema-only read - what the edit-time check binds through."""
    table = _table()

    schema = table.get_dataset_schema()

    assert [column.name for column in schema.columns] == ["id", "name"]
    # Nothing was committed, so there is no commit to timestamp. None rather
    # than a fabricated value: this reaches telemetry as `committed_at`.
    assert table.dataset_committed_at is None
    assert table.snapshot is None
    assert table.snapshot_id is None


def test_dataset_metadata_is_the_declared_schema_and_an_empty_manifest():
    """The full read - what the run binds through. Same schema, no files."""
    table = _table()

    schema, manifest = table.get_dataset_metadata()

    assert [column.name for column in schema.columns] == ["id", "name"]
    assert manifest.files == []
    assert manifest.get_file_paths() == []
    assert table.dataset_committed_at is None


def test_the_two_read_paths_agree_on_the_schema():
    """The check binds one, the run binds the other. They must not diverge."""
    checked = _table().get_dataset_schema()
    ran, _manifest = _table().get_dataset_metadata()

    assert [c.name for c in checked.columns] == [c.name for c in ran.columns]
    assert [str(c.column_type) for c in checked.columns] == [
        str(c.column_type) for c in ran.columns
    ]


def test_a_relation_with_no_registered_schema_is_still_refused():
    """Nothing committed is answerable; nothing DECLARED is not. A relation
    with neither has no shape to serve, and inventing an empty one would
    report a broken catalog document as an empty table."""

    class _SchemalessDataset:
        bounds_are_ordinal = True
        metadata = SimpleNamespace(current_snapshot_id=None, current_schema_id=None)

        def snapshot(self, snapshot_id=None, user_only=False):
            return None

        def schema(self, schema_id=None):
            return None

    class _SchemalessCatalog:
        def load_dataset(self, identifier, load_history=False):
            return _SchemalessDataset()

    table = OpteryxTable(
        dataset="ops.compaction_log",
        catalog=_SchemalessCatalog(),
        workspace="cat",
        telemetry=None,
    )

    with pytest.raises(DatasetReadError):
        table.get_dataset_schema()
    with pytest.raises(DatasetReadError):
        table.get_dataset_metadata()


# ---------------------------------------------------------------------------
# Time travel is not relaxed - a relation with no versions has none to name.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"version": 12345},  # VERSION AS OF <id>
        {"version": 0},  # VERSION AS OF PREVIOUS (the rewriter's sentinel)
        {"at_date": datetime.datetime(2020, 1, 1)},  # FOR <date>
    ],
    # `version_tag` is deliberately absent: the tag branch imports
    # opteryx_catalog.exceptions.TagNotFound before it reaches the virtual
    # `latest` case, and the installed opteryx_catalog carries no tag API at
    # all - so that arm cannot be exercised here for reasons unrelated to this
    # change. It resolves its snapshot in the same branch and raises the same
    # way; the two arms below are what this file can honestly assert.
    ids=["version", "previous", "at-date"],
)
def test_time_travel_still_refuses_a_relation_with_no_versions(kwargs):
    table = OpteryxTable(
        dataset="ops.compaction_log",
        catalog=_UncommittedCatalog(),
        workspace="cat",
        telemetry=None,
        **kwargs,
    )

    with pytest.raises(DatasetReadError):
        table.get_dataset_metadata()


# ---------------------------------------------------------------------------
# End to end: every statement, run and checked.
# ---------------------------------------------------------------------------


def test_select_star_returns_no_rows(catalog_workspace):
    assert _run(f"SELECT * FROM {RELATION}") == []


def test_select_count_star_returns_zero(catalog_workspace):
    assert _run(f"SELECT COUNT(*) FROM {RELATION}") == [(0,)]


def test_select_with_projection_and_filter_returns_no_rows(catalog_workspace):
    assert _run(f"SELECT id FROM {RELATION} WHERE id > 1") == []


def test_show_columns_lists_the_declared_columns(catalog_workspace):
    names = [row[0] for row in _run(f"SHOW COLUMNS FROM {RELATION}")]

    assert names == [column["name"] for column in _COLUMNS]


def test_show_manifest_returns_no_rows(catalog_workspace):
    assert _run(f"SHOW MANIFEST FOR {RELATION}") == []


def test_show_snapshots_returns_an_empty_history(catalog_workspace):
    assert _run(f"SHOW SNAPSHOTS FOR {RELATION}") == []


@pytest.mark.parametrize(
    "statement",
    [
        f"SELECT * FROM {RELATION}",
        f"SELECT COUNT(*) FROM {RELATION}",
        f"SELECT id FROM {RELATION} WHERE id > 1",
        f"SHOW COLUMNS FROM {RELATION}",
        f"SHOW MANIFEST FOR {RELATION}",
        # This one ran clean and CHECKED as an error before the change: the run
        # path skips the schema read for a snapshots-only Scan, the schema-only
        # path did not.
        f"SHOW SNAPSHOTS FOR {RELATION}",
    ],
)
def test_the_check_agrees_with_the_run(catalog_workspace, statement):
    """Every statement that runs must also check clean. A check that refuses
    what the engine will happily execute draws an error under valid SQL."""
    result = _check(statement)

    assert result.ok, result.error


# ---------------------------------------------------------------------------
# information_schema.columns must say the same thing SHOW COLUMNS does.
# ---------------------------------------------------------------------------


class _MetadataDataset:
    """A snapshot-less dataset, seen through the metadata views."""

    def __init__(self, identifier):
        self.identifier = identifier

    def snapshot(self):
        return None

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=list(_COLUMNS), name=self.identifier)


class _MetadataCatalog:
    """One collection, one dataset, nothing committed to it."""

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["ops"]

    def list_datasets(self, collection):
        return ["compaction_log"]

    def list_views(self, collection):
        return []

    def dataset_exists(self, identifier):
        return True

    def load_dataset(self, identifier):
        return _MetadataDataset(identifier)


def test_information_schema_columns_includes_an_uncommitted_relation():
    register_workspace("meta", OpteryxConnector, catalog=_MetadataCatalog)

    rows = _run(
        "SELECT table_name, column_name FROM meta.information_schema.columns "
        "ORDER BY ordinal_position"
    )

    assert rows == [
        ("compaction_log", column["name"]) for column in _COLUMNS
    ]
