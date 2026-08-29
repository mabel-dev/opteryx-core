# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""MERGE INTO a relation that exists but has never been committed to.

A dataset created by DDL has a schema and ZERO snapshots. A MERGE whose only
reachable arm is `WHEN NOT MATCHED THEN INSERT` is how such a target is
bootstrapped by the same statement that later keeps it up to date, so it must
not be gated on the target already holding data - the sibling case to
tests/storage/test_insert_into_uncommitted_relation.py.

This module pins the ENGINE's half of that contract, which is where the whole
statement is decided:

  * the target binds with its DECLARED schema and an EMPTY file list, so no
    snapshot is resolved for a relation that has none;
  * every source row is therefore NOT MATCHED, and the sink hands the store an
    insert-only commit - files to add, NO delete positions;
  * `$merge_file` never indexes the empty file list, because the scan that
    produces those addresses read no files.

`UPDATE` and `DELETE` reuse the merge sink whole. Against a relation with no
rows they affect nothing, which is the same answer they give for a predicate
that matches nothing in a populated relation - so they succeed having
committed NOTHING. Ruled 2026-08-29; asserted here because a snapshot
describing no change would be a lie about what ran.

The store's half - that an insert-only commit with no parent snapshot writes
the dataset's FIRST snapshot - belongs to the catalog and is covered by
opteryx-catalog/tests/test_merge_commit.py.
"""

from types import SimpleNamespace

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.models.file_entry import FileEntry

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_COLUMNS = [
    {"name": "id", "type": "INTEGER", "id": 1},
    {"name": "name", "type": "VARCHAR", "id": 2},
]

_RELATION = "cat.ops.compaction_log"
_DATASET = "ops.compaction_log"


class _UncommittedDataset:
    """A catalog dataset with a schema and no snapshots - what CREATE TABLE
    leaves behind before the first commit."""

    # Demanded of every dataset a read reaches, whether or not it holds files -
    # see the check in OpteryxTable.get_dataset_metadata.
    bounds_are_ordinal = True

    def __init__(self, identifier):
        self.identifier = identifier
        self.metadata = SimpleNamespace(
            current_snapshot_id=None,
            current_schema_id="sch-0001",
            location=f"gs://bucket/ws/{identifier.replace('.', '/')}",
            snapshots=[],
        )

    def snapshot(self, snapshot_id=None, user_only=False):
        return None

    def snapshots(self):
        return []

    def scan(self, snapshot_id=None):
        return []

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=list(_COLUMNS), name=self.identifier)


class _UncommittedCatalog:
    """Just enough catalog for `OpteryxTable` to load the dataset above."""

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace or "default"

    def load_dataset(self, identifier, load_history=False):
        from opteryx_catalog.exceptions import DatasetNotFound

        if identifier != _DATASET:
            raise DatasetNotFound(identifier)
        return _UncommittedDataset(identifier)


class _UncommittedConnector(BaseConnector, Writable):
    """Catalog-shaped Writable connector over the snapshot-less dataset above.

    Reads go through a real `OpteryxTable` - that is the reader whose schema is
    snapshot-scoped, so it is the one that had to stop resolving a snapshot to
    serve a write. The commit is captured rather than performed, so the test
    asserts on exactly what the engine asked the store to do.
    """

    files_written = []
    commits = []

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")

    def relation_exists(self, relation_name):
        return relation_name == _RELATION

    def relation_column_names(self, relation_name):
        return [column["name"] for column in _COLUMNS]

    def table_engine(self, name, **kwargs):
        return OpteryxTable(
            dataset=_DATASET,
            catalog=_UncommittedCatalog(),
            workspace="cat",
            telemetry=kwargs.get("telemetry"),
        )

    def write_morsel(self, relation_name, morsel):
        from rugo.parquet import write_parquet

        data = write_parquet(morsel, compression="zstd")
        _UncommittedConnector.files_written.append(data)
        return FileEntry(
            file_path=f"memory://{relation_name}/{len(_UncommittedConnector.files_written)}",
            file_format="PARQUET",
            record_count=len(morsel),
            file_size_in_bytes=len(data),
        )

    def merge_commit(
        self,
        relation_name,
        file_entries,
        delete_positions,
        author=None,
        commit_message=None,
        operation="merge",
    ):
        _UncommittedConnector.commits.append(
            {
                "files": [entry.file_path for entry in file_entries],
                "positions": dict(delete_positions),
                "operation": operation,
                "author": author,
            }
        )


@pytest.fixture
def catalog_workspace():
    _UncommittedConnector.files_written = []
    _UncommittedConnector.commits = []
    register_workspace("cat", _UncommittedConnector)
    return _UncommittedConnector


def _rows_landed():
    """The rows the statement actually wrote, read back out of the parquet."""
    import io

    import pyarrow.parquet

    landed = []
    for data in _UncommittedConnector.files_written:
        pydict = pyarrow.parquet.read_table(io.BytesIO(bytes(data))).to_pydict()
        columns = list(pydict.values())
        for index in range(len(columns[0]) if columns else 0):
            landed.append(tuple(column[index] for column in columns))
    return sorted(landed)


def _run(statement):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels(statement))


# Both sides must be relations - MERGE v1 takes no sub-query source - so the
# source is $planets whole, and the MATCHED arm is present to prove it is
# UNREACHABLE against a target holding nothing rather than absent.
_UPSERT = f"""
MERGE INTO {_RELATION} AS t
USING $planets AS s
   ON t.id = s.id
 WHEN MATCHED THEN UPDATE SET name = s.name
 WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
"""


def _planets():
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    rows = []
    for morsel in session.execute_to_morsels("SELECT id, name FROM $planets"):
        for index in range(morsel.num_rows):
            row = morsel[index]
            rows.append((row[0], row[1]))
    return sorted(rows)


def test_merge_bootstraps_a_never_committed_relation(catalog_workspace):
    """Every source row is NOT MATCHED, so the whole source lands."""
    _run(_UPSERT)

    assert _rows_landed() == _planets()


def test_the_commit_is_insert_only(catalog_workspace):
    """No target row exists to retire, so the sink names no delete position.

    This is what makes the empty file list safe: `$merge_file` addresses come
    from the target scan, and a scan of no files produces none - so the sink
    never indexes the list the binder left empty.
    """
    _run(_UPSERT)

    assert len(_UncommittedConnector.commits) == 1
    commit = _UncommittedConnector.commits[0]
    assert len(commit["files"]) == 1
    assert commit["positions"] == {}
    assert commit["operation"] == "merge"
    assert commit["author"] == "olive"


def test_the_target_binds_without_resolving_a_snapshot(catalog_workspace):
    """The declared schema and an empty manifest, with no snapshot in sight."""
    table = _UncommittedConnector().table_engine(_RELATION)
    schema, manifest = table.get_dataset_metadata()

    assert [column.name for column in schema.columns] == ["id", "name"]
    assert list(manifest.get_file_paths()) == []
    assert table.snapshot is None


@pytest.mark.parametrize(
    "statement",
    [
        f"DELETE FROM {_RELATION} WHERE id = 1",
        f"UPDATE {_RELATION} SET name = 'x' WHERE id = 1",
    ],
)
def test_update_and_delete_affect_nothing_and_commit_nothing(catalog_workspace, statement):
    """Ruled 2026-08-29: zero rows affected is the correct answer.

    They reach the same sink as MERGE with a degenerate source. With no rows to
    match, there is nothing to retire and nothing to append - and committing a
    snapshot describing no change would be a lie about what ran.
    """
    _run(statement)

    assert _UncommittedConnector.commits == []
    assert _UncommittedConnector.files_written == []
