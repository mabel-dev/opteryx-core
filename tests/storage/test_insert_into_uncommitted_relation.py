# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""INSERT INTO a relation that exists but has never been committed to.

A dataset created by DDL (or by the catalog's `ensure_dataset`) has a schema
and ZERO snapshots. `visit_insert` read the target's schema through
`get_dataset_metadata()`, which resolves a SNAPSHOT before it reads anything,
so the first INSERT into a freshly-created relation failed with

    DatasetReadError: The dataset exists, but no data has been committed to it yet.

and no SQL-driven pipeline could bootstrap its own tables - the target had to
be seeded through the catalog's Python API first.

The target's schema is now read with `get_declared_schema()`, which is
snapshot-free by contract. The reproduction needs a real `OpteryxTable` (that
is the reader whose schema is snapshot-scoped; LocalStoreTable reads
dataset.json and never had the problem), so the connector below is
catalog-shaped: its `table_engine` hands back a real `OpteryxTable` over a
snapshot-less fake catalog dataset, and only the write half is in-memory.
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


class _UncommittedDataset:
    """A catalog dataset with a schema and no snapshots - what CREATE TABLE and
    `ensure_dataset` leave behind before the first commit."""

    # Declared because the real `Dataset` interface declares it, not because an
    # empty file list has bounds to encode - see the check in
    # OpteryxTable.get_dataset_metadata for why it is demanded on a read of a
    # relation with nothing in it.
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

        if identifier != "ops.compaction_log":
            raise DatasetNotFound(identifier)
        return _UncommittedDataset(identifier)


class _UncommittedConnector(BaseConnector, Writable):
    """Catalog-shaped Writable connector over the snapshot-less dataset above.

    Reads go through a real `OpteryxTable`; writes are collected in memory so
    the test can assert what landed without a metastore.
    """

    files_written = []

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")

    def relation_exists(self, relation_name):
        return relation_name == "cat.ops.compaction_log"

    def relation_column_names(self, relation_name):
        return [column["name"] for column in _COLUMNS]

    def table_engine(self, name, **kwargs):
        return OpteryxTable(
            dataset="ops.compaction_log",
            catalog=_UncommittedCatalog(),
            workspace="cat",
            telemetry=kwargs.get("telemetry"),
        )

    def write_morsel(self, relation_name, morsel):
        # The same primitive the real connector uses, into memory rather than
        # object storage - so what the test reads back is what would be stored.
        from rugo.parquet import write_parquet

        data = write_parquet(morsel, compression="zstd")
        _UncommittedConnector.files_written.append(data)
        return FileEntry(
            file_path=f"memory://{relation_name}/{len(_UncommittedConnector.files_written)}",
            file_format="PARQUET",
            record_count=len(morsel),
            file_size_in_bytes=len(data),
        )

    def insert(self, relation_name, file_entries, author=None, commit_message=None):
        self.committed = [entry.file_path for entry in file_entries]


@pytest.fixture
def catalog_workspace():
    _UncommittedConnector.files_written = []
    register_workspace("cat", _UncommittedConnector)
    return _UncommittedConnector


def _rows_landed():
    """The rows the INSERT actually wrote, read back out of the parquet."""
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


def test_insert_values_into_never_committed_relation(catalog_workspace):
    """The first INSERT into a relation with no snapshots lands its rows."""
    _run("INSERT INTO cat.ops.compaction_log VALUES (1, 'a'), (2, 'b')")

    assert _rows_landed() == [(1, "a"), (2, "b")]


def test_insert_select_into_never_committed_relation(catalog_workspace):
    """The production shape: INSERT ... SELECT into an uncommitted target."""
    _run(
        "INSERT INTO cat.ops.compaction_log "
        "SELECT id, name FROM $planets WHERE id < 3"
    )

    assert _rows_landed() == [(1, "Mercury"), (2, "Venus")]


def test_declared_schema_is_served_without_a_snapshot():
    """`get_declared_schema` reads dataset metadata rather than a snapshot.

    The read paths reach the same schema for a relation with nothing committed
    (see tests/storage/test_select_from_uncommitted_relation.py), but by a
    different route and with a different meaning - this one is the schema an
    INSERT must conform to, which is the CURRENT declaration whatever the head
    happens to be.
    """
    table = OpteryxTable(
        dataset="ops.compaction_log",
        catalog=_UncommittedCatalog(),
        workspace="cat",
        telemetry=None,
    )

    assert [column.name for column in table.get_declared_schema().columns] == [
        "id",
        "name",
    ]
