# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Regression test for the CTAS-against-a-catalog crash: every
`CREATE TABLE ... AS SELECT` (and `CREATE MATERIALIZED VIEW`, which plans
through the same `_plan_ctas`) creating a relation that did not exist yet died
with the catalog's own `DatasetNotFound`, naming the very table it was asked to
create.

The sink streams all of its data files before `create_relation` runs, so that a
statement dying mid-write leaves no catalog document behind. That is stated in
`Writable.open_data_file_writer`: "a connector's write target for this must not
depend on the relation already being registered there".
`OpteryxConnector.open_data_file_writer` broke it - it loaded the dataset to
find the location and field-ids - and so asked the catalog for the relation
this statement had not created yet.

`tests/storage/test_ctas.py` and `test_ctas_streams_files.py` never caught this
because both run on LocalStoreConnector, which honours the contract for free: a
directory's path follows from the relation name. The connector below is shaped
like a catalog instead - it can only answer for relations it has registered -
so reverting the fix reproduces the production failure here.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Writable
from opteryx.models.file_entry import FileEntry


class _RegistryTable(BaseTable):
    def __init__(self, schema):
        self.schema = schema

    def get_dataset_schema(self):
        return self.schema

    def get_dataset_metadata(self):
        return self.schema, None


class _RecordedDataFile:
    def __init__(self, path, column_names):
        self.path = path
        self.column_names = column_names
        self.rows = 0
        self.uncompressed_size_in_bytes = 0

    def write_row_group(self, morsel):
        self.rows += len(morsel)
        self.uncompressed_size_in_bytes += morsel.nbytes

    def close(self):
        return FileEntry(
            file_path=self.path,
            file_format="PARQUET",
            record_count=self.rows,
            file_size_in_bytes=1,
            catalog_entry={"file_path": self.path, "record_count": self.rows},
        )

    def abort(self):
        pass


class _CatalogShapedConnector(BaseConnector, Writable):
    """A store that can only describe relations it has registered.

    The real catalog's position: a data file's location and the field-ids its
    statistics are keyed by both come off the dataset document, so a writer for
    a relation that does not exist yet has to be told the schema it is about to
    be created with. Anything else is a lookup that cannot succeed.
    """

    def __init__(self, **kwargs):
        self._relations = {}  # name -> [schema, row_count]
        self.writer_calls = []  # (relation_name, pending_schema)

    def relation_exists(self, relation_name):
        return relation_name in self._relations

    def relation_column_names(self, relation_name):
        return [c.name for c in self._relations[relation_name][0].columns]

    def create_relation(self, relation_name, schema, author=None):
        self._relations[relation_name] = [schema, 0]

    def table_engine(self, name, **kwargs):
        schema, _ = self._relations[name]
        return _RegistryTable(schema)

    def open_data_file_writer(
        self,
        relation_name,
        sorted_by=None,
        sorted_descending=False,
        write_profile="fast",
        pending_schema=None,
    ):
        self.writer_calls.append((relation_name, pending_schema))
        if pending_schema is not None:
            schema = pending_schema
        elif relation_name in self._relations:
            schema = self._relations[relation_name][0]
        else:
            # What the catalog answered, and the whole bug: the statement was
            # asked to CREATE this relation, and the store was asked to find it.
            raise KeyError(f"Dataset not found: {relation_name}")
        return _RecordedDataFile(
            f"memory://{relation_name}", [c.name for c in schema.columns]
        )

    def insert(self, relation_name, file_entries, author=None, **kwargs):
        self._relations[relation_name][1] += sum(fe.record_count for fe in file_entries)

    def replace_relation(self, relation_name, schema, file_entries, author=None, **kwargs):
        self._relations[relation_name] = [
            schema,
            sum(fe.record_count for fe in file_entries),
        ]


def _connector(name):
    from opteryx.connectors import connector_factory

    return connector_factory(name, telemetry=None)


def test_ctas_creating_a_new_relation_does_not_ask_the_store_to_find_it():
    register_workspace("ctascat", _CatalogShapedConnector)
    session = opteryx.session()

    list(
        session.execute_to_morsels(
            "CREATE TABLE ctascat.made AS SELECT name, gravity FROM $planets"
        )
    )

    connector = _connector("ctascat.made")
    assert connector._relations["ctascat.made"][1] == 9

    # The schema the relation is about to be created with reached the store, so
    # it never had to look the relation up.
    (relation_name, pending_schema), = connector.writer_calls
    assert relation_name == "ctascat.made"
    assert pending_schema is not None
    assert [c.name for c in pending_schema.columns] == ["name", "gravity"]

    # ... and it IS the schema the create then registered, not a copy built
    # somewhere else. A second derivation is what would drift.
    assert connector._relations["ctascat.made"][0] is pending_schema


def test_writes_to_an_existing_relation_pass_no_pending_schema():
    register_workspace("ctascat2", _CatalogShapedConnector)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE ctascat2.dst (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ctascat2.dst VALUES (1), (2)"))

    connector = _connector("ctascat2.dst")
    assert connector._relations["ctascat2.dst"][1] == 2
    # The relation exists, so the store is expected to answer for it itself.
    assert connector.writer_calls == [("ctascat2.dst", None)]


if __name__ == "__main__":  # pragma: no cover
    test_ctas_creating_a_new_relation_does_not_ask_the_store_to_find_it()
    test_writes_to_an_existing_relation_pass_no_pending_schema()
    print("✅ okay")
