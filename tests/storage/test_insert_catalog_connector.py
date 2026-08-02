# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Regression test for the INSERT-into-existing-relation crash: visit_insert
(planner/binder/relation.py) used to read the target schema via
`node.connector._relation_dir(...)` / `_read_dataset_json(...)` - private
filesystem helpers that only exist on LocalStoreConnector. Every other
Writable connector (OpteryxConnector in production, and this test's
catalog-shaped fake) has no such attributes, so every INSERT into an
already-existing relation crashed with
`AttributeError: '...' object has no attribute '_relation_dir'` regardless
of role, on every non-local deployment. `tests/storage/test_ctas.py` never
caught this because it only exercises LocalStoreConnector, which happens to
implement those exact private method names.

This connector deliberately implements only the public Writable/table_engine
contract - no filesystem, no `_relation_dir` - so reverting the fix
reproduces the original crash here.
"""

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.models.file_entry import FileEntry


class _NoFilesystemTable:
    """Table engine returned by table_engine() - shaped like OpteryxTable:
    implements get_dataset_metadata(), nothing filesystem-specific."""

    def __init__(self, schema):
        self.schema = schema

    def get_dataset_metadata(self):
        return self.schema, None


class _NoFilesystemConnector(BaseConnector, Writable):
    """Minimal in-memory catalog-shaped connector - like OpteryxConnector,
    relations live in a dict, not a filesystem directory."""

    def __init__(self, **kwargs):
        self._relations = {}  # name -> [schema, row_count]

    def relation_exists(self, relation_name):
        return relation_name in self._relations

    def relation_column_names(self, relation_name):
        return [c.name for c in self._relations[relation_name][0].columns]

    def create_relation(self, relation_name, schema, author=None):
        self._relations[relation_name] = [schema, 0]

    def table_engine(self, name, **kwargs):
        schema, _ = self._relations[name]
        return _NoFilesystemTable(schema)

    def write_morsel(self, relation_name, morsel):
        return FileEntry(
            file_path=f"memory://{relation_name}/{id(morsel)}",
            file_format="PARQUET",
            record_count=len(morsel),
            file_size_in_bytes=0,
        )

    def insert(self, relation_name, file_entries, author=None):
        self._relations[relation_name][1] += sum(fe.record_count for fe in file_entries)


def test_insert_into_existing_relation_on_non_local_connector(tmp_path):
    register_workspace("cat", _NoFilesystemConnector)
    session = opteryx.session()

    list(session.execute_to_morsels("CREATE TABLE cat.dst (a BIGINT)"))
    # This is the call that used to crash with:
    # AttributeError: '_NoFilesystemConnector' object has no attribute '_relation_dir'
    list(session.execute_to_morsels("INSERT INTO cat.dst VALUES (-1), (1), (2), (3)"))

    from opteryx.connectors import connector_factory

    connector = connector_factory("cat.dst", telemetry=None)
    assert connector._relations["cat.dst"][1] == 4
