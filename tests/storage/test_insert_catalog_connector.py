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
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Writable
from opteryx.compiled.planner.native_manifest import NativeManifestBuilder



def _file_row(path, record_count, file_size, uncompressed_size=-1, row_group_count=-1):
    """A data file writer's close() result: the file as a native file row."""
    builder = NativeManifestBuilder((), (), True, True)
    builder.add_file(path, "PARQUET", record_count, file_size, row_group_count, uncompressed_size)
    return builder.build({})

class _NoFilesystemTable(BaseTable):
    """Table engine returned by table_engine() - shaped like OpteryxTable:
    serves a schema, nothing filesystem-specific."""

    def __init__(self, schema):
        self.schema = schema

    def get_dataset_schema(self):
        return self.schema

    def get_dataset_metadata(self):
        return self.schema, None


class _RecordedDataFile:
    """Stands in for a streaming data file: counts rows, hands back a native file row."""

    def __init__(self, path):
        self.path = path
        self.rows = 0
        self.uncompressed_size_in_bytes = 0

    def write_row_group(self, morsel):
        self.rows += len(morsel)
        self.uncompressed_size_in_bytes += morsel.nbytes

    def close(self):
        return _file_row(self.path, self.rows, 1)

    def abort(self):
        pass


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

    def open_data_file_writer(self, relation_name, sorted_by=None, sorted_descending=False,
                              write_profile="fast", pending_schema=None):
        return _RecordedDataFile(f"memory://{relation_name}/{id(self)}")

    def insert(self, relation_name, rows, author=None, **kwargs):
        self._relations[relation_name][1] += rows.record_count()


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
