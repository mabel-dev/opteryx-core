# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A CTAS creates its relation WHERE ITS FILES ALREADY ARE.

The catalog gives every dataset a fresh storage location nothing can derive from
the name (opteryx-catalog RENAME_METADATA_ONLY_DESIGN.md), so a CTAS - which
streams every file before the relation exists - cannot work the location out
again at create time. The first pending writer reports it as `.location`; the
sink hands it back to every later writer (`pending_location`) and to
`create_relation(location=...)`. The location travels with the statement, not in
some process's memory.

A store whose writers report no location (LocalStore, and the catalog-shaped
fake in test_ctas_catalog_connector.py, whose signatures predate this) is never
sent either argument - that file still passing is the compatibility half.
"""

import opteryx
from opteryx.compiled.planner.native_manifest import NativeManifestBuilder
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.opteryx_connector import OpteryxConnector

MINTED = "gs://bkt/ws/3f2a9c1e5b7d4a608c1d2e3f4a5b6c7d"


def _file_row(path, record_count):
    builder = NativeManifestBuilder((), (), True, True)
    builder.add_file(path, "PARQUET", record_count, 1, -1, -1)
    return builder.build({})


class _Table(BaseTable):
    def __init__(self, schema):
        self.schema = schema

    def get_dataset_schema(self):
        return self.schema

    def get_dataset_metadata(self):
        return self.schema, None


class _LocatedFile:
    def __init__(self, location, index):
        self.location = location
        self.path = f"{location}/data/{index}.parquet"
        self.rows = 0
        self.uncompressed_size_in_bytes = 0

    def write_row_group(self, morsel):
        self.rows += len(morsel)
        self.uncompressed_size_in_bytes += morsel.nbytes

    def close(self):
        return _file_row(self.path, self.rows)

    def abort(self):
        pass


class _LocatingConnector(BaseConnector, Writable):
    """Shaped like the catalog: it picks a pending relation's location itself, once,
    and reports it on the writer - and remembers nothing between calls."""

    def __init__(self, **kwargs):
        self._relations = {}
        self.writer_calls = []  # pending_location each writer was opened with
        self.created_at = {}

    def relation_exists(self, relation_name):
        return relation_name in self._relations

    def relation_column_names(self, relation_name):
        return [c.name for c in self._relations[relation_name][0].columns]

    def create_relation(self, relation_name, schema, author=None, location=None):
        self._relations[relation_name] = [schema, 0]
        self.created_at[relation_name] = location

    def table_engine(self, name, **kwargs):
        return _Table(self._relations[name][0])

    def open_data_file_writer(
        self,
        relation_name,
        sorted_by=None,
        sorted_descending=False,
        write_profile="fast",
        pending_schema=None,
        pending_location=None,
    ):
        self.writer_calls.append(pending_location)
        # A fresh location when none is handed back - so a sink that forgot to
        # carry it would scatter its files over several.
        location = pending_location or f"{MINTED[:-2]}{len(self.writer_calls):02d}"
        return _LocatedFile(location, len(self.writer_calls))

    def insert(self, relation_name, rows, author=None, **kwargs):
        self._relations[relation_name][1] += rows.record_count()
        self.inserted_paths = list(rows.file_paths())

    def create_relation_with_rows(self, relation_name, schema, rows, author=None, location=None, **kwargs):
        # A CTAS's one publish: created where its files are, holding them.
        self._relations[relation_name] = [schema, rows.record_count()]
        self.created_at[relation_name] = location
        self.inserted_paths = list(rows.file_paths())


def _connector(name):
    from opteryx.connectors import connector_factory

    return connector_factory(name, telemetry=None)


def test_ctas_creates_the_relation_at_the_location_its_writer_reported():
    register_workspace("locat1", _LocatingConnector)

    list(
        opteryx.session().execute_to_morsels(
            "CREATE TABLE locat1.made AS SELECT name, gravity FROM $planets"
        )
    )

    connector = _connector("locat1.made")
    assert connector._relations["locat1.made"][1] == 9
    location = connector.created_at["locat1.made"]
    assert location is not None
    assert all(path.startswith(location + "/data/") for path in connector.inserted_paths)


def test_every_file_of_a_ctas_is_written_to_the_first_files_location(monkeypatch):
    # One file per batch, two rows per batch: several writers for one statement.
    monkeypatch.setattr("opteryx.planner.compaction.constants.TARGET_SIZE_BYTES", 1)
    register_workspace("locat2", _LocatingConnector)
    session = opteryx.session()
    list(session.execute_to_morsels("SET write_coalesce_rows = 2"))

    list(session.execute_to_morsels("CREATE TABLE locat2.made AS SELECT name FROM $planets"))

    connector = _connector("locat2.made")
    first, *later = connector.writer_calls
    assert first is None  # nothing to hand back before the first file exists
    assert len(later) >= 1
    location = connector.created_at["locat2.made"]
    assert later == [location] * len(later)
    assert {path.rsplit("/data/", 1)[0] for path in connector.inserted_paths} == {location}


def test_a_ctas_that_writes_no_file_lets_the_store_choose():
    register_workspace("locat3", _LocatingConnector)

    list(
        opteryx.session().execute_to_morsels(
            "CREATE TABLE locat3.empty AS SELECT name FROM $planets WHERE id < 0"
        )
    )

    connector = _connector("locat3.empty")
    assert "locat3.empty" in connector._relations
    assert connector.created_at["locat3.empty"] is None


# --- OpteryxConnector: the hand-off to the catalog ----------------------------


class _Catalog:
    """Records what the connector hands the catalog."""

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace
        self.created = []
        self.pending = []

    def create_dataset(self, identifier, schema, author=None, **kwargs):
        self.created.append((identifier, kwargs))

    def open_pending_data_file_writer(self, identifier, schema, **kwargs):
        self.pending.append(kwargs.get("location", "<not sent>"))

        class _Inner:
            location = kwargs.get("location") or MINTED
            data_path = f"{location}/data/f.parquet"

        return _Inner()


def test_the_connector_sends_location_to_create_dataset_only_when_it_has_one():
    catalog = _Catalog(workspace="ws")
    connector = OpteryxConnector(catalog=catalog)

    connector.create_relation("ws.coll.plain", None, author="alice")
    connector.create_relation("ws.coll.ctas", None, author="alice", location=MINTED)

    assert catalog.created == [("coll.plain", {}), ("coll.ctas", {"location": MINTED})]


def test_the_connector_reports_and_hands_back_the_pending_location():
    catalog = _Catalog(workspace="ws")
    connector = OpteryxConnector(catalog=catalog)

    first = connector.open_data_file_writer("ws.coll.t", pending_schema=object())
    second = connector.open_data_file_writer(
        "ws.coll.t", pending_schema=object(), pending_location=first.location
    )

    assert first.location == MINTED
    assert second.location == MINTED
    # Not sent at all the first time - a catalog without the parameter still works.
    assert catalog.pending == ["<not sent>", MINTED]
