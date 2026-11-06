"""A dataset the connector already loaded is not loaded again at bind time.

Relation resolution prefetches every relation a statement names so the binder
does not re-read the catalog. For a metastore that does not serve its catalog
records as the dataset (Iceberg), that prefetch used to be thrown away and the
binder loaded every table again - once per scan node, so a table named in a CTE
and again in the outer query paid the catalog round trip three times for one
statement.

The prefetch is dropped only when it did not come from this connector: an
externally-bound workspace's view store is a different connector, and the record
it returns is a listing with no snapshot and no files, which bound an empty
relation. Where the view store and the data connector are the same object the
prefetch is the dataset this catalog already loaded, and two scans of one table
in one statement cannot legitimately see different snapshots.
"""

import os
import sys
import types

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

# OpteryxTable imports its exceptions at construction, and a unit test of the guard
# has no catalog checkout. The exception class is never raised on these paths.
_exceptions = types.ModuleType("opteryx_catalog.exceptions")
setattr(_exceptions, "DatasetNotFound", type("DatasetNotFound", (Exception,), {}))
sys.modules.setdefault("opteryx_catalog", types.ModuleType("opteryx_catalog"))
sys.modules.setdefault("opteryx_catalog.exceptions", _exceptions)

import pytest

from opteryx import connectors
from opteryx.connectors import Resolution
from opteryx.connectors import register_workspace
from opteryx.connectors import set_workspace_resolver
from opteryx.connectors import set_workspace_settings_resolver
from opteryx.connectors.opteryx_connector import OpteryxConnector


class _Dataset:
    """The minimum a dataset must offer: OpteryxTable reads its snapshot at bind."""

    def __init__(self, marker):
        self.marker = marker

    def snapshot(self):
        return None


class _Catalog:
    """Records which datasets were loaded, so a reuse shows up as a missing load."""

    def __init__(self, serves_catalog_records):
        self.serves_catalog_records = serves_catalog_records
        self.loaded = []

    def load_dataset(self, identifier):
        self.loaded.append(identifier)
        return _Dataset(f"loaded:{identifier}")


def _engine(catalog, prefix="ws"):
    connector = OpteryxConnector(catalog=catalog)
    connector._matched_prefix = prefix
    return connector


def _names():
    return (
        dict(connectors._storage_prefixes),
        connectors._workspace_resolver,
        connectors._workspace_settings_resolver,
    )


@pytest.fixture
def clean_routing():
    saved = _names()
    connectors._storage_prefixes.clear()
    connectors._workspace_resolver = None
    connectors._workspace_settings_resolver = None
    yield
    prefixes, resolver, settings = saved
    connectors._storage_prefixes.clear()
    connectors._storage_prefixes.update(prefixes)
    connectors._workspace_resolver = resolver
    connectors._workspace_settings_resolver = settings


def test_a_dataset_loaded_by_this_connector_is_not_loaded_again(clean_routing):
    catalog = _Catalog(serves_catalog_records=False)
    register_workspace("ws", lambda **kwargs: _engine(catalog))

    # The connector the query resolves to, not a second instance of it: the reuse
    # holds only when the view store and the data connector are the same object.
    from opteryx.connectors import connector_factory

    table = connector_factory("ws.ns.orders", telemetry=None).table_engine(
        "ws.ns.orders", telemetry=None, prefetched_table=_Dataset("the-dataset")
    )

    assert table.table.marker == "the-dataset"
    assert catalog.loaded == []


def test_a_record_from_another_connectors_catalog_is_loaded_from_this_one(clean_routing):
    """The listing case: the view store is a different connector, and the record it
    prefetched is not a dataset this catalog can read. Binding it as the relation
    bound an empty one, so it is dropped and the table loads from this metastore."""
    catalog = _Catalog(serves_catalog_records=False)

    class _SettingsStore:
        def __init__(self, telemetry=None, **kwargs):
            self._matched_prefix = "ws"

    set_workspace_resolver(lambda workspace: Resolution(lambda **kwargs: _engine(catalog), {}))
    set_workspace_settings_resolver(lambda workspace: Resolution(_SettingsStore, {}))

    table = connector_table("ws.ns.orders", catalog, prefetched="listing-with-no-files")

    assert table.table.marker == "loaded:ns.orders"
    assert catalog.loaded == ["ns.orders"]


def test_a_catalog_that_serves_its_records_keeps_the_prefetch(clean_routing):
    catalog = _Catalog(serves_catalog_records=True)

    table = connector_table("ws.ns.orders", catalog, prefetched=_Dataset("the-dataset"))

    assert table.table.marker == "the-dataset"
    assert catalog.loaded == []


def connector_table(name, catalog, prefetched=_Dataset("the-dataset")):
    connector = _engine(catalog)
    return connector.table_engine(name, telemetry=None, prefetched_table=prefetched)
