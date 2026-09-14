"""
Where a VIEW is stored.

A view is SQL text. It domiciles no rows, so holding one in the opteryx
catalog entry for an externally-bound workspace does not make that workspace
domicile opteryx data -- which is the whole of what the "relation-scoped DDL
goes through connector_factory" rule protects. The line is therefore:

    DDL that creates STORAGE       -> connector_factory (the data binding)
    DDL that creates catalog TEXT  -> view_store_connector (the catalog entry)

Routed at the data binding instead, CREATE VIEW on a PostgreSQL-bound
workspace tried to write the definition to the PostgreSQL server. That
connector is not Eidetic, so it surfaced as an AttributeError rather than as a
view silently created somewhere we do not own.

What is pinned here: the store is the catalog entry for a bound workspace and
the SAME object as the data connector for an unbound one, a store that cannot
hold views says so, a name the data source already holds is refused at CREATE,
and the read path finds a stored view without ever taking a DATASET answer
from the store.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx import connectors
from opteryx.connectors import Resolution
from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors import set_default_connector
from opteryx.connectors import set_workspace_resolver
from opteryx.connectors import set_workspace_settings_resolver
from opteryx.connectors import view_store_connector
from opteryx.connectors.capabilities import Eidetic
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.connectors.base.base_connector import TableType
from opteryx.exceptions import ReadOnlyConnectorError
from opteryx.exceptions import SqlError
from opteryx.planner.binder.view import _assert_name_free_in_source
from opteryx.planner.binder.view import _view_store


class DataConnector:
    """Stands in for PostgresConnector: serves rows, holds no views."""

    eidetic = False

    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry
        self.kwargs = kwargs
        self.names_held = set()

    def locate_object(self, name):
        if name in self.names_held:
            return TableType.Table, {"name": name}
        return None, None


class CatalogConnector(Eidetic):
    """Stands in for OpteryxConnector: holds the catalog text."""

    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry
        self.kwargs = kwargs
        self.views = {}

    def create_view(self, view_name, statement, update_if_exists=False, owner=None, schema=None):
        self.views[view_name] = statement

    def get_view(self, view_name):
        return ViewDefinition(name=view_name, statement=self.views[view_name])

    def locate_object(self, name):
        if name in self.views:
            return TableType.View, {"name": name}
        return None, None


@pytest.fixture
def clean_registry():
    saved = (
        dict(connectors._storage_prefixes),
        connectors._default_connector,
        connectors._workspace_resolver,
        connectors._workspace_settings_resolver,
        dict(connectors._connector_cache),
        dict(connectors._connector_versions),
    )
    connectors._storage_prefixes.clear()
    connectors._default_connector = None
    connectors._workspace_resolver = None
    connectors._workspace_settings_resolver = None
    connectors._connector_cache.clear()
    connectors._connector_versions.clear()
    try:
        yield
    finally:
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved[0])
        connectors._default_connector = saved[1]
        connectors._workspace_resolver = saved[2]
        connectors._workspace_settings_resolver = saved[3]
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved[4])
        connectors._connector_versions.clear()
        connectors._connector_versions.update(saved[5])


def _bind_a_workspace():
    """A workspace whose DATA is external and whose CATALOG ENTRY is ours."""
    set_workspace_resolver(lambda workspace: Resolution(DataConnector, {"marker": "data"}))
    set_workspace_settings_resolver(
        lambda workspace: Resolution(CatalogConnector, {"marker": "catalog"})
    )


def _context():
    return SimpleNamespace(
        telemetry=None,
        execution_context=SimpleNamespace(variables={}),
    )


# ---------------------------------------------------------------------------
# Which connector stores the definition
# ---------------------------------------------------------------------------


def test_bound_workspace_stores_views_in_the_catalog_entry(clean_registry):
    _bind_a_workspace()

    store = view_store_connector("aiven.public.orders_v", telemetry=None)
    data = connector_factory("aiven.public.orders_v", telemetry=None)

    assert isinstance(store, CatalogConnector)
    assert isinstance(data, DataConnector)
    assert store is not data


def test_unbound_workspace_stores_views_where_its_data_lives(clean_registry):
    # No external binding: the two questions have one answer, and the view path
    # costs nothing extra.
    register_workspace("ws", CatalogConnector, marker="both")

    assert view_store_connector("ws.orders_v", telemetry=None) is connector_factory(
        "ws.orders_v", telemetry=None
    )


def test_a_store_that_cannot_hold_views_says_so(clean_registry):
    # Both questions answered by a connector with no view surface. Before, this
    # was an AttributeError from deep in the operator.
    set_default_connector(DataConnector)

    with pytest.raises(ReadOnlyConnectorError) as raised:
        _view_store("ws.orders_v", _context())
    assert "orders_v" in str(raised.value)


# ---------------------------------------------------------------------------
# CREATE VIEW refuses a name the data source already holds
# ---------------------------------------------------------------------------


def test_create_view_refuses_a_name_the_source_holds(clean_registry):
    # The two catalogs share one namespace and neither can see the other's
    # names, so bind time is the only place this collision can be caught.
    # Uncaught, the view shadows the table and makes it unreachable.
    _bind_a_workspace()
    data = connector_factory("aiven.public.orders", telemetry=None)
    data.names_held.add("aiven.public.orders")

    store = view_store_connector("aiven.public.orders", telemetry=None)

    with pytest.raises(SqlError) as raised:
        _assert_name_free_in_source("aiven.public.orders", store, _context())
    assert "orders" in str(raised.value)


def test_create_view_allows_a_name_the_source_does_not_hold(clean_registry):
    _bind_a_workspace()
    store = view_store_connector("aiven.public.orders_v", telemetry=None)

    _assert_name_free_in_source("aiven.public.orders_v", store, _context())


def test_the_source_is_not_asked_when_it_is_the_store(clean_registry):
    # One catalog can see its own names and refuses there; asking twice would
    # put a second round trip on every ordinary CREATE VIEW.
    asked = []

    class _Store(CatalogConnector):
        def locate_object(self, name):
            asked.append(name)
            return super().locate_object(name)

    register_workspace("ws", _Store)
    store = view_store_connector("ws.orders_v", telemetry=None)

    _assert_name_free_in_source("ws.orders_v", store, _context())

    assert asked == []


# ---------------------------------------------------------------------------
# The read path matches the write path
# ---------------------------------------------------------------------------


def test_a_view_stored_in_the_catalog_entry_is_found_again(clean_registry):
    from opteryx.managers.views import resolve_relation

    _bind_a_workspace()
    store = view_store_connector("aiven.public.orders_v", telemetry=None)
    store.create_view("aiven.public.orders_v", "SELECT 1 AS one")

    kind, resolved = resolve_relation("aiven.public.orders_v", None)

    assert kind == "view"
    assert resolved is not None


def test_the_store_never_answers_with_a_dataset_for_a_bound_workspace(clean_registry):
    # The data answer is the data binding's to give. A dataset document in the
    # catalog entry of a bound workspace is one that workspace cannot domicile,
    # so it is not honoured here.
    from opteryx.managers.views import resolve_relation

    class _StoreWithADataset(CatalogConnector):
        def get_relation(self, relation):
            return "dataset", "handle"

    set_workspace_resolver(lambda workspace: Resolution(DataConnector, {"marker": "data"}))
    set_workspace_settings_resolver(lambda workspace: Resolution(_StoreWithADataset, {}))

    kind, resolved = resolve_relation("aiven.public.orders", None)

    assert kind is None
    assert resolved is None
