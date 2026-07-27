# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""DDL against a catalog-backed connector delegates to the catalog, carrying the
session user. The catalog records that user as the dropper on its tombstone, so
losing it here would leave every drop unattributed."""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


class _FakeCatalog:
    """Records the calls the connector makes, standing in for the real catalog."""

    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def dataset_exists(self, identifier):
        return True

    def drop_dataset(self, identifier, author=None):
        _FakeCatalog.calls.append(("drop_dataset", identifier, author))

    def drop_view(self, identifier, author=None):
        _FakeCatalog.calls.append(("drop_view", identifier, author))

    def create_dataset(self, identifier, schema, properties=None, author=None):
        _FakeCatalog.calls.append(("create_dataset", identifier, author))

    def get_relation(self, identifier):
        return (None, None)


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def test_drop_table_delegates_to_catalog_with_user(catalog_workspace):
    """DROP TABLE reaches the catalog carrying the session user as the dropper."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP TABLE cat.coll.tbl"))

    assert catalog_workspace.calls == [("drop_dataset", "coll.tbl", "alice")]


def test_drop_table_unauthenticated_passes_none(catalog_workspace):
    """No session user means no dropper - not an invented one."""
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP TABLE cat.coll.tbl"))

    assert catalog_workspace.calls == [("drop_dataset", "coll.tbl", None)]


def test_catalog_connector_is_writable():
    """The binder gates table DDL on this; without it DROP TABLE cannot bind."""
    from opteryx.connectors.capabilities import Writable

    assert issubclass(OpteryxConnector, Writable)


def test_drop_table_requires_owner_on_catalog(catalog_workspace):
    """The owner-only rule applies to catalog-backed relations too."""
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])

    with pytest.raises(PermissionError, match="permission to drop table"):
        list(writer.execute_to_morsels("DROP TABLE cat.coll.tbl"))

    assert catalog_workspace.calls == []
