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
from opteryx.exceptions import CollectionNotEmptyError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


class _FakeCatalog:
    """Records the calls the connector makes, standing in for the real catalog."""

    calls = []
    collection_is_empty = True
    # Datasets the catalog should report as absent; everything else exists.
    # RENAME needs both answers - the source present, the target not.
    missing_datasets = set()

    def __init__(self, workspace=None, **kwargs):
        pass

    def dataset_exists(self, identifier):
        return identifier not in _FakeCatalog.missing_datasets

    def drop_dataset(self, identifier, author=None):
        _FakeCatalog.calls.append(("drop_dataset", identifier, author))

    def drop_view(self, identifier, author=None):
        _FakeCatalog.calls.append(("drop_view", identifier, author))

    def create_dataset(self, identifier, schema, properties=None, author=None):
        _FakeCatalog.calls.append(("create_dataset", identifier, author))

    def update_dataset_sort_order(self, identifier, columns, author=None):
        _FakeCatalog.calls.append(("update_dataset_sort_order", identifier, columns, author))

    def get_relation(self, identifier):
        return (None, None)

    def rename_dataset(self, identifier, new_identifier, author=None):
        _FakeCatalog.calls.append(("rename_dataset", identifier, new_identifier, author))

    def set_workspace_properties(self, properties, author=None):
        _FakeCatalog.calls.append(("set_workspace_properties", properties, author))

    def collection_exists(self, collection):
        return True

    def create_collection(self, collection, properties=None, exists_ok=False, author=None):
        _FakeCatalog.calls.append(("create_collection", collection, exists_ok, author))

    def drop_collection(self, collection, author=None):
        from opteryx_catalog.exceptions import CollectionNotEmpty

        if not _FakeCatalog.collection_is_empty:
            raise CollectionNotEmpty(f"Collection is not empty: {collection}")
        _FakeCatalog.calls.append(("drop_collection", collection, author))


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    _FakeCatalog.collection_is_empty = True
    _FakeCatalog.missing_datasets = set()
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


def test_cluster_by_delegates_to_catalog_with_user(catalog_workspace):
    """ALTER TABLE ... CLUSTER BY reaches the catalog carrying the session user."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl CLUSTER BY (name)"))

    assert catalog_workspace.calls == [
        ("update_dataset_sort_order", "coll.tbl", ["name"], "alice")
    ]


def test_cluster_by_multi_column_preserves_order(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl CLUSTER BY (region, name)"))

    assert catalog_workspace.calls == [
        ("update_dataset_sort_order", "coll.tbl", ["region", "name"], "alice")
    ]


def test_cluster_by_unauthenticated_passes_none(catalog_workspace):
    """No session user means no author - not an invented one."""
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl CLUSTER BY (name)"))

    assert catalog_workspace.calls == [
        ("update_dataset_sort_order", "coll.tbl", ["name"], None)
    ]


def test_cluster_by_requires_owner_on_catalog(catalog_workspace):
    """The owner-only rule applies to CLUSTER BY too - a writer cannot change layout."""
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])

    with pytest.raises(PermissionError, match="permission to alter table"):
        list(writer.execute_to_morsels("ALTER TABLE cat.coll.tbl CLUSTER BY (name)"))

    assert catalog_workspace.calls == []


def test_drop_collection_delegates_to_catalog_with_user(catalog_workspace):
    """DROP COLLECTION reaches the catalog carrying the session user."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP COLLECTION cat.coll"))

    assert catalog_workspace.calls == [("drop_collection", "coll", "alice")]


def test_drop_collection_unauthenticated_passes_none(catalog_workspace):
    """No session user means no dropper - not an invented one."""
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP COLLECTION cat.coll"))

    assert catalog_workspace.calls == [("drop_collection", "coll", None)]


def test_drop_collection_requires_owner_on_workspace(catalog_workspace):
    """DROP COLLECTION is owner-only, same tier as DROP TABLE/VIEW - a writer cannot do it."""
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])

    with pytest.raises(PermissionError, match="permission to drop collection"):
        list(writer.execute_to_morsels("DROP COLLECTION cat.coll"))

    assert catalog_workspace.calls == []


def test_drop_collection_rejects_non_empty(catalog_workspace):
    """A non-empty collection is rejected rather than cascade-dropped."""
    catalog_workspace.collection_is_empty = False
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(CollectionNotEmptyError):
        list(session.execute_to_morsels("DROP COLLECTION cat.coll"))

    assert catalog_workspace.calls == []


def test_rename_delegates_to_catalog_with_user(catalog_workspace):
    """ALTER TABLE ... RENAME TO reaches the catalog carrying the session user,
    with the workspace stripped from both names."""
    catalog_workspace.missing_datasets = {"coll.renamed"}
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl RENAME TO cat.coll.renamed"))

    assert catalog_workspace.calls == [("rename_dataset", "coll.tbl", "coll.renamed", "alice")]


def test_rename_moves_between_collections(catalog_workspace):
    """A rename may change the collection as well as the dataset name."""
    catalog_workspace.missing_datasets = {"newcoll.newtbl"}
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl RENAME TO cat.newcoll.newtbl"))

    assert catalog_workspace.calls == [("rename_dataset", "coll.tbl", "newcoll.newtbl", "alice")]


def test_rename_unauthenticated_passes_none(catalog_workspace):
    """No session user means no author - not an invented one."""
    catalog_workspace.missing_datasets = {"coll.renamed"}
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl RENAME TO cat.coll.renamed"))

    assert catalog_workspace.calls == [("rename_dataset", "coll.tbl", "coll.renamed", None)]


def test_rename_requires_owner_on_source(catalog_workspace):
    """A rename destroys the source name, so it is owner-only there."""
    catalog_workspace.missing_datasets = {"coll.renamed"}
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])

    with pytest.raises(PermissionError, match="permission to rename table cat.coll.tbl"):
        list(writer.execute_to_morsels("ALTER TABLE cat.coll.tbl RENAME TO cat.coll.renamed"))

    assert catalog_workspace.calls == []


def test_rename_requires_grant_on_target(catalog_workspace):
    """Owning the source does not license moving it into a collection the user
    has no grant on."""
    catalog_workspace.missing_datasets = {"locked.tbl"}
    session = opteryx.session(
        user="alice", access_policies=[{"pattern": "cat.coll.*", "role": "owner"}]
    )

    with pytest.raises(PermissionError, match="permission to rename table to cat.locked.tbl"):
        list(session.execute_to_morsels("ALTER TABLE cat.coll.tbl RENAME TO cat.locked.tbl"))

    assert catalog_workspace.calls == []


def test_alter_workspace_delegates_to_catalog_with_user(catalog_workspace):
    """ALTER WORKSPACE reaches the catalog with the property already typed."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO OFF"))

    assert catalog_workspace.calls == [
        ("set_workspace_properties", {"delete_protection": False}, "alice")
    ]


def test_alter_workspace_on_maps_to_true(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO ON"))

    assert catalog_workspace.calls == [
        ("set_workspace_properties", {"delete_protection": True}, "alice")
    ]


def test_alter_workspace_accepts_boolean_literals(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO TRUE"))

    assert catalog_workspace.calls == [
        ("set_workspace_properties", {"delete_protection": True}, "alice")
    ]


def test_alter_workspace_unauthenticated_passes_none(catalog_workspace):
    """No session user means no author - not an invented one."""
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO OFF"))

    assert catalog_workspace.calls == [
        ("set_workspace_properties", {"delete_protection": False}, None)
    ]


def test_alter_workspace_requires_owner(catalog_workspace):
    """A writer cannot change workspace-level settings."""
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])

    with pytest.raises(PermissionError, match="permission to alter workspace cat"):
        list(writer.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO OFF"))

    assert catalog_workspace.calls == []


def test_alter_workspace_owner_within_workspace_is_not_enough(catalog_workspace):
    """Owning everything *inside* a workspace does not make you owner *of* it -
    workspace-level settings need a grant naming the workspace itself."""
    session = opteryx.session(
        user="alice", access_policies=[{"pattern": "cat.*", "role": "owner"}]
    )

    with pytest.raises(PermissionError, match="permission to alter workspace cat"):
        list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO OFF"))

    assert catalog_workspace.calls == []


def test_alter_workspace_named_owner_grant_is_enough(catalog_workspace):
    """A grant naming the workspace itself is what unlocks it."""
    session = opteryx.session(user="alice", access_policies=[{"pattern": "cat", "role": "owner"}])
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET delete_protection TO OFF"))

    assert catalog_workspace.calls == [
        ("set_workspace_properties", {"delete_protection": False}, "alice")
    ]


def test_create_collection_delegates_to_catalog_with_user(catalog_workspace):
    """CREATE COLLECTION reaches the catalog carrying the session user."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE COLLECTION cat.coll"))

    assert catalog_workspace.calls == [("create_collection", "coll", False, "alice")]


def test_create_collection_if_not_exists_passes_exists_ok(catalog_workspace):
    """IF NOT EXISTS is settled by the catalog in one atomic call, not by an
    exists-check here - the connector must forward it, not pre-check."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE COLLECTION IF NOT EXISTS cat.coll"))

    assert catalog_workspace.calls == [("create_collection", "coll", True, "alice")]


def test_create_collection_unauthenticated_passes_none(catalog_workspace):
    """No session user means no author - not an invented one."""
    session = opteryx.session(access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE COLLECTION cat.coll"))

    assert catalog_workspace.calls == [("create_collection", "coll", False, None)]


def test_create_collection_allowed_for_writer(catalog_workspace):
    """Creating a collection risks nothing existing, so it is the writer tier -
    NOT the owner tier DROP COLLECTION requires."""
    writer = opteryx.session(user="wendy", access_policies=[{"pattern": "*", "role": "writer"}])
    list(writer.execute_to_morsels("CREATE COLLECTION cat.coll"))

    assert catalog_workspace.calls == [("create_collection", "coll", False, "wendy")]


def test_create_collection_rejected_for_reader(catalog_workspace):
    reader = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])

    with pytest.raises(PermissionError, match="permission to create collection"):
        list(reader.execute_to_morsels("CREATE COLLECTION cat.coll"))

    assert catalog_workspace.calls == []


def test_create_schema_is_an_alias_for_create_collection(catalog_workspace):
    """DROP SCHEMA already aliases DROP COLLECTION; CREATE matches it."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE SCHEMA cat.coll"))

    assert catalog_workspace.calls == [("create_collection", "coll", False, "alice")]
