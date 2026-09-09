# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW SOURCES FOR <table> - a relation's standing source list.

The list is a field on the dataset document (opteryx-catalog
PROVENANCE_DESIGN.md S2.2), so unlike SHOW SNAPSHOTS and SHOW LINEAGE this
statement must NOT load the history: the point of materialising the list is
that reading it costs one document. That, the shape, the one-row empty case
that keeps `complete` readable, and S4.4 elision are what is tested here.
"""

from types import SimpleNamespace

import pytest

import opteryx
from opteryx import managers
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.permissions import register_permissions_capability

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_A, _B = "cat.coll1.a", "other.coll.b"


class _FakeDataset:
    def __init__(self, sources, complete, with_history):
        self._with_history = with_history
        self.metadata = SimpleNamespace(current_snapshot_id=1, sources=sources, sources_complete=complete)
        if sources is None:
            # A dataset object from a catalog older than the field has NO
            # `sources` attribute, rather than an empty one.
            del self.metadata.sources
            del self.metadata.sources_complete

    def snapshot(self, snapshot_id=None):
        return SimpleNamespace(snapshot_id=1, timestamp_ms=1786421691019)

    def snapshots(self):
        raise AssertionError("SHOW SOURCES must not read the history")

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=[{"name": "id", "type": "INTEGER", "id": 1}], name="src")


class _FakeCatalog:
    loads = []
    sources = []
    complete = True

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace

    def load_dataset(self, identifier, load_history=False):
        _FakeCatalog.loads.append((identifier, load_history))
        return _FakeDataset(_FakeCatalog.sources, _FakeCatalog.complete, load_history)

    def list_tags(self, identifier):
        return []

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _ScriptedCapability:
    name = "scripted-sources"

    def __init__(self, readable):
        self.readable = set(readable)
        self.asked = []

    def can_perform_action(self, execution_context, resource, action):
        self.asked.append((resource, action))
        return action == "READ" and resource in self.readable

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return False

    def can_principal_perform_action(self, principal, resource, action):
        return False

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return []

    def apply_grant(self, execution_context, pattern, role, principal):
        raise AssertionError("not reached")

    def apply_revoke(self, execution_context, pattern, role, principal):
        raise AssertionError("not reached")

    def grants_on(self, execution_context, pattern):
        raise AssertionError("not reached")

    def effective_grants_on(self, execution_context, pattern):
        raise AssertionError("not reached")

    def effective_grants_in(self, execution_context, workspace, objects):
        raise AssertionError("not reached")


@pytest.fixture(autouse=True)
def permissions_state():
    module = managers.permissions
    saved = module._active, module._consulted
    yield
    module._active, module._consulted = saved


@pytest.fixture
def install():
    module = managers.permissions

    def _install(capability):
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(capability)
        return capability

    return _install


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.loads = []
    _FakeCatalog.sources = [_A, _B]
    _FakeCatalog.complete = True
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _rows(statement, user="olive", access_policies=_OWNER_POLICY):
    session = opteryx.session(user=user, access_policies=access_policies)
    collected = []
    for morsel in session.execute_to_morsels(statement):
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        length = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(length):
            collected.append({key: values[index] for key, values in pydict.items()})
    return collected


# --- the shape


def test_show_sources_returns_the_column_set_in_order(catalog_workspace):
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert list(rows[0].keys()) == ["source_dataset", "position", "complete"]


def test_one_row_per_name_most_recent_first_with_its_position(catalog_workspace):
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert [(row["source_dataset"], row["position"]) for row in rows] == [(_A, 0), (_B, 1)]


def test_complete_is_repeated_on_every_row(catalog_workspace):
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert [row["complete"] for row in rows] == [True, True]


def test_an_incomplete_list_says_so_on_every_row(catalog_workspace):
    catalog_workspace.complete = False
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert [row["complete"] for row in rows] == [False, False]


def test_an_empty_list_is_one_null_row_so_complete_is_still_readable(catalog_workspace):
    """"Built from nothing that was recorded" and "built from nothing" differ
    only in `complete`, and a statement with no rows could not say which."""
    catalog_workspace.sources = []
    catalog_workspace.complete = False
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert rows == [{"source_dataset": None, "position": None, "complete": False}]


def test_a_catalog_without_the_field_reports_complete_as_unknown(catalog_workspace):
    """Null, not false: the catalog did not say, and a fabricated `false`
    would claim it had."""
    catalog_workspace.sources = None
    rows = _rows("SHOW SOURCES FOR cat.coll1.src")

    assert rows == [{"source_dataset": None, "position": None, "complete": None}]


def test_show_sources_does_not_load_the_history(catalog_workspace):
    """One document read is the budget (S2.2); the fake's `snapshots()`
    raises, so a history walk would have failed the statement outright."""
    _rows("SHOW SOURCES FOR cat.coll1.src")

    assert all(load_history is False for _, load_history in catalog_workspace.loads)


# --- elision (S4.4)


def test_a_name_the_caller_cannot_read_is_still_named(catalog_workspace, install):
    """Every source is named (decision 2026-09-09); see the same test in
    test_show_lineage.py for why. The gate is READ on the relation asked
    about, and no name is checked against the caller at all."""
    capability = install(_ScriptedCapability(readable={"cat.coll1.src", _A}))
    rows = _rows("SHOW SOURCES FOR cat.coll1.src", access_policies=None)

    assert [(row["source_dataset"], row["position"], row["complete"]) for row in rows] == [
        (_A, 0, True),
        (_B, 1, True),
    ]
    assert (_B, "READ") not in capability.asked


def test_show_sources_is_gated_at_read_on_the_relation(catalog_workspace, install):
    install(_ScriptedCapability(readable=set()))

    with pytest.raises(PermissionError):
        _rows("SHOW SOURCES FOR cat.coll1.src", access_policies=None)


# --- edges and grammar


def test_a_connector_with_no_commit_log_says_so(tmp_path):
    register_workspace("nolog", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE nolog.dst AS SELECT 1 AS a"))

    with pytest.raises(UnsupportedSyntaxError, match="no source list"):
        list(session.execute_to_morsels("SHOW SOURCES FOR nolog.dst"))


def test_bare_show_sources_is_rejected(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW SOURCES FOR"):
        list(session.execute_to_morsels("SHOW SOURCES"))


def test_show_sources_from_is_not_the_spelling(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("SHOW SOURCES FROM cat.coll1.src"))
