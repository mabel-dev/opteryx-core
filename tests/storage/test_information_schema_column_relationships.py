# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.column_relationships` -- reading declared relationships back.

A relationship is a declaration that two columns hold corresponding values.
Nothing enforces it, and this projection is the only way to read one back.

What this file is really about is the second READ check. Every other table in
information_schema describes ONE dataset per row and checks one grant. A
relationship row names two datasets -- the far one's collection, dataset and
column -- so a single check would disclose the existence and shape of data the
caller has no grant on. Rows are built only where both ends are readable.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_NOW_MS = 1754000000000  # 2025-07-31T21:33:20Z


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        count = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(count):
            row = {}
            for key, values in pydict.items():
                value = values[index]
                if isinstance(value, bytes):
                    value = value.decode()
                row[key] = value
            rows.append(row)
    return rows


class _FakeCatalog:
    """Two collections. `helpdesk.tickets` declares one relationship pointing at
    `crm.customers`, which lives in the other collection -- so a caller granted
    only `helpdesk` can read the near end and not the far one."""

    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        _FakeCatalog.calls.append(("list_collections",))
        return ["helpdesk", "crm"]

    def list_datasets(self, collection):
        _FakeCatalog.calls.append(("list_datasets", collection))
        return {"helpdesk": ["tickets", "agents"], "crm": ["customers"]}[collection]

    def list_relationships(self, identifier):
        _FakeCatalog.calls.append(("list_relationships", identifier))
        if identifier != "helpdesk.tickets":
            return []
        return [
            {
                "name": "tickets_customer_fk",
                "kind": "maps",
                "workspace": "cat",
                "collection": "helpdesk",
                "dataset": "tickets",
                "column": "customer_ref",
                "references-workspace": "cat",
                "references-collection": "crm",
                "references-dataset": "customers",
                "references-column": "id",
                "cardinality": "many_to_one",
                "origin": "asserted",
                "status": "active",
                "asserted-by": "olive",
                "asserted-at-ms": _NOW_MS,
                "verified-at-ms": None,
            }
        ]

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _ScriptedCapability:
    """Permits READ on exactly the resources it is told to.

    Needed because the intrinsic capability permits everything: a session's
    `access_policies` decide nothing on their own, so a test that only varied
    them would pass whatever the engine did with the far end.
    """

    name = "scripted"

    def __init__(self, readable):
        self.readable = set(readable)

    def can_perform_action(self, execution_context, resource, action):
        if "." not in resource:
            return action == "READ"
        return resource in self.readable

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return True

    def can_principal_perform_action(self, principal, resource, action):
        return False

    def can_principal_own_materialized_view(self, principal):
        return False

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


@pytest.fixture
def permissions_state():
    """Restore the capability module afterwards.

    Merely RUNNING a query marks the capability as consulted, and the module
    refuses a registration after that -- correct in a process, unworkable
    across tests in one interpreter.
    """
    from opteryx import managers

    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield module
    module._active, module._consulted = saved_active, saved_consulted


def _install(module, readable):
    from opteryx.managers.permissions import register_permissions_capability

    module._active, module._consulted = module._CORE, False
    register_permissions_capability(_ScriptedCapability(readable))


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(policies=_OWNER_POLICY, where=""):
    session = opteryx.session(user="alice", access_policies=policies)
    return _morsels_to_rows(
        session.execute_to_morsels(f"SELECT * FROM cat.information_schema.column_relationships{where}")
    )


def test_row_shape(catalog_workspace):
    rows = _read()

    assert len(rows) == 1
    row = rows[0]
    assert row["constraint_catalog"] == "cat"
    assert row["constraint_collection"] == "helpdesk"
    assert row["constraint_name"] == "tickets_customer_fk"
    assert row["table_name"] == "helpdesk.tickets"
    assert row["column_name"] == "customer_ref"
    assert row["referenced_table_name"] == "crm.customers"
    assert row["referenced_column_name"] == "id"
    assert row["relationship_kind"] == "maps"
    assert row["cardinality"] == "many_to_one"
    assert row["origin"] == "asserted"
    assert row["status"] == "active"
    assert row["asserted_by"] == "olive"
    assert row["asserted_at"] is not None
    assert row["verified_at"] is None


def test_a_row_needs_BOTH_ends_readable(catalog_workspace, permissions_state):
    """The check every other information_schema table does not have.

    The caller can read the near dataset and holds nothing on the far one. A
    single READ check would show them `crm.customers.id` exists -- its
    collection, its dataset and its column name -- which is most of what was
    worth hiding.
    """
    _install(permissions_state, readable={"cat.helpdesk.tickets", "cat.helpdesk.agents"})
    assert _read() == []


def test_both_ends_readable_yields_the_row(catalog_workspace, permissions_state):
    """The other direction, so the test above is not passing for some unrelated
    reason -- adding the far grant is the only change."""
    _install(
        permissions_state,
        readable={"cat.helpdesk.tickets", "cat.helpdesk.agents", "cat.crm.customers"},
    )
    assert len(_read()) == 1


def test_an_unreadable_near_end_yields_nothing(catalog_workspace, permissions_state):
    _install(permissions_state, readable={"cat.crm.customers"})
    assert _read() == []


def test_a_predicate_on_the_table_prunes_the_catalog_round_trips(catalog_workspace):
    """This is where the read pattern pays: `$metadata` asks "what relates to
    THIS dataset", and the pushdown turns that into one listing rather than one
    per dataset in the workspace."""
    _FakeCatalog.calls = []
    rows = _read(where=" WHERE table_name = 'helpdesk.tickets'")

    assert len(rows) == 1
    listed = [call[1] for call in _FakeCatalog.calls if call[0] == "list_relationships"]
    assert listed == ["helpdesk.tickets"]


def test_a_predicate_on_the_catalog_skips_enumeration_entirely(catalog_workspace):
    _FakeCatalog.calls = []
    assert _read(where=" WHERE constraint_catalog = 'other'") == []
    assert _FakeCatalog.calls == []


def test_denies_without_execution_context():
    """Secure by default: no execution context means zero rows, not all rows."""
    from opteryx.connectors.information_schema import InformationSchemaColumnRelationshipsTable

    table = InformationSchemaColumnRelationshipsTable(
        dataset="information_schema.column_relationships",
        catalog=_FakeCatalog(),
        workspace="cat",
        telemetry=None,
    )
    rows = _morsels_to_rows(table.read_dataset())
    assert rows == []
