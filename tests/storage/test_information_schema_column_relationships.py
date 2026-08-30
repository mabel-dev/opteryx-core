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

    _RELATIONSHIP = {
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

    def list_relationships(self, identifier):
        _FakeCatalog.calls.append(("list_relationships", identifier))
        if identifier != "helpdesk.tickets":
            return []
        return [dict(_FakeCatalog._RELATIONSHIP)]

    def list_workspace_relationships(self):
        """The whole workspace in one read - what the reader uses when no
        predicate pins a dataset. Each row carries its own near address, which
        is what makes enumerating datasets unnecessary."""
        _FakeCatalog.calls.append(("list_workspace_relationships",))
        return [dict(_FakeCatalog._RELATIONSHIP)]

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
    THIS dataset", and the pushdown turns that into one keyed subcollection
    read."""
    _FakeCatalog.calls = []
    rows = _read(where=" WHERE table_name = 'helpdesk.tickets'")

    assert len(rows) == 1
    assert _FakeCatalog.calls == [("list_relationships", "helpdesk.tickets")]


def test_the_reader_never_enumerates_datasets(catalog_workspace):
    """The whole point of the read shape. Enumerating collections and then each
    one's datasets costs `1 + collections + datasets` sequential round trips to
    return rows that number in the tens, and it grows with the workspace rather
    than with the relationships in it. Neither read may do it - so this asserts
    on the calls, not on the rows, which were identical before and after."""
    _FakeCatalog.calls = []
    assert len(_read()) == 1
    assert _FakeCatalog.calls == [("list_workspace_relationships",)]

    _FakeCatalog.calls = []
    assert len(_read(where=" WHERE table_name = 'helpdesk.tickets'")) == 1
    assert _FakeCatalog.calls == [("list_relationships", "helpdesk.tickets")]


def test_a_predicate_on_the_collection_still_filters(catalog_workspace):
    """constraint_collection no longer prunes a round trip - it is pushed so it
    settles here rather than in a Filter node, and it must still be applied."""
    assert len(_read(where=" WHERE constraint_collection = 'helpdesk'")) == 1
    assert _read(where=" WHERE constraint_collection = 'crm'") == []


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


# --- inferred proposals -------------------------------------------------
#
# A proposal is a row like any other here, and that is deliberate: it is
# VISIBLE (in this projection and in the Studio, carrying its evidence) but
# INERT (never a NavigationProperty - that filter lives in odata.opteryx).
#
# What must NOT be different is the visibility rule. The inference job runs as
# a service identity and can see pairs no single person can, so a proposal it
# writes may name a dataset the reader holds no grant on. The both-ends check
# is what stops that reaching them, and it has to cover inferred rows exactly
# as it covers asserted ones.

_PROPOSAL = {
    **_FakeCatalog._RELATIONSHIP,
    "name": "inferred_customer_ref_a1b2c3d4e5f60718",
    "origin": "inferred",
    "status": "unverified",
    "confidence": 0.94,
    "evidence": {"overlap": 0.94, "values-compared": 1685},
    "asserted-by": None,
    "asserted-at-ms": None,
    "proposed-by": "inference-job",
}


@pytest.fixture
def proposal_workspace(monkeypatch):
    """A catalog whose only relationship is an unconfirmed proposal."""
    _FakeCatalog.calls = []
    monkeypatch.setattr(
        _FakeCatalog, "list_workspace_relationships", lambda self: [dict(_PROPOSAL)]
    )
    monkeypatch.setattr(
        _FakeCatalog,
        "list_relationships",
        lambda self, identifier: (
            [dict(_PROPOSAL)] if identifier == "helpdesk.tickets" else []
        ),
    )
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def test_a_proposal_is_visible_with_its_evidence(proposal_workspace):
    """Visible but inert. The owner has to be able to see it to judge it, and
    the evidence is what they judge -- a bare confidence number is not
    something anyone can act on."""
    (row,) = _read()
    assert row["origin"] == "inferred"
    assert row["status"] == "unverified"
    assert row["confidence"] == pytest.approx(0.94)
    assert '"overlap":0.94' in row["evidence"]
    assert '"values-compared":1685' in row["evidence"]
    # A proposal has no author. The job's name goes in `proposed-by`, which is
    # not this column: filling it in here would make a guess read as a person's
    # statement to everything that reads the graph.
    assert row["asserted_by"] is None


def test_an_asserted_row_carries_no_confidence_or_evidence(catalog_workspace):
    """Absent, not zero. A relationship someone typed was never measured, and
    an empty evidence object would suggest it had been."""
    (row,) = _read()
    assert row["confidence"] is None
    assert row["evidence"] is None


def test_a_proposal_naming_an_unreadable_dataset_is_not_emitted(
    proposal_workspace, permissions_state
):
    """The disclosure the inference job creates, and the check that closes it.

    The job runs as a service identity, so it can compare `helpdesk.tickets`
    against `crm.customers` for a caller who holds nothing on `crm`. The
    proposal it writes NAMES that dataset and its column. Emitting it would
    tell the caller that `crm.customers.id` exists and that their data lines up
    with it -- a leak the job itself created, out of a pair no person could
    have put together.

    Nothing here is special-cased for inferred rows. This asserts that the
    both-ends rule already covers them, which is the whole reason the job may
    run as one identity and be read by many.
    """
    _install(permissions_state, readable={"cat.helpdesk.tickets", "cat.helpdesk.agents"})
    assert _read() == []


def test_a_proposal_is_emitted_when_both_ends_are_readable(
    proposal_workspace, permissions_state
):
    """The other half: the rule is not simply refusing every inferred row."""
    _install(
        permissions_state,
        readable={"cat.helpdesk.tickets", "cat.helpdesk.agents", "cat.crm.customers"},
    )
    (row,) = _read()
    assert row["origin"] == "inferred"
