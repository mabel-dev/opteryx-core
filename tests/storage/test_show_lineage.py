# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW LINEAGE FOR <table> - a relation's receipts.

Driven through a fake catalog, as test_show_snapshots.py is: what is under
test is the shape the engine produces from the receipts (opteryx-catalog
PROVENANCE_DESIGN.md S6.1), the three-state rule that keeps "not recorded"
apart from "read nothing", the existence lookup for each named source, and
the S4.4 elision of names the caller may not READ.

The history has one snapshot in each of the three states, deliberately, so a
single statement exercises all three rows-shapes at once.
"""

import datetime
from types import SimpleNamespace

import pytest

import opteryx
from opteryx import managers
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.permissions import register_permissions_capability
from opteryx_catalog.exceptions import DatasetNotFound

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_T0 = 1786421691019
_T1 = 1786500897203
_T2 = 1786587302881

_OLDEST, _MIDDLE, _HEAD = 7283449002, 7283774102, 7284091337

# Sources named by the head's receipt. `cat.coll1.a` is in the target's own
# workspace; `other.coll.b` is in a second workspace the connector has to
# resolve a catalog for; `cat.coll1.new` had no commits when it was read.
_A, _B = "cat.coll1.a", "other.coll.b"
_A_SNAPSHOT, _B_SNAPSHOT = 1786400000001, 1786400000002

_HEAD_RECEIPT = [
    # Listed out of name order: the statement, not the store, decides.
    {"dataset": _B, "snapshot-id": _B_SNAPSHOT, "resolved-by": "version"},
    {"dataset": "cat.coll1.new", "snapshot-id": None, "resolved-by": "current"},
    {"dataset": _A, "snapshot-id": _A_SNAPSHOT, "resolved-by": "current"},
]


def _snapshot(snapshot_id, timestamp_ms, parent=None, **provenance):
    """A catalog Snapshot. `read_sources` / `produced_by` are set ONLY when
    passed, so a snapshot built without them has no such attribute at all -
    which is what a record from a catalog older than the feature looks like,
    and is the "not recorded" state."""
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        timestamp_ms=timestamp_ms,
        author="seed",
        user_created=True,
        sequence_number=None,
        manifest_list=f"metadata/manifest-{snapshot_id}.parquet",
        operation_type="append",
        parent_snapshot_id=parent,
        schema_id="sch-0001",
        commit_message=None,
        summary={},
        **provenance,
    )


def _history():
    return [
        # Not in chronological order: the connector sorts.
        _snapshot(_MIDDLE, _T1, parent=_OLDEST, read_sources=[], produced_by=None),
        _snapshot(_OLDEST, _T0),
        _snapshot(
            _HEAD,
            _T2,
            parent=_MIDDLE,
            read_sources=list(_HEAD_RECEIPT),
            produced_by="task:cat.coll1.ingest",
        ),
    ]


class _FakeDataset:
    def __init__(self, history, with_history, live_snapshot_ids=()):
        self._history = history
        self._with_history = with_history
        self._live = set(live_snapshot_ids)
        self.metadata = SimpleNamespace(current_snapshot_id=_HEAD if history else None)

    def snapshot(self, snapshot_id=None):
        """For the TARGET, its history; for a SOURCE, the ids configured as
        still live - the receipt names an id and the question is only whether
        it still resolves."""
        if self._history:
            if snapshot_id is None:
                return max(self._history, key=lambda s: s.timestamp_ms)
            return next((s for s in self._history if s.snapshot_id == snapshot_id), None)
        if snapshot_id in self._live:
            return SimpleNamespace(snapshot_id=snapshot_id)
        return None

    def snapshots(self):
        return list(self._history) if self._with_history else []

    def previous_user_snapshot(self):
        return None

    def schema(self, schema_id=None):
        return SimpleNamespace(columns=[{"name": "id", "type": "INTEGER", "id": 1}], name="src")


class _FakeCatalog:
    """One class serves every workspace - the connector instantiates it per
    workspace name, which is how a cross-workspace source gets looked up."""

    loads = []
    history = []
    # (workspace, relative id) -> the snapshot ids that still exist there.
    live = {}
    # Relative ids that raise on load, by workspace, to drive the two "could
    # not look" outcomes apart: gone (False) and broken (unknown).
    gone = set()
    broken = set()

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace

    def load_dataset(self, identifier, load_history=False):
        _FakeCatalog.loads.append((self.workspace, identifier, load_history))
        if (self.workspace, identifier) in _FakeCatalog.gone:
            raise DatasetNotFound(identifier)
        if (self.workspace, identifier) in _FakeCatalog.broken:
            raise RuntimeError("catalog unreachable")
        if self.workspace == "cat" and identifier == "coll1.src":
            return _FakeDataset(_FakeCatalog.history, with_history=load_history)
        return _FakeDataset(
            [], with_history=False,
            live_snapshot_ids=_FakeCatalog.live.get((self.workspace, identifier), ()),
        )

    def list_tags(self, identifier):
        return []

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)

    def get_workspace_properties(self):
        return {"name": self.workspace}


class _ScriptedCapability:
    """Permits READ on exactly the names it is given. The elision under test
    is the engine's: which column it nulls, and that the row stays."""

    name = "scripted-lineage"

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
    """Restore the capability after every test, as test_permissions_capability
    does: a query marks the capability consulted, and a later registration
    would refuse."""
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
    _FakeCatalog.history = _history()
    _FakeCatalog.live = {
        ("cat", "coll1.a"): {_A_SNAPSHOT},
        # `other.coll.b` exists but the snapshot the receipt names has expired.
        ("other", "coll.b"): set(),
    }
    _FakeCatalog.gone = set()
    _FakeCatalog.broken = set()
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


def test_show_lineage_returns_the_column_set_in_order(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert list(rows[0].keys()) == [
        "snapshot_id",
        "committed_at",
        "is_current",
        "produced_by",
        "source_dataset",
        "source_snapshot_id",
        "resolved_by",
        "source_exists",
        "recorded",
    ]


def test_one_row_per_snapshot_and_source_newest_snapshot_first(catalog_workspace):
    """Three entries for the head, one marker each for the other two - and
    within the head, by source name, whatever order the receipt was stored in."""
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert [(row["snapshot_id"], row["source_dataset"]) for row in rows] == [
        (_HEAD, _A),
        (_HEAD, "cat.coll1.new"),
        (_HEAD, _B),
        (_MIDDLE, None),
        (_OLDEST, None),
    ]


def test_the_head_is_current_on_every_one_of_its_rows(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert [row["is_current"] for row in rows] == [True, True, True, False, False]


def test_committed_at_is_the_snapshot_timestamp(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert rows[0]["committed_at"] == datetime.datetime.fromtimestamp(
        _T2 / 1000, tz=datetime.timezone.utc
    ).replace(tzinfo=None)


def test_show_lineage_loads_the_dataset_with_history(catalog_workspace):
    """The receipt is a field of each snapshot document; without the history
    load only the head's would ever be seen."""
    _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert ("cat", "coll1.src", True) in catalog_workspace.loads


# --- the three states


def test_a_snapshot_with_no_receipt_is_one_marker_row_not_recorded(catalog_workspace):
    """The oldest snapshot predates the feature: it has no `read_sources`
    attribute at all. It is still LISTED, with `recorded` false, so a history
    with a gap shows the gap."""
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    marker = rows[-1]

    assert marker["snapshot_id"] == _OLDEST
    assert marker["recorded"] is False
    assert marker["source_dataset"] is None
    assert marker["source_snapshot_id"] is None
    assert marker["resolved_by"] is None
    assert marker["source_exists"] is None


def test_a_snapshot_that_read_nothing_is_one_row_recorded_with_null_sources(
    catalog_workspace,
):
    """`[]` is a fact - INSERT ... VALUES read no catalog relation - and the
    only thing telling it apart from the marker above is `recorded`."""
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    read_nothing = rows[-2]

    assert read_nothing["snapshot_id"] == _MIDDLE
    assert read_nothing["recorded"] is True
    assert read_nothing["source_dataset"] is None
    assert read_nothing["source_snapshot_id"] is None
    assert read_nothing["resolved_by"] is None
    assert read_nothing["source_exists"] is None


def test_a_snapshot_with_a_receipt_reports_each_entry(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    by_source = {row["source_dataset"]: row for row in rows[:3]}

    assert all(row["recorded"] is True for row in rows[:3])
    assert by_source[_A]["source_snapshot_id"] == _A_SNAPSHOT
    assert by_source[_A]["resolved_by"] == "current"
    assert by_source[_B]["source_snapshot_id"] == _B_SNAPSHOT
    assert by_source[_B]["resolved_by"] == "version"


def test_a_source_read_before_its_first_commit_has_a_null_snapshot(catalog_workspace):
    """Recorded, because the statement did read it; null, because there was
    nothing to read. Existence is null too: there is no snapshot to look for,
    and False would say one had expired."""
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    new = next(row for row in rows if row["source_dataset"] == "cat.coll1.new")

    assert new["recorded"] is True
    assert new["source_snapshot_id"] is None
    assert new["resolved_by"] == "current"
    assert new["source_exists"] is None


# --- produced_by


def test_produced_by_is_reported_per_snapshot(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert [row["produced_by"] for row in rows] == [
        "task:cat.coll1.ingest",
        "task:cat.coll1.ingest",
        "task:cat.coll1.ingest",
        None,
        None,
    ]


# --- source_exists


def test_source_exists_is_true_when_the_named_snapshot_is_still_live(catalog_workspace):
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    a = next(row for row in rows if row["source_dataset"] == _A)

    assert a["source_exists"] is True


def test_source_exists_is_false_when_the_named_snapshot_has_expired(catalog_workspace):
    """A receipt does not pin what it names. `other.coll.b` is there; the
    version the head was built from is not - and it is resolved through the
    OTHER workspace's catalog, which is the point of naming sources fully."""
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    b = next(row for row in rows if row["source_dataset"] == _B)

    assert b["source_exists"] is False
    assert ("other", "coll.b", False) in catalog_workspace.loads


def test_source_exists_is_false_when_the_source_dataset_is_gone(catalog_workspace):
    catalog_workspace.gone = {("other", "coll.b")}
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    b = next(row for row in rows if row["source_dataset"] == _B)

    assert b["source_exists"] is False


def test_source_exists_is_null_when_the_lookup_fails(catalog_workspace):
    """Unknown, and never a failed statement: the receipt is the history, and
    a source workspace being unreachable must not take the history down."""
    catalog_workspace.broken = {("other", "coll.b")}
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")
    b = next(row for row in rows if row["source_dataset"] == _B)

    assert b["source_exists"] is None
    assert len(rows) == 5


def test_a_source_named_by_many_snapshots_is_looked_up_once(catalog_workspace):
    """Fifty appends against a slow-moving source name the same version fifty
    times; that is one catalog read, not fifty."""
    catalog_workspace.history = [
        _snapshot(
            _HEAD - index,
            _T2 - index,
            read_sources=[{"dataset": _A, "snapshot-id": _A_SNAPSHOT, "resolved-by": "current"}],
        )
        for index in range(50)
    ]
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src")

    assert len(rows) == 50
    assert all(row["source_exists"] is True for row in rows)
    assert catalog_workspace.loads.count(("cat", "coll1.a", False)) == 1


# --- elision (S4.4)


def test_a_source_the_caller_cannot_read_is_nulled_and_the_row_stays(
    catalog_workspace, install
):
    """The name is the secret, not the existence of an upstream. `source_exists`
    goes with the name - whether a snapshot of a dataset you cannot name still
    exists is a fact about that dataset. The version and how it was resolved
    stay: they describe the read, not the relation."""
    capability = install(
        _ScriptedCapability(readable={"cat.coll1.src", _A, "cat.coll1.new", "cat.coll1.ingest"})
    )
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src", access_policies=None)

    assert len(rows) == 5
    hidden = rows[2]
    assert hidden["source_dataset"] is None
    assert hidden["source_exists"] is None
    assert hidden["source_snapshot_id"] == _B_SNAPSHOT
    assert hidden["resolved_by"] == "version"
    assert hidden["recorded"] is True
    # The visible neighbour is untouched.
    assert rows[0]["source_dataset"] == _A
    assert rows[0]["source_exists"] is True
    assert (_B, "READ") in capability.asked


def test_a_producer_the_caller_cannot_read_is_nulled_whole(catalog_workspace, install):
    """The prefix alone would still say "a task wrote this", which is the half
    of the fact the design keeps; but the value is name-shaped, so the whole
    value goes rather than a `task:` stub that looks like a broken name."""
    capability = install(_ScriptedCapability(readable={"cat.coll1.src", _A, _B, "cat.coll1.new"}))
    rows = _rows("SHOW LINEAGE FOR cat.coll1.src", access_policies=None)

    assert all(row["produced_by"] is None for row in rows)
    assert rows[0]["source_dataset"] == _A
    assert ("cat.coll1.ingest", "READ") in capability.asked


def test_elision_asks_once_per_name(catalog_workspace, install):
    capability = install(_ScriptedCapability(readable={"cat.coll1.src"}))
    _rows("SHOW LINEAGE FOR cat.coll1.src", access_policies=None)

    asked = [resource for resource, action in capability.asked if action == "READ"]
    for name in (_A, _B, "cat.coll1.new", "cat.coll1.ingest"):
        assert asked.count(name) == 1


def test_show_lineage_is_gated_at_read_on_the_relation(catalog_workspace, install):
    install(_ScriptedCapability(readable=set()))

    with pytest.raises(PermissionError):
        _rows("SHOW LINEAGE FOR cat.coll1.src", access_policies=None)


# --- edges


def test_a_relation_with_nothing_committed_has_no_rows(catalog_workspace):
    catalog_workspace.history = []

    assert _rows("SHOW LINEAGE FOR cat.coll1.src") == []


def test_a_connector_with_no_commit_log_says_so(tmp_path):
    register_workspace("nolog", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE nolog.dst AS SELECT 1 AS a"))

    with pytest.raises(UnsupportedSyntaxError, match="no lineage"):
        list(session.execute_to_morsels("SHOW LINEAGE FOR nolog.dst"))


# --- the grammar


def test_bare_show_lineage_is_rejected(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW LINEAGE FOR"):
        list(session.execute_to_morsels("SHOW LINEAGE"))


def test_show_lineage_from_is_not_the_spelling(catalog_workspace):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("SHOW LINEAGE FROM cat.coll1.src"))
