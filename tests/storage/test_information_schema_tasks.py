# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.tasks` -- listing the workspace's tasks.

Before this table there was no listing at all: a task read only as the single
row of its own definition, so seeing what a workspace runs cost one request per
task and knowing every name in advance.

The column that earns the table is `writes`. A trigger records which dataset
FIRES a task; nothing recorded which dataset it FEEDS, so a pipeline of
`raw -> task -> curated` read as disconnected fragments. `writes` is derived
from the task's own statement at registration, never declared, so it cannot
disagree with what the task will do.
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


_TASKS = {
    "pipelines.curate": {
        "sql": "INSERT INTO pipelines.curated SELECT * FROM raw.events",
        "writes": ["pipelines.curated"],
    },
    "pipelines.audit": {
        # Reads and writes nothing back: not a pipeline edge, and it says so.
        "sql": "SELECT count(*) FROM raw.events",
        "writes": [],
    },
    "reporting.rebuild": {
        "sql": "TRUNCATE TABLE reporting.a, reporting.b",
        "writes": ["reporting.a", "reporting.b"],
    },
}


class _FakeCatalog:
    """Two collections holding three tasks between them."""

    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        _FakeCatalog.calls.append(("list_collections",))
        return ["pipelines", "reporting"]

    def list_datasets(self, collection):
        return []

    def list_tasks(self, collection):
        _FakeCatalog.calls.append(("list_tasks", collection))
        return [
            name.split(".")[1]
            for name in _TASKS
            if name.split(".")[0] == collection
        ]

    def get_task(self, identifier):
        _FakeCatalog.calls.append(("get_task", identifier))
        task = _TASKS[identifier]
        collection, name = identifier.split(".")
        return {
            "identifier": f"cat.{identifier}",
            "name": name,
            "collection": collection,
            "workspace": "cat",
            "sql": task["sql"],
            "statement-id": str(_NOW_MS),
            "writes": list(task["writes"]),
            "description": None,
            "created-by": "olive",
            "created-at-ms": _NOW_MS,
            "last-updated-by": "rhea",
            "last-updated-at-ms": _NOW_MS,
            "suspended-at-ms": None,
            "suspended-by": None,
            "last-fired-at-ms": _NOW_MS,
            "last-fired-status": "queued",
            "last-window-to": 7,
        }

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _ScriptedCapability:
    """Permits READ on the resources it is told to, and AUTOMATE on a
    separate set - a task row is gated on AUTOMATE, so the two have to be
    distinguishable for the gate to be testable."""

    name = "scripted"

    def __init__(self, readable, automatable=()):
        self.readable = set(readable)
        self.automatable = set(automatable)

    def can_perform_action(self, execution_context, resource, action):
        if "." not in resource:
            return action == "READ"
        if action == "AUTOMATE":
            return resource in self.automatable
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

    def effective_grants_in(self, execution_context, workspace, objects):
        raise AssertionError("not reached")


@pytest.fixture
def permissions_state():
    """Restore the capability module afterwards - running a query marks it
    consulted, and the module refuses a registration after that."""
    from opteryx import managers

    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield module
    module._active, module._consulted = saved_active, saved_consulted


def _install(module, readable, automatable=()):
    from opteryx.managers.permissions import register_permissions_capability

    module._active, module._consulted = module._CORE, False
    register_permissions_capability(_ScriptedCapability(readable, automatable))


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(where=""):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    return _morsels_to_rows(
        session.execute_to_morsels(f"SELECT * FROM cat.information_schema.tasks{where}")
    )


def test_every_task_in_the_workspace_is_listed(catalog_workspace):
    rows = _read()

    assert sorted(row["task_name"] for row in rows) == ["audit", "curate", "rebuild"]


def test_row_shape(catalog_workspace):
    row = next(row for row in _read() if row["task_name"] == "curate")

    assert row["task_catalog"] == "cat"
    assert row["task_collection"] == "pipelines"
    assert row["statement"] == "INSERT INTO pipelines.curated SELECT * FROM raw.events"
    assert row["statement_id"] == str(_NOW_MS)
    assert row["writes"] == "pipelines.curated"
    assert row["created_by"] == "olive"
    assert row["last_updated_by"] == "rhea"
    assert row["last_fired_status"] == "queued"
    assert row["last_window_to"] == 7


def test_writes_carries_the_edge_out_of_the_task(catalog_workspace):
    """The whole point: `pipelines.curate` is fed by a trigger on some dataset
    and FEEDS `pipelines.curated`, so a pipeline can be followed through it."""
    rows = {row["task_name"]: row["writes"] for row in _read()}

    assert rows["curate"] == "pipelines.curated"
    # Several targets, comma-separated - unambiguous because a relation name
    # cannot contain a comma.
    assert rows["rebuild"] == "reporting.a,reporting.b"
    # A task that writes nothing says so, rather than leaving a reader to guess
    # whether the question was ever asked.
    assert rows["audit"] == ""


def test_a_task_the_caller_may_not_automate_is_not_listed(catalog_workspace, permissions_state):
    """AUTOMATE is checked on the task's own name - a task shares one namespace
    with tables and views, so the grant on the name governs the row."""
    _install(permissions_state, readable=(), automatable={"cat.pipelines.curate"})

    rows = _read()

    assert [row["task_name"] for row in rows] == ["curate"]


def test_read_on_a_task_name_shows_no_row(catalog_workspace, permissions_state):
    """A reader holds nothing over automation: a task's statement, what it
    writes and who runs it are an owner's to see, however many names the
    reader may SELECT from."""
    _install(permissions_state, readable={"cat.pipelines.curate", "cat.pipelines.audit"})

    assert _read() == []


def test_an_unlistable_task_costs_no_round_trip(catalog_workspace, permissions_state):
    """Checked BEFORE `get_task`, so a task the caller may not see is never
    fetched and its statement is never read."""
    _install(permissions_state, readable=(), automatable={"cat.pipelines.curate"})

    _read()

    fetched = [call[1] for call in _FakeCatalog.calls if call[0] == "get_task"]
    assert fetched == ["pipelines.curate"]


def test_a_collection_predicate_skips_the_other_listing(catalog_workspace):
    """task_collection is known before the per-collection listing, so pushing it
    skips that call entirely rather than pruning rows after it."""
    rows = _read(" WHERE task_collection = 'reporting'")

    assert [row["task_name"] for row in rows] == ["rebuild"]
    listed = [call[1] for call in _FakeCatalog.calls if call[0] == "list_tasks"]
    assert listed == ["reporting"]


def test_a_name_predicate_prunes_before_the_fetch(catalog_workspace):
    """task_name prunes before `get_task`, which is the expensive call."""
    rows = _read(" WHERE task_name = 'curate'")

    assert [row["task_name"] for row in rows] == ["curate"]
    fetched = [call[1] for call in _FakeCatalog.calls if call[0] == "get_task"]
    assert fetched == ["pipelines.curate"]


def test_a_catalog_predicate_skips_enumeration_entirely(catalog_workspace):
    rows = _read(" WHERE task_catalog = 'other'")

    assert rows == []
    assert _FakeCatalog.calls == []
