# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.listeners`, and the `listening` column on `.tasks`.

The listeners table is the PRIMARY surface for a subscriber, not a convenience.
`information_schema.tasks` is AUTOMATE-gated and so readable only by a task's
OWNER, while LISTEN is gated on READ over what the task writes - so a
subscriber who does not own the task cannot read that table at all, and would
otherwise have no way to see what they had subscribed to.

Both are SELF-SCOPED: they answer for the caller and nobody else. `listening`
is deliberately the caller's own subscription rather than a subscriber count,
which would tell everyone who can read the table how many people watch a task -
on a small team, the subscriber list.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]

_NOW_MS = 1754000000000  # 2025-07-31T21:33:20Z

_TASKS = {
    "pipelines.curate": {
        "sql": "INSERT INTO pipelines.curated SELECT * FROM raw.events",
        "writes": ["pipelines.curated"],
    },
    "pipelines.audit": {
        "sql": "SELECT count(*) FROM raw.events",
        "writes": [],
    },
}

# Keyed by user, as the catalog's collection-group query returns them.
_LISTENERS = {
    "alice": [
        {
            "workspace": "cat",
            "collection": "pipelines",
            "object": "curate",
            "kind": "task",
            "outcome": "ERROR",
            "created-at-ms": _NOW_MS,
        }
    ],
    "rhea": [
        {
            "workspace": "cat",
            "collection": "pipelines",
            "object": "audit",
            "kind": "task",
            "outcome": "EVERYTHING",
            "created-at-ms": _NOW_MS,
        }
    ],
}


class _FakeCatalog:
    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["pipelines"]

    def list_datasets(self, collection):
        return []

    def list_tasks(self, collection):
        return [name.split(".")[1] for name in _TASKS if name.split(".")[0] == collection]

    def get_task(self, identifier):
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
            "last-updated-by": "olive",
            "last-updated-at-ms": _NOW_MS,
            "suspended-at-ms": None,
            "suspended-by": None,
            "last-fired-at-ms": _NOW_MS,
            "last-fired-status": "success",
            "last-window-to": 7,
        }

    def list_listeners_for_user(self, user):
        _FakeCatalog.calls.append(("list_listeners_for_user", user))
        return [dict(row) for row in _LISTENERS.get(user, [])]

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _CatalogWithoutListeners(_FakeCatalog):
    """An installed catalog wheel that predates subscriptions.

    Not an error: it has none, which is a complete answer. The same skew
    tolerance the task API itself is read with.
    """

    list_listeners_for_user = None


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


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(relation, user):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    return _morsels_to_rows(session.execute_to_morsels(f"SELECT * FROM cat.{relation}"))


# --- information_schema.listeners


def test_listeners_reports_the_callers_own_subscriptions(catalog_workspace):
    rows = _read("information_schema.listeners", "alice")

    assert len(rows) == 1
    assert rows[0]["object_catalog"] == "cat"
    assert rows[0]["object_collection"] == "pipelines"
    assert rows[0]["object_name"] == "curate"
    assert rows[0]["kind"] == "task"
    assert rows[0]["outcome"] == "ERROR"
    assert rows[0]["created_at"] is not None


def test_another_users_subscriptions_are_not_visible(catalog_workspace):
    """Self-scoped: there is no form that lists someone else's, and no form that
    lists a task's subscribers."""
    rows = _read("information_schema.listeners", "rhea")

    assert [row["object_name"] for row in rows] == ["audit"]


def test_a_materialized_view_subscription_reads_back_with_its_kind(catalog_workspace):
    """One table for both kinds - the subscribable object is whatever a trigger
    targets, and `kind` is the answer the caller never had to write down."""
    _LISTENERS["vic"] = [
        {
            "workspace": "cat",
            "collection": "security",
            "object": "vulnerabilities_per_week",
            "kind": "materialized_view",
            "outcome": "ERROR",
            "created-at-ms": _NOW_MS,
        }
    ]
    try:
        rows = _read("information_schema.listeners", "vic")
    finally:
        del _LISTENERS["vic"]

    assert rows[0]["object_name"] == "vulnerabilities_per_week"
    assert rows[0]["kind"] == "materialized_view"


def test_a_user_with_no_subscriptions_gets_no_rows(catalog_workspace):
    assert _read("information_schema.listeners", "mallory") == []


def test_show_listeners_is_refused_and_names_the_form_that_works(catalog_workspace):
    """SHOW LISTENERS names no object, and `information_schema` here is always
    workspace-qualified - there is no session current-workspace to read it in.
    Refused by name rather than resolving to a workspace called
    `information_schema` and reporting a missing dataset. See
    `plan_show_listeners`."""
    from opteryx.exceptions import UnsupportedSyntaxError

    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    with pytest.raises(UnsupportedSyntaxError, match="information_schema.listeners"):
        list(session.execute_to_morsels("SHOW LISTENERS"))


def test_a_catalog_without_listeners_answers_empty(catalog_workspace):
    register_workspace("cat", OpteryxConnector, catalog=_CatalogWithoutListeners)

    assert _read("information_schema.listeners", "alice") == []


# --- the `listening` column on information_schema.tasks


def test_tasks_reports_your_own_subscription(catalog_workspace):
    rows = {row["task_name"]: row for row in _read("information_schema.tasks", "alice")}

    assert rows["curate"]["listening"] == "ERROR"
    # Null, not "nobody is listening": rhea listens to audit, and alice is not
    # told so.
    assert rows["audit"]["listening"] is None


def test_the_listening_column_is_not_a_subscriber_count(catalog_workspace):
    """rhea listens to `audit` and alice does not. Each sees only their own."""
    alice = {row["task_name"]: row for row in _read("information_schema.tasks", "alice")}
    rhea = {row["task_name"]: row for row in _read("information_schema.tasks", "rhea")}

    assert alice["audit"]["listening"] is None
    assert rhea["audit"]["listening"] == "EVERYTHING"
    assert rhea["curate"]["listening"] is None


def test_the_column_costs_one_read_for_the_whole_listing(catalog_workspace):
    """One collection-group query answers every row, rather than one per task."""
    _FakeCatalog.calls = []
    _read("information_schema.tasks", "alice")

    subscription_reads = [c for c in _FakeCatalog.calls if c[0] == "list_listeners_for_user"]
    assert len(subscription_reads) == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
