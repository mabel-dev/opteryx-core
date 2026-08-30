# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""DROP TRIGGER / SHOW TRIGGERS / information_schema.triggers.

Triggers exist only as the automatic artifact of CREATE MATERIALIZED VIEW: a
refresh trigger lands on each SOURCE table (the table whose commits fire it),
so a trigger is always found against that source table, never the target view.

Two harnesses, mirroring test_materialized_views.py / test_catalog_ddl_delegation.py:
- LocalStoreConnector executes the whole statement surface against its
  `triggers.json` sidecar (written next to each source's dataset.json).
- A fake catalog behind OpteryxConnector proves the catalog delegation carries
  the session user, and drives information_schema.triggers / SHOW TRIGGERS FOR
  end to end.
"""

import datetime
import json

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetNotFoundError, UnsupportedSyntaxError

try:
    from opteryx_catalog.exceptions import TriggerNotFound
except ImportError:  # wheel predates triggers; the connector catches KeyError then
    TriggerNotFound = KeyError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        n = len(next(iter(pydict.values()))) if pydict else 0
        for i in range(n):
            row = {}
            for k, vs in pydict.items():
                v = vs[i]
                if isinstance(v, bytes):
                    v = v.decode()
                row[k] = v
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# LocalStoreConnector: full statement surface against the triggers.json sidecar
# ---------------------------------------------------------------------------


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _seed_source(session, name="ws.src"):
    list(session.execute_to_morsels(f"CREATE TABLE {name} (a BIGINT)"))
    list(session.execute_to_morsels(f"INSERT INTO {name} VALUES (-1), (1), (2), (3)"))


def _source_triggers(tmp_path, relation="ws.src"):
    p = tmp_path
    for part in relation.split("."):
        p = p / part
    triggers_path = p / "triggers.json"
    if not triggers_path.exists():
        return []
    with open(triggers_path) as f:
        return json.load(f)


def _create_mv(session, mv="ws.mv", source="ws.src"):
    list(
        session.execute_to_morsels(
            f"CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {source} WHERE a > 0"
        )
    )


def test_create_mv_lands_trigger_on_source(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_mv(owner)

    triggers = _source_triggers(tmp_path)
    assert len(triggers) == 1
    record = triggers[0]
    assert record["name"] == "refresh__mv"
    assert record["kind"] == "materialized_view_refresh"
    assert record["target-view"] == "ws.mv"
    assert record["created-by"] == "olive"
    assert record["created-at-ms"] is not None
    assert record["last-fired-at-ms"] is None
    assert record["last-fired-status"] is None


def test_drop_trigger_removes_record(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_mv(owner)
    assert len(_source_triggers(tmp_path)) == 1

    list(owner.execute_to_morsels("DROP TRIGGER refresh__mv ON ws.src"))

    assert _source_triggers(tmp_path) == []
    # The MV is orphaned, not gone: still queryable, just never refreshed.
    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.mv"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3]


def test_drop_trigger_missing_without_if_exists_errors(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    with pytest.raises(ValueError, match="does not exist"):
        list(owner.execute_to_morsels("DROP TRIGGER no_such ON ws.src"))


def test_drop_trigger_if_exists_missing_is_quiet(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    list(owner.execute_to_morsels("DROP TRIGGER IF EXISTS no_such ON ws.src"))


def test_drop_trigger_missing_table_errors_even_with_if_exists(tmp_path):
    """IF EXISTS speaks about the trigger, not the table it hangs off."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(DatasetNotFoundError):
        list(owner.execute_to_morsels("DROP TRIGGER IF EXISTS t ON ws.no_such_table"))


def test_drop_materialized_view_removes_source_triggers(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_mv(owner)
    assert len(_source_triggers(tmp_path)) == 1

    list(owner.execute_to_morsels("DROP MATERIALIZED VIEW ws.mv"))

    assert _source_triggers(tmp_path) == []


def test_replace_mv_reconciles_triggers_against_new_sources(tmp_path):
    """CREATE OR REPLACE with a changed source list must not leave the old
    source still firing."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner, "ws.first")
    _seed_source(owner, "ws.second")
    _create_mv(owner, source="ws.first")
    assert len(_source_triggers(tmp_path, "ws.first")) == 1

    list(
        owner.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.second WHERE a > 0"
        )
    )

    assert _source_triggers(tmp_path, "ws.first") == []
    assert len(_source_triggers(tmp_path, "ws.second")) == 1


def test_postgres_style_create_trigger_rejected(tmp_path):
    """CREATE TRIGGER exists now, but only in this dialect's one form:
    `ON <table> EXECUTE <task>`. The postgres row-trigger shape (AFTER INSERT,
    EXECUTE FUNCTION) has no meaning here and is refused by pointing at the
    grammar that does - it used to be refused because no CREATE TRIGGER existed
    at all."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="EXECUTE"):
        list(
            owner.execute_to_morsels(
                "CREATE TRIGGER t AFTER INSERT ON ws.src EXECUTE FUNCTION f()"
            )
        )


def test_drop_trigger_cascade_rejected(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="CASCADE"):
        list(owner.execute_to_morsels("DROP TRIGGER t ON ws.src CASCADE"))


def test_drop_trigger_without_table_rejected(tmp_path):
    """The ON <table> clause is required - trigger names are only unique per
    dataset, and the table is the permission target."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="ON <table>"):
        list(owner.execute_to_morsels("DROP TRIGGER t"))


# ---------------------------------------------------------------------------
# Catalog-backed connector: delegation, information_schema.triggers, SHOW TRIGGERS
# ---------------------------------------------------------------------------

_NOW_MS = 1754000000000  # 2025-07-31T21:33:20Z


class _FakeCatalog:
    """Stands in for the real catalog: two collections, one trigger on
    coll1.src, none anywhere else. Records the trigger calls it receives."""

    calls = []
    missing_triggers = set()

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        _FakeCatalog.calls.append(("list_collections",))
        return ["coll1", "coll2"]

    def list_datasets(self, collection):
        _FakeCatalog.calls.append(("list_datasets", collection))
        return {"coll1": ["src", "other"], "coll2": ["empty"]}[collection]

    def list_triggers(self, identifier):
        _FakeCatalog.calls.append(("list_triggers", identifier))
        if identifier != "coll1.src":
            return []
        return [
            {
                "name": "refresh__coll1__mv",
                "kind": "materialized_view_refresh",
                "target-view": "coll1.mv",
                "statement-id": "stmt-1",
                "created-by": "olive",
                "created-at-ms": _NOW_MS,
                "last-fired-at-ms": None,
                "last-fired-status": None,
            }
        ]

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)

    def drop_trigger(self, identifier, name, author=None, missing_ok=False):
        if name in _FakeCatalog.missing_triggers:
            if missing_ok:
                return
            raise TriggerNotFound(f"Trigger not found: {name} on {identifier}")
        _FakeCatalog.calls.append(("drop_trigger", identifier, name, author, missing_ok))


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    _FakeCatalog.missing_triggers = set()
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def test_drop_trigger_delegates_to_catalog_with_user(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP TRIGGER refresh__coll1__mv ON cat.coll1.src"))

    assert catalog_workspace.calls == [
        ("drop_trigger", "coll1.src", "refresh__coll1__mv", "alice", False)
    ]


def test_drop_trigger_if_exists_passes_missing_ok(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(
        session.execute_to_morsels(
            "DROP TRIGGER IF EXISTS refresh__coll1__mv ON cat.coll1.src"
        )
    )

    assert catalog_workspace.calls == [
        ("drop_trigger", "coll1.src", "refresh__coll1__mv", "alice", True)
    ]


def test_drop_trigger_not_found_translated(catalog_workspace):
    catalog_workspace.missing_triggers = {"gone"}
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(ValueError, match="does not exist"):
        list(session.execute_to_morsels("DROP TRIGGER gone ON cat.coll1.src"))

    list(session.execute_to_morsels("DROP TRIGGER IF EXISTS gone ON cat.coll1.src"))


# --- information_schema.triggers


def test_information_schema_triggers_row_shape(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.triggers")
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["trigger_catalog"] == "cat"
    assert row["trigger_collection"] == "coll1"
    assert row["trigger_name"] == "refresh__coll1__mv"
    assert row["event_object_table"] == "coll1.src"
    assert row["action_kind"] == "materialized_view_refresh"
    assert row["target"] == "coll1.mv"
    assert row["created_by"] == "olive"
    assert row["created_at"] is not None
    assert row["last_fired_at"] is None
    assert row["last_fired_status"] is None


def test_information_schema_reports_what_a_task_trigger_runs(catalog_workspace, monkeypatch):
    """A task trigger used to show nothing under `target_view`: the view read
    that field only, so the one thing you consult SHOW TRIGGERS to learn - what
    it fires - was the one thing missing. `target` now carries either kind.

    The task trigger is scoped to this test rather than added to the shared
    fixture, which several tests assert holds exactly one row.
    """
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {
                    "name": "task__coll1__ingest",
                    "kind": "task",
                    "target-task": "cat.coll1.ingest",
                    "created-by": "olive",
                    "created-at-ms": _NOW_MS,
                    "last-fired-at-ms": None,
                    "last-fired-status": None,
                }
            ]
            if identifier == "coll1.src"
            else []
        ),
    )
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.triggers")
    )

    row = next(r for r in rows if r["action_kind"] == "task")
    assert row["trigger_name"] == "task__coll1__ingest"
    assert row["target"] == "cat.coll1.ingest"


def test_information_schema_triggers_denies_without_execution_context():
    """Secure by default: no execution context means zero rows, not all rows."""
    from opteryx.connectors.information_schema import InformationSchemaTriggersTable

    table = InformationSchemaTriggersTable(
        dataset="information_schema.triggers",
        catalog=_FakeCatalog(),
        workspace="cat",
        telemetry=None,
        execution_context=None,
    )
    morsels = list(table.read_dataset())
    assert sum(m.num_rows for m in morsels) == 0


def test_information_schema_triggers_pushdown_skips_collections(catalog_workspace):
    """An excluding predicate on a key column skips catalog round trips, not
    just rows - coll2 is never enumerated."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels(
            "SELECT trigger_name FROM cat.information_schema.triggers "
            "WHERE trigger_collection = 'coll1'"
        )
    )
    assert [r["trigger_name"] for r in rows] == ["refresh__coll1__mv"]
    assert ("list_datasets", "coll2") not in catalog_workspace.calls


# --- SHOW TRIGGERS


def test_show_triggers_for_table(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SHOW TRIGGERS FOR cat.coll1.src")
    )

    assert len(rows) == 1
    assert rows[0]["trigger_name"] == "refresh__coll1__mv"
    assert rows[0]["event_object_table"] == "coll1.src"


def test_show_triggers_for_table_without_triggers_is_empty(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SHOW TRIGGERS FOR cat.coll1.other")
    )
    assert rows == []


def test_show_triggers_bare_rejected(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SHOW TRIGGERS FOR"):
        list(session.execute_to_morsels("SHOW TRIGGERS"))


def test_show_triggers_unqualified_table_rejected(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="workspace-qualified"):
        list(session.execute_to_morsels("SHOW TRIGGERS FOR src"))


def test_information_schema_triggers_created_at_is_timestamp(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels(
            "SELECT created_at FROM cat.information_schema.triggers"
        )
    )
    assert len(rows) == 1
    created_at = rows[0]["created_at"]
    assert isinstance(created_at, datetime.datetime)
    assert created_at.year == 2025
