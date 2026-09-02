# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""CREATE / ALTER / DROP TRIGGER, SHOW TRIGGERS and information_schema.triggers.

A trigger is an EVENT plus the identity its unattended runs carry, and it lives
under whatever holds the event. A commit trigger (a materialized view's refresh
trigger, or `ON <table> EXECUTE <task>`) lands on the SOURCE table whose commits
fire it, never on the target. A schedule or signal trigger has no source table
and lands on the TASK it fires - the holder is the task - and a task holds at
most one trigger of any kind.

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
try:
    from opteryx_catalog.exceptions import TaskNotFound
except ImportError:  # wheel predates tasks; the connector then sees no task API
    TaskNotFound = KeyError

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
# LocalStoreConnector: schedule and signal triggers, held by the task they fire
# ---------------------------------------------------------------------------


def _create_task(session, name="ws.t", statement="SELECT a FROM ws.src"):
    list(session.execute_to_morsels(f"CREATE TASK {name} AS {statement}"))


_WINDOWED = "SELECT a FROM ws.src WHERE a > :parent_version AND a <= :current_version"


def test_schedule_trigger_lands_under_the_task(tmp_path):
    """No source dataset, so the record hangs off the task: the sidecar sits
    next to task.json, and the source table carries nothing."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)

    list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.t"))

    assert _source_triggers(tmp_path, "ws.src") == []
    triggers = _source_triggers(tmp_path, "ws.t")
    assert len(triggers) == 1
    record = triggers[0]
    assert record["name"] == "tick"
    # `kind` keeps meaning what the trigger RUNS; the event is its own axis.
    assert record["kind"] == "task"
    assert record["target-task"] == "ws.t"
    assert record["event-kind"] == "schedule"
    assert record["schedule"] == "0 * * * *"
    assert record["time-zone"] == "UTC"
    assert record["window-source"] is None
    assert record["runs-as"] == "olive"
    assert record["created-by"] == "olive"


def test_schedule_trigger_with_time_zone_and_window(tmp_path):
    """OVER names the dataset the run is windowed over, which is what lets a
    windowed task be fired by a clock at all."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner, "ws.delta", _WINDOWED)

    list(
        owner.execute_to_morsels(
            "CREATE TRIGGER nightly ON SCHEDULE '0 3 * * *' AT TIME ZONE 'Europe/London' "
            "OVER ws.src EXECUTE ws.delta"
        )
    )

    record = _source_triggers(tmp_path, "ws.delta")[0]
    assert record["event-kind"] == "schedule"
    assert record["schedule"] == "0 3 * * *"
    assert record["time-zone"] == "Europe/London"
    assert record["window-source"] == "ws.src"


def test_signal_trigger_lands_under_the_task(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)

    list(owner.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL EXECUTE ws.t"))

    record = _source_triggers(tmp_path, "ws.t")[0]
    assert record["name"] == "poke"
    assert record["event-kind"] == "signal"
    assert record["schedule"] is None
    assert record["time-zone"] is None
    assert record["window-source"] is None


def test_signal_trigger_with_window(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner, "ws.delta", _WINDOWED)

    list(owner.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL OVER ws.src EXECUTE ws.delta"))

    record = _source_triggers(tmp_path, "ws.delta")[0]
    assert record["event-kind"] == "signal"
    assert record["window-source"] == "ws.src"


def test_a_windowed_task_needs_over(tmp_path):
    """THE WINDOWLESS CHECK. A commit binds `:parent_version` and
    `:current_version`; a clock has no commit. Without OVER the task must not
    consume a window - refused at arming, before anything is written, and the
    refusal names the parameters and the two ways out."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner, "ws.delta", _WINDOWED)

    for sql in (
        "CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.delta",
        "CREATE TRIGGER poke ON SIGNAL EXECUTE ws.delta",
    ):
        with pytest.raises(UnsupportedSyntaxError) as caught:
            list(owner.execute_to_morsels(sql))
        message = str(caught.value)
        assert ":parent_version" in message
        assert ":current_version" in message
        assert "OVER" in message
    assert _source_triggers(tmp_path, "ws.delta") == []

    # The same task, windowed over its source, is accepted.
    list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' OVER ws.src EXECUTE ws.delta"))
    assert _source_triggers(tmp_path, "ws.delta")[0]["window-source"] == "ws.src"


def test_over_must_name_a_dataset_in_the_tasks_workspace(tmp_path):
    """The window is read from the catalog the trigger lives in; a run may not
    be windowed over another workspace's data."""
    _setup_workspace(tmp_path)
    register_workspace("elsewhere", LocalStoreConnector, store_root=str(tmp_path / "elsewhere"))
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _seed_source(owner, "elsewhere.src")
    _create_task(owner, "ws.delta", _WINDOWED)

    with pytest.raises(UnsupportedSyntaxError, match="own workspace"):
        list(
            owner.execute_to_morsels(
                "CREATE TRIGGER tick ON SCHEDULE '0 * * * *' OVER elsewhere.src EXECUTE ws.delta"
            )
        )
    assert _source_triggers(tmp_path, "ws.delta") == []


def test_over_must_name_a_dataset_that_exists(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)

    with pytest.raises(DatasetNotFoundError):
        list(
            owner.execute_to_morsels(
                "CREATE TRIGGER tick ON SCHEDULE '0 * * * *' OVER ws.no_such EXECUTE ws.t"
            )
        )


def test_a_schedule_trigger_must_execute_a_task(tmp_path):
    """The holder is the task, so EXECUTE must name one - a table of that name
    is the wrong kind of thing to hang a clock off."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    with pytest.raises(UnsupportedSyntaxError, match="is not a task"):
        list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.src"))
    with pytest.raises(UnsupportedSyntaxError, match="is not a task"):
        list(owner.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL EXECUTE ws.nope"))


def test_a_task_holds_one_trigger(tmp_path):
    """The one-trigger rule: a task's window is one sequence, so a second
    trigger under it is refused whatever its event."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)
    list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.t"))

    with pytest.raises(ValueError, match="one trigger"):
        list(owner.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL EXECUTE ws.t"))
    with pytest.raises(ValueError, match="one trigger"):
        list(owner.execute_to_morsels("CREATE TRIGGER hourly ON SCHEDULE '30 * * * *' EXECUTE ws.t"))

    assert [t["name"] for t in _source_triggers(tmp_path, "ws.t")] == ["tick"]


def test_or_replace_rewrites_the_schedule_and_keeps_the_owner(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)
    list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.t"))
    list(owner.execute_to_morsels("ALTER TRIGGER tick ON ws.t OWNER TO rhea"))

    list(
        owner.execute_to_morsels(
            "CREATE OR REPLACE TRIGGER tick ON SCHEDULE '*/5 * * * *' EXECUTE ws.t"
        )
    )

    triggers = _source_triggers(tmp_path, "ws.t")
    assert len(triggers) == 1
    assert triggers[0]["schedule"] == "*/5 * * * *"
    assert triggers[0]["runs-as"] == "rhea"


def test_alter_and_drop_against_a_task_holder(tmp_path):
    """ON <holder> in ALTER and DROP names the task the way it names a table;
    the grammar does not change and the engine works out which it was."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _create_task(owner)
    list(owner.execute_to_morsels("CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE ws.t"))

    list(owner.execute_to_morsels("ALTER TRIGGER tick ON ws.t SUSPEND"))
    assert _source_triggers(tmp_path, "ws.t")[0]["suspended-at-ms"] is not None
    list(owner.execute_to_morsels("ALTER TRIGGER tick ON ws.t RESUME"))
    assert _source_triggers(tmp_path, "ws.t")[0]["suspended-at-ms"] is None

    list(owner.execute_to_morsels("ALTER TRIGGER tick ON ws.t OWNER TO rhea"))
    assert _source_triggers(tmp_path, "ws.t")[0]["runs-as"] == "rhea"

    list(owner.execute_to_morsels("DROP TRIGGER tick ON ws.t"))
    assert _source_triggers(tmp_path, "ws.t") == []
    # And the task is still there: the trigger was the task's, not the task.
    list(owner.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL EXECUTE ws.t"))
    assert _source_triggers(tmp_path, "ws.t")[0]["event-kind"] == "signal"


def test_drop_trigger_on_a_missing_holder_errors_even_with_if_exists(tmp_path):
    """IF EXISTS speaks about the trigger, not the task it hangs off - the same
    rule as for a table."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(DatasetNotFoundError):
        list(owner.execute_to_morsels("DROP TRIGGER IF EXISTS tick ON ws.no_such_task"))


def test_the_commit_form_takes_neither_window_nor_zone(tmp_path):
    """A commit supplies its own window and happens in no time zone; both
    modifiers are refused by name rather than as a generic syntax error."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="OVER.*does not apply to a commit trigger"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON ws.src OVER ws.other EXECUTE ws.t"))
    with pytest.raises(UnsupportedSyntaxError, match="AT TIME ZONE.*does not apply to a commit trigger"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON ws.src AT TIME ZONE 'UTC' EXECUTE ws.t"))


def test_words_that_are_not_events_are_refused_by_name(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    for sql in (
        "CREATE TRIGGER t ON EVERY 5 MINUTES EXECUTE ws.t",
        "CREATE TRIGGER t ON EVENT deploy EXECUTE ws.t",
    ):
        with pytest.raises(UnsupportedSyntaxError, match="is not a trigger event"):
            list(owner.execute_to_morsels(sql))


def test_malformed_schedule_and_signal_forms_are_refused_with_their_grammar(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="not a cron expression"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON SCHEDULE 'hourly' EXECUTE ws.t"))
    with pytest.raises(UnsupportedSyntaxError, match="ON SCHEDULE"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON SCHEDULE 0 * * * * EXECUTE ws.t"))
    with pytest.raises(UnsupportedSyntaxError, match="ON SIGNAL"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON SIGNAL AT TIME ZONE 'UTC' EXECUTE ws.t"))


def test_the_commit_form_is_unchanged(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    _seed_source(owner, "ws.other")
    _create_task(owner)

    list(owner.execute_to_morsels("CREATE TRIGGER fire ON ws.other EXECUTE ws.t"))

    record = _source_triggers(tmp_path, "ws.other")[0]
    assert record["name"] == "fire"
    assert record["target-task"] == "ws.t"
    assert record["event-kind"] == "commit"
    assert record["window-source"] is None
    assert _source_triggers(tmp_path, "ws.t") == []


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

    def set_trigger_minimum_interval(self, identifier, name, seconds, author=None):
        if name in _FakeCatalog.missing_triggers:
            raise TriggerNotFound(f"Trigger not found: {name} on {identifier}")
        _FakeCatalog.calls.append(("set_trigger_minimum_interval", identifier, name, seconds, author))

    # Deliberately NO `holder_kind` and no event keywords: this is the shape the
    # catalog had before task-held triggers existed, and a commit trigger's call
    # must still fit it - a TypeError here is the connector leaking the new
    # keywords onto the old path.
    def create_trigger(
        self, identifier, name, target_task=None, kind="materialized_view_refresh", author=None
    ):
        _FakeCatalog.calls.append(("create_trigger", identifier, name, target_task, kind, author))

    def set_trigger_suspended(self, identifier, name, suspended, author=None):
        _FakeCatalog.calls.append(("set_trigger_suspended", identifier, name, suspended, author))


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    _FakeCatalog.missing_triggers = set()
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


class _FakeTaskCatalog(_FakeCatalog):
    """The fake above, plus tasks - so a trigger can be held by one. Two tasks
    in coll1: `nightly`, windowless, and `delta`, which consumes a window. One
    schedule trigger, `tick`, held by `nightly`. Its trigger methods take
    `holder_kind`, as the catalog's do, and record what they were passed."""

    tasks = {
        "coll1.nightly": "SELECT a FROM cat.coll1.src",
        "coll1.delta": (
            "SELECT a FROM cat.coll1.src "
            "WHERE a > :parent_version AND a <= :current_version"
        ),
    }

    def get_task(self, identifier):
        if identifier not in self.tasks:
            raise TaskNotFound(f"Task not found: {identifier}")
        return {"sql": self.tasks[identifier]}

    def list_tasks(self, collection):
        _FakeCatalog.calls.append(("list_tasks", collection))
        prefix = f"{collection}."
        return [name[len(prefix):] for name in self.tasks if name.startswith(prefix)]

    def dataset_exists(self, identifier):
        return identifier in {"coll1.src", "coll1.other", "coll2.empty"}

    def list_triggers(self, identifier, holder_kind="dataset"):
        if holder_kind == "dataset":
            return _FakeCatalog.list_triggers(self, identifier)
        _FakeCatalog.calls.append(("list_triggers", identifier, holder_kind))
        if identifier != "coll1.nightly":
            return []
        return [
            {
                "name": "tick",
                "kind": "task",
                "target-task": "cat.coll1.nightly",
                "runs-as": "olive",
                "created-by": "olive",
                "created-at-ms": _NOW_MS,
                "event-kind": "schedule",
                "schedule": "0 3 * * *",
                "time-zone": "UTC",
                "next-due-at-ms": _NOW_MS + 3_600_000,
                "window-source": None,
                "last-fired-at-ms": None,
                "last-fired-status": None,
            }
        ]

    def create_trigger(self, identifier, name, **kwargs):
        _FakeCatalog.calls.append(("create_trigger", identifier, name, kwargs))

    def drop_trigger(self, identifier, name, author=None, missing_ok=False, holder_kind="dataset"):
        _FakeCatalog.calls.append(
            ("drop_trigger", identifier, name, author, missing_ok, holder_kind)
        )

    def set_trigger_owner(self, identifier, name, new_owner, author=None, holder_kind="dataset"):
        _FakeCatalog.calls.append(
            ("set_trigger_owner", identifier, name, new_owner, author, holder_kind)
        )

    def set_trigger_suspended(self, identifier, name, suspended, author=None, holder_kind="dataset"):
        _FakeCatalog.calls.append(
            ("set_trigger_suspended", identifier, name, suspended, author, holder_kind)
        )


@pytest.fixture
def task_catalog_workspace():
    _FakeCatalog.calls = []
    _FakeCatalog.missing_triggers = set()
    register_workspace("cat", OpteryxConnector, catalog=_FakeTaskCatalog)
    return _FakeTaskCatalog


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


def test_commit_trigger_call_shape_is_unchanged(catalog_workspace):
    """The fake's `create_trigger` takes no `holder_kind` and no event keyword,
    so this passing IS the proof that a commit trigger's call is byte-for-byte
    what it was before schedule and signal triggers existed."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TRIGGER fire ON cat.coll1.src EXECUTE cat.coll1.ingest"))

    assert catalog_workspace.calls == [
        ("create_trigger", "coll1.src", "fire", "cat.coll1.ingest", "task", "alice")
    ]


def test_commit_trigger_alter_and_drop_shapes_are_unchanged(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TRIGGER refresh__coll1__mv ON cat.coll1.src SUSPEND"))
    list(session.execute_to_morsels("DROP TRIGGER refresh__coll1__mv ON cat.coll1.src"))

    assert catalog_workspace.calls == [
        ("set_trigger_suspended", "coll1.src", "refresh__coll1__mv", True, "alice"),
        ("drop_trigger", "coll1.src", "refresh__coll1__mv", "alice", False),
    ]


def test_schedule_trigger_is_created_under_the_task(task_catalog_workspace):
    """Holder is the task, `holder_kind="task"`, the event goes with it, and the
    window source is passed relative to the workspace as every identifier is."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(
        session.execute_to_morsels(
            "CREATE TRIGGER nightly ON SCHEDULE '0 3 * * *' AT TIME ZONE 'Europe/London' "
            "OVER cat.coll1.src EXECUTE cat.coll1.delta"
        )
    )

    assert [c for c in task_catalog_workspace.calls if c[0] == "create_trigger"] == [
        (
            "create_trigger",
            "coll1.delta",
            "nightly",
            {
                "target_task": "cat.coll1.delta",
                "kind": "task",
                "author": "alice",
                "holder_kind": "task",
                "event_kind": "schedule",
                "schedule": "0 3 * * *",
                "time_zone": "Europe/London",
                "window_source": "coll1.src",
            },
        )
    ]


def test_signal_trigger_is_created_under_the_task(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TRIGGER poke ON SIGNAL EXECUTE cat.coll1.nightly"))

    assert [c for c in task_catalog_workspace.calls if c[0] == "create_trigger"] == [
        (
            "create_trigger",
            "coll1.nightly",
            "poke",
            {
                "target_task": "cat.coll1.nightly",
                "kind": "task",
                "author": "alice",
                "holder_kind": "task",
                "event_kind": "signal",
                "schedule": None,
                "time_zone": None,
                "window_source": None,
            },
        )
    ]


def test_or_replace_drops_under_the_task_first(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE cat.coll1.nightly"
        )
    )

    calls = [c for c in task_catalog_workspace.calls if c[0] in ("drop_trigger", "create_trigger")]
    assert calls[0] == ("drop_trigger", "coll1.nightly", "tick", "alice", True, "task")
    assert calls[1][0:3] == ("create_trigger", "coll1.nightly", "tick")


def test_windowed_task_without_over_is_refused_before_the_catalog_is_written(
    task_catalog_workspace,
):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="parent_version"):
        list(
            session.execute_to_morsels(
                "CREATE TRIGGER tick ON SCHEDULE '0 3 * * *' EXECUTE cat.coll1.delta"
            )
        )

    assert not [c for c in task_catalog_workspace.calls if c[0] == "create_trigger"]


def test_over_across_workspaces_is_refused_on_the_catalog_path(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="own workspace"):
        list(
            session.execute_to_morsels(
                "CREATE TRIGGER tick ON SCHEDULE '0 3 * * *' OVER other.coll1.src "
                "EXECUTE cat.coll1.delta"
            )
        )


def test_alter_and_drop_against_a_task_holder_pass_holder_kind(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER TRIGGER tick ON cat.coll1.nightly SUSPEND"))
    list(session.execute_to_morsels("ALTER TRIGGER tick ON cat.coll1.nightly OWNER TO bob"))
    list(session.execute_to_morsels("DROP TRIGGER tick ON cat.coll1.nightly"))

    writes = [
        c
        for c in task_catalog_workspace.calls
        if c[0] in ("set_trigger_suspended", "set_trigger_owner", "drop_trigger")
    ]
    assert writes == [
        ("set_trigger_suspended", "coll1.nightly", "tick", True, "alice", "task"),
        ("set_trigger_owner", "coll1.nightly", "tick", "bob", "alice", "task"),
        ("drop_trigger", "coll1.nightly", "tick", "alice", False, "task"),
    ]
    # The owner transfer read the trigger back off the TASK, not off a dataset.
    assert ("list_triggers", "coll1.nightly", "task") in task_catalog_workspace.calls


def test_dataset_holder_still_takes_the_old_shape_beside_task_holders(task_catalog_workspace):
    """A catalog that knows task holders is still called the old way for a
    dataset: `holder_kind` is never passed for one."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP TRIGGER refresh__coll1__mv ON cat.coll1.src"))

    assert ("drop_trigger", "coll1.src", "refresh__coll1__mv", "alice", False, "dataset") in (
        task_catalog_workspace.calls
    )


# --- information_schema.triggers


def test_set_minimum_interval_delegates_to_catalog_with_user(catalog_workspace):
    """The catalog gets the relative identifier, the seconds (MINUTES already
    converted) and the session user; the engine asserts nothing else."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(
        session.execute_to_morsels(
            "ALTER TRIGGER refresh__coll1__mv ON cat.coll1.src SET MINIMUM INTERVAL TO 2 MINUTES"
        )
    )
    assert ("set_trigger_minimum_interval", "coll1.src", "refresh__coll1__mv", 120, "alice") in (
        catalog_workspace.calls
    )


def test_set_minimum_interval_not_found_translated(catalog_workspace):
    catalog_workspace.missing_triggers = {"ghost"}
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    with pytest.raises(ValueError, match="does not exist"):
        list(
            session.execute_to_morsels(
                "ALTER TRIGGER ghost ON cat.coll1.src SET MINIMUM INTERVAL TO 30"
            )
        )


def test_information_schema_reports_the_firing_floor(catalog_workspace, monkeypatch):
    """Null for a record that predates the field - which fires on every
    commit - so it is not the same row as one whose floor was set to 0."""
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {
                    "name": "refresh__coll1__mv",
                    "kind": "materialized_view_refresh",
                    "target-view": "coll1.mv",
                    "created-by": "olive",
                    "created-at-ms": _NOW_MS,
                    "minimum-interval-seconds": 120,
                },
                {
                    "name": "refresh__coll1__legacy",
                    "kind": "materialized_view_refresh",
                    "target-view": "coll1.legacy",
                    "created-by": "olive",
                    "created-at-ms": _NOW_MS,
                },
            ]
            if identifier == "coll1.src"
            else []
        ),
    )
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.triggers")
    )

    by_name = {row["trigger_name"]: row for row in rows}
    assert by_name["refresh__coll1__mv"]["minimum_interval_seconds"] == 120
    assert by_name["refresh__coll1__legacy"]["minimum_interval_seconds"] is None


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
    # A trigger nobody has suspended holds neither field, so the projection
    # reads them off a document that does not have them at all.
    assert row["suspended_at"] is None
    assert row["suspended_by"] is None


def test_information_schema_reports_a_suspended_trigger(catalog_workspace, monkeypatch):
    """Suspension used to become visible only AFTER a write to the source, when
    the refused fire stamped `last_fired_status: suspended`. On a quiet source a
    paused trigger was indistinguishable from a healthy one, which is the whole
    thing ALTER TRIGGER ... SUSPEND exists to be distinguishable from."""
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {
                    "name": "refresh__coll1__mv",
                    "kind": "materialized_view_refresh",
                    "target-view": "coll1.mv",
                    "created-by": "olive",
                    "created-at-ms": _NOW_MS,
                    "suspended-at-ms": _NOW_MS,
                    "suspended-by": "justin",
                    # Never fired since - the point of the row is that this
                    # says nothing about whether it is running.
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

    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row["suspended_at"], datetime.datetime)
    assert row["suspended_at"].year == 2025
    assert row["suspended_by"] == "justin"
    assert row["last_fired_status"] is None


def test_information_schema_resumed_trigger_reports_no_suspension(
    catalog_workspace, monkeypatch
):
    """RESUME clears both fields to None rather than deleting them, so the
    projection must read a present-but-null field as "running" - not carry the
    stamp of the suspension it came out of."""
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {
                    "name": "refresh__coll1__mv",
                    "kind": "materialized_view_refresh",
                    "target-view": "coll1.mv",
                    "created-by": "olive",
                    "created-at-ms": _NOW_MS,
                    "suspended-at-ms": None,
                    "suspended-by": None,
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

    assert rows[0]["suspended_at"] is None
    assert rows[0]["suspended_by"] is None


def test_information_schema_triggers_filters_on_suspension(catalog_workspace, monkeypatch):
    """suspended_at is not a pushable key column - it is known only once the
    per-dataset listing has been read - so the predicate stays an ordinary
    Filter downstream. It still has to WORK, and the pushdown layer must not
    have quietly accepted and dropped it."""
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {"name": "paused", "kind": "task", "target-task": "cat.coll1.a",
                 "suspended-at-ms": _NOW_MS, "suspended-by": "justin"},
                {"name": "running", "kind": "task", "target-task": "cat.coll1.b"},
            ]
            if identifier == "coll1.src"
            else []
        ),
    )
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels(
            "SELECT trigger_name FROM cat.information_schema.triggers "
            "WHERE suspended_at IS NOT NULL"
        )
    )

    assert [r["trigger_name"] for r in rows] == ["paused"]


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
                    "runs-as": "olive",
                    "created-by": "mallory",
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
    # Whose authority the unattended run carries, which is NOT who created the
    # trigger - the two are deliberately different here, because a projection
    # that showed only `created_by` reads as if it answered this question.
    assert row["runs_as"] == "olive"
    assert row["created_by"] == "mallory"


def test_information_schema_reports_what_a_refresh_trigger_runs_as(
    catalog_workspace, monkeypatch
):
    """`runs_as` used to be null for a refresh row: the identity lived on the
    view and was never projected onto the trigger. It lives on the trigger now,
    like a task's, and the view carries none - so this row is the one place the
    identity behind an unattended refresh is visible."""
    monkeypatch.setattr(
        catalog_workspace,
        "list_triggers",
        lambda self, identifier: (
            [
                {
                    "name": "refresh__coll1__mv",
                    "kind": "materialized_view_refresh",
                    "target-view": "cat.coll1.mv",
                    "runs-as": "olive",
                    "created-by": "mallory",
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

    row = next(r for r in rows if r["action_kind"] == "materialized_view_refresh")
    assert row["target"] == "cat.coll1.mv"
    assert row["runs_as"] == "olive"
    assert row["created_by"] == "mallory"


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


# --- task-held triggers in information_schema.triggers and SHOW TRIGGERS


def test_information_schema_lists_task_held_triggers(task_catalog_workspace):
    """Two holders, two rows. The commit trigger's holder is its
    event_object_table; the schedule trigger's holder is the task, its
    event_object_table the OVER dataset (here none), and the event columns say
    what fires it and when."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.triggers")
    )

    by_name = {row["trigger_name"]: row for row in rows}
    assert set(by_name) == {"refresh__coll1__mv", "tick"}

    commit = by_name["refresh__coll1__mv"]
    assert commit["trigger_holder"] == "coll1.src"
    assert commit["event_object_table"] == "coll1.src"
    # Written before events were told apart: no `event-kind` on the record,
    # and it is a commit trigger, the only kind there was.
    assert commit["event_kind"] == "commit"
    assert commit["schedule"] is None
    assert commit["time_zone"] is None
    assert commit["next_due_at"] is None

    tick = by_name["tick"]
    assert tick["trigger_collection"] == "coll1"
    assert tick["trigger_holder"] == "coll1.nightly"
    assert tick["event_object_table"] is None
    assert tick["event_kind"] == "schedule"
    assert tick["schedule"] == "0 3 * * *"
    assert tick["time_zone"] == "UTC"
    assert isinstance(tick["next_due_at"], datetime.datetime)
    assert tick["next_due_at"].year == 2025
    assert tick["action_kind"] == "task"
    assert tick["target"] == "cat.coll1.nightly"
    assert tick["runs_as"] == "olive"


def test_information_schema_triggers_without_a_task_api_lists_datasets_only(catalog_workspace):
    """A catalog with no `list_tasks` has nothing a task could hold, and is
    never asked - the base fake would raise on `holder_kind`."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT trigger_holder, event_kind FROM cat.information_schema.triggers")
    )
    assert rows == [{"trigger_holder": "coll1.src", "event_kind": "commit"}]


def test_information_schema_triggers_filters_on_event_kind(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(
        session.execute_to_morsels(
            "SELECT trigger_name FROM cat.information_schema.triggers "
            "WHERE event_kind = 'schedule'"
        )
    )
    assert [r["trigger_name"] for r in rows] == ["tick"]


def test_show_triggers_for_task(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(session.execute_to_morsels("SHOW TRIGGERS FOR cat.coll1.nightly"))

    assert len(rows) == 1
    assert rows[0]["trigger_name"] == "tick"
    assert rows[0]["trigger_holder"] == "coll1.nightly"
    assert rows[0]["event_kind"] == "schedule"
    # The holder predicate is pushed: the dataset holders were never listed.
    assert ("list_triggers", "coll1.src") not in task_catalog_workspace.calls


def test_show_triggers_for_table_lists_only_that_tables_triggers(task_catalog_workspace):
    """SHOW TRIGGERS FOR a table filters on the holder, which for a commit
    trigger is its event_object_table - so this reads as it always did, and a
    task's trigger does not leak into a table's listing."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(session.execute_to_morsels("SHOW TRIGGERS FOR cat.coll1.src"))

    assert [r["trigger_name"] for r in rows] == ["refresh__coll1__mv"]
    assert rows[0]["event_object_table"] == "coll1.src"
    assert ("list_triggers", "coll1.nightly", "task") not in task_catalog_workspace.calls


def test_show_triggers_for_task_without_triggers_is_empty(task_catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _morsels_to_rows(session.execute_to_morsels("SHOW TRIGGERS FOR cat.coll1.delta"))
    assert rows == []
