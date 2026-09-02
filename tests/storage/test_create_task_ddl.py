# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""CREATE TASK / DROP TASK.

sqlparser has no TASK object type, so both are intercepted before the parser and
synthesized into AST nodes - the route REFRESH MATERIALIZED VIEW and DROP TRIGGER
take. EXECUTE needed none of that; it parses natively.

A task is stored SQL with no identity of its own: EXECUTE runs it as the
invoker, and an unattended run carries the TRIGGER's owner. So CREATE TASK
confers no authority and is gated only on the name it registers; the
materialized-view-style gates (billable owner, WRITE on the firing table) sit on
trigger creation, which is where unattended authority is actually granted.
"""

import json
import os

import pytest

import opteryx
from opteryx import managers
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.permissions import register_permissions_capability


class ScriptedCapability:
    """Permits exactly what it is told to.

    A local minimum of the harness in `test_permissions_capability.py` — that
    file is not importable from here, and restating only the members these
    scenarios exercise keeps the failure legible when `_REQUIRED_MEMBERS`
    changes. The grant-administration members raise: no test here runs GRANT,
    so a call reaching one is itself a bug.
    """

    name = "scripted"

    def __init__(self, allow=(), refuse_ownership=()):
        self.allow = set(allow)
        self.refuse_ownership = set(refuse_ownership)

    def can_perform_action(self, execution_context, resource, action):
        # A name with no dot is a local or internal relation; the engine asks
        # about those like any other and every capability has to answer.
        if "." not in resource:
            return action == "READ"
        return (resource, action) in self.allow

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return False

    def can_principal_perform_action(self, principal, resource, action):
        return False

    def can_principal_own_materialized_view(self, principal):
        return principal not in self.refuse_ownership

    def grants(self, identity, policies):
        return []

    def apply_grant(self, execution_context, pattern, role, principal):
        raise AssertionError("apply_grant should not be reached by these tests")

    def apply_revoke(self, execution_context, pattern, role, principal):
        raise AssertionError("apply_revoke should not be reached by these tests")

    def grants_on(self, execution_context, pattern):
        raise AssertionError("grants_on should not be reached by these tests")

    def effective_grants_on(self, execution_context, pattern):
        raise AssertionError("effective_grants_on should not be reached by these tests")

    def effective_grants_in(self, execution_context, workspace, objects):
        raise AssertionError("effective_grants_in should not be reached by these tests")


_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
_READER_POLICY = [{"pattern": "*", "role": "reader"}]


@pytest.fixture(autouse=True)
def permissions_state():
    """Put the permissions capability back after every test in this file."""
    module = managers.permissions
    before = (module._active, module._consulted)
    yield
    module._active, module._consulted = before


@pytest.fixture
def install():
    """Install a capability for one test — the harness from
    tests/storage/test_permissions_capability.py, reused rather than restated."""
    module = managers.permissions

    def _install(capability):
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(capability)
        return capability

    return _install


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _task_record(tmp_path, relation):
    path = tmp_path
    for part in relation.split("."):
        path = path / part
    with open(path / "task.json") as f:
        return json.load(f)


def _seed(session, name="ws.src"):
    list(session.execute_to_morsels(f"CREATE TABLE {name} (a BIGINT)"))
    list(session.execute_to_morsels(f"INSERT INTO {name} VALUES (1), (2)"))


# --- creation


def test_create_task_registers_the_statement(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    list(
        owner.execute_to_morsels(
            "CREATE TASK ws.copier AS INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low"
        )
    )

    record = _task_record(tmp_path / "ws", "copier")
    assert "INSERT INTO ws.sink" in record["sql"]
    # The placeholder survives registration - it is bound at EXECUTE, not now.
    assert ":low" in record["sql"]
    # No identity: EXECUTE runs as the invoker; unattended runs carry the
    # trigger's owner.
    assert "runs-as" not in record


def test_a_registered_task_is_executable(tmp_path):
    """The whole point: CREATE TASK then EXECUTE, with no sidecar written by hand."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    list(
        owner.execute_to_morsels(
            "CREATE TASK ws.copier AS INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low"
        )
    )
    list(owner.execute_to_morsels("EXECUTE ws.copier USING 1 AS low"))

    rows = []
    for morsel in owner.execute_to_morsels("SELECT * FROM ws.sink"):
        if morsel is None:
            continue
        rows.extend(morsel.to_arrow().to_pydict()["a"])
    assert sorted(rows) == [2]


def test_create_or_replace_updates_the_statement(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))
    list(owner.execute_to_morsels("CREATE OR REPLACE TASK ws.t AS SELECT a FROM ws.src WHERE a > 1"))

    assert "WHERE" in _task_record(tmp_path / "ws", "t")["sql"]


def test_creating_twice_without_or_replace_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    with pytest.raises(Exception, match="already exists"):
        list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))


# --- the statement must be valid, and must be a statement a task may run


def test_a_task_whose_statement_does_not_parse_is_refused_now(tmp_path):
    """Refused at creation, not discovered when it fires - by which time the
    failure is a job nobody is watching."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(Exception):
        list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT FROM WHERE"))


def test_a_task_cannot_be_defined_as_another_task(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="may not create, drop or run another task"):
        list(owner.execute_to_morsels("CREATE TASK ws.outer AS EXECUTE ws.inner"))


def test_malformed_create_task_is_rejected_by_name(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="CREATE"):
        list(owner.execute_to_morsels("CREATE TASK ws.t"))


# --- the gates


def test_creating_a_task_is_bounded_by_its_authors_own_grants(tmp_path, install):
    """A task may only do what its author could do at the moment they wrote it.

    Execution-time checks alone are not enough: the principal a task RUNS as
    need not be the one who last edited it. A trigger pins its owner and keeps
    it across edits, so a name-only gate would let anyone holding write on that
    name rewrite the statement and have it fire under the trigger owner's
    authority - the editor supplying the instructions, a higher-privileged
    principal supplying the permissions."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner, "ws.secret")

    # May write the task name; holds NOTHING on what the statement reads.
    install(ScriptedCapability(allow={("ws.leak", "AUTOMATE")}))
    mallory = opteryx.session(user="mallory")

    with pytest.raises(PermissionError, match="permission to read ws.secret"):
        list(mallory.execute_to_morsels("CREATE TASK ws.leak AS SELECT a FROM ws.secret"))


def test_an_author_who_can_read_the_source_may_create_the_task(tmp_path, install):
    """The bound is the author's own grants, not ownership of the source."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner, "ws.secret")

    install(ScriptedCapability(allow={("ws.ok", "AUTOMATE"), ("ws.secret", "READ")}))
    rhea = opteryx.session(user="rhea")

    list(rhea.execute_to_morsels("CREATE TASK ws.ok AS SELECT a FROM ws.secret"))


def test_replacing_a_task_is_bounded_the_same_way(tmp_path, install):
    """The confused deputy is created by the EDIT, so CREATE OR REPLACE is
    checked on every registration rather than only the first."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner, "ws.secret")

    install(ScriptedCapability(allow={("ws.t", "AUTOMATE"), ("ws.src", "READ")}))
    mallory = opteryx.session(user="mallory")

    list(mallory.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    # Repointing it at data mallory cannot read is refused, even though the task
    # already exists and mallory may write its name.
    with pytest.raises(PermissionError, match="permission to read ws.secret"):
        list(
            mallory.execute_to_morsels(
                "CREATE OR REPLACE TASK ws.t AS SELECT a FROM ws.secret"
            )
        )


# --- what a task writes


def _writes(tmp_path, task):
    return _task_record(tmp_path / "ws", task)["writes"]


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO ws.sink SELECT a FROM ws.src",
        "CREATE TABLE ws.sink AS SELECT a FROM ws.src",
        "UPDATE ws.sink SET a = 2",
        "DELETE FROM ws.sink WHERE a = 1",
        "MERGE INTO ws.sink t USING ws.src s ON t.a = s.a WHEN MATCHED THEN UPDATE SET t.a = s.a",
        "TRUNCATE TABLE ws.sink",
    ],
)
def test_every_write_form_records_what_it_writes(tmp_path, statement):
    """Derived from the statement's own AST, so it cannot disagree with it.

    Every form is here because the derivation used to read INSERT's target
    only: a task doing anything else recorded no output, and its target fell
    through into the SOURCE list where it was checked at READ.
    """
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    list(owner.execute_to_morsels(f"CREATE TASK ws.t AS {statement}"))

    assert _writes(tmp_path, "t") == ["ws.sink"]


def test_a_task_that_writes_nothing_records_nothing(tmp_path):
    """A SELECT task is not a pipeline edge, and says so - empty rather than
    absent, so a reader never has to tell "writes nothing" from "not asked"."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    assert _writes(tmp_path, "t") == []


def test_truncate_records_every_table_it_names(tmp_path):
    """The one form with more than one target, and the reason `writes` is a
    list rather than a single name."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.one (a BIGINT)"))
    list(owner.execute_to_morsels("CREATE TABLE ws.two (a BIGINT)"))

    list(owner.execute_to_morsels("CREATE TASK ws.t AS TRUNCATE TABLE ws.one, ws.two"))

    assert _writes(tmp_path, "t") == ["ws.one", "ws.two"]


def test_replacing_a_task_re_derives_what_it_writes(tmp_path):
    """It describes THIS statement. A replacement that no longer writes the old
    target must not leave it standing - a stale edge draws a pipeline that does
    not exist, which is worse than drawing none."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    list(
        owner.execute_to_morsels("CREATE TASK ws.t AS INSERT INTO ws.sink SELECT a FROM ws.src")
    )
    assert _writes(tmp_path, "t") == ["ws.sink"]

    list(owner.execute_to_morsels("CREATE OR REPLACE TASK ws.t AS SELECT a FROM ws.src"))
    assert _writes(tmp_path, "t") == []


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO ws.sink SELECT a FROM ws.src",
        "CREATE TABLE ws.sink AS SELECT a FROM ws.src",
        "UPDATE ws.sink SET a = 2",
        "DELETE FROM ws.sink WHERE a = 1",
        "MERGE INTO ws.sink t USING ws.src s ON t.a = s.a WHEN MATCHED THEN UPDATE SET t.a = s.a",
        "TRUNCATE TABLE ws.sink",
    ],
)
def test_the_authoring_bound_covers_every_write_form(tmp_path, install, statement):
    """A task may only WRITE what its author could write.

    The author here holds READ on the target and no more. That used to be
    enough for everything but INSERT: the target was classed as a source, so a
    READ grant satisfied a check that should have demanded WRITE - and the
    statement would then fire under a trigger owner's authority.
    """
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    install(
        ScriptedCapability(
            allow={("ws.t", "AUTOMATE"), ("ws.src", "READ"), ("ws.sink", "READ")}
        )
    )
    mallory = opteryx.session(user="mallory")

    with pytest.raises(PermissionError, match="permission to write ws.sink"):
        list(mallory.execute_to_morsels(f"CREATE TASK ws.t AS {statement}"))


def test_an_author_who_can_write_the_target_may_create_the_task(tmp_path, install):
    """The bound is the author's own grants - held, it lets the task through."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    install(
        ScriptedCapability(
            allow={("ws.t", "AUTOMATE"), ("ws.src", "READ"), ("ws.sink", "WRITE")}
        )
    )
    rhea = opteryx.session(user="rhea")

    list(rhea.execute_to_morsels("CREATE TASK ws.t AS UPDATE ws.sink SET a = 2"))

    assert _writes(tmp_path, "t") == ["ws.sink"]


def test_creating_a_task_needs_automate_on_its_name(tmp_path, install):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    # WRITE on the name is a writer's grant, and a writer may not register
    # something the platform runs on its own.
    install(ScriptedCapability(allow={("ws.src", "READ"), ("ws.t", "WRITE")}))
    rhea = opteryx.session(user="rhea")

    with pytest.raises(PermissionError, match="create task"):
        list(rhea.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))


def test_a_platform_identity_cannot_own_a_trigger(tmp_path, install):
    """Not a permissions question but a billing one: the trigger's unattended
    runs execute as its owner, and a platform identity has no billing account -
    so work pinned to one runs on a schedule forever and lands on nobody's
    bill. A plain CREATE TASK is fine (it confers nothing); it is the ON clause
    - the trigger - that is refused."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    install(
        ScriptedCapability(
            allow={("ws.src", "AUTOMATE"), ("ws.t", "AUTOMATE"), ("ws.t2", "AUTOMATE")},
            refuse_ownership={"federator"},
        )
    )
    federator = opteryx.session(user="federator")

    list(federator.execute_to_morsels("CREATE TASK ws.t AS SELECT 1"))

    with pytest.raises(PermissionError, match="billed to nobody"):
        list(federator.execute_to_morsels("CREATE TASK ws.t2 ON ws.src AS SELECT 1"))
    with pytest.raises(PermissionError, match="billed to nobody"):
        list(federator.execute_to_morsels("CREATE TRIGGER f ON ws.src EXECUTE ws.t"))


# --- drop


def test_drop_task_removes_it(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    list(owner.execute_to_morsels("DROP TASK ws.t"))

    assert not os.path.isfile(tmp_path / "ws" / "t" / "task.json")


def test_drop_missing_task_is_refused_unless_if_exists(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(Exception, match="not found"):
        list(owner.execute_to_morsels("DROP TASK ws.absent"))

    # IF EXISTS makes it a no-op rather than an error.
    list(owner.execute_to_morsels("DROP TASK IF EXISTS ws.absent"))


def test_malformed_drop_task_is_rejected_by_name(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="DROP TASK"):
        list(owner.execute_to_morsels("DROP TASK ws.t CASCADE"))


# --- triggers: the three variations of how a task gets fired


def test_a_task_with_no_on_clause_has_no_trigger(tmp_path):
    """Variation one: defined, and only ever run by hand. That is what a
    backfill or a replay is."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    assert owner.execute_to_morsels is not None
    assert not os.path.isfile(tmp_path / "ws" / "src" / "triggers.json")


def test_create_task_on_a_table_creates_the_trigger_too(tmp_path):
    """Variation two: one statement, both objects - the bargain CREATE
    MATERIALIZED VIEW already strikes."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))

    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        triggers = json.load(f)
    assert len(triggers) == 1
    assert triggers[0]["kind"] == "task"
    assert triggers[0]["target-task"] == "ws.t"


def test_create_trigger_standalone(tmp_path):
    """Variation three: the trigger authored separately, for a task that is
    fired by a dataset it does not itself read."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    _seed(owner, "ws.other")
    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    list(owner.execute_to_morsels("CREATE TRIGGER fire_t ON ws.other EXECUTE ws.t"))

    with open(tmp_path / "ws" / "other" / "triggers.json") as f:
        triggers = json.load(f)
    assert triggers[0]["name"] == "fire_t"
    assert triggers[0]["target-task"] == "ws.t"


def test_one_task_can_be_fired_by_several_datasets(tmp_path):
    """The reason a trigger is not implied by the task's own sources."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    _seed(owner, "ws.other")
    list(owner.execute_to_morsels("CREATE TASK ws.t AS SELECT a FROM ws.src"))

    list(owner.execute_to_morsels("CREATE TRIGGER a ON ws.src EXECUTE ws.t"))
    list(owner.execute_to_morsels("CREATE TRIGGER b ON ws.other EXECUTE ws.t"))

    for source in ("src", "other"):
        with open(tmp_path / "ws" / source / "triggers.json") as f:
            assert json.load(f)[0]["target-task"] == "ws.t"


def test_a_trigger_will_not_be_repointed_silently(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.a AS SELECT a FROM ws.src"))
    list(owner.execute_to_morsels("CREATE TASK ws.b AS SELECT a FROM ws.src"))
    list(owner.execute_to_morsels("CREATE TRIGGER t ON ws.src EXECUTE ws.a"))

    with pytest.raises(Exception, match="refusing to repoint"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON ws.src EXECUTE ws.b"))

    list(owner.execute_to_morsels("CREATE OR REPLACE TRIGGER t ON ws.src EXECUTE ws.b"))


def test_suspend_and_resume_a_trigger(tmp_path):
    """Suspending keeps the trigger and records the suppression - a dropped
    trigger and a paused one must not look the same to an operator."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))
    name = "task__ws__t"

    list(owner.execute_to_morsels(f"ALTER TRIGGER {name} ON ws.src SUSPEND"))
    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        assert json.load(f)[0]["suspended-at-ms"] is not None

    list(owner.execute_to_morsels(f"ALTER TRIGGER {name} ON ws.src RESUME"))
    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        assert json.load(f)[0]["suspended-at-ms"] is None


def _trigger_field(tmp_path, field):
    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        return json.load(f)[0][field]


def test_a_new_trigger_carries_the_default_firing_floor(tmp_path):
    """Written onto the record at creation, as the catalog does, so a sidecar
    and a catalog record read the same for the same statement."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))

    assert _trigger_field(tmp_path, "minimum-interval-seconds") == 120


@pytest.mark.parametrize(
    "clause, seconds",
    [
        ("30", 30),
        ("30 SECONDS", 30),
        ("1 SECOND", 1),
        ("2 MINUTES", 120),
        ("1 minute", 60),
        ("0", 0),
    ],
)
def test_set_minimum_interval_records_seconds(tmp_path, clause, seconds):
    """SECONDS is the unit the record holds; MINUTES is converted before the
    planner sees it, and 0 removes the floor rather than deleting the field."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))

    list(
        owner.execute_to_morsels(
            f"ALTER TRIGGER task__ws__t ON ws.src SET MINIMUM INTERVAL TO {clause}"
        )
    )
    assert _trigger_field(tmp_path, "minimum-interval-seconds") == seconds


def test_the_floor_survives_re_registration(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))
    list(owner.execute_to_morsels("ALTER TRIGGER task__ws__t ON ws.src SET MINIMUM INTERVAL TO 7"))

    list(owner.execute_to_morsels("CREATE OR REPLACE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))
    assert _trigger_field(tmp_path, "minimum-interval-seconds") == 7


@pytest.mark.parametrize(
    "clause",
    [
        "SET MIN INTERVAL TO 120",  # MIN reads as minutes next to a unit of time
        "SET MINIMUM INTERVAL TO -5",
        "SET MINIMUM INTERVAL TO 1.5 MINUTES",
        "SET MINIMUM INTERVAL TO 120 HOURS",
        "SET MINIMUM INTERVAL 120",
        "SET MINIMUM INTERVAL TO :n",
    ],
)
def test_malformed_minimum_interval_is_rejected_by_name(tmp_path, clause):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="SET MINIMUM INTERVAL TO"):
        list(owner.execute_to_morsels(f"ALTER TRIGGER task__ws__t ON ws.src {clause}"))


def test_set_minimum_interval_on_a_missing_trigger_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    with pytest.raises(ValueError, match="trigger not found"):
        list(owner.execute_to_morsels("ALTER TRIGGER nope ON ws.src SET MINIMUM INTERVAL TO 5"))


def test_words_that_are_not_events_are_refused_by_name(tmp_path):
    """ON EVERY and ON EVENT are not forms. They are refused by name, and the
    refusal says what the forms are - a commit, a schedule or a signal - rather
    than that the word is unknown."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    for sql in (
        "CREATE TRIGGER t ON EVERY 1 MINUTE EXECUTE ws.t",
        "CREATE TRIGGER t ON EVENT something EXECUTE ws.t",
    ):
        with pytest.raises(UnsupportedSyntaxError, match="is not a trigger event"):
            list(owner.execute_to_morsels(sql))


def test_a_schedule_must_be_a_cron_expression(tmp_path):
    """Five fields, checked for shape here; the catalog parses it properly."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="not a cron expression"):
        list(owner.execute_to_morsels("CREATE TRIGGER t ON SCHEDULE '1 minute' EXECUTE ws.t"))


# --- ALTER TASK


def test_alter_trigger_owner_transfers_runs_as(tmp_path):
    """The owner is the identity an UNATTENDED run carries, so it lives on the
    trigger. A person running EXECUTE runs it as themselves."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))
    name = "task__ws__t"

    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        assert json.load(f)[0]["runs-as"] == "olive"

    list(owner.execute_to_morsels(f"ALTER TRIGGER {name} ON ws.src OWNER TO rhea"))

    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        assert json.load(f)[0]["runs-as"] == "rhea"


def test_alter_trigger_owner_to_current_user(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))
    name = "task__ws__t"
    list(owner.execute_to_morsels(f"ALTER TRIGGER {name} ON ws.src OWNER TO rhea"))

    list(owner.execute_to_morsels(f"ALTER TRIGGER {name} ON ws.src OWNER TO CURRENT_USER"))

    with open(tmp_path / "ws" / "src" / "triggers.json") as f:
        assert json.load(f)[0]["runs-as"] == "olive"


def test_a_platform_identity_cannot_be_given_a_trigger(tmp_path, install):
    """The billing gate on the way IN as well as at creation - otherwise the
    ownership rule is one statement away from being bypassed."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))

    install(ScriptedCapability(allow={("ws.src", "AUTOMATE")}, refuse_ownership={"federator"}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError, match="billed to nobody"):
        list(
            session.execute_to_morsels(
                "ALTER TRIGGER task__ws__t ON ws.src OWNER TO federator"
            )
        )


def test_altering_a_trigger_needs_automate_on_its_table(tmp_path, install):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.t ON ws.src AS SELECT a FROM ws.src"))

    install(ScriptedCapability(allow={("ws.src", "READ"), ("ws.src", "WRITE")}))
    rhea = opteryx.session(user="rhea")

    with pytest.raises(PermissionError, match="alter a trigger"):
        list(rhea.execute_to_morsels("ALTER TRIGGER task__ws__t ON ws.src OWNER TO rhea"))


def test_a_task_has_no_owner_or_suspension_of_its_own(tmp_path):
    """Both live on the TRIGGER, which is what fires unattended. A task is a
    statement; who runs it and whether it runs are the trigger's questions."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="ALTER TRIGGER"):
        list(owner.execute_to_morsels("ALTER TASK ws.t SUSPEND"))
    with pytest.raises(UnsupportedSyntaxError, match="ALTER TRIGGER"):
        list(owner.execute_to_morsels("ALTER TASK ws.t OWNER TO rhea"))


def test_what_a_task_runs_cannot_be_altered_in_place(tmp_path):
    """Changed with CREATE OR REPLACE, so the statement history records it as a
    new version rather than an in-place edit nothing remembers."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="CREATE OR REPLACE TASK"):
        list(owner.execute_to_morsels("ALTER TASK ws.t AS SELECT 1"))


# --- one namespace


def test_a_task_cannot_take_a_table_name(tmp_path):
    """`workspace.collection.<object>` is ONE namespace: a name identifies a
    table, a view or a task, never two of them."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner, "ws.thing")

    with pytest.raises(Exception, match="already exists as a table"):
        list(owner.execute_to_morsels("CREATE TASK ws.thing AS SELECT 1"))


def test_a_table_cannot_take_a_task_name(tmp_path):
    """The check runs in both directions, or it is not a namespace."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TASK ws.thing AS SELECT 1"))

    with pytest.raises(Exception, match="already exists as a task"):
        list(owner.execute_to_morsels("CREATE TABLE ws.thing (a BIGINT)"))


def test_a_view_cannot_take_a_task_name(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TASK ws.thing AS SELECT 1"))

    with pytest.raises(Exception, match="already exists as a task"):
        list(owner.execute_to_morsels("CREATE VIEW ws.thing AS SELECT a FROM ws.src"))


def test_replacing_a_task_is_not_a_collision(tmp_path):
    """The same-kind case belongs to the creator's own replace logic."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TASK ws.thing AS SELECT 1"))

    list(owner.execute_to_morsels("CREATE OR REPLACE TASK ws.thing AS SELECT 2"))

    assert "SELECT 2" in _task_record(tmp_path / "ws", "thing")["sql"]
