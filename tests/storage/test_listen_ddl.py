# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""LISTEN TO / UNLISTEN — task notification subscriptions.

sqlparser HAS a LISTEN grammar and it is deliberately unused: it is Postgres's
session-scoped `LISTEN <channel>`, gated behind a dialect flag OpteryxDialect
does not set, and its AST reports no source span. These subscriptions are
durable, owned by a user, and fire when nobody is connected - so they take the
pre-parse route CREATE TASK takes, for the same reason.

The gate is the design's central ruling: **LISTEN is a READ activity**, decided
on the caller's access to what the task WRITES, not on AUTOMATE over the task.
A subscription reports that a dataset was refreshed or failed to be, which is a
fact about the dataset - so the people entitled to it are the people who can
read the dataset. See docs/LISTEN_SQL_DESIGN.md §6.
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
    """Permits exactly the (resource, action) pairs it is told to.

    The same local minimum as `test_create_task_ddl.py`'s, restated rather than
    imported so a change to `_REQUIRED_MEMBERS` fails legibly here too.
    """

    name = "scripted"

    def __init__(self, allow=(), allow_all=False):
        self.allow = set(allow)
        self.allow_all = allow_all

    def can_perform_action(self, execution_context, resource, action):
        if "." not in resource:
            return action == "READ"
        if self.allow_all:
            return True
        return (resource, action) in self.allow

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return False

    def can_principal_perform_action(self, principal, resource, action):
        return False

    def can_principal_own_materialized_view(self, principal):
        return True

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


@pytest.fixture(autouse=True)
def permissions_state():
    module = managers.permissions
    before = (module._active, module._consulted)
    yield
    module._active, module._consulted = before


@pytest.fixture
def install():
    module = managers.permissions

    def _install(capability):
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(capability)
        return capability

    return _install


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _listeners(tmp_path, relation):
    path = tmp_path
    for part in relation.split("."):
        path = path / part
    listeners_file = path / "listeners.json"
    if not listeners_file.is_file():
        return {}
    with open(listeners_file) as f:
        return json.load(f)


def _seed_task(session, task="ws.loader", sink="ws.sink"):
    """A task that writes one relation - the relation LISTEN is gated on.

    Seeded under a permissive capability: what CREATE TABLE and CREATE TASK are
    gated on is `test_create_task_ddl.py`'s subject, not this file's. The
    capability that matters here is the one installed AFTER seeding, which is
    what LISTEN is then decided against.
    """
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    list(session.execute_to_morsels(f"CREATE TABLE {sink} (a BIGINT)"))
    list(
        session.execute_to_morsels(
            f"CREATE TASK {task} AS INSERT INTO {sink} SELECT a FROM ws.src"
        )
    )


# --- grammar


@pytest.mark.parametrize(
    "statement",
    [
        "LISTEN daily_load",  # no TO
        "LISTEN TO",  # no task
        "LISTEN TO t FOR NONSENSE",  # not an outcome
        "LISTEN TO t FOR ERROR, SUCCESS",  # a list, not a keyword
        "UNLISTEN",  # no task
        "UNLISTEN t FOR ERROR",  # UNLISTEN takes no filter
        "SHOW LISTENERS ON t",  # takes no arguments
    ],
)
def test_malformed_statements_are_refused_by_name(statement):
    from opteryx.planner.pre_parse import pre_parse

    with pytest.raises(UnsupportedSyntaxError):
        pre_parse(statement)


def test_unlisten_wildcard_is_refused():
    """`UNLISTEN *` is sqlparser's grammar and deliberately not ours: a
    statement that silently empties every subscription a user holds should not
    be one keystroke away from `UNLISTEN t`."""
    from opteryx.planner.pre_parse import pre_parse

    with pytest.raises(UnsupportedSyntaxError, match="wildcard"):
        pre_parse("UNLISTEN *")


def test_no_for_clause_means_every_outcome():
    from opteryx.planner.pre_parse import pre_parse

    assert pre_parse("LISTEN TO t")[0]["Listen"]["outcome"] == "EVERYTHING"


# --- recording


def test_listen_records_a_subscription(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)

    list(session.execute_to_morsels("LISTEN TO ws.loader FOR ERROR"))

    listeners = _listeners(tmp_path, "ws.loader")
    assert list(listeners) == ["olive"]
    assert listeners["olive"]["outcome"] == "ERROR"


def test_listen_defaults_to_everything(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)

    list(session.execute_to_morsels("LISTEN TO ws.loader"))

    assert _listeners(tmp_path, "ws.loader")["olive"]["outcome"] == "EVERYTHING"


def test_a_second_listen_is_refused_with_the_sql_that_changes_it(tmp_path, install):
    """One subscription per user per task. The refusal knows both halves - the
    task and the outcome just asked for - so it renders the pair to run rather
    than describing them."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)
    list(session.execute_to_morsels("LISTEN TO ws.loader FOR ERROR"))

    with pytest.raises(Exception) as caught:
        list(session.execute_to_morsels("LISTEN TO ws.loader FOR EVERYTHING"))

    message = str(caught.value)
    assert "already listen" in message
    assert "UNLISTEN" in message and "ws.loader" in message
    # The outcome just ASKED for, not the one already held: the statement it
    # hands back is the one that gets you what you wanted.
    assert "EVERYTHING" in message
    # And nothing was changed by the refused statement.
    assert _listeners(tmp_path, "ws.loader")["olive"]["outcome"] == "ERROR"


def test_unlisten_removes_the_subscription(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)
    list(session.execute_to_morsels("LISTEN TO ws.loader"))

    list(session.execute_to_morsels("UNLISTEN ws.loader"))

    assert _listeners(tmp_path, "ws.loader") == {}


def test_unlisten_without_a_subscription_is_an_error(tmp_path, install):
    """Not a no-op. A delete that succeeds on zero rows tells someone they have
    stopped receiving notifications they were never receiving, and leaves the
    real subscription - under the name they meant - running."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)

    with pytest.raises(Exception, match="do not listen"):
        list(session.execute_to_morsels("UNLISTEN ws.loader"))


def test_subscriptions_are_per_user(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    olive = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(olive)
    rhea = opteryx.session(user="rhea", access_policies=_OWNER_POLICY)

    list(olive.execute_to_morsels("LISTEN TO ws.loader FOR ERROR"))
    list(rhea.execute_to_morsels("LISTEN TO ws.loader FOR SUCCESS"))

    listeners = _listeners(tmp_path, "ws.loader")
    assert listeners["olive"]["outcome"] == "ERROR"
    assert listeners["rhea"]["outcome"] == "SUCCESS"


# --- lifecycle


def test_drop_task_takes_the_subscriptions_with_it(tmp_path, install):
    """Subscriptions are a property of the task (architect ruling 2026-09-02)."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)
    list(session.execute_to_morsels("LISTEN TO ws.loader"))

    list(session.execute_to_morsels("DROP TASK ws.loader"))

    assert _listeners(tmp_path, "ws.loader") == {}


def test_create_or_replace_task_keeps_the_subscriptions(tmp_path, install):
    """People subscribed to the TASK, not to its body: the name is the identity
    and it survives. A replace therefore hands the new statement an existing
    audience, which is deliberate."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_task(session)
    list(session.execute_to_morsels("LISTEN TO ws.loader FOR ERROR"))

    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE TASK ws.loader AS INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > 1"
        )
    )

    assert _listeners(tmp_path, "ws.loader")["olive"]["outcome"] == "ERROR"


# --- materialized views, on the same terms
#
# A trigger either EXECUTEs a task or REFRESHes a view, and the two paths differ
# only in the statement they build - so the subscribable object is whatever a
# trigger targets. The caller never says which kind: a table, a view and a task
# share one namespace, so the name identifies exactly one of them.


def _seed_mv(session, view="ws.daily"):
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    # A view materializes on creation, and the writer refuses an empty morsel -
    # so the source needs rows before there is a view to subscribe to.
    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (1), (2)"))
    list(
        session.execute_to_morsels(
            f"CREATE MATERIALIZED VIEW {view} AS SELECT a FROM ws.src"
        )
    )


def test_a_materialized_view_can_be_listened_to(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_mv(session)

    list(session.execute_to_morsels("LISTEN TO ws.daily FOR ERROR"))

    assert _listeners(tmp_path, "ws.daily")["olive"]["outcome"] == "ERROR"


def test_a_view_is_gated_on_reading_the_view_itself(tmp_path, install):
    """A view IS what it writes, so the task's `writes` lookup - and its
    empty-writes refusal - cannot arise."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_mv(owner)

    install(ScriptedCapability(allow={("ws.daily", "READ")}))
    reader = opteryx.session(user="rhea", access_policies=_OWNER_POLICY)
    list(reader.execute_to_morsels("LISTEN TO ws.daily"))

    assert "rhea" in _listeners(tmp_path, "ws.daily")


def test_without_read_on_the_view_there_is_no_subscription(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_mv(owner)

    install(ScriptedCapability(allow=set()))
    stranger = opteryx.session(user="mallory", access_policies=_OWNER_POLICY)
    with pytest.raises(PermissionError):
        list(stranger.execute_to_morsels("LISTEN TO ws.daily"))

    assert _listeners(tmp_path, "ws.daily") == {}


def test_a_plain_table_cannot_be_listened_to(tmp_path, install):
    """Nothing fires a table, so the subscription could never be delivered -
    and the refusal is the same one an unreadable object gets."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE ws.plain (a BIGINT)"))

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("LISTEN TO ws.plain"))


def test_dropping_the_view_takes_its_subscriptions(tmp_path, install):
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_mv(session)
    list(session.execute_to_morsels("LISTEN TO ws.daily"))

    list(session.execute_to_morsels("DROP MATERIALIZED VIEW ws.daily"))

    assert _listeners(tmp_path, "ws.daily") == {}


# --- the READ gate


def test_read_on_what_the_task_writes_is_what_admits_a_subscriber(tmp_path, install):
    """Not AUTOMATE on the task. A user with READ on the written relation and no
    ownership of the task may subscribe."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    install(ScriptedCapability(allow_all=True))
    _seed_task(owner)

    # A reader of the sink, with NO authority over the task itself.
    install(ScriptedCapability(allow={("ws.sink", "READ")}))
    reader = opteryx.session(user="rhea", access_policies=_OWNER_POLICY)
    list(reader.execute_to_morsels("LISTEN TO ws.loader"))

    assert "rhea" in _listeners(tmp_path, "ws.loader")


def test_without_read_on_what_it_writes_there_is_no_subscription(tmp_path, install):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    install(ScriptedCapability(allow_all=True))
    _seed_task(owner)

    install(ScriptedCapability(allow=set()))
    stranger = opteryx.session(user="mallory", access_policies=_OWNER_POLICY)
    with pytest.raises(PermissionError):
        list(stranger.execute_to_morsels("LISTEN TO ws.loader"))

    assert _listeners(tmp_path, "ws.loader") == {}


def test_a_task_that_records_no_writes_admits_nobody(tmp_path, install):
    """No table to gate on means no grant that admits a subscriber. Failing open
    here would make every task registered before `writes` existed subscribable
    by anyone."""
    _setup_workspace(tmp_path)
    install(ScriptedCapability(allow_all=True))
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    # Reads and writes nothing back, so `writes` is empty.
    list(session.execute_to_morsels("CREATE TASK ws.auditor AS SELECT a FROM ws.src"))

    with pytest.raises(PermissionError, match="no relations that it writes"):
        list(session.execute_to_morsels("LISTEN TO ws.auditor"))


def test_the_refusal_does_not_reveal_whether_the_task_exists(tmp_path, install):
    """Distinguishing "no such task" from "you cannot see what it writes" makes
    LISTEN a probe: a caller with no grants could enumerate task names by
    reading which refusal came back."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    install(ScriptedCapability(allow_all=True))
    _seed_task(owner)

    install(ScriptedCapability(allow=set()))
    stranger = opteryx.session(user="mallory", access_policies=_OWNER_POLICY)

    with pytest.raises(PermissionError) as real:
        list(stranger.execute_to_morsels("LISTEN TO ws.loader"))
    with pytest.raises(PermissionError) as absent:
        list(stranger.execute_to_morsels("LISTEN TO ws.no_such_task"))

    # Same sentence, differing only in the name the caller themselves typed.
    assert str(real.value).replace("ws.loader", "X") == str(absent.value).replace(
        "ws.no_such_task", "X"
    )


def test_unlisten_is_gated_the_same_way(tmp_path, install):
    """Not weakened to "anyone may stop listening": an ungated UNLISTEN answers
    whether a task exists by refusing differently for a name that does."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    install(ScriptedCapability(allow_all=True))
    _seed_task(owner)

    install(ScriptedCapability(allow=set()))
    stranger = opteryx.session(user="mallory", access_policies=_OWNER_POLICY)
    with pytest.raises(PermissionError):
        list(stranger.execute_to_morsels("UNLISTEN ws.loader"))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
