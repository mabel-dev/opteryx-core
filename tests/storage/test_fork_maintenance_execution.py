# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`ALTER TABLE ... RESYNC | DETACH`, executed - not just planned.

`tests/unit/planner/test_fork_statements.py` holds the text-to-logical-plan half.
This runs the whole statement through a session - parse, plan, bind, physical
plan, the Relation Management operator - against a stub connector that records
what the operator asked of it. What is held: that each statement reaches the
connector once, naming the fork, carrying the session user as the author and
FORCE as written; that the binder's three gates (a writable connector, ALTER on
the fork, a metastore that can hold forks) refuse before anything is called;
and that the receipt says what was done, including how many files a DETACH
copied.

Whether the dataset is a fork, and whether FORCE was needed, are the catalog's
to decide (`opteryx-catalog/tests/test_fork_resync_detach.py`), so nothing here
models them.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.exceptions import ReadOnlyConnectorError
from opteryx.exceptions import UnsupportedSyntaxError

FORK = "forkws.coll.lineitem"
_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
# The number of borrowed files the stub reports a DETACH as having copied.
_FILES_COPIED = 3


class _ForkConnector(BaseConnector, Writable):
    """Records the fork-maintenance calls the operator makes."""

    calls = []

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")

    def supports_forking(self, relation_name):
        return True

    def resync_relation(self, relation_name, author=None, force=False):
        _ForkConnector.calls.append(("resync_relation", relation_name, author, force))

    def detach_relation(self, relation_name, author=None):
        _ForkConnector.calls.append(("detach_relation", relation_name, author))
        return _FILES_COPIED


class _ReadOnlyConnector(BaseConnector):
    """A connector that cannot be written to."""

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")


class _NoForksConnector(BaseConnector, Writable):
    """Writable, but its metastore cannot hold forks - `supports_forking` is the
    base class's False, and it has no fork operations to call."""

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")


@pytest.fixture
def workspace():
    _ForkConnector.calls = []
    register_workspace("forkws", _ForkConnector)
    register_workspace("forkro", _ReadOnlyConnector)
    register_workspace("forknf", _NoForksConnector)
    return _ForkConnector


def _run(statement, *, user="olive"):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels(statement))
    return session.messages


# --------------------------------------------------------------------------
# 1. RESYNC
# --------------------------------------------------------------------------


def test_resync_reaches_the_connector_naming_the_fork_and_the_author(workspace):
    _run(f"ALTER TABLE {FORK} RESYNC")

    assert workspace.calls == [("resync_relation", FORK, "olive", False)]


def test_resync_force_is_passed_through(workspace):
    _run(f"ALTER TABLE {FORK} RESYNC FORCE")

    assert workspace.calls == [("resync_relation", FORK, "olive", True)]


def test_resync_says_what_it_did(workspace):
    assert _run(f"ALTER TABLE {FORK} RESYNC") == [f"resynced `{FORK}` with its upstream"]


def test_resync_resolves_me_to_the_session_user(workspace):
    _run("ALTER TABLE forkws.$me.lineitem RESYNC", user="olive")

    assert workspace.calls == [("resync_relation", "forkws.olive.lineitem", "olive", False)]


# --------------------------------------------------------------------------
# 2. DETACH
# --------------------------------------------------------------------------


def test_detach_reaches_the_connector_naming_the_fork_and_the_author(workspace):
    _run(f"ALTER TABLE {FORK} DETACH")

    assert workspace.calls == [("detach_relation", FORK, "olive")]


def test_detach_reports_how_many_files_it_copied(workspace):
    assert _run(f"ALTER TABLE {FORK} DETACH") == [
        f"detached `{FORK}` ({_FILES_COPIED:,} borrowed file(s) materialized)"
    ]


# --------------------------------------------------------------------------
# 3. The binder's gates refuse before anything is called
# --------------------------------------------------------------------------


@pytest.fixture
def alter_refused():
    """A permissions capability that refuses ALTER, recording what it was asked.

    The engine's own capability permits everything - deployments register the
    one that enforces policy - so the gate is exercised by installing one here,
    and restoring the active capability after.
    """
    from opteryx import managers
    from opteryx.managers.permissions import register_permissions_capability

    module = managers.permissions

    class _RefusesAlter(type(module._CORE)):
        name = "refuses-alter"

        def __init__(self):
            self.asked = []

        def can_perform_action(self, execution_context, resource, action):
            self.asked.append((resource, action))
            return action != "ALTER"

    saved = (module._active, module._consulted)
    try:
        module._active, module._consulted = module._CORE, False
        capability = _RefusesAlter()
        register_permissions_capability(capability)
        yield capability
    finally:
        module._active, module._consulted = saved


@pytest.mark.parametrize("clause", ["RESYNC", "RESYNC FORCE", "DETACH"])
def test_without_alter_on_the_fork_nothing_is_called(workspace, alter_refused, clause):
    with pytest.raises(PermissionError):
        _run(f"ALTER TABLE {FORK} {clause}")

    assert (FORK, "ALTER") in alter_refused.asked, alter_refused.asked
    assert workspace.calls == []


@pytest.mark.parametrize("clause", ["RESYNC", "DETACH"])
def test_a_read_only_connector_is_refused(workspace, clause):
    with pytest.raises(ReadOnlyConnectorError):
        _run(f"ALTER TABLE forkro.coll.lineitem {clause}")

    assert workspace.calls == []


@pytest.mark.parametrize("clause", ["RESYNC", "RESYNC FORCE", "DETACH"])
def test_a_relation_that_cannot_be_a_fork_is_refused_at_bind(workspace, clause):
    # Before this gate, a writable connector without fork support bound
    # cleanly and failed in the operator with an AttributeError.
    with pytest.raises(UnsupportedSyntaxError, match="not held in an Opteryx metastore"):
        _run(f"ALTER TABLE forknf.coll.lineitem {clause}")


@pytest.mark.parametrize("clause", ["RESYNC", "DETACH"])
def test_the_permission_gate_answers_before_the_metastore_gate(workspace, alter_refused, clause):
    # As CLONE's: a caller who may not alter the relation learns that, not
    # where it is held.
    with pytest.raises(PermissionError):
        _run(f"ALTER TABLE forknf.coll.lineitem {clause}")
