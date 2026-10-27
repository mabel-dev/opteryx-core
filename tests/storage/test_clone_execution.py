# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`CREATE TABLE | COLLECTION ... CLONE`, executed through a session.

`tests/unit/planner/test_fork_statements.py` holds the planning half and
`test_clone_metastore_gate.py` the `supports_forking` answers themselves. This
runs the whole statement - parse, plan, bind, physical plan, the Relation
Management operator - against stub connectors, and holds the binder's metastore
gate at BOTH ends: the source must be forkable (a fork borrows its manifest) and
so must the target (only the native metastore can create a fork). A writable
target without fork support used to bind cleanly and fail in the operator with
an AttributeError.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.exceptions import UnsupportedSyntaxError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
# The number of datasets the stub reports a collection clone as having forked.
_DATASETS_CLONED = 8


class _ForkableConnector(BaseConnector, Writable):
    """A native-metastore stand-in: can hold forks, records the clones asked of it."""

    calls = []

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")

    def supports_forking(self, relation_name):
        return True

    def clone_relation(self, relation_name, source_relation, author=None):
        _ForkableConnector.calls.append(("clone_relation", relation_name, source_relation, author))

    def clone_collection(self, collection_name, source_collection, author=None):
        _ForkableConnector.calls.append(
            ("clone_collection", collection_name, source_collection, author)
        )
        return _DATASETS_CLONED


class _NoForksConnector(BaseConnector, Writable):
    """Writable, but its metastore cannot hold forks - `supports_forking` is the
    base class's False, and it has no clone operations to call."""

    def __init__(self, **kwargs):
        self.telemetry = kwargs.get("telemetry")


@pytest.fixture
def workspaces():
    _ForkableConnector.calls = []
    register_workspace("clonesrc", _ForkableConnector)
    register_workspace("clonedst", _ForkableConnector)
    register_workspace("clonenf", _NoForksConnector)
    return _ForkableConnector


@pytest.fixture
def create_refused():
    """A permissions capability that refuses CREATE (see
    `test_fork_maintenance_execution.alter_refused` for why one is installed)."""
    from opteryx import managers
    from opteryx.managers.permissions import register_permissions_capability

    module = managers.permissions

    class _RefusesCreate(type(module._CORE)):
        name = "refuses-create"

        def can_perform_action(self, execution_context, resource, action):
            return action != "CREATE"

    saved = (module._active, module._consulted)
    try:
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(_RefusesCreate())
        yield
    finally:
        module._active, module._consulted = saved


def _run(statement):
    session = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels(statement))
    return session.messages


# --------------------------------------------------------------------------
# 1. A forkable source into a forkable target reaches the connector
# --------------------------------------------------------------------------


def test_a_table_clone_reaches_the_target_connector(workspaces):
    messages = _run("CREATE TABLE clonedst.coll.lineitem CLONE clonesrc.coll.lineitem")

    assert workspaces.calls == [
        ("clone_relation", "clonedst.coll.lineitem", "clonesrc.coll.lineitem", "olive")
    ]
    assert messages == ["cloned `clonesrc.coll.lineitem` to `clonedst.coll.lineitem`"]


def test_a_collection_clone_reaches_the_target_connector(workspaces):
    messages = _run("CREATE COLLECTION clonedst.tpch CLONE clonesrc.tpch")

    assert workspaces.calls == [("clone_collection", "clonedst.tpch", "clonesrc.tpch", "olive")]
    assert messages == [
        f"cloned collection `clonesrc.tpch` to `clonedst.tpch` ({_DATASETS_CLONED:,} relation(s))"
    ]


# --------------------------------------------------------------------------
# 2. Either end that cannot hold a fork is refused at bind
# --------------------------------------------------------------------------


def test_a_table_clone_into_a_target_that_cannot_hold_forks_is_refused(workspaces):
    with pytest.raises(UnsupportedSyntaxError, match="so a fork cannot be created there"):
        _run("CREATE TABLE clonenf.coll.lineitem CLONE clonesrc.coll.lineitem")

    assert workspaces.calls == []


def test_a_collection_clone_into_a_target_that_cannot_hold_forks_is_refused(workspaces):
    with pytest.raises(UnsupportedSyntaxError, match="so forks cannot be created there"):
        _run("CREATE COLLECTION clonenf.tpch CLONE clonesrc.tpch")

    assert workspaces.calls == []


def test_a_table_clone_from_a_source_that_cannot_be_forked_is_refused(workspaces):
    with pytest.raises(UnsupportedSyntaxError, match="so it cannot be cloned"):
        _run("CREATE TABLE clonedst.coll.lineitem CLONE clonenf.coll.lineitem")

    assert workspaces.calls == []


def test_a_collection_clone_from_a_source_that_cannot_be_forked_is_refused(workspaces):
    with pytest.raises(UnsupportedSyntaxError, match="so it cannot be cloned"):
        _run("CREATE COLLECTION clonedst.tpch CLONE clonenf.tpch")

    assert workspaces.calls == []


# --------------------------------------------------------------------------
# 3. The permission gate answers first
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE clonenf.coll.lineitem CLONE clonesrc.coll.lineitem",
        "CREATE COLLECTION clonenf.tpch CLONE clonesrc.tpch",
    ],
)
def test_without_create_on_the_target_the_metastore_is_not_reported(
    workspaces, create_refused, statement
):
    with pytest.raises(PermissionError):
        _run(statement)
