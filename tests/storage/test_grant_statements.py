"""GRANT / REVOKE / SHOW GRANTS ON — the SQL grant-administration surface.

The engine parses, gates, and hands off: every rule (owner authority, the
no-self-service rule, 1:1 resolution, conflicts, audit) lives in the
registered permissions capability, and these tests pin the seam — the
pre-parse grammar, the object-kind arity mapping, the binder's owner gate,
the capability calls the statements make, and the loud refusal under the
intrinsic PermitAll (a GRANT that "succeeds" with no policy store would be
fake green).

Design: docs/GRANT_SQL_DESIGN.md (architect rulings 2026-08-27).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx import managers
from opteryx.exceptions import InvalidConfigurationError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.pre_parse import pre_parse


class RecordingAdminCapability:
    """Permits the binder's gates and records the administration calls."""

    name = "recording-admin"

    def __init__(self, rows=(), allow_gates=True):
        self.rows = list(rows)
        self.allow_gates = allow_gates
        self.applied = []

    def can_perform_action(self, execution_context, resource, action):
        if "." not in resource:
            return action == "READ"
        return self.allow_gates

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return self.allow_gates

    def can_principal_perform_action(self, principal, resource, action):
        return True

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return []

    def apply_grant(self, execution_context, pattern, role, principal):
        self.applied.append(("grant", pattern, role, principal, execution_context.user))
        return "policy-1"

    def apply_revoke(self, execution_context, pattern, role, principal):
        self.applied.append(("revoke", pattern, role, principal, execution_context.user))
        return "policy-1"

    def grants_on(self, execution_context, pattern):
        self.applied.append(("grants_on", pattern, execution_context.user))
        return self.rows


@pytest.fixture(autouse=True)
def permissions_state():
    """Restore the module's capability state after every test in this file."""
    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield
    module._active, module._consulted = saved_active, saved_consulted


@pytest.fixture
def install():
    def _install(capability):
        module = managers.permissions
        module._active, module._consulted = capability, False
        return capability

    return _install


def _run(sql, user="alice"):
    session = opteryx.session(user=user)
    return list(session.execute_to_morsels(sql))


# --- pre-parse grammar


def test_grant_parses_to_a_synthesized_statement():
    [statement] = pre_parse("GRANT reader ON DATASET a.b.c TO USER bob")
    assert statement == {
        "GrantAccess": {
            "role": "reader",
            "object_kind": "dataset",
            "object_name": "a.b.c",
            "principal": "bob",
        }
    }


def test_revoke_parses_to_a_synthesized_statement():
    [statement] = pre_parse("REVOKE OWNER ON WORKSPACE ws FROM USER 'x@y.z'")
    assert statement == {
        "RevokeAccess": {
            "role": "owner",
            "object_kind": "workspace",
            "object_name": "ws",
            "principal": "x@y.z",
        }
    }


def test_show_grants_on_parses_and_bare_show_grants_is_untouched():
    [statement] = pre_parse("SHOW GRANTS ON COLLECTION ws.coll")
    assert statement == {
        "ShowGrantsOn": {"object_kind": "collection", "object_name": "ws.coll"}
    }
    # Bare SHOW GRANTS is the session's own grants and stays on the parser path.
    assert pre_parse("SHOW GRANTS") is None


def test_crossed_prepositions_are_refused():
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("GRANT reader ON DATASET a.b.c FROM USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("REVOKE reader ON DATASET a.b.c TO USER bob")


def test_unknown_roles_and_kinds_are_refused_by_name():
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("GRANT admin ON DATASET a.b.c TO USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("GRANT reader ON TABLE a.b.c TO USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        # USER is mandatory — it reserves the grammar for TO ROLE later.
        pre_parse("GRANT reader ON DATASET a.b.c TO bob")


# --- object kind → pattern (arity is asserted, never guessed)


def test_object_kinds_map_to_patterns(install):
    capability = install(RecordingAdminCapability())
    _run("GRANT reader ON WORKSPACE ws TO USER bob")
    _run("GRANT reader ON COLLECTION ws.coll TO USER bob")
    _run("GRANT reader ON DATASET ws.coll.ds TO USER bob")
    assert [call[1] for call in capability.applied] == ["ws.*", "ws.coll.*", "ws.coll.ds"]


def test_arity_mismatches_are_errors_not_reinterpretations(install):
    install(RecordingAdminCapability())
    with pytest.raises(UnsupportedSyntaxError):
        _run("GRANT reader ON DATASET ws.coll TO USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        _run("GRANT reader ON WORKSPACE ws.coll TO USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        _run("SHOW GRANTS ON COLLECTION ws")


# --- the statements delegate whole to the capability


def test_grant_and_revoke_hand_off_to_the_capability(install):
    capability = install(RecordingAdminCapability())
    _run("GRANT writer ON COLLECTION ws.sales TO USER bob")
    _run("REVOKE writer ON COLLECTION ws.sales FROM USER bob")
    assert capability.applied == [
        ("grant", "ws.sales.*", "writer", "bob", "alice"),
        ("revoke", "ws.sales.*", "writer", "bob", "alice"),
    ]


def test_show_grants_on_renders_the_capability_rows(install):
    install(
        RecordingAdminCapability(
            rows=[
                {"user": "alice", "pattern": "ws.*", "level": "workspace", "role": "owner"},
                {"user": "bob", "pattern": "ws.c.d", "level": "dataset", "role": "reader"},
            ]
        )
    )
    [morsel] = _run("SHOW GRANTS ON WORKSPACE ws")
    assert morsel.column_names == [b"user", b"pattern", b"level", b"role"]
    assert [tuple(row) for row in morsel] == [
        ("alice", "ws.*", "workspace", "owner"),
        ("bob", "ws.c.d", "dataset", "reader"),
    ]


# --- the binder's owner gate


def test_all_three_statements_are_refused_without_the_gate(install):
    capability = install(RecordingAdminCapability(allow_gates=False))
    for sql in (
        "GRANT reader ON DATASET ws.c.d TO USER bob",
        "REVOKE reader ON DATASET ws.c.d FROM USER bob",
        "SHOW GRANTS ON WORKSPACE ws",
    ):
        with pytest.raises(PermissionError):
            _run(sql)
    # Refused at bind: the capability's apply/list surface was never reached.
    assert capability.applied == []


# --- the intrinsic capability refuses, loudly


def test_permit_all_refuses_grant_administration():
    # Embedded/CLI opteryx has no policy service: a GRANT that "succeeded"
    # would have granted nothing and enforced nothing. Loud error, never a
    # no-op.
    for sql in (
        "GRANT reader ON DATASET ws.c.d TO USER bob",
        "REVOKE reader ON DATASET ws.c.d FROM USER bob",
        "SHOW GRANTS ON WORKSPACE ws",
    ):
        with pytest.raises(InvalidConfigurationError):
            _run(sql)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
