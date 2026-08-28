"""GRANT / REVOKE / SHOW [EFFECTIVE] GRANTS ON — the SQL grant-administration
surface.

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
from opteryx.exceptions import ParameterError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.logical_planner.logical_planner import (
    plan_alter_materialized_view_owner,
)
from opteryx.planner.pre_parse import pre_parse


class RecordingAdminCapability:
    """Permits the binder's gates and records the administration calls."""

    name = "recording-admin"

    def __init__(self, rows=(), effective_rows=(), allow_gates=True):
        self.rows = list(rows)
        self.effective_rows = list(effective_rows)
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

    def effective_grants_on(self, execution_context, pattern):
        self.applied.append(("effective_grants_on", pattern, execution_context.user))
        return self.effective_rows


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


def _run(sql, user="alice", params=None):
    session = opteryx.session(user=user)
    return list(session.execute_to_morsels(sql, params))


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


def test_every_statement_is_refused_without_the_gate(install):
    capability = install(RecordingAdminCapability(allow_gates=False))
    for sql in (
        "GRANT reader ON DATASET ws.c.d TO USER bob",
        "REVOKE reader ON DATASET ws.c.d FROM USER bob",
        "SHOW GRANTS ON WORKSPACE ws",
        # Reports strictly more than the attached listing, so gated no more
        # loosely — never at a read tier.
        "SHOW EFFECTIVE GRANTS ON DATASET ws.c.d",
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
        "SHOW EFFECTIVE GRANTS ON WORKSPACE ws",
    ):
        with pytest.raises(InvalidConfigurationError):
            _run(sql)


# --- parameter binding
#
# These statements are synthesized from a regex, not parsed, so a `:name` in one
# was captured as TEXT and used as data: `TO USER :username` granted access to a
# principal literally called ":username". The value slots now emit the same
# Placeholder node sqlparser emits, and the AST rewriter binds them.


def test_value_slots_parse_to_placeholder_nodes():
    [statement] = pre_parse("GRANT reader ON DATASET :ds TO USER :username")
    assert statement == {
        "GrantAccess": {
            "role": "reader",
            "object_kind": "dataset",
            "object_name": {"Placeholder": ":ds"},
            "principal": {"Placeholder": ":username"},
        }
    }
    [statement] = pre_parse("SHOW GRANTS ON COLLECTION :coll")
    assert statement == {
        "ShowGrantsOn": {"object_kind": "collection", "object_name": {"Placeholder": ":coll"}}
    }


def test_a_bound_principal_reaches_the_capability(install):
    capability = install(RecordingAdminCapability())
    _run(
        "GRANT writer ON COLLECTION ws.sales TO USER :username",
        params={"username": "bob"},
    )
    assert capability.applied == [("grant", "ws.sales.*", "writer", "bob", "alice")]


def test_a_bound_object_reaches_the_capability(install):
    capability = install(RecordingAdminCapability())
    _run("GRANT reader ON DATASET :ds TO USER bob", params={"ds": "ws.coll.orders"})
    _run("SHOW GRANTS ON WORKSPACE :ws", params={"ws": "ws"})
    assert [call[1] for call in capability.applied] == ["ws.coll.orders", "ws.*"]


def test_qmark_parameters_bind_too(install):
    capability = install(RecordingAdminCapability())
    _run("GRANT reader ON DATASET ? TO USER ?", params=["ws.coll.orders", "bob"])
    assert capability.applied == [("grant", "ws.coll.orders", "reader", "bob", "alice")]


def test_an_unsupplied_placeholder_is_an_error_not_a_literal(install):
    """The failure mode this exists to remove: `:username` reaching the
    capability as the name of a principal."""
    capability = install(RecordingAdminCapability())
    with pytest.raises(ParameterError):
        _run("GRANT reader ON DATASET ws.c.d TO USER :username")
    with pytest.raises(ParameterError):
        _run("GRANT reader ON DATASET :ds TO USER bob")
    with pytest.raises(ParameterError):
        _run("SHOW GRANTS ON WORKSPACE :ws")
    assert capability.applied == []


def test_a_placeholder_is_never_granted_to_as_a_principal(install):
    """Regression: `TO USER :username` with nothing supplied used to create a
    grant for a principal named ':username'."""
    capability = install(RecordingAdminCapability())
    with pytest.raises(ParameterError):
        _run("GRANT owner ON WORKSPACE ws TO USER :username")
    assert capability.applied == []
    assert not any(":" in str(call) for call in capability.applied)


def test_a_wrongly_named_parameter_is_reported_by_name(install):
    install(RecordingAdminCapability())
    with pytest.raises(ParameterError, match="username"):
        _run("GRANT reader ON DATASET ws.c.d TO USER :username", params={"user": "bob"})


def test_a_non_string_parameter_is_refused(install):
    """A principal names a person. Binding a number to that slot fails rather
    than being coerced into one."""
    capability = install(RecordingAdminCapability())
    with pytest.raises(ParameterError):
        _run("GRANT reader ON DATASET ws.c.d TO USER :username", params={"username": 7})
    assert capability.applied == []


def test_a_bound_object_is_still_arity_checked(install):
    """Binding happens before planning, so the kind is still an assertion on the
    name - a parameter cannot smuggle a two-part name into a DATASET."""
    install(RecordingAdminCapability())
    with pytest.raises(UnsupportedSyntaxError):
        _run("GRANT reader ON DATASET :ds TO USER bob", params={"ds": "ws.coll"})


def test_roles_and_kinds_do_not_take_parameters():
    """They are keywords from a closed set: a parameter there would make the
    statement's shape - which authority, which arity - depend on runtime data."""
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("GRANT :role ON DATASET a.b.c TO USER bob")
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("GRANT reader ON :kind a.b.c TO USER bob")


def test_identifier_slots_do_not_take_parameters():
    """A parameterised relation name would let runtime data choose what the
    statement acts on; the parser refuses `SELECT * FROM :t` for the same
    reason."""
    for sql in (
        "DROP STATISTICS ON :table",
        "DROP TRIGGER trg ON :table",
        "REFRESH MATERIALIZED VIEW :view",
        "ALTER MATERIALIZED VIEW :view OWNER TO bob",
    ):
        with pytest.raises(UnsupportedSyntaxError):
            pre_parse(sql)


def test_alter_materialized_view_owner_takes_a_parameter():
    """The owner is a principal, the same kind of value GRANT's is."""
    [statement] = pre_parse("ALTER MATERIALIZED VIEW a.b.c OWNER TO :who")
    assert statement["AlterMaterializedViewOwner"] == {
        "name": "a.b.c",
        "owner": {"Placeholder": ":who"},
        # A parameter carries a value, never the CURRENT_USER keyword.
        "current_user": False,
    }
    [bound] = do_ast_rewriter([statement], {"who": "bob"})
    plan = plan_alter_materialized_view_owner(bound)
    [node] = [plan[nid] for nid in plan.nodes()]
    assert node.new_owner == "bob"
    assert node.owner_is_current_user is False


def test_an_identity_containing_a_quote_is_expressible():
    """`'[^']+'` had no escape, so an identity with a quote in it could not be
    written at all."""
    [statement] = pre_parse("GRANT reader ON DATASET a.b.c TO USER 'o''brien'")
    assert statement["GrantAccess"]["principal"] == "o'brien"


def test_placeholders_are_reported_before_execution():
    """`analyze_query` names the parameters a statement needs. Its object is not
    resolved until they are bound, so it reports no table for a parameterised
    one - a pre-flight caller must read `parameters`, not read an empty `tables`
    as "touches nothing"."""
    info = opteryx.analyze_query("GRANT reader ON DATASET :ds TO USER :username")
    assert info["parameters"] == ["ds", "username"]
    assert info["tables"] == []
    info = opteryx.analyze_query("GRANT reader ON DATASET ws.c.d TO USER :username")
    assert info["parameters"] == ["username"]
    assert info["tables"] == ["ws.c.d"]


# --- SHOW EFFECTIVE GRANTS ON
#
# Two statements, two questions about one object: what is stored AT it (SHOW
# GRANTS ON, 1:1 with what a GRANT or REVOKE there acts on) and who can reach
# it at all (SHOW EFFECTIVE GRANTS ON, the covering collection and workspace
# policies included). The engine routes; the capability decides coverage with
# the matcher that decides real queries.


def test_show_effective_grants_on_parses_to_its_own_statement():
    [statement] = pre_parse("SHOW EFFECTIVE GRANTS ON DATASET ws.c.d")
    assert statement == {
        "ShowEffectiveGrantsOn": {"object_kind": "dataset", "object_name": "ws.c.d"}
    }
    # Bare SHOW GRANTS is still the session's own grants, on the parser path.
    assert pre_parse("SHOW GRANTS") is None


def test_show_effective_grants_without_on_is_refused_by_name():
    """Rejected here rather than left to sqlparser, which knows no EFFECTIVE and
    would report a syntax error several words away from the cause."""
    with pytest.raises(UnsupportedSyntaxError, match="EFFECTIVE"):
        pre_parse("SHOW EFFECTIVE GRANTS")
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("SHOW EFFECTIVE GRANTS ON TABLE ws.c.d")


def test_the_two_listings_ask_the_capability_different_questions(install):
    capability = install(RecordingAdminCapability())
    _run("SHOW GRANTS ON DATASET ws.c.d")
    _run("SHOW EFFECTIVE GRANTS ON DATASET ws.c.d")
    assert capability.applied == [
        ("grants_on", "ws.c.d", "alice"),
        ("effective_grants_on", "ws.c.d", "alice"),
    ]


def test_the_reported_case_a_dataset_covered_only_from_above(install):
    """The case this statement exists for: a dataset with nothing stored on it,
    reachable through the workspace owner's `w.*`. The attached listing is empty
    and the effective one names the owner and the policy that reaches it."""
    install(
        RecordingAdminCapability(
            rows=[],
            effective_rows=[
                {"user": "bob", "pattern": "ws.*", "level": "workspace", "role": "owner"}
            ],
        )
    )
    [attached] = _run("SHOW GRANTS ON DATASET ws.c.d")
    assert [tuple(row) for row in attached] == []
    [effective] = _run("SHOW EFFECTIVE GRANTS ON DATASET ws.c.d")
    assert effective.column_names == [b"user", b"pattern", b"level", b"role"]
    assert [tuple(row) for row in effective] == [("bob", "ws.*", "workspace", "owner")]


def test_an_empty_attached_listing_points_at_the_effective_one(install):
    """An empty result reads as "nobody can reach this", which is what prompted
    this work. The message names the other statement; it does not claim a
    covering policy exists - knowing that would need a second store read."""
    install(RecordingAdminCapability(rows=[]))
    session = opteryx.session(user="alice")
    list(session.execute_to_morsels("SHOW GRANTS ON DATASET ws.c.d"))
    assert any("SHOW EFFECTIVE GRANTS ON" in message for message in session.messages)


def test_a_workspace_listing_carries_no_such_hint(install):
    """A workspace listing is already every policy at every level, so an empty
    one really does mean nobody holds anything here."""
    install(RecordingAdminCapability(rows=[]))
    session = opteryx.session(user="alice")
    list(session.execute_to_morsels("SHOW GRANTS ON WORKSPACE ws"))
    assert not any("SHOW EFFECTIVE GRANTS ON" in message for message in session.messages)


def test_the_effective_listing_takes_the_same_object_kinds_and_arity(install):
    capability = install(RecordingAdminCapability())
    _run("SHOW EFFECTIVE GRANTS ON WORKSPACE ws")
    _run("SHOW EFFECTIVE GRANTS ON COLLECTION ws.coll")
    _run("SHOW EFFECTIVE GRANTS ON DATASET ws.coll.ds")
    assert [call[1] for call in capability.applied] == ["ws.*", "ws.coll.*", "ws.coll.ds"]
    with pytest.raises(UnsupportedSyntaxError):
        _run("SHOW EFFECTIVE GRANTS ON DATASET ws.coll")


def test_the_effective_listing_takes_a_parameter_on_the_same_terms(install):
    capability = install(RecordingAdminCapability())
    [statement] = pre_parse("SHOW EFFECTIVE GRANTS ON COLLECTION :coll")
    assert statement["ShowEffectiveGrantsOn"]["object_name"] == {"Placeholder": ":coll"}
    _run("SHOW EFFECTIVE GRANTS ON COLLECTION :coll", params={"coll": "ws.sales"})
    assert capability.applied == [("effective_grants_on", "ws.sales.*", "alice")]
    with pytest.raises(ParameterError):
        _run("SHOW EFFECTIVE GRANTS ON COLLECTION :coll")


def test_analyze_query_reports_the_effective_listing(install):
    info = opteryx.analyze_query("SHOW EFFECTIVE GRANTS ON DATASET ws.c.d")
    assert info["tables"] == ["ws.c.d"]
    assert info["permission_required"] == "owner"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
