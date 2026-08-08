# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`SHOW GRANTS` — what this session is allowed to do, and why.

It REPORTS the access policies the session was handed at construction; it never
confers them. Opteryx has no GRANT/REVOKE: policies are issued by the platform's
policy service, so the engine can only narrow access, never widen it. This
statement exists so a caller can answer "why can't I see this table?" without
leaving SQL.

`$grants` is the SINGLE surface behind it, on the same internal-only rule as
`$user` and `$variables`, so the two cannot drift into disagreeing about what
the caller holds. The `actions` column is DERIVED from ACTION_MAP rather than
restated — a second list would drift from the one actually enforced, and the
implicit grants are read from `implicit_policies` for the same reason.

Two grants are hard-coded in the ENGINE rather than issued by the policy
service — `personal.<username>.*` as owner and `public.*` as reader. They are
enforced on every session, so they are reported on every session.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def _rows(session, statement):
    for morsel in session.execute_to_morsels(statement):
        return [tuple(r) for r in morsel]
    return []


def _actions_for(rows, pattern):
    """The actions reported against `pattern`, as a set."""
    matched = [r for r in rows if r[0] == pattern]
    assert len(matched) == 1, (pattern, rows)
    return {a.strip() for a in matched[0][2].split(",")}


def test_show_grants_reports_the_session_policies():
    session = opteryx.session(
        user="olive",
        access_policies=[{"pattern": "ws.*", "role": "owner"}],
    )
    rows = _rows(session, "SHOW GRANTS")

    patterns = {(r[0], r[1]) for r in rows}
    assert ("ws.*", "owner") in patterns, rows
    # ...alongside the two the engine hard-codes.
    assert ("personal.olive.*", "owner") in patterns, rows
    assert ("public.*", "reader") in patterns, rows
    assert len(rows) == 3, rows


def test_implicit_grants_are_reported_before_the_issued_ones():
    """They short-circuit in can_perform_action, so reading the table top-down
    reaches the same decision the engine does."""
    session = opteryx.session(user="olive", access_policies=[{"pattern": "ws.*", "role": "owner"}])
    rows = _rows(session, "SHOW GRANTS")

    assert [r[0] for r in rows] == ["personal.olive.*", "public.*", "ws.*"], rows


def test_show_grants_actions_are_derived_from_action_map():
    """The actions column must agree with what is enforced, not restate it."""
    from opteryx.managers.permissions import ACTION_MAP

    session = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])
    rows = _rows(session, "SHOW GRANTS")

    expected = {action for action, roles in ACTION_MAP.items() if "reader" in roles}
    assert _actions_for(rows, "*") == expected
    # The implicit reader grant is the same role, so it reports the same actions.
    assert _actions_for(rows, "public.*") == expected


def test_owner_holds_strictly_more_than_writer():
    owner = opteryx.session(user="o", access_policies=[{"pattern": "*", "role": "owner"}])
    writer = opteryx.session(user="w", access_policies=[{"pattern": "*", "role": "writer"}])

    owner_actions = _actions_for(_rows(owner, "SHOW GRANTS"), "*")
    writer_actions = _actions_for(_rows(writer, "SHOW GRANTS"), "*")

    assert writer_actions < owner_actions, (writer_actions, owner_actions)
    # DROP is the tier that separates them; it is why DROP TABLE is owner-only.
    assert "DROP" in owner_actions and "DROP" not in writer_actions


def test_a_session_with_no_issued_policies_still_holds_the_implicit_ones():
    """No ISSUED policy is not no access: the engine grants every session its own
    personal namespace and read of public, so those must be reported — and
    nothing else. Never a blank, all-permitting row."""
    session = opteryx.session(user="nobody", access_policies=[])
    rows = _rows(session, "SHOW GRANTS")

    assert [(r[0], r[1]) for r in rows] == [
        ("personal.nobody.*", "owner"),
        ("public.*", "reader"),
    ], rows


def test_an_anonymous_session_owns_no_personal_namespace():
    """There is no `personal.<nobody>` to own, so no row may claim one."""
    session = opteryx.session(access_policies=[])
    rows = _rows(session, "SHOW GRANTS")

    assert [(r[0], r[1]) for r in rows] == [("public.*", "reader")], rows


def test_reported_grants_agree_with_what_is_enforced():
    """The point of the table: every row it shows must be a decision
    can_perform_action actually makes."""
    from opteryx.managers.permissions import ACTION_MAP
    from opteryx.managers.permissions import can_perform_action

    session = opteryx.session(user="olive", access_policies=[{"pattern": "ws.*", "role": "writer"}])
    context = session.context

    probes = {
        "personal.olive.*": "personal.olive.tbl",
        "public.*": "public.coll.tbl",
        "ws.*": "ws.coll.tbl",
    }

    for pattern, role, reported in _rows(session, "SHOW GRANTS"):
        reported_actions = {a.strip() for a in reported.split(",")}
        for action in ACTION_MAP:
            assert can_perform_action(context, probes[pattern], action=action) == (
                action in reported_actions
            ), (pattern, role, action)


def test_grants_relation_is_not_addressable_by_name():
    """`$grants` is internal-only: SHOW GRANTS is its single surface, so the two
    cannot drift."""
    session = opteryx.session(user="olive", access_policies=[{"pattern": "*", "role": "owner"}])

    with pytest.raises(UnsupportedSyntaxError, match="SHOW GRANTS"):
        list(session.execute_to_morsels("SELECT * FROM $grants"))


def test_show_grants_takes_no_arguments():
    """SHOW GRANTS reports the CURRENT session; there is no way to ask about
    another principal, and asking must fail rather than silently answer for self."""
    session = opteryx.session(user="olive", access_policies=[{"pattern": "*", "role": "owner"}])

    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels("SHOW GRANTS FOR rita"))


def test_a_session_cannot_set_its_own_policies():
    """access_policies is INTERNAL-owned; a caller widening it via SET would be
    privilege escalation."""
    from opteryx.exceptions import PermissionsError

    session = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])

    with pytest.raises(PermissionsError):
        list(session.execute_to_morsels("SET access_policies = 'owner'"))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
