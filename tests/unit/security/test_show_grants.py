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
restated — a second list would drift from the one actually enforced.
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


def test_show_grants_reports_the_session_policies():
    session = opteryx.session(
        user="olive",
        access_policies=[
            {"pattern": "ws.*", "role": "owner"},
            {"pattern": "public.*", "role": "reader"},
        ],
    )
    rows = _rows(session, "SHOW GRANTS")

    patterns = {(r[0], r[1]) for r in rows}
    assert ("ws.*", "owner") in patterns, rows
    assert ("public.*", "reader") in patterns, rows
    assert len(rows) == 2, rows


def test_show_grants_actions_are_derived_from_action_map():
    """The actions column must agree with what is enforced, not restate it."""
    from opteryx.managers.permissions import ACTION_MAP

    session = opteryx.session(user="rita", access_policies=[{"pattern": "*", "role": "reader"}])
    rows = _rows(session, "SHOW GRANTS")

    assert len(rows) == 1, rows
    actions = {a.strip() for a in rows[0][2].split(",")}
    expected = {action for action, roles in ACTION_MAP.items() if "reader" in roles}
    assert actions == expected, (actions, expected)


def test_owner_holds_strictly_more_than_writer():
    owner = opteryx.session(user="o", access_policies=[{"pattern": "*", "role": "owner"}])
    writer = opteryx.session(user="w", access_policies=[{"pattern": "*", "role": "writer"}])

    owner_actions = {a.strip() for a in _rows(owner, "SHOW GRANTS")[0][2].split(",")}
    writer_actions = {a.strip() for a in _rows(writer, "SHOW GRANTS")[0][2].split(",")}

    assert writer_actions < owner_actions, (writer_actions, owner_actions)
    # DROP is the tier that separates them; it is why DROP TABLE is owner-only.
    assert "DROP" in owner_actions and "DROP" not in writer_actions


def test_a_session_with_no_policies_shows_no_rows():
    """No grants is reported as no rows — never as a blank, all-permitting one."""
    session = opteryx.session(user="nobody", access_policies=[])
    assert _rows(session, "SHOW GRANTS") == []


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
