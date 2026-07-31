# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`SHOW USER` — who the session says I am, and who pays for it.

`SHOW USER` is the SINGLE surface, on the same rule as `SHOW VARIABLES`: the
`$user` relation behind it is internal-only and not addressable by name, so the
two cannot drift into disagreeing about the caller's identity. The cost of that
rule is real and taken deliberately — `$user` used to be joinable
(`missions INNER JOIN $user ON Mission = value`), and no `SHOW` form can stand in
a FROM clause, so that query is now unexpressible rather than rewritable.

The billing account is reported alongside the username because it is a DIFFERENT
identity: many users bill to one account, so "who am I" does not answer "who is
this charged to". It is INTERNAL-owned — a caller cannot re-point their own
billing — and always present, because the session substitutes the house account
when the submitting service asserts none.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import PermissionsError, UnsupportedSyntaxError
from opteryx.managers.billing import DEFAULT_BILLING_ACCOUNT


def _rows(session, statement):
    for morsel in session.execute_to_morsels(statement):
        return [(r[0], r[1]) for r in morsel]
    return []


def test_show_user_reports_the_session_identity():
    session = opteryx.session(user="bastian", memberships=["crew"], billing_account="acct-1")
    rows = _rows(session, "SHOW USER")
    assert ("username", "bastian") in rows, rows
    assert ("membership", "crew") in rows, rows
    assert ("billing_account", "acct-1") in rows, rows


def test_the_relation_is_not_addressable_by_name():
    # SHOW USER is the single surface; the relation behind it must have no other
    # route, and the error must name the statement that replaces it rather than
    # sending the caller to `SHOW VARIABLES`.
    session = opteryx.session(user="mallory")
    for statement in (
        "SELECT * FROM $user",
        "SELECT value FROM $user",
        "SELECT * FROM $USER",
        "SELECT * FROM (SELECT * FROM $user) AS x",
        "SELECT u.value FROM $planets p CROSS JOIN $user u",
    ):
        with pytest.raises(UnsupportedSyntaxError) as caught:
            for _ in session.execute_to_morsels(statement):
                pass
        assert "SHOW USER" in str(caught.value), (statement, str(caught.value))


def test_show_user_has_the_relation_shape():
    session = opteryx.session(user="bastian")
    for morsel in session.execute_to_morsels("SHOW USER"):
        names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
        assert names == ["attribute", "value", "type"], names
        break


def test_billing_account_defaults_to_the_house_account():
    # Unattributed usage is not a thing: a session with no caller-supplied account
    # lands on the house account, so the row is always present.
    rows = _rows(opteryx.session(user="bastian"), "SHOW USER")
    assert ("billing_account", DEFAULT_BILLING_ACCOUNT) in rows, rows


def test_user_cannot_repoint_their_own_billing():
    # INTERNAL-owned: the submitting service asserts who pays, not the caller.
    session = opteryx.session(user="mallory", billing_account="acct-1")
    with pytest.raises(PermissionsError):
        for _ in session.execute_to_morsels("SET billing_account TO 'someone-else';"):
            pass
    assert ("billing_account", "acct-1") in _rows(session, "SHOW USER")


def test_billing_account_is_readable_by_the_caller():
    # UNRESTRICTED on the same reasoning as `external_user`: it is the caller's OWN
    # attribution, not another tenant's, so it needs no entitlement to read.
    session = opteryx.session(user="mallory", billing_account="acct-1")
    for morsel in session.execute_to_morsels("SELECT @@billing_account"):
        assert morsel[0][0] == "acct-1"
        break


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
