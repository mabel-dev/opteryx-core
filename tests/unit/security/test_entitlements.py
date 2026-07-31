# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Entitlements carried on the session.

Entitlements are platform capabilities held by the CALLER (e.g. `data_admin`),
asserted by the submitting service. They are distinct from `access_policies`,
which are per-dataset pattern/role grants.

They are carried for REPORTING — so a caller can see what they hold — and are
deliberately NOT an enforcement mechanism inside the engine: the submitting
service still derives visibility filters from them before handing over a query.
The tests that matter here are therefore the ones proving a caller cannot
inflate their own entitlements, and that absence is never read as presence.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import PermissionsError, ProgrammingError


def _user_rows(session):
    # `SHOW USER` is the only surface — `$user` is internal-only, so there is no
    # `SELECT * FROM $user` route to the same rows.
    for morsel in session.execute_to_morsels("SHOW USER;"):
        return [(r[0], r[1]) for r in morsel]
    return []


def test_entitlements_are_reported():
    rows = _user_rows(opteryx.session(user="bastian", entitlements=["data_admin", "billing_reader"]))
    assert ("entitlement", "data_admin") in rows, rows
    assert ("entitlement", "billing_reader") in rows, rows


def test_absent_entitlements_yield_no_rows():
    # Absence must render as absence — never as a blank/empty grant.
    rows = _user_rows(opteryx.session(user="bastian"))
    assert not [r for r in rows if r[0] == "entitlement"], rows
    assert ("username", "bastian") in rows, rows


def test_entitlements_default_is_empty_not_a_house_default():
    # `memberships` defaults to ["public"] when unsupplied; entitlements MUST NOT
    # acquire a default like that — an unset list means "holds none".
    session = opteryx.session(user="bastian")
    assert list(session.context.variables["user_entitlements"]) == []


def test_user_cannot_grant_themselves_entitlements():
    # The whole point of SERVER ownership: entitlements are asserted by the
    # submitting service, so `SET` must be refused.
    session = opteryx.session(user="mallory")
    with pytest.raises(PermissionsError):
        for _ in session.execute_to_morsels("SET user_entitlements TO ['data_admin'];"):
            pass


def test_entitlements_survive_into_variables():
    session = opteryx.session(user="bastian", entitlements=["data_admin"])
    assert list(session.context.variables["user_entitlements"]) == ["data_admin"]


def test_non_string_entitlements_rejected():
    with pytest.raises(ProgrammingError):
        opteryx.session(user="bastian", entitlements=[{"not": "a string"}])


def test_entitlements_visible_in_show_variables():
    session = opteryx.session(user="bastian", entitlements=["data_admin"])
    for morsel in session.execute_to_morsels("SHOW VARIABLES;"):
        row = [r for r in morsel if r[0] == "user_entitlements"]
        assert row, "user_entitlements missing from SHOW VARIABLES"
        # INTERNAL-owned (asserted by the submitting service, not by an env var), so
        # the surface advertises that a user cannot set it — INTERNAL outranks USER.
        assert row[0][3] == "INTERNAL", row
        assert "data_admin" in row[0][1], row
        break


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
