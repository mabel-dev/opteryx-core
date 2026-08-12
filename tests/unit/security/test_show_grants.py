# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`SHOW GRANTS` — the surface, not the semantics.

It REPORTS what the session is allowed to do; it never confers it. Opteryx has
no GRANT/REVOKE. This statement exists so a caller can answer "why can't I see
this table?" without leaving SQL.

`$grants` is the SINGLE surface behind it, on the same internal-only rule as
`$user` and `$variables`. Its rows come from the registered permissions
capability (see `opteryx.managers.permissions`), so what is reported and what
is enforced come from one place.

What the rows CONTAIN is the capability's business and is tested where that
capability lives — the engine's intrinsic capability allows everything, so
there is nothing here for the engine itself to assert. These tests cover only
what the statement is: internal-only, argument-free, and not settable.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


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
