# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`SHOW VARIABLES` withholds RESTRICTED variables from non-administrators, and
RESTRICTED variables additionally require `platform_admin` to SET.

RESTRICTED variables are listed, AND settable, only for a caller holding
`platform_admin`. The entitlement is SERVER-owned, so it cannot be self-granted,
and `$variables` (the relation behind SHOW VARIABLES) is internal-only, so the
read-side filter cannot be side-stepped by querying the relation directly.

Visibility (RESTRICTED/UNRESTRICTED) and VariableOwner (SERVER/INTERNAL/USER) are
independent axes that BOTH gate writes: owner rank decides who is even eligible
to write (checked first, in SystemVariablesContainer.__setitem__), and — as of
this file — RESTRICTED additionally requires platform_admin regardless of owner
rank. `trace` is deliberately UNRESTRICTED so it needs neither.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import PermissionsError, UnsupportedSyntaxError
from opteryx.models import Node
from opteryx.types.logical_type import ARRAY, BOOLEAN, VARIANT
from opteryx.variables import (
    PLATFORM_ADMIN_ENTITLEMENT,
    SYSTEM_VARIABLES_DEFAULTS,
    SystemVariables,
    VariableOwner,
    Visibility,
)

ADMIN = [PLATFORM_ADMIN_ENTITLEMENT]


def _shown(**session_kwargs):
    """(names, visibilities) shown by SHOW VARIABLES for this caller."""
    session = opteryx.session(user="bastian", **session_kwargs)
    for morsel in session.execute_to_morsels("SHOW VARIABLES;"):
        return [(r[0], r[4]) for r in morsel]
    return []


def test_non_admin_sees_no_restricted_variables():
    rows = _shown()
    assert rows, "expected some variables"
    assert not [n for n, v in rows if v == "RESTRICTED"], rows


def test_admin_sees_restricted_variables():
    restricted = [n for n, v in _shown(entitlements=ADMIN) if v == "RESTRICTED"]
    assert restricted, "platform_admin should see RESTRICTED variables"


def test_admin_sees_strictly_more_than_non_admin():
    non_admin = {n for n, _ in _shown()}
    admin = {n for n, _ in _shown(entitlements=ADMIN)}
    assert non_admin < admin, (non_admin, admin)


def test_unrestricted_are_shown_to_everyone():
    # Whatever the entitlement, every UNRESTRICTED variable must be listed —
    # the filter must not over-withhold.
    expected = {
        name
        for name, (_t, _v, _o, visibility) in SYSTEM_VARIABLES_DEFAULTS.items()
        if visibility == Visibility.UNRESTRICTED
    }
    shown = {n for n, _ in _shown()}
    assert expected <= shown, expected - shown


def test_admin_listing_matches_the_declared_visibility():
    # Nothing is mislabelled: what an admin sees as RESTRICTED is exactly what the
    # defaults table declares RESTRICTED (for variables that come from that table).
    declared = {
        name
        for name, (_t, _v, _o, visibility) in SYSTEM_VARIABLES_DEFAULTS.items()
        if visibility == Visibility.RESTRICTED
    }
    seen = {n for n, v in _shown(entitlements=ADMIN) if v == "RESTRICTED"}
    assert declared <= seen, declared - seen


def test_wrong_entitlement_does_not_reveal():
    # Only `platform_admin` lifts the filter; another real entitlement must not.
    rows = _shown(entitlements=["data_admin"])
    assert not [n for n, v in rows if v == "RESTRICTED"], rows


def test_cannot_self_grant_the_entitlement():
    session = opteryx.session(user="mallory")
    with pytest.raises(PermissionsError):
        for _ in session.execute_to_morsels(
            "SET user_entitlements TO ['platform_admin']; SHOW VARIABLES;"
        ):
            pass


def test_cannot_bypass_by_querying_the_relation():
    # The filter lives in the relation's reader, and the relation is internal-only,
    # so there is no route to the unfiltered list.
    session = opteryx.session(user="mallory")
    with pytest.raises(UnsupportedSyntaxError):
        for _ in session.execute_to_morsels("SELECT * FROM $variables"):
            pass


def test_trace_is_unrestricted_and_freely_settable():
    # `trace` is deliberately UNRESTRICTED: a user must be able to enable tracing
    # on their OWN query for us to debug it, with no entitlement required. It is
    # visible in SHOW VARIABLES for everyone AND settable by everyone.
    session = opteryx.session(user="mallory")
    for _ in session.execute_to_morsels("SET trace TO true;"):
        pass
    assert session.context.variables["trace"] is True
    assert ("trace", "UNRESTRICTED") in _shown()


def _container_with_restricted_var(entitlements=()):
    """A USER-owned container carrying a synthetic USER + RESTRICTED variable.

    The RESTRICTED write-gate is a property of the CONTAINER, not of any particular
    variable, and there is currently no USER-owned RESTRICTED variable registered
    (the last one, `disable_optimizer`, became SERVER). Injecting the subject keeps
    these tests exercising the gate itself rather than whatever the table happens to
    hold — which is what let the earlier version of this file silently stop testing
    anything when its subject was reclassified.
    """
    container = SystemVariables.snapshot(VariableOwner.USER)
    container._variables["a_restricted_knob"] = (
        BOOLEAN, False, VariableOwner.USER, Visibility.RESTRICTED,
    )
    container._variables["user_entitlements"] = (
        ARRAY(VARIANT), list(entitlements), VariableOwner.INTERNAL, Visibility.UNRESTRICTED,
    )
    return container


def test_restricted_user_owned_variable_requires_admin_to_set():
    # Owner rank ALONE would allow this write (USER writing a USER-owned variable).
    # RESTRICTED must gate it too — otherwise "RESTRICTED" would mean nothing beyond
    # "hidden", while staying freely changeable by anyone who knows the name.
    container = _container_with_restricted_var()
    with pytest.raises(PermissionsError):
        container["a_restricted_knob"] = Node(node_type="VARIABLE", type=BOOLEAN, value=True)


def test_restricted_user_owned_variable_settable_by_admin():
    container = _container_with_restricted_var(entitlements=ADMIN)
    container["a_restricted_knob"] = Node(node_type="VARIABLE", type=BOOLEAN, value=True)
    assert container["a_restricted_knob"] is True


def test_wrong_entitlement_cannot_set_restricted_variable():
    container = _container_with_restricted_var(entitlements=["data_admin"])
    with pytest.raises(PermissionsError):
        container["a_restricted_knob"] = Node(node_type="VARIABLE", type=BOOLEAN, value=True)


def test_informational_variables_declare_system_behaviour():
    # These are an INTERFACE CONTRACT: a client reads them to learn what the system
    # will do (encoding, timezone, dialect, timeout). Nothing in the engine reads
    # them, so nothing else would catch them drifting away from real behaviour —
    # which would make them a lie told to every client that trusts them.
    # Visible to everyone, and fixed: not settable by anyone, admin included.
    expected = {
        "character_set_client": "utf8",
        "system_time_zone": "UTC",
        "sql_mode": "opteryx",
        "default_storage_engine": "rugo-parquet",
        # Mirrors jobs.opteryx's JOB_MAX_RUNTIME (20 minutes), in SECONDS.
        "max_execution_time": "1200",
        # Mirrors jobs.opteryx's submit-time sql_text guard, in CHARACTERS.
        "max_sql_length": "256000",
        "sql_select_limit": "1073741824",
        # Two DIFFERENT lifetimes, deliberately: the job RECORD outlives its DATA, so
        # a job stays inspectable for 14 days but downloadable for only 7.
        "job_retention_days": "14",
        "result_retention_days": "7",
    }
    # _shown() yields (name, visibility); this test needs the VALUE column, so it
    # reads the morsel directly.
    session = opteryx.session(user="bastian")
    shown = {}
    for morsel in session.execute_to_morsels("SHOW VARIABLES;"):
        shown = {r[0]: r[1] for r in morsel}
        break
    for name, value in expected.items():
        assert name in shown, f"{name} missing from SHOW VARIABLES for a normal caller"
        assert shown[name] == value, f"{name}: declared {shown[name]!r}, expected {value!r}"

    session = opteryx.session(user="bastian", entitlements=ADMIN)
    for name in expected:
        with pytest.raises(PermissionsError):
            for _ in session.execute_to_morsels(f"SET {name} TO 1;"):
                pass


def test_server_owned_variable_is_not_settable_even_by_admin():
    # SERVER means "the environment sets this" — a platform admin does not get to
    # change it mid-query either. Owner rank is checked before the entitlement gate.
    session = opteryx.session(user="bastian", entitlements=ADMIN)
    with pytest.raises(PermissionsError):
        for _ in session.execute_to_morsels("SET disable_optimizer TO true;"):
            pass


def test_unrestricted_variable_needs_no_entitlement_to_set():
    session = opteryx.session(user="mallory")
    for _ in session.execute_to_morsels("SET match_threshold TO 0.9;"):
        pass
    assert session.context.variables["match_threshold"] == 0.9


def test_like_selectivity_decay_is_unrestricted_and_settable():
    # Same shape as match_threshold above: tuning a cost-estimation
    # coefficient for one's own query is not a data-access grant.
    assert SYSTEM_VARIABLES_DEFAULTS["like_selectivity_decay"][3] == Visibility.UNRESTRICTED
    assert SYSTEM_VARIABLES_DEFAULTS["like_selectivity_decay"][2] == VariableOwner.USER

    session = opteryx.session(user="mallory")
    for _ in session.execute_to_morsels("SET like_selectivity_decay TO 0.8;"):
        pass
    assert session.context.variables["like_selectivity_decay"] == 0.8

    shown = {n for n, _ in _shown()}
    assert "like_selectivity_decay" in shown


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
