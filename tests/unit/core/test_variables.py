import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
from opteryx.types.logical_type import VARCHAR

from opteryx.exceptions import PermissionsError
from opteryx.models import Node
from opteryx.variables import (
    SYSTEM_VARIABLES_DEFAULTS,
    SystemVariables,
    VariableOwner,
    Visibility,
)


def _a_variable_owned_by(owner, visibility=None):
    """Pick a registered variable with the given owner (and visibility).

    Chosen from the live table rather than hardcoded, so this keeps exercising the
    OWNERSHIP MECHANISM as variables come and go. It previously named `license`, a
    MySQL-compat entry that was later dropped — at which point this test failed with
    VariableNotFoundError, i.e. it stopped testing permissions without saying so.
    """
    for name, (_type, _value, var_owner, var_visibility) in SYSTEM_VARIABLES_DEFAULTS.items():
        if var_owner is owner and (visibility is None or var_visibility is visibility):
            return name
    raise AssertionError(f"no registered variable with owner={owner} visibility={visibility}")


def test_variables_permissions():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.snapshot()

    # A SERVER-owned variable outranks both the INTERNAL-owned global container and
    # a USER-owned session snapshot, so neither may write it.
    server_var = _a_variable_owned_by(VariableOwner.SERVER)
    with pytest.raises(PermissionsError):
        SystemVariables[server_var] = Node(node_type="VARIABLE", type=VARCHAR, value="system")
    with pytest.raises(PermissionsError):
        connection_vars[server_var] = Node(node_type="VARIABLE", type=VARCHAR, value="system")

    # we shouldn't be able to set the user
    with pytest.raises(PermissionsError):
        connection_vars["external_user"] = Node(
            node_type="VARIABLE", type=VARCHAR, value="user"
        )


def test_restricted_variable_needs_entitlement_even_when_owner_allows():
    # Visibility is a SECOND, independent gate: a USER-owned RESTRICTED variable
    # passes the owner-rank check yet must still be refused without platform_admin.
    # The subject is injected because no USER-owned RESTRICTED variable is currently
    # registered — the gate is a property of the container, not of any one variable.
    session_vars = SystemVariables.snapshot(VariableOwner.USER)
    session_vars._variables["a_restricted_knob"] = (
        VARCHAR, "", VariableOwner.USER, Visibility.RESTRICTED,
    )
    with pytest.raises(PermissionsError):
        session_vars["a_restricted_knob"] = Node(
            node_type="VARIABLE", type=VARCHAR, value="anything"
        )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
