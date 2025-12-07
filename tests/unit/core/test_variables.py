import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
from orso.types import OrsoTypes

from opteryx.exceptions import PermissionsError
from opteryx.models import Node
from opteryx.shared.variables import SystemVariables


def test_variables_permissions():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.snapshot()

    # we shouldn't be able to change the licence
    with pytest.raises(PermissionsError):
        SystemVariables["license"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="system"
        )
    with pytest.raises(PermissionsError):
        connection_vars["license"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="system"
        )

    # we shouldn't be able to set the user
    with pytest.raises(PermissionsError):
        connection_vars["external_user"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="user"
        )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
