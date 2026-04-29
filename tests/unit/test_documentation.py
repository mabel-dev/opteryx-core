"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests import is_version, skip_if
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all


@skip_if(is_version("3.9"))
def test_documentation_connect_example():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    rows = cur.fetchall()

    # below here is not in the documentation
    rows = list(rows)
    assert len(rows) == 9
    conn.close()


@skip_if(is_version("3.9"))
def test_readme_1():
    import opteryx

    result = execute_and_get_arrow("SELECT 4 * 7;")


@skip_if(is_version("3.9"))
def test_readme_4():
    import opteryx
    from opteryx.connectors import GcpCloudStorageConnector

    # Register the store, so we know queries for this store should be handled by
    # the GCS connector
    opteryx.register_workspace("opteryx", GcpCloudStorageConnector)
    result = execute_and_get_arrow("SELECT * FROM opteryx.space_missions WITH(NO_PARTITION) LIMIT 5;")


@skip_if(is_version("3.9"))
def test_get_started():
    import opteryx

    result = execute_and_get_arrow("SELECT * FROM $planets;")


@skip_if(is_version("3.9"))
def test_python_client():
    import opteryx

    # Establish a connection
    conn = opteryx.connect()
    # Create a cursor object
    cursor = conn.cursor()

    # Execute a SQL query
    cursor.execute("SELECT * FROM $planets;")

    # Fetch all rows
    rows = cursor.fetchall()

    import opteryx

    # Establish a connection
    conn = opteryx.connect()
    # Create a cursor object
    cursor = conn.cursor()

    # Execute a SQL query
    cursor.execute("SELECT * FROM $planets WHERE id = :user_provided_id;", {"user_provided_id": 1})

    # Fetch all rows
    rows = cursor.fetchall()

    import opteryx

    # Execute a SQL query and get the results
    rows = execute_and_fetch_all("SELECT * FROM $planets WHERE id = :user_provided_id;", {"user_provided_id": 1})


@skip_if(is_version("3.9"))
def test_permissions_example():
    import opteryx

    conn = opteryx.connect(permissions={"Query"})
    curr = conn.cursor()
    # The user does not have permissions to execute a SHOW COLUMNS statement
    # and this will return a oPermissionsError
    try:
        curr.execute("SHOW COLUMNS FROM $planets")
        print(curr.head())
    except opteryx.exceptions.PermissionsError:
        print("User does not have permission to execute this query")


@skip_if(is_version("3.9"))
def test_membership_permissions():
    import opteryx

    conn = opteryx.connect(memberships=["Apollo 11", "opteryx"])
    curr = conn.cursor()

    # the missions field is an ARRAY
    curr.execute("SELECT * FROM testdata.astronauts WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)")
    assert execute_and_get_rowcount("SELECT * FROM testdata.astronauts WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)") == 3

    conn = opteryx.connect(
        memberships=["Apollo 11", "opteryx"],
    )
    curr = conn.cursor()
    curr.execute(
        "SELECT testdata.missions.* FROM testdata.missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'"
    )
    assert execute_and_get_rowcount("SELECT testdata.missions.* FROM testdata.missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'") == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
