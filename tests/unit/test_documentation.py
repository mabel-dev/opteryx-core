"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests import is_version, skip_if


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

    result = opteryx.query("SELECT 4 * 7;")
    result.head()



@skip_if(is_version("3.9"))
def test_readme_4():
    import opteryx
    from opteryx.connectors import GcpCloudStorageConnector

    # Register the store, so we know queries for this store should be handled by
    # the GCS connector
    opteryx.register_store("opteryx", GcpCloudStorageConnector)
    result = opteryx.query("SELECT * FROM opteryx.space_missions WITH(NO_PARTITION) LIMIT 5;")
    result.head()


@skip_if(is_version("3.9"))
def test_get_started():
    import opteryx

    result = opteryx.query("SELECT * FROM $planets;").arrow()


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
    cursor = opteryx.query("SELECT * FROM $planets;").fetchall()

    import opteryx

    # Execute a SQL query and get a cursor
    cursor = opteryx.query(
        "SELECT * FROM $planets WHERE id = :user_provided_id;", {"user_provided_id": 1}
    ).fetchall()


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
def test_role_based_permissions():
    import opteryx

    role_permissions = {
        "admin": opteryx.constants.PERMISSIONS,
        "user": {"Query"},
        "agent": {"Analyze"},
    }

    def get_user_permissions(user_roles):
        permissions = set()
        for role in user_roles:
            if role in role_permissions:
                permissions |= role_permissions[role]
        return permissions

    perms = get_user_permissions(["admin"])
    assert perms == opteryx.constants.PERMISSIONS
    perms = get_user_permissions(["user"])
    assert perms == {"Query"}
    perms = get_user_permissions(["admin", "user"])
    assert perms == opteryx.constants.PERMISSIONS
    perms = get_user_permissions(["user", "agent"])
    assert perms == {"Query", "Analyze"}


@skip_if(is_version("3.9"))
def test_membership_permissions():
    import opteryx

    conn = opteryx.connect(memberships=["Apollo 11", "opteryx"])
    curr = conn.cursor()

    # the missions field is an ARRAY
    curr.execute("SELECT * FROM testdata.astronauts WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)")
    assert curr.rowcount == 3

    res = opteryx.query(
        "SELECT * FROM testdata.astronauts WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)",
        memberships=["Apollo 11", "opteryx"],
    )
    assert res.rowcount == 3

    curr = conn.cursor()
    curr.execute(
        "SELECT testdata.missions.* FROM testdata.missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'"
    )
    assert curr.rowcount == 1

    res = opteryx.query(
        "SELECT testdata.missions.* FROM testdata.missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'",
        memberships=["Apollo 11", "opteryx"],
    )
    assert res.rowcount == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
