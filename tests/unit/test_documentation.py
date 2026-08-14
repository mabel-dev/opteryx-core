"""
The examples in the documentation have to run.

Every test here is an example a reader is told to copy - the README quick start,
and the docstrings on `opteryx`, `opteryx.session`, `opteryx.analyze_query` and
`opteryx.connectors`. Documentation which does not run is worse than none: this
file tested `opteryx.connect()` / `cursor()` / `fetchall()` long after those were
removed, and the same rot in `opteryx/__main__.py` shipped a command line which
failed on every invocation.

Anything needing credentials or a network is deliberately not tested here - a
test which cannot run is not a guard.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests import is_version, skip_if


@skip_if(is_version("3.9"))
def test_readme_quick_start():
    """README: register a local workspace and query it with dot-separated names."""
    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)

    session = opteryx.session()

    for morsel in session.execute_to_morsels(
        "SELECT Company, Mission FROM testdata.flat.space_missions LIMIT 5"
    ):
        print(morsel)

    # below here is not in the documentation
    assert session.rowcount == 5
    assert session.column_names == ["Company", "Mission"]
    session.close()


@skip_if(is_version("3.9"))
def test_get_started():
    """`opteryx` module docstring: a session, and a result read as morsels."""
    import opteryx

    session = opteryx.session()
    for morsel in session.execute_to_morsels("SELECT * FROM $planets"):
        print(morsel)

    # below here is not in the documentation
    assert session.rowcount == 9
    session.close()


@skip_if(is_version("3.9"))
def test_session_example():
    """`opteryx.session` docstring: a session carrying a caller's identity."""
    import opteryx

    session = opteryx.session(user="alice", memberships=["finance"])
    for morsel in session.execute_to_morsels("SELECT 1"):
        print(morsel)

    # below here is not in the documentation
    assert session.rowcount == 1
    session.close()


@skip_if(is_version("3.9"))
def test_analyze_query_example():
    """`opteryx.analyze_query` docstring: metadata without executing."""
    import opteryx

    info = opteryx.analyze_query("SELECT * FROM users WHERE id = 1")

    assert info["query_type"] == "Query"
    assert info["tables"] == ["users"]
    assert info["permission_required"] == "reader"
    # the docstring documents these keys, so they have to be there
    assert info["is_read"] is True
    assert info["is_mutation"] is False
    assert info["is_ddl"] is False


@skip_if(is_version("3.9"))
def test_analyze_query_reports_parameters():
    """`:name` placeholders are reported so a caller can resolve them up front."""
    import opteryx

    info = opteryx.analyze_query("SELECT * FROM t WHERE dept = :department")
    assert info["parameters"] == ["department"]


@skip_if(is_version("3.9"))
def test_connectors_usage_pattern():
    """`opteryx.connectors` docstring: register a prefix, then query through it."""
    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)
    session = opteryx.session()
    rows = sum(
        morsel.num_rows
        for morsel in session.execute_to_morsels("SELECT * FROM testdata.flat.space_missions")
    )

    assert rows == 4630
    session.close()


@skip_if(is_version("3.9"))
def test_membership_permissions():
    """A caller's memberships are readable in SQL as `@@user_memberships`."""
    import opteryx

    session = opteryx.session(memberships=["Apollo 11", "opteryx"])

    # the missions field is an ARRAY
    rows = sum(
        morsel.num_rows
        for morsel in session.execute_to_morsels(
            "SELECT * FROM testdata.astronauts "
            "WHERE missions @> @@user_memberships"
        )
    )
    assert rows == 3
    session.close()

    # The `INNER JOIN $user` example that used to sit here is gone, not rewritten:
    # `$user` is internal-only and `SHOW USER` is its only surface, so a caller's
    # memberships can no longer be joined to a relation. `@@user_memberships`
    # above is the remaining route to the same information.


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
