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


MAX_EXAMPLE_ROWS = 6


def format_example_value(value):
    """One cell, as the operator reference prints it."""
    import datetime
    import decimal

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, bytes):
        return value.decode("utf8")
    if isinstance(value, (str, int, decimal.Decimal)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime.datetime) else value.isoformat()
    return str(value)


def format_example_rows(session, sql):
    """Run *sql* and render its answer the way the docs page shows it.

    One string per row, columns joined by ` | `. This is the ONLY renderer: the
    catalog stores what this produced and the test below asserts it still does, so
    a page cannot claim a result the engine no longer gives.
    """
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for index in range(morsel.num_rows):
            rows.append(" | ".join(format_example_value(v) for v in morsel[index]))
    return rows


@skip_if(is_version("3.9"))
def test_operator_catalog_examples_run():
    """Every example on an operator's reference page executes, and still answers what the page says.

    The pages under docs.opteryx `/reference/sql/operators/` are generated from
    `reference/operators.json`, whose examples come from `OPERATOR_DEFINITIONS`.
    Nothing else runs them, so without this an operator gaining a restriction -
    or an answer changing - leaves a published example that is wrong when copied.

    They deliberately use `$planets` or bare literals: it is the one sample
    dataset every install has, so the example a reader copies is the query this
    test runs.
    """
    import opteryx
    from opteryx.expression.operator_catalog import OPERATOR_DEFINITIONS

    session = opteryx.session()
    failures = []
    for operator, definition in OPERATOR_DEFINITIONS.items():
        for example in definition.examples:
            try:
                actual = format_example_rows(session, example.sql)
            except Exception as err:  # noqa: BLE001 - the message is the report
                failures.append(f"{operator}: {example.sql} -> {type(err).__name__}: {err}")
                continue
            if len(actual) > MAX_EXAMPLE_ROWS:
                failures.append(
                    f"{operator}: {example.sql} -> returns {len(actual)} rows; add a LIMIT "
                    "so the page can show the whole answer"
                )
                continue
            if list(example.result) != actual:
                failures.append(
                    f"{operator}: {example.sql} -> catalog says {list(example.result)}, "
                    f"engine says {actual}"
                )
    session.close()

    assert not failures, "operator examples which do not match the engine:\n" + "\n".join(failures)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
