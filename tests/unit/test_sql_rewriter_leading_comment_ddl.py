"""
COLLECTION / WORKSPACE statements still reach the parser when a comment leads them.

dbt prefixes every statement with `/* {"app": "dbt"} */`; the anchored rewrites used
to skip only whitespace, so the statement reached sqlparser still saying COLLECTION
or WORKSPACE and failed to parse.
"""

import pytest

from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide

_PREFIXES = (
    '/* {"app": "dbt"} */ ',
    "-- tagged\n",
    "/* a */\n  -- b\n/* multi\nline */ ",
)

_STATEMENTS = (
    ("CREATE COLLECTION IF NOT EXISTS ws.c", "CREATE SCHEMA IF NOT EXISTS ws.c"),
    ("DROP COLLECTION IF EXISTS ws.c", "DROP SCHEMA IF EXISTS ws.c"),
    ("ALTER WORKSPACE ws OWNER TO bob", "ALTER FUNCTION ws OWNER TO bob"),
    ("DROP WORKSPACE ws", "DROP FUNCTION ws"),
)


@pytest.mark.parametrize("prefix", _PREFIXES)
@pytest.mark.parametrize("statement, expected", _STATEMENTS)
def test_leading_comment_does_not_hide_the_statement_head(prefix, statement, expected):
    rewritten = do_sql_rewrite(prefix + statement)
    # The comment is passed through untouched; only the object keyword changes.
    assert str(rewritten) == prefix + expected
    sqloxide.parse_sql(str(rewritten), _dialect="opteryx")


def test_comment_mentioning_collection_is_not_rewritten():
    sql = "/* CREATE COLLECTION */ SELECT 1"
    assert str(do_sql_rewrite(sql)) == sql


@pytest.mark.parametrize(
    "lead",
    (
        " " * 5000,  # dbt's delete+insert: 33 leading spaces hung the jobs instance
        "-- back compat for old kwarg name\n" + "  \n" * 2000,  # dbt's merge preamble
    ),
)
def test_long_leading_trivia_is_linear(lead):
    import time

    start = time.monotonic()
    assert str(do_sql_rewrite(lead + "delete from t where x = 1")) == lead + "delete from t where x = 1"
    assert str(do_sql_rewrite(lead + "DROP WORKSPACE ws")) == lead + "DROP FUNCTION ws"
    # The nested `(\s+)*` took 2^n steps: 26 spaces were already ~2 s.
    assert time.monotonic() - start < 1.0


def test_long_leading_whitespace_before_version_is_linear():
    import time

    from opteryx.exceptions import UnsupportedSyntaxError

    # `_STATEMENT_HEAD` gates the AS OF VERSION check; `\s*(\s)*` made a miss quadratic.
    miss = " " * 20000 + "1 VERSION"
    start = time.monotonic()
    assert str(do_sql_rewrite(miss)) == miss
    assert time.monotonic() - start < 1.0
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite(" ( SELECT * FROM t AS OF VERSION 3)")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
