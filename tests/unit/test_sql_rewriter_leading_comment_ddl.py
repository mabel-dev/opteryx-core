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


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
