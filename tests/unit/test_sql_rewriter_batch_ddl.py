"""
COLLECTION / WORKSPACE statements are rewritten at the start of EVERY statement in a batch.

The rewrites anchored on the start of the whole text, so in a script handed over whole
(`Session.check`, a platform editor) only the first statement was rewritten and the
second `CREATE COLLECTION` failed: "Expected: an object type after CREATE, found:
COLLECTION".
"""

from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def test_every_statement_of_a_batch_is_rewritten():
    sql = (
        "CREATE COLLECTION IF NOT EXISTS ws.a;\n"
        "CREATE VIEW ws.a.v AS SELECT 'CREATE COLLECTION x' AS s;\n"
        "  /* tagged */ CREATE COLLECTION IF NOT EXISTS ws.b;\n"
        "DROP WORKSPACE ws"
    )
    rewritten = do_sql_rewrite(sql)
    assert str(rewritten) == (
        "CREATE SCHEMA IF NOT EXISTS ws.a;\n"
        "CREATE VIEW ws.a.v AS SELECT 'CREATE COLLECTION x' AS s;\n"
        "  /* tagged */ CREATE SCHEMA IF NOT EXISTS ws.b;\n"
        "DROP FUNCTION ws"
    )
    assert len(sqloxide.parse_sql(str(rewritten), _dialect="opteryx")) == 4


def test_positions_after_batch_edits_map_back_to_the_source():
    # Each COLLECTION -> SCHEMA shortens the text by 4, so a position in the third
    # statement must map back across both edits.
    sql = "CREATE COLLECTION ws.a;\nCREATE COLLECTION ws.b;\nSELECT bad FROM t"
    rewritten = do_sql_rewrite(sql)
    target = str(rewritten).index("bad")
    assert rewritten.to_source_offset(target) == sql.index("bad")


def test_text_without_the_object_words_is_untouched():
    sql = "SELECT 1; SELECT 2"
    assert str(do_sql_rewrite(sql)) == sql
