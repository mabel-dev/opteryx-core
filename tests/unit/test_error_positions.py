"""
Errors point at the statement the READER wrote.

sqlparser reports a line and column for a parse failure and hangs a span off every
identifier it returns. Both index the text the PARSER was given, which is not the text
that was submitted: comments used to be stripped, whitespace collapsed onto one line,
and the whole statement re-spaced by a tokenise-and-rejoin pass. Every position was
therefore an offset into a string nobody had ever seen.

These tests pin the property that makes a position worth carrying: whatever the rewriter
does on the way past, `error.position` slices exactly the offending text out of the
statement as SUBMITTED. That is what the editor underlines.

The assertions are deliberately made against the RANGE and not against any rendering of
it. This repo owns the text and the position; the drawing belongs to whoever displays
them, and a test that read a caret out of a message would be pinning the wrong thing.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import QueryParseError
from opteryx.utils.sql import split_sql_statements


def _run(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass


def _marked(sql: str, error) -> str:
    """The text `error.position` covers, sliced out of `sql`.

    Checked both ways round - by offset and by line/column - because the two are
    separate fields describing one range, and a consumer picking the wrong one must not
    be able to get a different answer.
    """
    position = error.position
    assert position is not None, f"{type(error).__name__} carries no position"

    by_offset = sql[position.start_offset : position.end_offset]
    lines = sql.split("\n")
    assert position.start_line == position.end_line, "multi-line ranges are not expected here"
    by_line = lines[position.start_line - 1][position.start_column - 1 : position.end_column - 1]
    assert by_offset == by_line, f"{by_offset!r} by offset, {by_line!r} by line/column"

    # No message ever draws the position - that is the renderer's job.
    assert "^" not in str(error)
    return by_offset


def test_parse_error_reports_the_line_the_reader_wrote():
    sql = "-- a report\nSELECT name,\n       gravity\n  FROM $planets\n WEHRE id > 1"
    with pytest.raises(QueryParseError) as raised:
        _run(sql)
    assert raised.value.line == 5, raised.value.line
    assert _marked(sql, raised.value) == "WEHRE"


def test_parse_error_in_the_second_statement_of_a_batch():
    """Offsets rebase past the earlier statements, not just past the rewriter."""
    sql = "SELECT 1;\n-- a note\nSELECT name\n  FROM $planets\n ODER BY id"
    with pytest.raises(QueryParseError) as raised:
        _run(sql)
    assert raised.value.line == 5
    assert _marked(sql, raised.value) == "ODER"


def test_column_error_points_at_the_column():
    sql = "-- planets\nSELECT name,\n       gravty\n  FROM $planets"
    with pytest.raises(ColumnNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "gravty"


def test_column_error_points_correctly_past_a_length_changing_rewrite():
    """`b'abc'` becomes `CAST('abc' AS VARBINARY)` before the parser sees it.

    Everything to the right of it shifts by fourteen characters in the parser's text.
    If the span were used as-is the range would land in the middle of `FROM`.
    """
    sql = "SELECT b'abc' AS tag, gravty FROM $planets"
    with pytest.raises(ColumnNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "gravty"


def test_column_error_inside_a_subquery_points_at_the_subquery():
    sql = (
        "SELECT name\n"
        "  FROM $planets\n"
        " WHERE id IN (SELECT planetId\n"
        "                FROM testdata.satellites\n"
        "               WHERE magnitud < 5)"
    )
    with pytest.raises(ColumnNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "magnitud"


def test_function_error_points_at_the_function():
    """Function errors are raised in the logical planner, not the binder.

    They get their position from the same mechanism - `SqlError.span` set at the raise
    site, `SqlError.position` filled in at the planner boundary - which is the whole
    reason that mechanism is not specific to the binder.
    """
    sql = "-- report\nSELECT name,\n       CONT(*)\n  FROM $planets"
    with pytest.raises(FunctionNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "CONT"


def test_function_error_inside_a_predicate_points_at_the_function():
    sql = "SELECT name FROM $planets WHERE LENGT(name) > 4"
    with pytest.raises(FunctionNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "LENGT"


@pytest.mark.parametrize("written", ["name", "NAME", "NaMe", "`NAME`"])
def test_column_names_are_not_case_sensitive(written):
    """The binder resolves identifiers with `case_insensitive=True`, so no error this
    engine raises can be caused by casing.

    One spelling per query on purpose: putting several in one projection selects the
    SAME column repeatedly, and the duplicate-output-column error that produces is
    correct - it just is not what this test is about.
    """
    _run(f"SELECT {written} FROM $planets WHERE {written} = 'Earth'")


def test_the_column_error_does_not_blame_casing():
    """It used to open its advice with "Column names are case sensitive", which sent
    readers off to audit the one thing that could not possibly be wrong."""
    with pytest.raises(ColumnNotFoundError) as raised:
        _run("SELECT NME FROM $planets")
    assert "case sensitive" not in str(raised.value).lower()
    # the genuinely useful half of the old advice stays
    assert "SHOW COLUMNS FROM" in str(raised.value)


def test_an_error_with_no_span_carries_no_position():
    """No position is a correct outcome - it must not become a crash or a made-up range."""
    error = ColumnNotFoundError(column="x", suggestion="y")
    assert error.span is None and error.position is None
    assert "\n" not in str(error)


def test_a_qualified_name_is_marked_whole():
    """`t.gravty` is one reference; underlining only `gravty` would point past the dot."""
    sql = "SELECT t.gravty FROM $planets AS t"
    with pytest.raises(ColumnNotFoundError) as raised:
        _run(sql)
    assert _marked(sql, raised.value) == "t.gravty"


def test_the_terminal_renderer_underlines_the_whole_range():
    """The one surface with no editor gets a drawing - built explicitly, not baked in."""
    from opteryx.utils.sql import underline

    sql = "SELECT name,\n       CONT(*)\n  FROM $planets"
    with pytest.raises(FunctionNotFoundError) as raised:
        _run(sql)
    drawn = underline(sql, raised.value.position)
    assert drawn.split("\n") == ["           CONT(*)", "           ^~~~"]


def test_a_comment_survives_to_the_parser():
    """The parser tokenizes comments; nothing upstream needs to remove them."""
    _run("-- leading\nSELECT 1 /* inline */ AS one -- trailing\n")


def test_a_semicolon_inside_a_comment_does_not_split_the_batch():
    sql = "SELECT 1 -- one; not two\n"
    assert [statement.text for statement in split_sql_statements(sql)] == [
        "SELECT 1 -- one; not two"
    ]


def test_a_double_hyphen_inside_a_quoted_identifier_is_not_a_comment():
    """Backticks are the only way to write a hyphenated name in this dialect.

    Stripping comments with a regex that did not protect them truncated
    ``SELECT `a--b` FROM t`` to ``SELECT `a`` - so a blob-store path containing a
    double hyphen silently lost the rest of the statement.
    """
    sql = "SELECT `a--b`, 1 FROM $planets"
    assert [statement.text for statement in split_sql_statements(sql)] == [sql]


def test_a_trailing_comment_is_not_a_statement():
    sql = "SELECT 1;\n-- nothing to run here\n"
    assert [statement.text for statement in split_sql_statements(sql)] == ["SELECT 1"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
