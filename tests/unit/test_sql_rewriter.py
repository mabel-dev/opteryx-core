import pytest

from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.exceptions import UnsupportedSyntaxError


def test_rewrite_explain_json_unsupported():
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite("EXPLAIN ANALYZE FORMAT JSON SELECT 1")


def test_rewrite_explain_graphviz_unsupported():
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite("EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT 1")


def test_rewrite_explain_mermaid_rewrites_to_graphviz():
    out = do_sql_rewrite("EXPLAIN ANALYZE FORMAT MERMAID SELECT 1")
    assert "FORMAT GRAPHVIZ" in out
    # ensure we didn't accidentally raise
    assert "MERMAID" not in out


def test_real_tab_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\t' AS x")
    assert "'\t'" in out


def test_real_newline_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\n' AS x")
    assert "'\n'" in out


def test_real_carriage_return_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\r' AS x")
    assert "'\r'" in out


def test_escaped_tab_sequence_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\\t' AS x")
    assert "'\\t'" in out


def test_escaped_newline_sequence_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\\n' AS x")
    assert "'\\n'" in out


def test_escaped_carriage_return_sequence_in_string_literal_survives():
    out = do_sql_rewrite("SELECT '\\r' AS x")
    assert "'\\r'" in out


def test_real_tab_in_function_argument_survives():
    out = do_sql_rewrite("SELECT READ_CSV(path, separator=>'\t')")
    assert "'\t'" in out


def test_escaped_tab_in_function_argument_survives():
    out = do_sql_rewrite("SELECT READ_CSV(path, separator=>'\\t')")
    assert "'\\t'" in out


def test_real_whitespace_outside_quotes_is_preserved():
    """Line structure survives the rewriter.

    It used to be collapsed - the whole statement arrived at the parser on one line
    with single spaces between tokens - which made every line and column the parser
    reported index a text the reader never wrote. The parser tokenises whitespace
    perfectly well itself, so there was nothing to gain for the cost.
    """
    statement = "SELECT\n\t1,\n\t2"
    out = do_sql_rewrite(statement)
    assert out == statement


def test_escaped_line_breaks_outside_quotes_become_spaces():
    """A backslash-n is two characters, not a newline, and the parser has no use for it.

    This one is still rewritten because a caller that carried the statement through JSON
    or a shell can deliver it, and a bare backslash is a parse error.
    """
    out = do_sql_rewrite("SELECT 1\\nFROM t")
    assert out == "SELECT 1 FROM t"


def test_a_statement_with_nothing_to_rewrite_is_returned_verbatim():
    statement = "SELECT name, gravity / 2 AS half FROM $planets WHERE id > 1"
    assert do_sql_rewrite(statement) == statement


def test_positions_map_back_through_a_length_changing_rewrite():
    """The point of the whole exercise: a position in the parser's text is answerable
    in the reader's text, even when a rewrite either side of it changed length."""
    source = "SELECT\n  CAST(g AS TIMESTAMP[d]),\n  name\nFROM $planets"
    out = do_sql_rewrite(source)
    assert str(out) != source  # TIMESTAMP[d] -> _TIMESTAMP_DAYS is 3 characters longer

    # `name` sits AFTER the rewrite, so a naive offset would be off by three.
    line, column = out.to_source_position(3, 3)
    assert (line, column) == (3, 3)
    assert source.split("\n")[line - 1][column - 1 :].startswith("name")


def test_a_literal_that_looks_like_syntax_is_left_alone():
    """Rewrites skip quoted spans - a string is a value, not syntax to be normalised."""
    assert do_sql_rewrite("SELECT 'TIMESTAMP[ns]' AS lit") == "SELECT 'TIMESTAMP[ns]' AS lit"
