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


def test_real_whitespace_outside_quotes_is_still_normalized():
    out = do_sql_rewrite("SELECT\n\t1,\n\t2")
    assert "\n" not in out
    assert "\t" not in out
    assert out == "SELECT 1 , 2"
