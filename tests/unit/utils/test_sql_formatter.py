import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.formatter import format_sql


def test_format_sql():
    sql = "SELECT * FROM mytable"
    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == "\x1b[38;2;139;233;253mSELECT\x1b[0m \x1b[38;2;189;147;249m*\x1b[0m \x1b[38;2;139;233;253mFROM\x1b[0m \x1b[38;2;166;226;46mmytable\x1b[0m \x1b[0m"
    ), str(formatted_sql.encode()) + "\n" + formatted_sql


def test_format_sql_keyword_as_column_name():
    """Test that keywords used as column names are not uppercased"""
    sql = "SELECT group, count(*) FROM table GROUP BY group"
    formatted_sql = format_sql(sql)
    # The 'group' after SELECT should be lowercase (identifier), but the GROUP in GROUP BY should be uppercase (keyword)
    assert "group" in formatted_sql, "Column name 'group' should be lowercase"
    assert "\x1b[38;2;139;233;253mGROUP\x1b[0m" in formatted_sql, "Keyword GROUP should be colored and uppercase"


def test_format_sql_order_as_column_name():
    """Test that ORDER as a column name is not uppercased"""
    sql = "SELECT order FROM table ORDER BY order"
    formatted_sql = format_sql(sql)
    # The 'order' after SELECT should be lowercase, the ORDER keyword should be uppercase
    assert "order " in formatted_sql or "order\x1b" in formatted_sql, "Column name 'order' should be lowercase"
    assert "\x1b[38;2;139;233;253mORDER\x1b[0m" in formatted_sql, "Keyword ORDER should be colored and uppercase"


def test_format_sql_relation_coloring():
    """Test that relation names (tables) are colored distinctly"""
    sql = "SELECT * FROM testdata.astronauts WHERE name LIKE '%o%' AND group IN(1, 2, 3)"
    formatted_sql = format_sql(sql)
    # Relations should be colored with relation color (166,226,46)
    relation_color = "\x1b[38;2;166;226;46m"
    assert f"{relation_color}testdata\x1b[0m" in formatted_sql, "Schema name should be relation color"
    assert f"{relation_color}astron" in formatted_sql, "Table name should be relation color"


def test_format_sql_column_coloring():
    """Test that column names are colored in grey"""
    sql = "SELECT name, age FROM users WHERE status = 'active' AND group IN(1, 2, 3)"
    formatted_sql = format_sql(sql)
    # Columns should be colored with column color (150,150,150)
    column_color = "\x1b[38;2;150;150;150m"
    assert f"{column_color}name\x1b[0m" in formatted_sql, "Column 'name' should be grey"
    assert f"{column_color}age\x1b[0m" in formatted_sql, "Column 'age' should be grey"
    assert f"{column_color}status\x1b[0m" in formatted_sql, "Column 'status' should be grey"
    assert f"{column_color}group\x1b[0m" in formatted_sql, "Column 'group' should be grey (not keyword)"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
