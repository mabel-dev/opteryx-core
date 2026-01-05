"""
Unit tests for SQL rewriter - COMMENT ON TABLE/VIEW rewriting
"""

from opteryx.planner.sql_rewriter import do_sql_rewrite


def test_rewrite_comment_on_table_to_extension():
    """Test that COMMENT ON TABLE is rewritten to COMMENT ON EXTENSION"""
    result = do_sql_rewrite("COMMENT ON TABLE workspace.collection.table IS 'test comment'")
    assert "COMMENT ON EXTENSION" in result
    assert "COMMENT ON TABLE" not in result


def test_rewrite_comment_on_view_to_extension():
    """Test that COMMENT ON VIEW is rewritten to COMMENT ON EXTENSION"""
    result = do_sql_rewrite("COMMENT ON VIEW workspace.collection.view IS 'test comment'")
    assert "COMMENT ON EXTENSION" in result
    assert "COMMENT ON VIEW" not in result


def test_rewrite_comment_on_extension_unchanged():
    """Test that COMMENT ON EXTENSION is left unchanged"""
    result = do_sql_rewrite("COMMENT ON EXTENSION workspace.collection.view IS 'test comment'")
    assert "COMMENT ON EXTENSION" in result


def test_rewrite_comment_with_if_exists():
    """Test that IF EXISTS is preserved in the rewrite"""
    result = do_sql_rewrite("COMMENT IF EXISTS ON TABLE test.table IS 'comment'")
    assert "COMMENT" in result
    assert "IF EXISTS" in result
    assert "EXTENSION" in result


def test_rewrite_comment_case_insensitive():
    """Test that rewriter works with different case"""
    result = do_sql_rewrite("comment on table test.table is 'comment'")
    assert "EXTENSION" in result.upper()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
