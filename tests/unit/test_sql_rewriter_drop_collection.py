"""
Unit tests for SQL rewriter - DROP COLLECTION rewriting
"""

from opteryx.planner.sql_rewriter import do_sql_rewrite


def test_rewrite_drop_collection_to_schema():
    """Test that DROP COLLECTION is rewritten to DROP SCHEMA"""
    result = do_sql_rewrite("DROP COLLECTION workspace.collection")
    assert "DROP SCHEMA" in result
    assert "COLLECTION" not in result


def test_rewrite_drop_collection_if_exists():
    """Test that IF EXISTS is preserved in the rewrite"""
    result = do_sql_rewrite("DROP COLLECTION IF EXISTS workspace.collection")
    assert "DROP SCHEMA" in result
    assert "IF EXISTS" in result


def test_rewrite_drop_collection_case_insensitive():
    """Test that rewriter works with different case"""
    result = do_sql_rewrite("drop collection workspace.collection")
    assert "SCHEMA" in result.upper()


def test_rewrite_drop_table_unchanged():
    """Test that DROP TABLE is not affected by the DROP COLLECTION rewrite"""
    result = do_sql_rewrite("DROP TABLE workspace.collection.table")
    assert "DROP TABLE" in result


def test_rewrite_drop_schema_unchanged():
    """A literal DROP SCHEMA (not produced via the COLLECTION rewrite) passes through untouched"""
    result = do_sql_rewrite("DROP SCHEMA workspace.collection")
    assert "DROP SCHEMA" in result


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
