"""
Test opteryx.analyze_query - SQL metadata extracted without executing.

Named `parse_query_info` when written; that is now the internal function in
opteryx/utils/query_parser.py, and `analyze_query` is the public name.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def test_parse_simple_select():
    """Test parsing a simple SELECT query"""
    info = opteryx.analyze_query("SELECT * FROM users")
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert info["is_read"] is True
    assert info["is_mutation"] is False
    assert info["is_ddl"] is False


def test_parse_select_with_join():
    """Test parsing a SELECT query with JOIN"""
    info = opteryx.analyze_query("""
        SELECT u.name, o.amount 
        FROM users u 
        JOIN orders o ON u.id = o.user_id
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert len(info["tables"]) == 2
    assert info["is_read"] is True
    assert info["is_mutation"] is False


def test_parse_select_with_multiple_joins():
    """Test parsing a SELECT query with multiple JOINs"""
    info = opteryx.analyze_query("""
        SELECT u.name, o.amount, p.product_name
        FROM users u 
        JOIN orders o ON u.id = o.user_id
        LEFT JOIN products p ON o.product_id = p.id
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert "products" in info["tables"]
    assert len(info["tables"]) == 3
    assert info["is_read"] is True


def test_parse_select_with_subquery():
    """Test parsing a SELECT query with subquery"""
    info = opteryx.analyze_query("""
        SELECT * FROM users 
        WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert len(info["tables"]) == 2
    assert info["is_read"] is True


def test_parse_select_with_cte():
    """Test parsing a SELECT query with CTE (WITH clause)"""
    info = opteryx.analyze_query("""
        WITH high_value_orders AS (
            SELECT user_id FROM orders WHERE amount > 100
        )
        SELECT u.* FROM users u
        JOIN high_value_orders h ON u.id = h.user_id
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert info["is_read"] is True


def test_parse_union():
    """Test parsing a UNION query"""
    info = opteryx.analyze_query("""
        SELECT name FROM users
        UNION
        SELECT name FROM customers
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "customers" in info["tables"]
    assert len(info["tables"]) == 2
    assert info["is_read"] is True


def test_parse_insert():
    """Test parsing an INSERT query"""
    info = opteryx.analyze_query("""
        INSERT INTO users (name, email) VALUES ('John', 'john@example.com')
    """)
    
    assert info["query_type"] == "Insert"
    assert "users" in info["tables"]
    assert info["is_read"] is False
    assert info["is_mutation"] is True
    assert info["is_ddl"] is False


def test_parse_insert_select():
    """Test parsing an INSERT ... SELECT query"""
    info = opteryx.analyze_query("""
        INSERT INTO archive_users 
        SELECT * FROM users WHERE created_at < '2020-01-01'
    """)
    
    assert info["query_type"] == "Insert"
    assert "archive_users" in info["tables"]
    assert "users" in info["tables"]
    assert len(info["tables"]) == 2
    assert info["is_mutation"] is True


def test_parse_update():
    """Test parsing an UPDATE query"""
    info = opteryx.analyze_query("""
        UPDATE users SET email = 'new@example.com' WHERE id = 1
    """)
    
    assert info["query_type"] == "Update"
    assert "users" in info["tables"]
    assert info["is_read"] is False
    assert info["is_mutation"] is True


def test_parse_delete():
    """Test parsing a DELETE query"""
    info = opteryx.analyze_query("""
        DELETE FROM users WHERE id = 1
    """)
    
    assert info["query_type"] == "Delete"
    assert "users" in info["tables"]
    assert info["is_read"] is False
    assert info["is_mutation"] is True


def test_parse_qualified_table_names():
    """Test parsing queries with schema-qualified table names"""
    info = opteryx.analyze_query("SELECT * FROM schema.users")
    
    assert info["query_type"] == "Query"
    assert "schema.users" in info["tables"]
    assert info["is_read"] is True


def test_parse_multiple_qualified_tables():
    """Test parsing queries with multiple qualified table names"""
    info = opteryx.analyze_query("""
        SELECT * FROM db1.schema1.users u
        JOIN db2.schema2.orders o ON u.id = o.user_id
    """)
    
    assert info["query_type"] == "Query"
    assert "db1.schema1.users" in info["tables"]
    assert "db2.schema2.orders" in info["tables"]


def test_parse_system_tables_excluded():
    """Test that system tables (starting with $) are excluded"""
    info = opteryx.analyze_query("SELECT * FROM $planets")
    
    assert info["query_type"] == "Query"
    assert len(info["tables"]) == 0  # System tables should be filtered out
    assert info["is_read"] is True


def test_parse_mixed_system_and_user_tables():
    """Test parsing with both system and user tables"""
    info = opteryx.analyze_query("""
        SELECT u.*, p.name 
        FROM users u 
        CROSS JOIN $planets p
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "$planets" not in info["tables"]  # System table filtered out
    assert len(info["tables"]) == 1


def test_parse_invalid_sql():
    """Test that invalid SQL raises an error"""
    with pytest.raises(ValueError, match="Failed to parse SQL query"):
        opteryx.analyze_query("SELECT * FROM WHERE")


def test_parse_empty_sql():
    """Test that empty SQL raises an error"""
    with pytest.raises(ValueError):
        opteryx.analyze_query("")


def test_parse_select_with_derived_table():
    """Test parsing SELECT with derived table (subquery in FROM)"""
    info = opteryx.analyze_query("""
        SELECT * FROM (
            SELECT id, name FROM users WHERE active = true
        ) AS active_users
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert info["is_read"] is True


def test_parse_complex_nested_query():
    """Test parsing a complex nested query"""
    info = opteryx.analyze_query("""
        WITH recent_orders AS (
            SELECT user_id, SUM(amount) as total
            FROM orders
            WHERE order_date > '2024-01-01'
            GROUP BY user_id
        )
        SELECT u.name, r.total, p.title
        FROM users u
        JOIN recent_orders r ON u.id = r.user_id
        LEFT JOIN (
            SELECT user_id, title FROM purchases WHERE status = 'completed'
        ) p ON u.id = p.user_id
        WHERE u.active = true
    """)
    
    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert "purchases" in info["tables"]
    assert len(info["tables"]) == 3
    assert info["is_read"] is True


def test_parse_select_no_from():
    """Test parsing SELECT without FROM clause"""
    info = opteryx.analyze_query("SELECT 1 + 1")
    
    assert info["query_type"] == "Query"
    assert len(info["tables"]) == 0
    assert info["is_read"] is True


def test_parse_multiple_tables_in_from():
    """Test parsing SELECT with multiple tables in FROM clause (implicit cross join)"""
    info = opteryx.analyze_query("""
        SELECT * FROM users, orders WHERE users.id = orders.user_id
    """)

    assert info["query_type"] == "Query"
    assert "users" in info["tables"]
    assert "orders" in info["tables"]
    assert len(info["tables"]) == 2


def test_parse_no_parameters():
    """Test that a query with no placeholders reports an empty parameters list"""
    info = opteryx.analyze_query("SELECT * FROM users WHERE id = 1")

    assert info["parameters"] == []


def test_parse_named_parameter_in_where():
    """Test extracting a single `:name` placeholder from a WHERE clause"""
    info = opteryx.analyze_query("SELECT * FROM users WHERE department = :department")

    assert info["parameters"] == ["department"]


def test_parse_multiple_named_parameters():
    """Test extracting several `:name` placeholders, sorted and deduplicated"""
    info = opteryx.analyze_query("""
        SELECT * FROM users
        WHERE department = :department
          AND active = :is_active
        LIMIT :lim
    """)

    assert info["parameters"] == ["department", "is_active", "lim"]


def test_parse_repeated_named_parameter_deduplicated():
    """Test that the same placeholder used twice is only reported once"""
    info = opteryx.analyze_query("""
        SELECT * FROM users
        WHERE department = :department OR backup_department = :department
    """)

    assert info["parameters"] == ["department"]


def test_parse_qmark_parameter_not_named():
    """Test that a positional `?` placeholder is not reported as a named parameter"""
    info = opteryx.analyze_query("SELECT * FROM users WHERE id = ?")

    assert info["parameters"] == []


def test_parse_named_parameter_in_subquery():
    """Test extracting a `:name` placeholder referenced inside a subquery"""
    info = opteryx.analyze_query("""
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE amount > :min_amount)
    """)

    assert info["parameters"] == ["min_amount"]


def test_permission_required_for_each_kind_of_statement():
    """The role a statement needs, for a caller checking before it queues one"""
    assert opteryx.analyze_query("SELECT * FROM users")["permission_required"] == "reader"
    assert opteryx.analyze_query("SHOW COLUMNS FROM users")["permission_required"] == "reader"
    assert (
        opteryx.analyze_query("INSERT INTO users (name) VALUES ('John')")["permission_required"]
        == "writer"
    )
    assert opteryx.analyze_query("DROP TABLE users")["permission_required"] == "owner"
    # a statement no role permits, rather than one every role does
    assert opteryx.analyze_query("SET x = 1")["permission_required"] == "denied"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
