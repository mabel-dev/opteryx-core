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
    """Invalid SQL raises the same parse error the planner raises.

    This entry point used to raise a bare ValueError wrapping the parser's own
    text, so the same statement produced two different errors depending on which
    door it came through. QueryParseError is a SqlError, not a ValueError.
    """
    from opteryx.exceptions import QueryParseError

    with pytest.raises(QueryParseError, match="could not be parsed"):
        opteryx.analyze_query("SELECT * FROM users WHERE")


def test_parse_agrees_with_the_planner_about_what_is_valid():
    """`SELECT * FROM WHERE` was expected to be a parse error here. It is not one
    for the engine either - the dialect reads `WHERE` as the relation's name and
    the query fails later, on the dataset not existing. This reports what the
    engine will do with the statement, so it has to accept what the engine
    accepts: rejecting a statement the planner would run is the worse answer for
    a caller using this to decide whether to queue it."""
    info = opteryx.analyze_query("SELECT * FROM WHERE")

    assert info["query_type"] == "Query"
    assert info["tables"] == ["WHERE"]


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


def test_parse_scalar_subquery_in_the_select_list():
    """A subquery is a subquery wherever it appears, not only in the FROM"""
    info = opteryx.analyze_query("SELECT (SELECT MAX(amount) FROM orders) AS m, name FROM users")

    assert sorted(info["tables"]) == ["orders", "users"]


def test_parse_exists_subquery():
    info = opteryx.analyze_query(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM audit WHERE audit.uid = users.id)"
    )

    assert sorted(info["tables"]) == ["audit", "users"]


def test_parse_subquery_in_having():
    info = opteryx.analyze_query(
        "SELECT COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > (SELECT AVG(n) FROM stats)"
    )

    assert sorted(info["tables"]) == ["stats", "users"]


def test_parse_cte_reports_what_it_reads_not_its_own_name():
    """`h` is a result, not a relation - nothing can hold a permission on it"""
    info = opteryx.analyze_query(
        "WITH h AS (SELECT user_id FROM orders) SELECT u.* FROM users u JOIN h ON u.id = h.user_id"
    )

    assert sorted(info["tables"]) == ["orders", "users"]
    assert "h" not in info["tables"]


def test_parse_update_reports_its_target():
    info = opteryx.analyze_query("UPDATE users SET email = 'new@example.com' WHERE id = 1")

    assert info["tables"] == ["users"]


def test_parse_delete_reports_its_target():
    info = opteryx.analyze_query("DELETE FROM users WHERE id = 1")

    assert info["tables"] == ["users"]


def test_parse_drop_reports_its_target():
    """A DDL target is named by the statement, not by a relation node"""
    info = opteryx.analyze_query("DROP TABLE users")

    assert info["tables"] == ["users"]
    assert info["is_ddl"] is True


def test_parse_create_table_reports_its_target():
    info = opteryx.analyze_query("CREATE TABLE new_users (id INTEGER)")

    assert info["tables"] == ["new_users"]


def test_parse_alter_table_reports_its_target():
    info = opteryx.analyze_query("ALTER TABLE users ADD COLUMN nickname VARCHAR")

    assert info["tables"] == ["users"]


def test_parse_guarded_alter_table_reports_its_target():
    """`ADD COLUMN IF NOT EXISTS` is built by the Opteryx dialect itself rather
    than by upstream's ALTER TABLE production, so it gets its own check that the
    statement it produces is read exactly like the unguarded one."""
    info = opteryx.analyze_query("ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname VARCHAR")

    assert info["tables"] == ["users"]
    assert info["is_ddl"] is True


def test_parse_show_columns_reports_the_table_it_describes():
    info = opteryx.analyze_query("SHOW COLUMNS FROM users")

    assert info["tables"] == ["users"]
    assert info["is_read"] is True


def test_parse_explain_reports_the_tables_of_the_statement_it_explains():
    info = opteryx.analyze_query("EXPLAIN SELECT * FROM users")

    assert info["tables"] == ["users"]


def test_parse_table_functions_are_not_relations():
    """They read their arguments, not a dataset"""
    assert opteryx.analyze_query("SELECT * FROM UNNEST((1, 2)) AS x")["tables"] == []
    assert opteryx.analyze_query("SELECT * FROM GENERATE_SERIES(1, 10) AS g")["tables"] == []


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


# ── Statements the parser has no grammar for ────────────────────────────────
# These four are recognized before the parser, by opteryx.planner.pre_parse, and
# synthesized straight into an AST. This function used to skip that step and hand
# them to sqlparser unprepared, so it reported a syntax error for statements the
# engine runs - and a caller that pre-flights a query before queueing it (the jobs
# API does, to check permissions) rejected every one of them. `REFRESH
# MATERIALIZED VIEW` reached a user as "Unable to parse query" from the Trigger
# refresh button on a materialized view, which is the whole reason these exist.


def test_parse_refresh_materialized_view():
    info = opteryx.analyze_query("REFRESH MATERIALIZED VIEW opteryx.public.daily;")

    assert info["query_type"] == "RefreshMaterializedView"
    assert info["tables"] == ["opteryx.public.daily"]
    assert info["is_read"] is False
    # It replaces the view's contents, not its definition, and a refresh is a
    # writer-tier act because those contents are derived rather than authored.
    assert info["is_mutation"] is True
    assert info["is_ddl"] is False
    assert info["permission_required"] == "writer"


def test_parse_drop_trigger():
    info = opteryx.analyze_query("DROP TRIGGER IF EXISTS refresh_daily ON opteryx.public.orders;")

    assert info["query_type"] == "DropTrigger"
    # The table, not the trigger: a trigger name is not a relation, and the table
    # it hangs off is what a permission is held on.
    assert info["tables"] == ["opteryx.public.orders"]
    assert info["is_ddl"] is True
    assert info["permission_required"] == "writer"


def test_parse_alter_materialized_view_owner():
    info = opteryx.analyze_query(
        "ALTER MATERIALIZED VIEW opteryx.public.daily OWNER TO 'olive@example.com';"
    )

    assert info["query_type"] == "AlterMaterializedViewOwner"
    assert info["tables"] == ["opteryx.public.daily"]
    assert info["is_ddl"] is True
    assert info["permission_required"] == "owner"


def test_parse_drop_statistics():
    info = opteryx.analyze_query("DROP STATISTICS ON opteryx.public.orders FOR COLUMNS id, name;")

    assert info["query_type"] == "DropStatistics"
    assert info["tables"] == ["opteryx.public.orders"]
    assert info["is_ddl"] is True
    assert info["permission_required"] == "owner"


def test_parse_rejects_a_near_miss_by_name():
    """A statement opening with one of these keywords but matching none of them.

    Rejected by name here rather than left to the parser, which would report a
    syntax error pointing at a token several words from the actual problem - the
    same reason the planner rejects it by name.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError, match="REFRESH MATERIALIZED VIEW"):
        opteryx.analyze_query("REFRESH VIEW opteryx.public.daily;")


def test_the_planner_and_analyze_query_agree_on_what_parses():
    """Both go through the same pre-parse layer, so neither can drift from the other."""
    from opteryx.planner.pre_parse import pre_parse

    for statement in (
        "REFRESH MATERIALIZED VIEW opteryx.public.daily;",
        "DROP TRIGGER t ON opteryx.public.orders;",
        "ALTER MATERIALIZED VIEW opteryx.public.daily OWNER TO 'olive';",
        "DROP STATISTICS ON opteryx.public.orders;",
    ):
        assert pre_parse(statement) is not None, statement
        assert opteryx.analyze_query(statement)["tables"] != []

    # An ordinary statement is left for the parser
    assert pre_parse("SELECT * FROM users") is None


# --- SAVE RESULTS OF <job> AS <dataset> -------------------------------------
#
# The statement that copies a completed job's results into a dataset. Same
# pre-parse route as REFRESH, and for the same reason: the jobs API pre-flights
# a statement with analyze_query before queueing it, so a statement the parser
# has never heard of is rejected at submission with "Expected: an SQL statement,
# found: SAVE" rather than running.


def test_parse_save_results():
    info = opteryx.analyze_query(
        "SAVE RESULTS OF 20260829145017-34mo5tqwk8n77jsr AS personal.bastian.cve_stuff"
    )

    assert info["query_type"] == "SaveResults"
    # The TARGET only. The job is not a relation - no catalog knows it and no
    # policy covers it, so listing it would have the caller's read permission
    # checked against a name the permission system cannot answer for.
    assert info["tables"] == ["personal.bastian.cve_stuff"]
    assert info["is_read"] is False
    # Creates a dataset, so it is classed and gated as CTAS is.
    assert info["is_ddl"] is True
    assert info["is_mutation"] is False
    assert info["permission_required"] == "owner"


def test_save_results_accepts_a_real_job_handle():
    """A job id opens with a digit and carries a hyphen, so it is not
    identifier-shaped and cannot borrow the object slot the other statements use."""
    from opteryx.planner.pre_parse import pre_parse
    parsed = pre_parse("SAVE RESULTS OF 20260829145017-34mo5tqwk8n77jsr AS personal.b.x")

    assert parsed == [
        {
            "SaveResults": {
                "handle": "20260829145017-34mo5tqwk8n77jsr",
                "name": "personal.b.x",
            }
        }
    ]


def test_save_results_takes_no_placeholder():
    """The handle chooses WHOSE results get copied into the caller's workspace.
    A parameterised one would let runtime data make that choice."""
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.planner.pre_parse import pre_parse
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("SAVE RESULTS OF :job AS personal.b.x")


def test_a_malformed_save_names_the_statement_it_is_not():
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.planner.pre_parse import pre_parse
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("SAVE THE WHALES")
    with pytest.raises(UnsupportedSyntaxError):
        pre_parse("SAVE RESULTS OF abc")


def test_save_is_not_planned_by_the_engine():
    """Parsed and classified here, executed by the service that owns the results
    bucket. Reaching the planner means a dispatcher did not recognise it, and the
    message has to say that rather than 'Opteryx does not support SAVE RESULTS'."""
    from opteryx.exceptions import UnsupportedSyntaxError
    session = opteryx.session(user="bastian")
    with pytest.raises(UnsupportedSyntaxError) as err:
        list(session.execute_to_morsels("SAVE RESULTS OF 20260829145017-abc AS personal.b.x"))
    assert "not by the query engine" in str(err.value)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
