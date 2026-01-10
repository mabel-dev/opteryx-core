# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Query Parser Utility

This module provides functionality to parse SQL queries and extract metadata
without executing the query. This is useful for:
- Pre-flight permission checks
- Query validation
- Resource planning
- Query analysis
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set


def _extract_tables_from_relation(relation: Dict[str, Any]) -> Set[str]:
    """
    Extract table names from a relation object in the AST.

    Parameters:
        relation: Dict containing relation information from the parsed AST

    Returns:
        Set of table names found in the relation
    """
    tables = set()

    if "Table" in relation:
        table = relation["Table"]
        # Extract table name from the name array
        if "name" in table and table["name"]:
            table_name = ".".join(
                part.get("Identifier", {}).get("value", "") for part in table["name"]
            )
            if table_name:
                tables.add(table_name)

    if "Derived" in relation:
        # Handle subqueries
        derived = relation["Derived"]
        if "subquery" in derived and derived["subquery"]:
            # Recursively extract tables from subquery
            subquery_tables = _extract_tables_from_ast(derived["subquery"])
            tables.update(subquery_tables)

    return tables


def _extract_tables_from_join(join: Dict[str, Any]) -> Set[str]:
    """
    Extract table names from a join object in the AST.

    Parameters:
        join: Dict containing join information from the parsed AST

    Returns:
        Set of table names found in the join
    """
    tables = set()

    if "relation" in join:
        tables.update(_extract_tables_from_relation(join["relation"]))

    # Handle nested joins
    if "joins" in join:
        for nested_join in join["joins"]:
            tables.update(_extract_tables_from_join(nested_join))

    return tables


def _extract_tables_from_ast(ast: Dict[str, Any]) -> Set[str]:
    """
    Recursively extract all table names from a parsed AST.

    Parameters:
        ast: Parsed AST dictionary from sqloxide.parse_sql

    Returns:
        Set of table names found in the query
    """
    tables = set()

    # Handle Query statements
    if "Query" in ast:
        query = ast["Query"]
        if "body" in query:
            body = query["body"]

            # Handle SELECT statements
            if "Select" in body:
                select = body["Select"]

                # Extract from FROM clause
                if "from" in select and select["from"]:
                    for relation in select["from"]:
                        if "relation" in relation:
                            tables.update(_extract_tables_from_relation(relation["relation"]))

                        # Handle joins
                        if "joins" in relation and relation["joins"]:
                            for join in relation["joins"]:
                                tables.update(_extract_tables_from_join(join))

                # Extract from WHERE clause subqueries
                if "selection" in select and select["selection"]:
                    # Recursively search for subqueries in the selection
                    tables.update(_extract_subquery_tables(select["selection"]))

            # Handle SetOperation (UNION, INTERSECT, EXCEPT)
            if "SetOperation" in body:
                set_op = body["SetOperation"]
                if "left" in set_op:
                    tables.update(_extract_tables_from_ast({"Query": {"body": set_op["left"]}}))
                if "right" in set_op:
                    tables.update(_extract_tables_from_ast({"Query": {"body": set_op["right"]}}))

        # Handle CTEs (WITH clauses)
        if "with" in query and query["with"]:
            cte_tables = query["with"].get("cte_tables", [])
            for cte in cte_tables:
                if "query" in cte:
                    tables.update(_extract_tables_from_ast(cte["query"]))

    # Handle INSERT statements
    if "Insert" in ast:
        insert = ast["Insert"]
        if "table_name" in insert:
            table_name = _extract_table_name(insert["table_name"])
            if table_name:
                tables.add(table_name)

        # Handle INSERT ... SELECT
        if "source" in insert and insert["source"]:
            source = insert["source"]
            if "Query" in source:
                tables.update(_extract_tables_from_ast(source))

    # Handle UPDATE statements
    if "Update" in ast:
        update = ast["Update"]
        if "table" in update and "Table" in update["table"]:
            table_name = _extract_table_name(update["table"]["Table"].get("name", []))
            if table_name:
                tables.add(table_name)

    # Handle DELETE statements
    if "Delete" in ast:
        delete = ast["Delete"]
        if "tables" in delete:
            for table in delete["tables"]:
                table_name = _extract_table_name(table.get("name", []))
                if table_name:
                    tables.add(table_name)
        if "from" in delete:
            for relation in delete["from"]:
                if "relation" in relation:
                    tables.update(_extract_tables_from_relation(relation["relation"]))

    FUNCTIONS_NOT_TABLES = ("UNNEST", "GENERATE_SERIES", "VALUES")
    tables = [
        table for table in tables if table and table.upper() not in FUNCTIONS_NOT_TABLES
    ]  # Remove empty names and functions
    return tables


def _extract_table_name(name_parts: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extract a table name from an array of identifier parts.

    Parameters:
        name_parts: List of identifier dictionaries

    Returns:
        Dot-separated table name or None
    """
    if not name_parts:
        return None

    parts = []
    for part in name_parts:
        if "Identifier" in part and "value" in part["Identifier"]:
            parts.append(part["Identifier"]["value"])

    return ".".join(parts) if parts else None


def _extract_subquery_tables(expression: Any) -> Set[str]:
    """
    Recursively extract tables from subqueries within expressions.

    Parameters:
        expression: Expression object that may contain subqueries

    Returns:
        Set of table names found in subqueries
    """
    tables = set()

    if not isinstance(expression, dict):
        return tables

    # Look for Subquery nodes
    if "Subquery" in expression:
        subquery = expression["Subquery"]
        tables.update(_extract_tables_from_ast(subquery))

    # Recursively search nested structures
    for value in expression.values():
        if isinstance(value, dict):
            tables.update(_extract_subquery_tables(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    tables.update(_extract_subquery_tables(item))

    return tables


def parse_query_info(sql: str) -> Dict[str, Any]:
    """
    Parse a SQL query and extract metadata without executing it.

    This function analyzes the SQL query structure to extract:
    - Query type (SELECT, INSERT, UPDATE, DELETE, etc.)
    - Tables being queried
    - Other metadata available from the SQL syntax alone

    This is useful for:
    - Pre-flight permission checks
    - Query validation before queueing
    - Resource planning
    - Query analysis

    Parameters:
        sql: SQL query string to parse

    Returns:
        Dictionary containing:
        - query_type: str - Type of query (e.g., "Query", "Insert", "Update")
        - tables: List[str] - List of table names referenced in the query
        - is_select: bool - True if this is a SELECT query
        - is_mutation: bool - True if this modifies data (INSERT, UPDATE, DELETE)
        - is_ddl: bool - True if this is a DDL operation (CREATE, ALTER, DROP)

    Raises:
        ValueError: If the SQL cannot be parsed

    Example:
        >>> info = parse_query_info("SELECT * FROM users WHERE id = 1")
        >>> info["query_type"]
        'Query'
        >>> info["tables"]
        ['users']
        >>> info["is_select"]
        True
    """
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    # Clean the SQL using the same rewriter as the main query planner
    clean_sql = do_sql_rewrite(sql)

    # Parse the SQL to get the AST
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    except ValueError as e:
        raise ValueError(f"Failed to parse SQL query: {e}") from e

    if not parsed_statements or len(parsed_statements) == 0:
        raise ValueError("No statements found in SQL query")

    # For now, only handle the first statement
    # Multiple statements could be handled in the future
    parsed_statement = parsed_statements[0]

    # Determine query type
    query_type = next(iter(parsed_statement))

    # Extract tables
    tables = _extract_tables_from_ast(parsed_statement)

    # Remove system tables (those starting with $)
    filtered_tables = [t for t in sorted(tables) if not t.startswith("$")]

    # Determine query characteristics

    reader_actions = ["Query", "ShowColumns", "ShowTables", "Use", "ShowCreate"]
    mutation_actions = ["Insert", "Update", "Delete"]
    ddl_actions = ["CreateTable", "CreateView", "AlterTable", "Drop"]

    return {
        "query_type": query_type,
        "tables": filtered_tables,
        "is_read": query_type in reader_actions,
        "is_mutation": query_type in mutation_actions,
        "is_ddl": query_type in ddl_actions,
        "permission_required": "owner"
        if query_type in ddl_actions
        else "writer"
        if query_type in mutation_actions
        else "reader"
        if query_type in reader_actions
        else "denied",
    }
