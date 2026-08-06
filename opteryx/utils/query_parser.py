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


# Table-valued functions parse as a relation named after the function. They read
# their arguments, not a dataset, so they are not relations for this purpose.
_TABLE_FUNCTIONS = frozenset({"UNNEST", "GENERATE_SERIES", "VALUES"})

# Statements which name their target with a bare identifier path instead of a
# relation node. Each value is the path through the statement body to that path;
# `Drop` is handled separately because it names several targets at once.
_STATEMENT_TARGETS = {
    "AlterTable": ("name",),
    "CreateTable": ("name",),
    "CreateView": ("name",),
    "ShowColumns": ("show_options", "show_in", "parent_name"),
    "ShowCreate": ("obj_name",),
}


def _collect_relations(node: Any, tables: Set[str], cte_names: Set[str]) -> None:
    """
    Walk the whole AST, collecting every relation it names and every CTE alias.

    Deliberately generic. This walked the specific shape of each kind of
    statement, and each time the parser moved a shape underneath it - INSERT's
    target from `table_name` to `table.TableName`, DELETE's `from` gaining a
    `WithFromKeyword` wrapper - it reported no tables at all rather than failing,
    and a caller checking permissions saw a mutation which touched nothing.
    Wherever a relation appears, the parser names it with the same two nodes; so
    look for those two nodes anywhere, and stop knowing about statement shapes.

    Parameters:
        node: any node of the parsed AST.
        tables: collects relation names, mutated in place.
        cte_names: collects CTE aliases, mutated in place.
    """
    if isinstance(node, list):
        for item in node:
            _collect_relations(item, tables, cte_names)
        return

    if not isinstance(node, dict):
        return

    # `FROM users`, a join's relation, `UPDATE users`, `DELETE FROM users`
    relation = node.get("Table")
    if isinstance(relation, dict):
        name = _extract_table_name(relation.get("name"))
        if name and name.upper() not in _TABLE_FUNCTIONS:
            tables.add(name)

    # `INSERT INTO users`
    if "TableName" in node:
        name = _extract_table_name(node["TableName"])
        if name:
            tables.add(name)

    # A CTE names a result, not a relation: `WITH x AS (SELECT * FROM t)` reads
    # `t`, and the `x` below it is that result. Reporting `x` as a table names
    # something no permission can be held on, and hides the `t` that one can.
    with_clause = node.get("with")
    if isinstance(with_clause, dict):
        for cte in with_clause.get("cte_tables") or []:
            if not isinstance(cte, dict):
                continue
            alias = cte.get("alias")
            if isinstance(alias, dict) and isinstance(alias.get("name"), dict):
                cte_name = alias["name"].get("value")
                if cte_name:
                    cte_names.add(cte_name)

    for child in node.values():
        _collect_relations(child, tables, cte_names)


def _collect_statement_target(ast: Dict[str, Any], tables: Set[str]) -> None:
    """
    Add the relation a statement acts ON where the parser names it with a bare
    identifier path rather than a relation node.

    DDL and SHOW statements do this - `DROP TABLE users` carries `users` as
    `Drop.names[0]` - so the walk above cannot see it, and the caller checking
    permissions on a DROP is precisely the one who needs to know what it drops.

    Parameters:
        ast: one parsed statement.
        tables: collects relation names, mutated in place.
    """
    statement_type = next(iter(ast), None)
    body = ast.get(statement_type)
    if not isinstance(body, dict):
        return

    if statement_type == "Drop":
        # one statement, several targets: DROP TABLE a, b
        for name_parts in body.get("names") or []:
            name = _extract_table_name(name_parts)
            if name:
                tables.add(name)
        return

    path = _STATEMENT_TARGETS.get(statement_type)
    if path is None:
        return

    node: Any = body
    for step in path:
        if not isinstance(node, dict):
            return
        node = node.get(step)

    name = _extract_table_name(node)
    if name:
        tables.add(name)


def _extract_tables_from_ast(ast: Dict[str, Any]) -> List[str]:
    """
    Every relation a statement references, in sorted order.

    Parameters:
        ast: one parsed statement.

    Returns:
        Sorted list of relation names.
    """
    tables: Set[str] = set()
    cte_names: Set[str] = set()

    _collect_relations(ast, tables, cte_names)
    _collect_statement_target(ast, tables)

    return sorted(tables - cte_names)


def _extract_table_name(name_parts: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extract a table name from an array of identifier parts.

    Parameters:
        name_parts: List of identifier dictionaries

    Returns:
        Dot-separated table name or None
    """
    if not isinstance(name_parts, list) or not name_parts:
        return None

    parts = []
    for part in name_parts:
        if "Identifier" in part and "value" in part["Identifier"]:
            parts.append(part["Identifier"]["value"])

    return ".".join(parts) if parts else None


def _extract_placeholders(node: Any) -> Set[str]:
    """
    Recursively walk a parsed AST (or any sub-node of one) collecting the
    names of every `:name`-style placeholder referenced anywhere in the
    query - WHERE, SELECT list, LIMIT, anywhere a value expression can
    appear.

    Mirrors the traversal `parameter_dict_binder` in
    `opteryx.planner.ast_rewriter` performs at bind time (a `{"Placeholder":
    ":name"}` node, name recovered by stripping the leading marker
    character) - this is a preview of exactly the set of names that
    binding will require, without needing the parameters supplied.

    Positional `?` placeholders have no name (stripping their single-
    character value leaves an empty string) and so are not collected here;
    callers that mix `?` and `:name` placeholders in one statement are
    already rejected elsewhere in the binder.
    """
    names: Set[str] = set()

    if isinstance(node, dict):
        if "Placeholder" in node:
            value = node["Placeholder"]
            if isinstance(value, str) and len(value) > 1:
                names.add(value[1:])
        for child in node.values():
            names.update(_extract_placeholders(child))
    elif isinstance(node, list):
        for item in node:
            names.update(_extract_placeholders(item))

    return names


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
        - tables: List[str] - Every relation the statement references, sorted:
          those read through subqueries, derived tables and CTE bodies, and the
          target of a mutation or a DDL statement. Not included: a CTE's own
          alias (it names a result, not a relation), a table-valued function
          (it reads its arguments), or a `$` system dataset.
        - parameters: List[str] - Names of `:name` placeholders referenced in
          the query (sorted, deduplicated, no leading `:`) - lets a caller
          resolve exactly the parameters a query needs before execution,
          without waiting for the bind-time `Parameter not defined` error.
          Positional `?` placeholders aren't named and so aren't included.
        - is_read: bool - True if this only reads (SELECT, SHOW COLUMNS,
          SHOW TABLES, USE, SHOW CREATE)
        - is_mutation: bool - True if this modifies data (INSERT, UPDATE, DELETE)
        - is_ddl: bool - True if this is a DDL operation (CREATE, ALTER, DROP)
        - permission_required: str - the role the statement needs: "reader",
          "writer", "owner", or "denied" for a statement none of them permits

    Raises:
        ValueError: If the SQL cannot be parsed

    Example:
        >>> info = parse_query_info("SELECT * FROM users WHERE id = 1")
        >>> info["query_type"]
        'Query'
        >>> info["tables"]
        ['users']
        >>> info["is_read"]
        True
        >>> parse_query_info("SELECT * FROM t WHERE dept = :department")["parameters"]
        ['department']
    """
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide
    from opteryx.utils.sql import remove_comments

    # Clean the SQL using the same rewriter as the main query planner
    decommented_sql = remove_comments(sql)
    clean_sql = do_sql_rewrite(decommented_sql)

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

    # Extract named `:name` placeholder parameters
    parameters = sorted(_extract_placeholders(parsed_statement))

    # Determine query characteristics

    reader_actions = ["Query", "ShowColumns", "ShowTables", "Use", "ShowCreate"]
    mutation_actions = ["Insert", "Update", "Delete"]
    # "AlterFunction" is ALTER WORKSPACE - the SQL rewriter borrows the parser's
    # AlterFunction statement for it (see sql_rewriter.rewrite_alter_workspace).
    ddl_actions = ["CreateTable", "CreateView", "AlterTable", "AlterFunction", "Drop"]

    return {
        "query_type": query_type,
        "tables": filtered_tables,
        "parameters": parameters,
        "is_read": query_type in reader_actions,
        "is_mutation": query_type in mutation_actions,
        "is_ddl": query_type in ddl_actions,
        "permission_required": (
            "owner"
            if query_type in ddl_actions
            else (
                "writer"
                if query_type in mutation_actions
                else "reader"
                if query_type in reader_actions
                else "denied"
            )
        ),
    }
