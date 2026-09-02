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

# The statements synthesized by `opteryx.planner.pre_parse` rather than parsed:
# each names its target as a plain dotted string, not an identifier-part list, so
# it is read out directly instead of through `_extract_table_name`. The value is
# the key holding that name. A trigger's own name is not here: it names a trigger,
# not a relation, and the table it hangs off is the permission target.
_SYNTHESIZED_TARGETS = {
    "DropStatistics": "table_name",
    "DropTrigger": "table_name",
    "RefreshMaterializedView": "name",
    "AlterMaterializedViewOwner": "name",
    # The dataset being CREATED. The job whose results are copied is deliberately
    # NOT a relation: it is not in the catalog, nobody holds a policy on it, and
    # listing it here would have the caller's read permission checked against a
    # name no permission system knows. Who may copy a job's results is a
    # different question, answered where those results live.
    "SaveResults": "name",
    # The permission target is the object the grant is being administered on;
    # the principal receiving/losing it names a person, not a relation.
    "GrantAccess": "object_name",
    "RevokeAccess": "object_name",
    "ShowGrantsOn": "object_name",
    "ShowEffectiveGrantsOn": "object_name",
}

# What each synthesized statement is, and the role it needs. Kept beside the
# targets above rather than folded into the read/mutation/DDL lists below because
# the role does not follow from the category for any of them: a refresh rewrites
# a whole relation but is a writer-tier act (its contents are derived, not
# authored - see permissions.PERMISSIONS["REFRESH"]), and dropping a trigger is a
# WRITE on the table it hangs off rather than an owner-tier change.
#
# (is_mutation, is_ddl, permission_required); is_read is False for all of them.
_SYNTHESIZED_STATEMENTS = {
    # Replaces the view's contents from its definition. Nothing about the
    # relation itself changes, so it is a mutation and not DDL.
    "RefreshMaterializedView": (True, False, "writer"),
    # Creates a dataset, so it is classed and gated exactly as CTAS is: DDL,
    # owner-tier on the target. The rows come from a completed job rather than
    # from a SELECT, which changes who may run it, not what it makes.
    "SaveResults": (False, True, "owner"),
    # Drops a stored object rather than data; the binder gates it at ALTER.
    "DropStatistics": (False, True, "owner"),
    # WRITE on the table the trigger hangs off, symmetric with creating one.
    "DropTrigger": (False, True, "writer"),
    # Ownership is a workspace-level change; the binder gates it at ALTER.
    "AlterMaterializedViewOwner": (False, True, "owner"),
    # SET|DROP SECURE relaxes the SOURCE workspace's egress protection for one
    # object; the binder gates it at workspace ALTER, exactly as the property
    # form of ALTER WORKSPACE is.
    "AlterWorkspaceSecure": (False, True, "owner"),
    # Grant administration is owner-tier on the object, per ACTION_ROLES'
    # GRANT/REVOKE rows. Policy documents change, data does not: DDL, not
    # mutation.
    "GrantAccess": (False, True, "owner"),
    "RevokeAccess": (False, True, "owner"),
    # Reads policy documents and changes nothing — neither mutation nor DDL —
    # but gated at owner all the same: who may see the grants on an object is
    # who may change them. The effective listing reports strictly more (every
    # policy covering the object, not only those attached to it), so it is
    # gated identically rather than more loosely.
    "ShowGrantsOn": (False, False, "owner"),
    "ShowEffectiveGrantsOn": (False, False, "owner"),
    # A subscription to a task's run outcomes. Reader-tier, because LISTEN is a
    # read activity: it reports that a dataset was refreshed or failed to be,
    # which is a fact about that dataset.
    #
    # Deliberately absent from `_SYNTHESIZED_TARGETS`, so neither reports a
    # relation. The permission target is not the task - it is what the task
    # WRITES, read from the task's catalog record at bind time, and this is a
    # static walk of the AST with no catalog to read. Naming the task here
    # instead would check the wrong object: a grant covering a task's output
    # table does not cover the task's own name, so a legitimate subscriber
    # would be refused before the binder ever saw the statement. A caller
    # pre-flighting these gets the classification and no relation, which is the
    # honest answer; the binder holds the real gate.
    "Listen": (False, False, "reader"),
    "Unlisten": (False, False, "reader"),
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

    synthesized_key = _SYNTHESIZED_TARGETS.get(statement_type)
    if synthesized_key is not None:
        name = body.get(synthesized_key)
        # A grant's object can be a placeholder (see `pre_parse._slot_value`), and
        # this is the PRE-rewrite AST, so it is still one here. There is no name to
        # report - what the statement acts on is not decided until the parameters
        # are bound - and the placeholder is reported in `parameters` instead. A
        # caller pre-flighting permissions must read `parameters` and not treat an
        # empty `tables` as "touches nothing".
        if isinstance(name, str) and name:
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


# Where a statement names the relation whose CONTENTS it writes, as distinct
# from the relations it reads. Each value is the path through the statement body
# to an identifier-part list.
#
# `CreateTable` covers CTAS and a bare CREATE TABLE alike: both make the
# relation, and a reader asking "what does this write" wants the same answer for
# either. DELETE and TRUNCATE name theirs in shapes a fixed path cannot reach,
# and are read out in `extract_write_targets` itself.
#
# DDL that changes a relation's DEFINITION rather than its contents - DROP,
# ALTER TABLE, CREATE VIEW - is deliberately ABSENT. Those are owner-tier acts,
# and a caller gating on this map checks WRITE, so listing one here would answer
# an ownership question with a write grant. They are also not pipeline edges:
# nothing downstream reads rows that a DROP produced.
_WRITE_TARGET_PATHS = {
    "Insert": ("table", "TableName"),
    "CreateTable": ("name",),
    "Update": ("table", "relation", "Table", "name"),
    "Merge": ("table", "Table", "name"),
}


def _relation_node_name(node: Any) -> Optional[str]:
    """The name out of a `{"Table": {"name": [...]}}` relation node, or None."""
    if not isinstance(node, dict):
        return None
    table = node.get("Table")
    if not isinstance(table, dict):
        return None
    return _extract_table_name(table.get("name"))


def extract_write_targets(ast: Dict[str, Any]) -> List[str]:
    """Every relation whose CONTENTS a statement writes, in sorted order.

    The counterpart to `_extract_tables_from_ast`, which reports everything a
    statement touches without saying in which direction. Callers that must tell
    a read from a write - the authoring bound on CREATE TASK, and the `writes`
    recorded on a task so a pipeline can be followed through it - need the
    split, because a source needs only READ while a target needs WRITE.

    A list rather than a single name: TRUNCATE takes several tables, and every
    other form takes exactly one, so returning a list means a caller never has
    two shapes to handle. An empty list means the statement writes no relation
    contents - a SELECT, or DDL (see `_WRITE_TARGET_PATHS`).

    The target is always a static identifier. A parameterised one is not
    representable: placeholders are excluded from identifier slots, so
    `INSERT INTO :table` is a parse error rather than a name this cannot settle.
    """
    statement_type = next(iter(ast), None)
    body = ast.get(statement_type)
    if not isinstance(body, dict):
        return []

    targets: Set[str] = set()

    if statement_type == "Truncate":
        for entry in body.get("table_names") or []:
            if isinstance(entry, dict):
                name = _extract_table_name(entry.get("name"))
                if name:
                    targets.add(name)
        return sorted(targets)

    if statement_type == "Delete":
        # The parser wraps the FROM clause in a key naming whether the keyword
        # was written (`DELETE FROM t` / `DELETE t`), so the relations are taken
        # from whichever list is there rather than from a key spelled out here.
        from_clause = body.get("from")
        entries: List[Any] = []
        if isinstance(from_clause, dict):
            for value in from_clause.values():
                if isinstance(value, list):
                    entries.extend(value)
        elif isinstance(from_clause, list):
            entries = list(from_clause)
        for entry in entries:
            if isinstance(entry, dict):
                name = _relation_node_name(entry.get("relation"))
                if name:
                    targets.add(name)
        return sorted(targets)

    path = _WRITE_TARGET_PATHS.get(statement_type)
    if path is None:
        return []

    node: Any = body
    for step in path:
        if not isinstance(node, dict):
            return []
        node = node.get(step)

    name = _extract_table_name(node)
    return [name] if name else []


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


def describe_statement(parsed_statement: Dict[str, Any]) -> Dict[str, Any]:
    """
    Everything `analyze_query` reports, derived from one ALREADY-PARSED statement.

    Split from `parse_query_info` so `Session.check` can report the same fields
    alongside its bind-time diagnostics without parsing the statement a second time -
    two parses of one statement is two chances to disagree about what it is.

    Takes the PRE-rewrite AST. The AST rewriter substitutes placeholders, so a
    statement rewritten with its parameters supplied no longer records that a `:name`
    was ever written, and `parameters` would come back empty for the very statement
    that has them.

    Parameters:
        parsed_statement: one parsed statement, as `parse_statement` returns.

    Returns:
        The dict documented on `parse_query_info`, minus nothing.
    """
    query_type = next(iter(parsed_statement))

    tables = _extract_tables_from_ast(parsed_statement)
    # Remove system tables (those starting with $)
    filtered_tables = [t for t in sorted(tables) if not t.startswith("$")]

    parameters = sorted(_extract_placeholders(parsed_statement))

    synthesized = _SYNTHESIZED_STATEMENTS.get(query_type)
    if synthesized is not None:
        is_mutation, is_ddl, permission_required = synthesized
        return {
            "query_type": query_type,
            "tables": filtered_tables,
            "parameters": parameters,
            "is_read": False,
            "is_mutation": is_mutation,
            "is_ddl": is_ddl,
            "permission_required": permission_required,
        }

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
        QueryParseError: If the SQL cannot be parsed. This used to be a bare
            ValueError carrying the parser's own text; it is now the same error
            the query planner raises for the same statement, so a caller sees one
            parse failure rather than two spellings of it. QueryParseError is a
            SqlError (and so a PEP-249 ProgrammingError), NOT a ValueError.

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
    # The planner's own rewrite-and-parse, so a statement that parses here parses
    # there - including the pre-parse layer, without which a caller pre-flighting
    # REFRESH MATERIALIZED VIEW / DROP TRIGGER / DROP STATISTICS was told a statement
    # the engine runs happily does not parse.
    from opteryx.planner import parse_statement

    _clean_sql, parsed_statements = parse_statement(sql)

    if not parsed_statements or len(parsed_statements) == 0:
        raise ValueError("No statements found in SQL query")

    # For now, only handle the first statement
    # Multiple statements could be handled in the future
    return describe_statement(parsed_statements[0])
