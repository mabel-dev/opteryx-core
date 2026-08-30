# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Render the DDL that recreates an object, for `SHOW CREATE`.

A VIEW, a MATERIALIZED VIEW and a TASK each kept the statement that defined
them, so showing one is a read. A TABLE did not - its shape is the catalog's,
not a statement anybody stored - so its DDL is RECONSTRUCTED here from what the
catalog holds: columns, nullability, declared relationships and clustering.

The reconstruction is a statement that recreates the table's SHAPE. It is not
the statement that was typed, and two things follow from that which callers
should not be surprised by:

  - A table created by CTAS renders as an explicit-column CREATE TABLE. The
    defining query was never kept (unlike a materialized view's), so there is
    nothing else it could render as.
  - A column DEFAULT is never emitted. `ALTER TABLE ... ADD COLUMN ... DEFAULT`
    is a backfill value written into the rows that already existed, not stored
    state a later INSERT consults - so there is no default to show, and
    emitting one would assert a constraint the engine does not have.

Clustering is the reason this returns a SCRIPT rather than a statement:
`CREATE TABLE` has no CLUSTER BY clause to put it in (sqlparser gates that
grammar on the BigQuery and Generic dialects by concrete type, with no dialect
hook to opt into), so a clustered table recreates as a CREATE followed by an
ALTER. Constraints need no such split - CREATE TABLE takes them.
"""

from typing import Dict, List, Optional

# Quoting is by exception, not by default: a bare identifier reads better and is
# what the vast majority of names are. A name that would not survive a re-parse
# as a bare identifier is double-quoted, with embedded quotes doubled.
_BARE_IDENTIFIER_EXTRA = "_"


def _needs_quoting(name: str) -> bool:
    if not name:
        return True
    first = name[0]
    if not (first.isalpha() or first in _BARE_IDENTIFIER_EXTRA):
        return True
    return not all(char.isalnum() or char in _BARE_IDENTIFIER_EXTRA for char in name)


def quote_identifier(name: str) -> str:
    """A name, in the spelling that re-parses to the same name."""
    if _needs_quoting(name):
        return '"' + name.replace('"', '""') + '"'
    return name


def quote_qualified_name(name: str) -> str:
    """A dotted name, quoting each part that needs it.

    The parts are what the parser tokenised, so a name is split on dots the way
    it was written. A dataset name may itself contain a dot, which is why this
    is only ever used on names that arrived already split, or on names the
    reader typed.
    """
    return ".".join(quote_identifier(part) for part in name.split("."))


def _render_column(column) -> str:
    """`name TYPE [NOT NULL]` - the whole of what a column definition carries.

    The type is `str(ColumnType)`, which is draken's spelling and the same one
    a CAST target parses from (see logical_type.ColumnType.__str__): the two
    directions are pinned against each other by tests, so what is rendered here
    parses back to the type it came from.
    """
    rendered = f"  {quote_identifier(column.name)} {column.column_type}"
    if not column.nullable:
        rendered += " NOT NULL"
    return rendered


def _render_constraint(relationship: Dict) -> str:
    """An informational foreign key, in the spelling CREATE TABLE accepts.

    NOT ENFORCED is written out because it has to be: a bare FOREIGN KEY is an
    enforcing one, and the engine would refuse to re-parse this without it.
    """
    near = quote_identifier(relationship["column_name"])
    far_relation = ".".join(
        quote_identifier(part) for part in relationship["references_relation_parts"]
    )
    far = quote_identifier(relationship["references_column_name"])
    return (
        f"  CONSTRAINT {quote_identifier(relationship['constraint_name'])} "
        f"FOREIGN KEY ({near}) REFERENCES {far_relation} ({far}) NOT ENFORCED"
    )


def render_create_table(
    relation_name: str,
    schema,
    relationships: Optional[List[Dict]] = None,
    cluster_columns: Optional[List[str]] = None,
) -> str:
    """The DDL that recreates a table's shape.

    One statement, or two when the table is clustered - see the module docstring
    for why clustering cannot ride along in the CREATE.
    """
    name = quote_qualified_name(relation_name)

    body = [_render_column(column) for column in schema.columns]
    body.extend(_render_constraint(relationship) for relationship in relationships or [])

    statements = ["CREATE TABLE {} (\n{}\n)".format(name, ",\n".join(body))]

    if cluster_columns:
        columns = ", ".join(quote_identifier(column) for column in cluster_columns)
        statements.append(f"ALTER TABLE {name} CLUSTER BY ({columns})")

    return ";\n\n".join(statements) + ";"


def render_create_view(view_name: str, statement: str) -> str:
    """The stored view body, in the statement that defines it.

    The body is rendered back verbatim rather than re-parsed and re-printed: it
    is what the author wrote, and normalising it would change a definition
    nobody asked to change.
    """
    return f"CREATE VIEW {quote_qualified_name(view_name)} AS\n{statement.strip().rstrip(';')};"


def render_create_materialized_view(relation_name: str, statement: str) -> str:
    """The stored defining SELECT, in the statement that defines it.

    Unlike a table's, this really was kept: CREATE MATERIALIZED VIEW is CTAS
    plus registration, and the registration records the defining query so a
    refresh can re-run it.
    """
    return (
        f"CREATE MATERIALIZED VIEW {quote_qualified_name(relation_name)} AS\n"
        f"{statement.strip().rstrip(';')};"
    )


def render_create_task(task_name: str, statement: str) -> str:
    """The statement the task runs.

    The `ON <table>` clause is NOT rendered, and its absence is not a loss of
    fidelity: it never belonged to the task. It creates a TRIGGER alongside it,
    and the triggers on a table are shown by SHOW TRIGGERS FOR - so putting one
    here would either duplicate that or, when several tables trigger one task,
    have no single answer to give.
    """
    return f"CREATE TASK {quote_qualified_name(task_name)} AS\n{statement.strip().rstrip(';')};"
