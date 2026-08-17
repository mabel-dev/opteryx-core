"""
Generate the TPC-DS query set from DuckDB's `tpcds_queries()` table function
(the official TPC-DS query templates, one resolved variant per query number,
vendored inside the `tpcds` extension).

DEV TOOLING — not packaged, not run at test time. DuckDB's templates use bare
table names (`FROM store_sales, date_dim ...`); this rewrites every genuine
table reference to carry the `testdata.tpcds_tiny.` placeholder prefix,
matching the convention tests/performance/tpch/opteryx/queries/*.sql uses
(`testdata.tpch_tiny.`) — the runner replaces the placeholder with the real
dataset path at load time.

Table references are found via `json_serialize_sql()` — DuckDB's own parse of
the query — not by regexing for `\\btable_name\\b`. A blind word-boundary
regex once shipped here and it was wrong: several TPC-DS templates reuse a
table's name as a computed column's alias (Q31: `sum(...) AS store_sales`),
and the regex prefixed the alias too, producing invalid SQL
(`AS testdata.tpcds_tiny.store_sales`) — Q31/Q49/Q51 failed to parse for this
reason, not because of anything Opteryx did. `json_serialize_sql()`'s AST
distinguishes a `BASE_TABLE` node (a real table reference, tagged with the
exact source offset via `query_location`) from a `FUNCTION` alias or a bare
column reference, so only genuine table references get rewritten. CTE names
parse as `BASE_TABLE` too (DuckDB resolves CTE-vs-table at bind time, not
parse time) — filtering to the 24 known table names excludes them; confirmed
no TPC-DS query defines a CTE that collides with a real table name.

A handful of templates also lean on syntax DuckDB (and the TPC-DS spec's
assumed reference engine) accepts but Opteryx does not — a bare string
literal on one side of a DATE comparison, `CAST(x AS INT)` where Opteryx only
accepts the canonical `INT64` spelling, or an unqualified `*` sharing a
select list with other columns.
These aren't table-reference bugs; they're genuine dialect gaps in the
generated SQL, patched here (not by hand-editing the .sql files) so a
regeneration doesn't silently drop the fix. Each patch is a literal
find/replace checked against an exact expected occurrence count — if a
regenerated query's text no longer matches, the patch fails loudly instead of
silently no-op'ing.

Usage:
    python dev/tpcds/generate_queries.py
"""

from __future__ import annotations

import json
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb  # dev/test query source only — never imported by production code

_TABLES = {
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
}

_PREFIX = "testdata.tpcds_tiny."
_QUERY_DIR = os.path.join(_REPO_ROOT, "tests", "performance", "tpcds", "opteryx", "queries")

# query_nr -> [(find, replace, expected_count), ...], applied after table
# prefixing. See the module docstring for why these exist.
_DIALECT_PATCHES: dict[int, list[tuple[str, str, int]]] = {
    16: [
        (
            "d_date BETWEEN '2002-02-01' AND cast('2002-04-02' AS date)",
            "d_date BETWEEN cast('2002-02-01' AS date) AND cast('2002-04-02' AS date)",
            1,
        ),
    ],
    # Q23/Q62/Q99 all write the same shape — `SELECT <expr> alias, *` inside a
    # derived table. Two separate Opteryx restrictions apply, so the rewrite is a
    # REORDER, not just a qualifier:
    #   1. An UNQUALIFIED `*` may not share a select list with other items. This
    #      is what the SQL standard says (`<select list> ::= <asterisk> |
    #      <select sublist>...`, where only a QUALIFIED asterisk is a sublist),
    #      even though most engines accept the unqualified mix.
    #   2. A qualified `table.*` must be the FIRST item in the projection.
    # Reordering is safe here: all three derived tables are aliased and every
    # downstream reference is by column NAME, never by position.
    23: [
        (
            "SELECT SUBSTRING(i_item_desc, 1, 30) itemdesc,\n             *\n"
            "      FROM testdata.tpcds_tiny.item",
            "SELECT testdata.tpcds_tiny.item.*,\n             "
            "SUBSTRING(i_item_desc, 1, 30) itemdesc\n"
            "      FROM testdata.tpcds_tiny.item",
            1,
        ),
    ],
    32: [
        (
            "d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)",
            "d_date BETWEEN cast('2000-01-27' AS date) AND cast('2000-04-26' AS date)",
            2,
        ),
    ],
    54: [
        ("cast(round(revenue/50) AS int)", "cast(round(revenue/50) AS int64)", 1),
    ],
    58: [
        ("d_date = '2000-01-03'", "d_date = cast('2000-01-03' AS date)", 3),
    ],
    # See the Q23 comment above for why this is a reorder rather than a bare qualifier.
    62: [
        (
            "SELECT SUBSTRING(w_warehouse_name,1,20) w_substr,\n          *\n"
            "   FROM testdata.tpcds_tiny.warehouse",
            "SELECT testdata.tpcds_tiny.warehouse.*,\n          "
            "SUBSTRING(w_warehouse_name,1,20) w_substr\n"
            "   FROM testdata.tpcds_tiny.warehouse",
            1,
        ),
    ],
    72: [
        ("d3.d_date > d1.d_date + 5", "d3.d_date > d1.d_date + INTERVAL '5' DAY", 1),
    ],
    83: [
        (
            "('2000-06-30',\n                              '2000-09-27',\n                              '2000-11-17')",
            "(cast('2000-06-30' AS date),\n                              cast('2000-09-27' AS date),\n                              cast('2000-11-17' AS date))",
            3,
        ),
    ],
    92: [
        (
            "d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)",
            "d_date BETWEEN cast('2000-01-27' AS date) AND cast('2000-04-26' AS date)",
            2,
        ),
    ],
    94: [
        (
            "d_date BETWEEN '1999-02-01' AND cast('1999-04-02' AS date)",
            "d_date BETWEEN cast('1999-02-01' AS date) AND cast('1999-04-02' AS date)",
            1,
        ),
    ],
    # See the Q23 comment above for why this is a reorder rather than a bare qualifier.
    99: [
        (
            "SELECT SUBSTRING(w_warehouse_name,1,20) w_substr, *\n"
            "   FROM testdata.tpcds_tiny.warehouse",
            "SELECT testdata.tpcds_tiny.warehouse.*, "
            "SUBSTRING(w_warehouse_name,1,20) w_substr\n"
            "   FROM testdata.tpcds_tiny.warehouse",
            1,
        ),
    ],
    95: [
        (
            "d_date BETWEEN '1999-02-01' AND cast('1999-04-02' AS date)",
            "d_date BETWEEN cast('1999-02-01' AS date) AND cast('1999-04-02' AS date)",
            1,
        ),
    ],
}


def _table_ref_locations(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[int, str]]:
    """[(byte_offset, table_name), ...] for genuine table references, via
    DuckDB's own parse — not a text-level guess. Raises if DuckDB can't parse
    the query (it generated it, so that would mean tpcds_queries() changed
    shape upstream).

    Two node kinds carry a table name that needs the prefix:
      - BASE_TABLE: the FROM-clause declaration itself.
      - COLUMN_REF with >1 `column_names` entries, where the first entry is
        one of the 24 known tables: an unaliased table's columns are
        qualified by the table's own bare name elsewhere in the query (e.g.
        `WHERE store_sales.ss_sold_date_sk = ...`), and once the FROM-clause
        declaration gets prefixed, that qualifier has to match it or the
        binder can't resolve it. Aliased tables never hit this — an alias's
        column refs are qualified by the alias, which isn't in `_TABLES`...
        except when the alias IS one of the 24 table names. Q49 aliases a
        derived table `AS store` and then writes `store.return_rank` — a
        reference to THAT alias, not the real `store` table (which this
        query never scans at all) — but `names[0] in _TABLES` alone can't
        tell the two apart. Collecting every SUBQUERY alias in the query
        first and excluding those from the COLUMN_REF check is the same
        alias-vs-table-name collision this function already has to guard
        (see the Q31 case in the module docstring), just for a SUBQUERY
        alias instead of a computed-expression alias. Folded case: SQL
        identifiers are case-insensitive, and Q49's alias is `AS CATALOG`
        (see query 49's catalog leg) while the collision it needs to avoid
        for `store` happens to match case exactly — this must not depend on
        that coincidence.
    """
    raw = con.execute("SELECT json_serialize_sql($sql)", {"sql": sql}).fetchone()[0]
    data = json.loads(raw)
    if data.get("error"):
        raise ValueError(f"DuckDB could not parse its own generated query: {data.get('error_message')}")

    subquery_aliases: set[str] = set()

    def collect_subquery_aliases(node) -> None:
        if isinstance(node, dict):
            if node.get("type") == "SUBQUERY" and node.get("alias"):
                subquery_aliases.add(node["alias"].lower())
            for v in node.values():
                collect_subquery_aliases(v)
        elif isinstance(node, list):
            for v in node:
                collect_subquery_aliases(v)

    collect_subquery_aliases(data)

    found: list[tuple[int, str]] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            if node.get("type") == "BASE_TABLE" and node.get("table_name") in _TABLES:
                found.append((node["query_location"], node["table_name"]))
            elif node.get("class") == "COLUMN_REF":
                names = node.get("column_names") or []
                if (
                    len(names) > 1
                    and names[0] in _TABLES
                    and names[0].lower() not in subquery_aliases
                ):
                    found.append((node["query_location"], names[0]))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)
    return found


def _prefix_tables(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    locations = _table_ref_locations(con, sql)
    # Insert back-to-front so earlier offsets stay valid as we mutate.
    for offset, name in sorted(locations, reverse=True):
        assert sql[offset : offset + len(name)] == name, (
            f"query_location {offset} does not point at {name!r} — DuckDB's offsets "
            "are byte-based; a non-ASCII query would break this assumption (none do today)"
        )
        sql = sql[:offset] + _PREFIX + sql[offset:]
    return sql


def _apply_dialect_patches(query_nr: int, sql: str) -> str:
    for find, replace, expected_count in _DIALECT_PATCHES.get(query_nr, []):
        actual_count = sql.count(find)
        if actual_count != expected_count:
            raise ValueError(
                f"Q{query_nr:02d}: dialect patch expected {expected_count} occurrence(s) of "
                f"{find!r}, found {actual_count} — tpcds_queries() text changed upstream, patch needs review"
            )
        sql = sql.replace(find, replace)
    return sql


def main() -> int:
    con = duckdb.connect()
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")

    rows = con.execute("SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr").fetchall()
    if len(rows) != 99:
        print(f"ERROR: expected 99 queries from tpcds_queries(), got {len(rows)}")
        return 1

    os.makedirs(_QUERY_DIR, exist_ok=True)
    for query_nr, query in rows:
        body = _prefix_tables(con, query.strip())
        body = _apply_dialect_patches(query_nr, body)
        if not body.endswith(";"):
            body += ";"
        path = os.path.join(_QUERY_DIR, f"query{query_nr:02d}.sql")
        with open(path, "w") as f:
            f.write(body + "\n")

    print(f"Wrote {len(rows)} query files to {os.path.relpath(_QUERY_DIR, _REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
