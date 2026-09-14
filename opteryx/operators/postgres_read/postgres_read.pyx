# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Postgres Read Node — the physical operator for a scan of a PostgreSQL-bound
relation.

The node is a passive attribute bag, like SkeneReadNode: the plan compiler
(opteryx/managers/execution/compiler.py::_compile_postgres_scan) reads the
scan's projection, pushed predicates, pushed LIMIT, top-N spec, absorbed
aggregate or DISTINCT off it, builds the statement, pins everything in a PostgresScanPlan and hands the engine a
NativePostgresScanSource. There is NO Python read path: read_morsels() raises,
because a scan that reached it would mean the compiler routed a Postgres scan
somewhere other than the native Source.

This file is textually included by opteryx/operators/_operators.pyx, so the
Cython externs for the native client (PgConfig, pg_describe, ...) declared
there are in scope. The plan-time helpers at the bottom are what
opteryx/connectors/postgres_connector.py calls to bind a relation: describe a
statement's result columns, run a small metadata query, map an OID to a
Draken type. They are the only Python-facing surface of the client.
"""

# BasePlanNode/ReaderNode/QueryProperties in scope via _operators.pyx include.


cdef class PostgresReadNode(ReaderNode):
    """Read node for PostgreSQL-bound relations, served by NativePostgresScanSource."""

    # The PostgresScanPlan the compiler built for this scan. Held so that the
    # rows the server sent (spec.rows_read) can be read back after the run.
    cdef public object scan_plan
    # Pushed shapes the optimizer stamped on the logical Scan (each None when
    # not pushed). The compiler renders them into the statement; the node only
    # carries them. `topn_order_by` is [(schema_column, ascending), ...];
    # `pushed_groups` / `pushed_aggregates` the GROUP BY keys and AGGREGATOR
    # nodes of an absorbed Aggregate; `pushed_distinct` True for an absorbed
    # DISTINCT over the projection.
    cdef public object topn_order_by
    cdef public object topn_limit
    cdef public object pushed_groups
    cdef public object pushed_aggregates
    cdef public bint pushed_distinct

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.scan_plan = None
        self.topn_order_by = parameters.get("topn_order_by")
        self.topn_limit = parameters.get("topn_limit")
        self.pushed_groups = parameters.get("pushed_groups")
        self.pushed_aggregates = parameters.get("pushed_aggregates")
        self.pushed_distinct = bool(parameters.get("pushed_distinct", False))

    @property
    def name(self) -> str:  # pragma: no cover
        return "Postgres Reader"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.relation}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    def read_morsels(self):
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            "PostgresReadNode executes only as a native engine source "
            "(NativePostgresScanSource); it has no Python read path."
        )


# ---------------------------------------------------------------------------
# Plan-time helpers (GIL held by the caller; the wire I/O runs nogil)
# ---------------------------------------------------------------------------

def pg_describe_statement(dict config, str sql):
    """Result columns of `sql` without executing it: [(name, oid, typmod), ...].

    Raises RuntimeError carrying the server's message (SQLSTATE in the text)
    when the statement cannot be prepared — a missing relation, a permission
    refusal, a syntax error. The connector re-raises it as the engine's own
    error type."""
    cdef PgConfig cfg
    _pg_config_from_dict(&cfg, config)
    cdef string statement = sql.encode("utf-8")
    cdef cppvector[PgField] fields
    with nogil:
        fields = pg_describe(cfg, statement)
    return [(f.name.decode("utf-8"), int(f.oid), int(f.typmod)) for f in fields]


def pg_query_text_rows(dict config, str sql, list params):
    """Run a small metadata statement with text-format results and return every
    row as a list of str/None. Bind values are passed as text parameters
    ($1, $2, ...) — never interpolated."""
    cdef PgConfig cfg
    _pg_config_from_dict(&cfg, config)
    cdef string statement = sql.encode("utf-8")
    cdef cppvector[string] binds
    for value in params:
        binds.push_back((<str>value).encode("utf-8"))
    cdef cppvector[cppvector[optional[string]]] rows
    with nogil:
        rows = pg_query_text(cfg, statement, binds)
    out = []
    for row in rows:
        cells = []
        for cell in row:
            if cell.has_value():
                cells.append(cell.value().decode("utf-8"))
            else:
                cells.append(None)
        out.append(cells)
    return out


def pg_draken_type_for_oid(int oid):
    """The DrakenType tag (as int) a Postgres type OID decodes to; DRAKEN_NULL (101)
    means the type is not supported. Single source of truth shared with the
    execution decoder (src/cpp/pg/pg_client.cpp::pg_oid_to_draken)."""
    return pg_oid_to_draken(<uint32_t>oid)


def pg_type_name_for_oid(int oid):
    """Readable Postgres type name for a refused OID (for error messages)."""
    return pg_oid_name(<uint32_t>oid).decode("utf-8")
