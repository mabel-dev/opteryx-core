# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
PostgreSQL connector — a PostgreSQL server as a workspace's data source.

This is a CONNECTOR, not a catalog. The catalogs (native, iceberg, mabel) answer
"which files make up this relation" and the engine reads those files itself;
a PostgreSQL server holds the data AND executes the read. So the resolution
chain binds a workspace to this gateway directly (`Resolution(PostgresConnector,
config={...})`), the way `create_gcs_mabel_connector` is bound, and there is no
`catalog=` factory.

Plan time (this module, Python): resolve `<workspace>.<schema>.<table>` to a
relation, describe its result columns through the native client (a Parse +
Describe round trip — the exact OIDs and typmods the scan will stream), map
them to `ColumnType`, read the row count from wherever the declared `dialect`
keeps it (PostgreSQL's `pg_class.reltuples`, CockroachDB's `SHOW STATISTICS`),
and translate the optimizer's pushed predicates / LIMIT / top-N / aggregate /
DISTINCT into the scan statement with `$n` bind parameters (never interpolated
literals).

A predicate does NOT arrive spelled the way it was written: PredicateRewriteStrategy
lowers `col LIKE 'x%'` to `_STARTS_WITH(col, b'x')`, `'%x'` to `_ENDS_WITH`, `'%x%'`
to an `InStr` comparison, and negations of those to a NOT wrapping one. The renderer
recognises each and spells it back as `col LIKE $1` with the pattern escaped, because
the alternative is a table streamed in full to be filtered locally. `IN (...)` is
pushed as `col IN ($1, $2, ...)` from the one literal node holding the members. The
case-insensitive lowerings (ILIKE's `_CI_*` / `IInStr`) are NOT pushed: the server
folds case by its locale and the engine folds it its own way.

Every pushed shape is rendered with the ENGINE's semantics spelled out where
PostgreSQL's defaults differ: NULLS FIRST under ASC and NULLS LAST under DESC
(draken sorts NULL below every value), `COLLATE "C"` on text sort keys and text
MIN/MAX (draken compares bytes; the server would use the column's collation),
and an explicit cast wherever the server's result type is not the type the
binder bound the aggregate to. A shape that cannot be spelled that way is
DECLINED by `can_push_*` and stays a local operator — never mistranslated.

Listing (`Summarisable`): `information_schema.tables` derives its metadata
columns from a dataset document's snapshot, and a workspace bound here has
neither, so they all read NULL. `relation_summaries()` answers them from the
server instead - `pg_class.reltuples`, `pg_total_relation_size`, and the
CLUSTER order - in one statement per schema. See that method for what is
deliberately left unanswered, and why.

Execution (native): `PostgresReadNode` -> `NativePostgresScanSource`
(src/cpp/engine/native_postgres_scan_source.hpp) streams the server's BINARY
rows into morsels on a worker thread. Nothing in this module runs during
execution; `read_dataset` raises.

What is refused, loudly, at bind time:
  * a column whose type has no Draken mapping (interval, arrays, ranges, ...);
  * a `numeric` column with no declared precision/scale — the scan's type must
    be fixed before any row is seen, and an undeclared numeric has no scale;
  * `FOR ... AS OF` / `VERSION AS OF` — no snapshots to travel to;
  * every DDL/DML statement — the gateway is not `Writable`.

Case: PostgreSQL folds unquoted identifiers to lower case and Opteryx lowercases
relation names at bind, so the default lookup lowercases too. A binding that
sets `preserve_sql_case` uses the relation name exactly as typed, for schemas
whose objects were created with quoted mixed-case names.
"""

import decimal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.connectors.capabilities.aggregate_pushable import AggregatePushable
from opteryx.connectors.capabilities.distinct_pushable import DistinctPushable
from opteryx.connectors.capabilities.limit_pushable import LimitPushable
from opteryx.connectors.capabilities.predicate_pushable import PredicatePushable
from opteryx.connectors.capabilities.summarisable import RelationSummary
from opteryx.connectors.capabilities.summarisable import Summarisable
from opteryx.connectors.capabilities.topn_pushable import TopNPushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import QueryTelemetry
from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import ColumnType, DrakenType, LogicalCategory, LogicalKind
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

# Rows per morsel the native Source cuts the server stream into. One morsel of
# this size holds a few megabytes for a typical row width; the stream is
# consumed incrementally so memory is O(morsels in flight), not O(table).
POSTGRES_SCAN_BATCH_ROWS = 65536

# Postgres type OIDs the binder treats specially (everything else maps through
# the client's single OID table, pg_draken_type_for_oid).
_OID_NUMERIC = 1700
_OID_TIMESTAMP = 1114
_OID_TIMESTAMPTZ = 1184
_DRAKEN_NULL_TAG = 101  # DRAKEN_NULL: "no mapping" from pg_draken_type_for_oid

# SQLSTATEs that mean "that relation is not there", raised as not-found rather
# than as a read error so the caller gets the dataset-not-found contract.
_SQLSTATE_UNDEFINED_TABLE = "42P01"
_SQLSTATE_INVALID_SCHEMA = "3F000"

# Wire-compatible servers that are NOT PostgreSQL keep their statistics
# somewhere else, so the row-count statement is chosen by the binding, not
# sniffed from `version()` at plan time. A wrong guess here is not a wrong
# answer, it is a silently fabricated estimate - which is exactly what this
# dialect key exists to stop - so it is DECLARED, and an unknown spelling is
# refused rather than defaulted.
DIALECT_POSTGRES = "postgres"
DIALECT_COCKROACH = "cockroach"
_DIALECTS = (DIALECT_POSTGRES, DIALECT_COCKROACH)

_TYPE_BY_PHYSICAL: Dict[int, ColumnType] = {
    column_type.physical.value: column_type
    for column_type in (
        _lt.INT16,
        _lt.INT32,
        _lt.INT64,
        _lt.UINT32,
        _lt.FLOAT32,
        _lt.FLOAT64,
        _lt.BOOLEAN,
        _lt.DATE,
        _lt.VARCHAR,
        _lt.VARBINARY,
        _lt.VARIANT,
    )
}


def _quote_identifier(name: str) -> str:
    """Double-quote a PostgreSQL identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def _pg_helpers():
    # The native client lives in the engine's compiled module; imported here
    # rather than at module top so importing the connector package never pulls
    # the whole engine in.
    from opteryx.operators._operators import pg_describe_statement
    from opteryx.operators._operators import pg_draken_type_for_oid
    from opteryx.operators._operators import pg_query_text_rows
    from opteryx.operators._operators import pg_type_name_for_oid

    return pg_describe_statement, pg_query_text_rows, pg_draken_type_for_oid, pg_type_name_for_oid


def _sqlstate(error: RuntimeError) -> str:
    """The SQLSTATE the native client embeds as `[XXXXX]` in a server error."""
    text = str(error)
    start = text.find("[")
    end = text.find("]", start + 1)
    if start < 0 or end < 0 or end - start != 6:
        return ""
    return text[start + 1 : end]


def _positive_int(text: Optional[str], *, allow_zero: bool = False) -> Optional[int]:
    """A numeric column of the summary statement as an int, or None.

    The native client hands every value back as text. A server value of NULL,
    an unparseable one, and - for counts - a non-positive one all mean the same
    thing to a summary: the server did not say. `reltuples` is -1 when a
    relation has never been analysed and 0 when it has no estimate, and neither
    is the claim "this relation is empty". A SIZE of zero is a real measurement
    (a freshly truncated or partitioned-parent relation genuinely occupies
    nothing), so it is kept.
    """
    if text is None:
        return None
    try:
        value = int(float(text))
    except (TypeError, ValueError):
        return None
    if value < 0 or (value == 0 and not allow_zero):
        return None
    return value


class PostgresConnector(BaseConnector, Summarisable):
    """Long-lived gateway for one PostgreSQL database.

    Cached by the resolution chain per workspace; creates a transient
    `PostgresTable` per query via `table_engine()`. Connections are pooled
    inside the native client, keyed by the connection config.
    """

    __mode__ = "Sql"
    __type__ = "POSTGRES"

    supports_predicate_pushdown = True
    supports_limit_pushdown = True
    supports_filtered_limit_pushdown = True
    supports_topn_pushdown = True
    supports_aggregate_pushdown = True
    supports_distinct_pushdown = True
    # table_engine() needs the relation name as typed, for preserve_sql_case.
    requires_original_case = True

    def __init__(
        self,
        *,
        host: str,
        dbname: str,
        user: str,
        password: str,
        port: int = 5432,
        sslmode: str = "require",
        schema: str = "public",
        timeout_s: int = 30,
        preserve_sql_case: bool = False,
        dialect: str = DIALECT_POSTGRES,
        gcs_bucket: Optional[str] = None,
        telemetry: Optional[QueryTelemetry] = None,
        prefix: Optional[str] = None,
        **kwargs,
    ) -> None:
        if kwargs:
            raise ValueError(
                f"PostgresConnector: unknown configuration keys {sorted(kwargs)}; "
                "expected host, port, dbname, user, password, sslmode, schema, timeout_s, "
                "preserve_sql_case, dialect, gcs_bucket"
            )
        for label, value in (("host", host), ("dbname", dbname), ("user", user)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"PostgresConnector: '{label}' must be a non-empty string")
        if not isinstance(password, str):
            raise ValueError("PostgresConnector: 'password' must be a string")
        if sslmode not in ("disable", "require", "verify-full"):
            raise ValueError(
                f"PostgresConnector: sslmode must be disable, require or verify-full (got '{sslmode}')"
            )
        if dialect not in _DIALECTS:
            raise ValueError(
                f"PostgresConnector: dialect must be one of {', '.join(_DIALECTS)} (got '{dialect}')"
            )
        self.connection_config: Dict[str, Any] = {
            "host": host,
            "port": int(port),
            "dbname": dbname,
            "user": user,
            "password": password,
            "sslmode": sslmode,
            "timeout_s": int(timeout_s),
        }
        self.default_schema = schema
        self.preserve_sql_case = bool(preserve_sql_case)
        # Which server this binding actually talks to. Governs where the row
        # count is read from - see `PostgresTable._row_estimate`.
        self.dialect = dialect
        # Where this deployment keeps workspace metadata. Not part of the
        # customer's binding - the deployment's resolver supplies it, the same
        # value it hands a native workspace - and its absence simply means no
        # statistics manifest is read.
        self.gcs_bucket = gcs_bucket
        self.telemetry = telemetry
        # connector_factory overwrites this with the resolved workspace/prefix
        # after construction; a direct construction keeps what it was given.
        self._matched_prefix = prefix

    def __repr__(self) -> str:  # never the password
        cfg = self.connection_config
        return f"PostgresConnector(host={cfg['host']!r}, port={cfg['port']}, dbname={cfg['dbname']!r}, user={cfg['user']!r})"

    def split_relation(self, name: str, original: Optional[str] = None) -> Tuple[str, str]:
        """`<workspace>.<schema>.<table>` or `<workspace>.<table>` -> (schema, table).

        The workspace segment is the prefix this gateway was resolved for and
        is stripped when present. Lower-cased unless the binding preserves case,
        in which case the name as typed is used."""
        source = original if (self.preserve_sql_case and original) else name
        parts = source.split(".")
        prefix = self._matched_prefix
        if prefix and parts and parts[0].lower() == prefix.lower():
            parts = parts[1:]
        if len(parts) == 1:
            return self.default_schema, parts[0]
        if len(parts) == 2:
            return parts[0], parts[1]
        raise UnsupportedSyntaxError(
            f"'{name}' is not a PostgreSQL relation name; expected "
            "<workspace>.<schema>.<table> or <workspace>.<table>"
        )

    def locate_object(self, name: str) -> Tuple[Optional[TableType], Any]:
        """Table (or view: read the same way) if the relation exists, else (None, None)."""
        _, query_text, _, _ = _pg_helpers()
        schema, table = self.split_relation(name)
        rows = query_text(
            self.connection_config,
            "SELECT c.relkind::text FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2 "
            "AND c.relkind IN ('r', 'p', 'v', 'm', 'f')",
            [schema, table],
        )
        if not rows:
            return None, None
        return TableType.Table, {"relkind": rows[0][0], "schema": schema, "table": table}

    def table_engine(self, name: str, **kwargs):
        return PostgresTable(dataset=name, gateway=self, **kwargs)

    def relation_summaries(
        self, schema_name: str, relation_names: Sequence[str]
    ) -> Dict[str, RelationSummary]:
        """The `Summarisable` contract - one statement for a whole schema.

        `information_schema.tables` derives its metadata columns from a dataset
        document's snapshot, and a workspace bound here has neither, so every
        one of them read NULL. These come from the server that owns the
        relations instead, all of them in a SINGLE `pg_class` scan however many
        relations the schema holds: the requested names are filtered in Python,
        because the alternative - a statement per relation - is the cost this
        capability exists to avoid.

        What each column is taken from, and what is deliberately NOT:

        * rows - `pg_class.reltuples`, the planner's own estimate and the same
          number `PostgresTable._row_estimate` plans with. It is flagged as an
          ESTIMATE on the way out: the native path's record count is an exact
          committed total, and the two land in the same column. `-1` (never
          analysed) and `0` are both 'unknown' here, exactly as they are there.
        * bytes - `pg_total_relation_size`, which is the relation plus its
          indexes, TOAST and free space map: the server's notion of what the
          relation occupies, not a byte count of rows a query would read. Asked
          only of relations that have storage; a view or a foreign table has
          none, and CASE keeps the function from being called on them at all.
          A PARTITIONED parent (relkind 'p') reports its own size, which is
          zero - the partitions hold the data and are relations in their own
          right.
        * sort order - the CLUSTER order (`pg_index.indisclustered`), rendered
          as `<column> ASC|DESC` the way the native column renders a sort key.
          This engine does not own the relation, so 'sort order' can only mean
          what the server itself has been told to keep it in, and CLUSTER is
          the only thing that means that. A primary key or an arbitrary btree
          would have been available on nearly every table and would have said
          something that is not true: an index is an access path, not an
          ordering of the heap. Most tables have never been CLUSTERed and
          correctly report nothing. An expression index is skipped (its
          `indkey[0]` is 0, which names no attribute).
        * updated_at - LEFT NULL. PostgreSQL keeps no per-relation modification
          time. `pg_stat_user_tables.last_autoanalyze` is the usual stand-in
          and it answers a different question - when the statistics collector
          last ran, which moves without the data changing and stays still while
          it does - and the statistics views reset with the server. A column
          labelled "table updated at" holding that would be read as fact.
        * snapshot id / sequence / file count - not fields on RelationSummary
          at all; they describe a snapshot store. See the capability module.

        Only the `postgres` dialect is answered. CockroachDB keeps no
        PostgreSQL-shaped catalogue statistics (`reltuples` is NULL there for
        every relation - the same reason `_row_estimate` reads SHOW STATISTICS
        for it), so a wire-compatible server would return a table of zeroes and
        nulls that LOOK like measurements. Nothing is claimed for it instead,
        per this connector's rule that a statistic is declared, never guessed.

        A server that cannot be reached, or refuses the read, yields an empty
        mapping: the caller then renders these columns exactly as it did
        before this existed. Listing what a workspace contains must not fail
        because the workspace's server is down.
        """
        if self.dialect != DIALECT_POSTGRES:
            return {}

        wanted = {name.lower(): name for name in relation_names}
        if not wanted:
            return {}

        _, query_text, _, _ = _pg_helpers()
        try:
            rows = query_text(
                self.connection_config,
                "SELECT c.relname, "
                "       c.reltuples::float8::text, "
                "       CASE WHEN c.relkind IN ('r', 'p', 'm') "
                "            THEN pg_catalog.pg_total_relation_size(c.oid)::text END, "
                "       (SELECT a.attname || CASE WHEN (i.indoption[0] & 1) = 1 "
                "                                 THEN ' DESC' ELSE ' ASC' END "
                "          FROM pg_catalog.pg_index i "
                "          JOIN pg_catalog.pg_attribute a "
                "            ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] "
                "         WHERE i.indrelid = c.oid AND i.indisclustered "
                "         LIMIT 1) "
                "  FROM pg_catalog.pg_class c "
                "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                " WHERE n.nspname = $1 "
                "   AND c.relkind IN ('r', 'p', 'v', 'm', 'f')",
                [schema_name],
            )
        except RuntimeError:
            return {}

        summaries: Dict[str, RelationSummary] = {}
        for relname, reltuples, total_bytes, sort_order in rows:
            name = wanted.get((relname or "").lower())
            if name is None:
                continue
            summaries[name] = RelationSummary(
                record_count=_positive_int(reltuples),
                record_count_is_estimate=True,
                byte_count=_positive_int(total_bytes, allow_zero=True),
                sort_order=sort_order or None,
            )
        return summaries

    def stats_manifest_path(self, schema_name: str, table_name: str) -> Optional[str]:
        """Where this relation's statistics manifest lives, or None if unconfigured.

        The SAME location an Opteryx-backed relation's manifests use - the
        formula in the catalog's `_dataset_location` plus `metadata/` - so
        one storage layout serves both and the tooling that walks a workspace's
        metadata needs no special case. There are no snapshots here, so the file
        is named for the dataset rather than a snapshot id, and each refresh
        rewrites that one file.
        """
        if not self.gcs_bucket or not self._matched_prefix:
            return None
        return (
            f"gs://{self.gcs_bucket}/{self._matched_prefix}/{schema_name}/{table_name}"
            f"/metadata/manifest-{table_name}.parquet"
        )


class PostgresTable(
    BaseTable, PredicatePushable, LimitPushable, TopNPushable, AggregatePushable, DistinctPushable
):
    """Transient, per-query reader description for one PostgreSQL relation.

    Plan-time only: describes the relation, answers pushdown questions and
    builds the scan statement. The rows are read by NativePostgresScanSource.
    """

    __mode__ = "Sql"
    __type__ = "POSTGRES"
    __synchronousity__ = "synchronous"

    # The physical scan node that serves this reader (physical planner dispatch
    # for manifest-less readers — see BaseTable.scan_reader).
    scan_reader = "Postgres Reader"

    supports_predicate_pushdown = True
    supports_limit_pushdown = True
    # `WHERE ... LIMIT` is one statement; the server applies them in order.
    supports_filtered_limit_pushdown = True
    # One statement, one cursor, one stream: the server's ORDER BY/GROUP BY/
    # DISTINCT is the COMPLETE answer, which is what these three promise.
    supports_topn_pushdown = True
    supports_aggregate_pushdown = True
    supports_distinct_pushdown = True

    # Translated to `column <op> $n`; each op has identical semantics on both
    # sides (Opteryx LIKE is case-sensitive, as PostgreSQL's is).
    #
    # `InStr`/`NotInStr` are what PredicateRewriteStrategy lowers an UNANCHORED
    # `LIKE '%x%'` to before this gate ever sees it, so they are the same
    # predicate as `Like`/`NotLike` and are spelled back as one. The
    # case-insensitive twins (`IInStr`/`NotIInStr`, from ILIKE) stay OFF for the
    # reason `ILike` itself is absent: the server folds case by its own locale
    # and the engine folds it its way, so the two disagree on non-ASCII text.
    PUSHABLE_OPS = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
        "Like": True,
        "NotLike": True,
        "InStr": True,
        "NotInStr": True,
        "InList": True,
        "NotInList": True,
        "Between": True,
        "IsNull": True,
        "IsNotNull": True,
        "IsEmpty": False,
        "IsNotEmpty": False,
    }
    PUSHABLE_TYPES = {
        LogicalCategory.BOOLEAN,
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.DECIMAL,
        LogicalCategory.DATE,
        LogicalCategory.TIMESTAMP,
        LogicalCategory.VARCHAR,
    }
    # A translating connector: a FUNCTION on the left of a comparison has no SQL
    # to become. Must stay off (see PredicatePushable).
    PUSHABLE_SCALAR_FUNCTIONS = False

    def __init__(
        self,
        *,
        dataset: str,
        gateway: PostgresConnector,
        telemetry: QueryTelemetry,
        original_relation: Optional[str] = None,
        **kwargs,
    ) -> None:
        BaseTable.__init__(self, dataset=dataset, config=None, telemetry=telemetry)
        LimitPushable.__init__(self)
        self.gateway = gateway
        self.connection_config = gateway.connection_config
        self.schema_name, self.table_name = gateway.split_relation(dataset, original_relation)
        # lower-cased column name -> (name as PostgreSQL reports it, type OID, typmod)
        self._meta: Dict[str, Tuple[str, int, int]] = {}
        # The catalog's record of this relation, resolved by the catalog
        # resolution step. Present for a workspace whose catalog entry has been
        # synced from the source, and then it is what the schema is built from -
        # see `get_dataset_schema`. Absent only for a connector constructed
        # directly over a server, which is what the sync itself does.
        self._catalog_record = kwargs.get("prefetched_table")
        # True once a schema has been built from the catalog rather than from
        # the server, which is what lets a wire mismatch at execution blame the
        # right thing. See `schema_from_catalog`.
        self._schema_from_catalog = False

    @property
    def qualified_name(self) -> str:
        return f"{_quote_identifier(self.schema_name)}.{_quote_identifier(self.table_name)}"

    # ---- schema ---------------------------------------------------------------

    @property
    def schema_from_catalog(self) -> bool:
        """Whether this relation's schema was read from the catalog.

        Read at execution to say WHICH record a wire mismatch contradicts - the
        catalog's, or a description taken from the server moments earlier.
        """
        return self._schema_from_catalog

    def _catalog_columns(self) -> Optional[List[dict]]:
        """This relation's columns as the catalog holds them, or None if it holds
        no description of it.

        None is NOT "the relation is empty" and must never be read as one: it is
        "this catalog entry has never been refreshed from the source", which the
        caller turns into a refusal naming the refresh.
        """
        record = self._catalog_record
        if record is None:
            return None
        metadata = getattr(record, "metadata", None)
        if metadata is None:
            return None
        columns = metadata.schema
        if not columns:
            return None
        return list(columns)

    def _schema_from_catalog_record(self, columns: List[dict]) -> RelationSchema:
        """Build the relation's schema from the catalog's record of it.

        THE CATALOG IS AUTHORITATIVE. Nothing here asks the server what its
        columns are, which is the whole point: the refresh already asked, and
        planning a statement over eight relations paid sixteen round trips to
        be told what the catalog already held.

        A column with no `remote-type` is a refusal, not something to guess at.
        The OID picks the wire decoder, and inferring one from the stored engine
        type is not possible in the direction that matters - several PostgreSQL
        types bind to one engine type, so a guess would decode some relations as
        the wrong thing rather than fail. An entry written before `remote-type`
        existed lands here, and it names the refresh that fixes it.

        `remote-type` is the OID alone. The server's typmod is NOT stored, because
        the only thing this reads it for is a NUMERIC's precision and scale, and
        those are already stored as fields of their own - so it is rebuilt from
        them rather than recorded twice and allowed to disagree.
        """
        built: List[SchemaColumn] = []
        self._meta = {}
        for column in columns:
            name = column.get("name")
            oid = column.get("remote-type")
            if name is None or oid is None:
                raise DatasetReadError(
                    f"the catalog's record of {self.qualified_name} is out of date - it does "
                    "not say what type the server holds for every column. Refresh this "
                    "workspace's catalog statistics and run the statement again."
                )
            oid = int(oid)
            typmod = -1
            if oid == _OID_NUMERIC:
                precision = column.get("precision")
                scale = column.get("scale")
                if precision is None or scale is None:
                    raise DatasetReadError(
                        f"the catalog's record of {self.qualified_name} is out of date - "
                        f"column '{name}' is numeric but its precision and scale were not "
                        "recorded. Refresh this workspace's catalog statistics and run the "
                        "statement again."
                    )
                typmod = ((int(precision) << 16) | int(scale)) + 4
            if name.lower() in self._meta:
                raise DatasetReadError(
                    f"{self.qualified_name} has two columns spelled '{name}' differing only "
                    "by case; Opteryx column names are case-insensitive"
                )
            self._meta[name.lower()] = (name, oid, typmod)
            built.append(
                SchemaColumn(
                    name=name,
                    column_type=self._column_type(name, oid, typmod),
                    identity=mint_column_identity(self.dataset, name),
                )
            )

        statistics = getattr(self._catalog_record.metadata, "statistics", None) or {}
        row_count = statistics.get("row-count")
        self._schema_from_catalog = True
        self.schema = RelationSchema(
            name=self.dataset,
            columns=built,
            row_count_estimate=None if row_count is None else int(row_count),
        )
        return self.schema

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema is not None:
            return self.schema

        # The catalog first, and not as an optimization: it is the record of
        # this relation, and it holds strictly more than a description taken
        # from the server does (the refresh's row count falls back to `count(*)`
        # where `reltuples` is -1, which is every unanalysed relation). The
        # server is asked only by a connector built without a catalog record -
        # the refresh itself, and a connector constructed directly over a
        # server.
        catalog_columns = self._catalog_columns()
        if catalog_columns is not None:
            return self._schema_from_catalog_record(catalog_columns)

        describe, query_text, _, _ = _pg_helpers()
        try:
            fields = describe(self.connection_config, f"SELECT * FROM {self.qualified_name}")
        except RuntimeError as err:
            if _sqlstate(err) in (_SQLSTATE_UNDEFINED_TABLE, _SQLSTATE_INVALID_SCHEMA):
                raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__) from err
            raise DatasetReadError(str(err)) from err

        columns: List[SchemaColumn] = []
        self._meta = {}
        for name, oid, typmod in fields:
            if name.lower() in self._meta:
                raise DatasetReadError(
                    f"{self.qualified_name} has two columns spelled '{name}' differing only "
                    "by case; Opteryx column names are case-insensitive"
                )
            self._meta[name.lower()] = (name, oid, typmod)
            columns.append(
                SchemaColumn(
                    name=name,
                    column_type=self._column_type(name, oid, typmod),
                    identity=mint_column_identity(self.dataset, name),
                )
            )

        self.schema = RelationSchema(
            name=self.dataset, columns=columns, row_count_estimate=self._row_estimate(query_text)
        )
        return self.schema

    def _column_type(self, name: str, oid: int, typmod: int) -> ColumnType:
        _, _, draken_for_oid, type_name = _pg_helpers()
        if oid == _OID_NUMERIC:
            if typmod < 4:
                raise NotSupportedError(
                    f"column '{name}' of {self.qualified_name} is numeric with no declared "
                    "precision and scale; declare it as numeric(p, s) (or expose it through "
                    "a view that casts it) so the scan has a fixed DECIMAL type"
                )
            precision = ((typmod - 4) >> 16) & 0xFFFF
            scale = (typmod - 4) & 0xFFFF
            try:
                return _lt.DECIMAL(precision, scale)
            except ValueError as err:
                raise NotSupportedError(
                    f"column '{name}' of {self.qualified_name} is numeric({precision}, {scale}); {err}"
                ) from err
        if oid in (_OID_TIMESTAMP, _OID_TIMESTAMPTZ):
            # Both land as UTC microseconds: the session zone is pinned to UTC and
            # timestamptz binary output is always UTC. A plain `timestamp` has no
            # zone; it is carried as-is, like every other zoneless timestamp.
            return _lt.TIMESTAMP()
        tag = draken_for_oid(oid)
        if tag == _DRAKEN_NULL_TAG or tag not in _TYPE_BY_PHYSICAL:
            raise NotSupportedError(
                f"column '{name}' of {self.qualified_name} is PostgreSQL type "
                f"{type_name(oid)}, which Opteryx cannot read; exclude it or cast it in a view"
            )
        return _TYPE_BY_PHYSICAL[tag]

    def get_dataset_metadata(self) -> Tuple[RelationSchema, Optional["Manifest"]]:
        """The relation's schema, plus a HINT statistics manifest when one exists.

        The manifest holds nothing about files - the rows come over a socket -
        and exists purely so the planner's existing statistics surface
        (`estimate_cardinality`, `get_value_range`, `estimate_null_fraction`,
        ...) can answer for a PostgreSQL relation without inventing a second
        channel. It is written by the control plane's catalog refresh from the
        server's own `pg_stats`, to the same storage layout an Opteryx-backed
        relation uses.

        `stats_are_authoritative` is FALSE, always. These numbers describe the
        server as it was at the last refresh and the server moves underneath
        them, so they may shape a plan and must never decide an answer - see
        `Manifest.stats_are_authoritative`. A relation with NO manifest is not an
        error - it simply plans with the row estimate alone, exactly as it did
        before any of this existed. A manifest that is there but cannot be read
        IS an error and ends the query; see `_read_stats_manifest`.
        """
        schema = self.get_dataset_schema()
        return schema, self._read_stats_manifest(schema)

    def _read_stats_manifest(self, schema: RelationSchema) -> Optional["Manifest"]:
        path = self.gateway.stats_manifest_path(self.schema_name, self.table_name)
        if path is None:
            return None

        from opteryx.compiled.http_client import HttpStatusError
        from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem
        from opteryx.models.manifest import Manifest
        from opteryx.models.manifest_io import read_manifest_file_entries

        filesystem = OpteryxGcsFileSystem(bucket=self.gateway.gcs_bucket)

        # Is there a manifest at all? Asked as its own question, because the two
        # answers are not the same kind of thing: "this relation's statistics
        # have never been refreshed" is the ORDINARY state of a newly-bound
        # relation and plans without hints exactly as it did before any of this
        # existed, while a refused credential, an unreachable bucket or a
        # corrupt file is a FAILURE and must be seen. Only a 404 is absence -
        # the same rule `S3FileSystem.get_file_info` states, and for the same
        # reason: an object that exists being reported absent is a lie the
        # caller cannot see through. `HttpStatusError.status` is 0 for a
        # transport-level failure that never got a response, so that re-raises
        # here too.
        #
        # This used to be a bare `except Exception: return None` wrapped around
        # the read, which made every one of those failures invisible and
        # indistinguishable from "no statistics yet" (CLAUDE.md 9).
        try:
            filesystem.get_file_info(path)
        except HttpStatusError as err:
            if err.status != 404:
                raise
            return None

        # No guard around the read: the object was there a moment ago, so a
        # failure now (it was deleted in between, the bytes are not a manifest,
        # the credential expired) is a real failure and ends the query.
        #
        # `GcsFile` is NOT a context manager (no __enter__/__exit__), so the
        # `with` this replaces raised TypeError on every call - which the bare
        # except swallowed, making the manifest unreadable for every relation
        # whatever the storage held. try/finally here is resource release, not
        # error handling: nothing is caught.
        handle = filesystem.open_input_file(path)
        try:
            data = bytes(handle.memoryview)
        finally:
            handle.close()
        file_entries, _ = read_manifest_file_entries(data)

        if not file_entries:
            return None
        # `bounds_are_ordinal=True` because the refresh writes them that way, and
        # it MUST travel with them: `prune_files` ordinalizes a predicate literal
        # before comparing only when this is set, so a manifest carrying ordinals
        # without it compares a real value against an ordinal and matches
        # nothing. The bounds are ordinals for the reason the writer records -
        # one typed ARRAY column cannot hold a relation's mixed value types.
        return Manifest(
            file_entries, schema, stats_are_authoritative=False, bounds_are_ordinal=True
        )

    def _row_estimate(self, query_text) -> Optional[int]:
        """The server's own row count for this relation, or None if it has none.

        Free to read, and it is the ONLY measured number the cost estimator gets
        for a relation whose rows live behind a socket: without it every leaf
        plans at `_UNKNOWN_ROW_COUNT` (statistics_refresh) and the join order is
        decided by a constant. Where it comes from depends on the server, which
        is why the binding declares a `dialect`:

        * PostgreSQL - `pg_class.reltuples`, the planner's own estimate. -1
          (never analysed) and 0 both mean 'unknown' here: a real zero would
          make the estimator treat the relation as empty, which an unanalysed
          table is not.
        * CockroachDB - `SHOW STATISTICS`, the most recent sample. CockroachDB
          leaves `pg_class.reltuples` NULL for every relation (it keeps no
          PostgreSQL-shaped catalog statistics at all, and `pg_stats` is empty),
          so the PostgreSQL statement returns nothing there - measured against a
          v26.2 cluster. Its own statistics are collected automatically and are
          EXACT, not estimates. The relation is named inline because `SHOW
          STATISTICS FOR TABLE` takes an identifier, not a bind parameter;
          `qualified_name` is the same quoted, quote-escaped spelling every
          other statement this class builds uses.

        A relation the server holds no statistics for returns None, and the
        planner is told nothing rather than told a guess.
        """
        if self.gateway.dialect == DIALECT_COCKROACH:
            rows = query_text(
                self.connection_config,
                f"SELECT row_count::text FROM [SHOW STATISTICS FOR TABLE {self.qualified_name}] "
                "ORDER BY created DESC LIMIT 1",
                [],
            )
        else:
            rows = query_text(
                self.connection_config,
                "SELECT c.reltuples::float8::text FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = $1 AND c.relname = $2",
                [self.schema_name, self.table_name],
            )
        if not rows or rows[0][0] is None:
            return None
        estimate = float(rows[0][0])
        if estimate <= 0:
            return None
        return int(estimate)

    # ---- per-column facts the compiler needs -------------------------------

    def pg_name(self, schema_column: SchemaColumn) -> str:
        """The column's name as PostgreSQL reports it (for quoting in the statement)."""
        meta = self._meta.get(schema_column.name.lower())
        if meta is None:
            raise InvalidInternalStateError(
                f"'{schema_column.name}' is not a column of {self.qualified_name}"
            )
        return meta[0]

    def column_oid(self, schema_column: SchemaColumn) -> int:
        return self._meta[schema_column.name.lower()][1]

    @staticmethod
    def column_decimal_precision(schema_column: SchemaColumn) -> int:
        logical = schema_column.column_type.logical
        if logical is not None and logical.kind == LogicalKind.DECIMAL:
            return int(logical.precision)
        return 0

    @staticmethod
    def column_decimal_scale(schema_column: SchemaColumn) -> int:
        logical = schema_column.column_type.logical
        if logical is not None and logical.kind == LogicalKind.DECIMAL:
            return int(logical.scale)
        return 0

    # ---- pushdown -----------------------------------------------------------

    def can_push(self, operator, types: set = None) -> bool:
        condition = operator.condition
        # A NOT root is refused by the base gate - its node-type allowlist is the
        # set a generic reader can lower, and NOT is not in it. This connector
        # emits SQL text, where `NOT (...)` is just SQL, and the shape matters:
        # `col NOT LIKE 'x%'` is lowered by PredicateRewriteStrategy to a NOT
        # wrapping a _STARTS_WITH, so refusing NOT here would push every anchored
        # LIKE and strand every negation of one. The base gate is asked about what
        # the NOT wraps; `_predicate_sql` puts the NOT back.
        gate_operator = operator
        if condition.node_type == NodeType.NOT:
            if condition.centre is None:
                return False
            # A Filter over what the NOT wraps — can_push reads only its condition.
            from opteryx.planner.logical_planner import LogicalPlanNode
            from opteryx.planner.logical_planner import LogicalPlanStepType

            gate_operator = LogicalPlanNode(
                node_type=LogicalPlanStepType.Filter, condition=condition.centre
            )
        if not PredicatePushable.can_push(self, gate_operator, types):
            return False
        # BETWEEN is pushed only in its closed form; an open bound would need a
        # different SQL shape than `BETWEEN`, so decline rather than mistranslate.
        if condition.node_type == NodeType.BETWEEN and condition.value not in (None, (True, True)):
            return False
        # Every identifier must be one of this relation's own columns. This runs
        # BEFORE the trial render below, because `pg_name` resolves through
        # `_meta` and a foreign column has no entry to resolve.
        for node in get_all_nodes_of_type(condition, (NodeType.IDENTIFIER,)):
            if node.schema_column.name.lower() not in self._meta:
                return False
        # The renderer IS the gate. The builder has no fallback - a predicate
        # admitted here and then refused there is a failed query, not a missed
        # pushdown - so rather than restate what can be spelled (a restatement
        # that drifts: the base gate admits boolean-rooted FUNCTIONs, of which
        # only the LIKE-lowered ones have a spelling), the question is put to
        # `_predicate_sql` itself and its answer is the answer. The parameter list
        # is a throwaway: nothing else here is mutated by a render.
        return _predicate_sql(self, condition, []) is not _UNRENDERABLE

    def _is_own_column(self, node) -> bool:
        """A plain IDENTIFIER bound to one of this relation's own columns, of a
        type the statement builder can render (PUSHABLE_TYPES) whose values mean
        the same thing to the server as they do to the engine.

        This is THE place `char(n)` is excluded, and with it every shape built on
        this test: a top-N key, a GROUP BY key, a DISTINCT column, an aggregate
        operand, a pushed LIKE and a pushed IN-list. See `_blank_padded` for why.
        Declining here also declines `count(char_column)`, which would in fact be
        safe - counting non-nulls reads no value - but one gate that is always
        right beats a second, narrower one that has to stay in step with it.
        """
        if node is None or node.node_type != NodeType.IDENTIFIER:
            return False
        schema_column = node.schema_column
        if schema_column is None or schema_column.name is None:
            return False
        if schema_column.name.lower() not in self._meta:
            return False
        if schema_column.category not in self.PUSHABLE_TYPES:
            return False
        return not _blank_padded(self, node)

    def can_push_topn(self, order_by) -> bool:
        """Any number of keys, each a plain column of this relation. NULL order
        and text collation are spelled explicitly by the renderer, so nothing
        about the key's type beyond PUSHABLE_TYPES needs declining here."""
        if not order_by:
            return False
        return all(self._is_own_column(expression) for expression, _ascending in order_by)

    def can_push_aggregate(self, groups, aggregates) -> bool:
        """The renderer is the gate (see `_remote_aggregate`): a shape is pushable
        only if every group key is an own column and every aggregate has a remote
        spelling whose result type IS the type the binder bound it to."""
        if not all(self._is_own_column(group) for group in groups or []):
            return False
        return all(_remote_aggregate(self, aggregate) is not None for aggregate in aggregates or [])

    def can_push_distinct(self, columns) -> bool:
        if not columns:
            return False
        return all(self._is_own_column(column) for column in columns)

    # ---- execution is native ------------------------------------------------

    def read_dataset(self, **kwargs):
        raise InvalidInternalStateError(
            "PostgresTable has no Python read path; the scan is served by "
            "NativePostgresScanSource"
        )


# ---------------------------------------------------------------------------
# Scan statement
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EmitColumn:
    """One column the statement returns, as the native Source needs it: the
    plan identity the morsel names it by, the result OID the server MUST report
    (the Source refuses the stream otherwise), the DrakenType it is emitted as
    and, for DECIMAL, the precision/scale the decoder rescales to."""

    identity: bytes
    oid: int
    physical: int
    precision: int
    scale: int


@dataclass(frozen=True)
class ScanStatement:
    sql: str
    params: List[Optional[str]]
    zero_columns: bool
    # Parallel to the statement's select list, in emit order. Empty for the
    # zero-column (`SELECT 1`) shape.
    emit: Tuple[EmitColumn, ...] = ()


# Result-type OIDs the renderer casts TO. Everything else the statement returns
# is a relation column's own OID (from the describe at bind time).
_OID_INT8 = 20
_OID_TEXT = 25
_OID_FLOAT4 = 700
_OID_FLOAT8 = 701
_OID_BPCHAR = 1042  # char(n): blank-padded, never cast to text


_COMPARISON_SQL = {
    "Eq": "=",
    "NotEq": "<>",
    "Gt": ">",
    "GtEq": ">=",
    "Lt": "<",
    "LtEq": "<=",
    "Like": "LIKE",
    "NotLike": "NOT LIKE",
}
_UNARY_SQL = {
    "IsNull": "IS NULL",
    "IsNotNull": "IS NOT NULL",
}

# The FUNCTION nodes PredicateRewriteStrategy lowers an ANCHORED LIKE into,
# mapped to which end of the pattern the anchor was on. `col LIKE 'x%'` never
# reaches a connector as a LIKE - it arrives as `_STARTS_WITH(col, b'x')` (and
# its negation as NOT over that) - so this table is how the LIKE is recognised
# and spelled back. The `_CI_` twins that ILIKE lowers to are deliberately
# absent: see PUSHABLE_OPS.
_LIKE_FUNCTIONS = {
    "_STARTS_WITH": "prefix",
    "_ENDS_WITH": "suffix",
}

# Longest IN-list pushed as `IN ($1, ..., $n)`. The wire client refuses a
# statement with more than 32767 bind parameters, so an uncapped list would let
# the gate admit a predicate that fails at execution instead of one that filters
# locally. 1024 keeps 32 such predicates inside that ceiling, and a longer list
# is a set the server would scan linearly anyway.
_MAX_IN_LIST = 1024


# `_render_literal` returns this for a literal it cannot spell as PostgreSQL
# input syntax. It is a value, not an exception, because `can_push` has to ask
# the same question as `build_scan_statement` and get an answer rather than
# control flow - the two MUST agree, or a predicate admitted by the gate reaches
# a builder that raises, and the query fails instead of filtering above the scan.
_UNRENDERABLE = object()


def _pg_input_syntax(text: str) -> Any:
    """Decline the one rendering the engine's formatters emit that PostgreSQL
    cannot read back: a year before 1 CE ('-0221-09-05', '0000-01-01'). Postgres
    spells those with a `BC` suffix; rather than invent that spelling here,
    decline and let the predicate filter above the scan."""
    if text[0] == "-" or text.startswith("0000"):
        return _UNRENDERABLE
    return text


def _render_literal(node) -> Any:
    """A bound LITERAL node as a TEXT bind parameter, `None` for NULL, or
    `_UNRENDERABLE`."""
    return _render_value(node.value, node.type)


def _render_value(value, column_type) -> Any:
    """One value of a known ColumnType as a TEXT bind parameter, `None` for NULL,
    or `_UNRENDERABLE`.

    The server casts the parameter to the column's type, so this is the
    literal's input-syntax form, never SQL.

    Dispatch is on the TYPE TAG, never on the Python type of the value. A
    temporal literal reaches the connector as its PHYSICAL storage integer -
    DATE32 is days since the epoch, TIMESTAMP64 microseconds - so the `10470` of
    `CAST('1998-09-01' AS DATE)` is indistinguishable from the integer 10470,
    and only the type tells them apart. Rendering from the value alone sent
    '10470' to the server as a date.

    Value and type are separate arguments rather than a node because an IN-list
    is ONE literal node holding many values under a single `ARRAY<element>` type:
    its members are rendered from `element`, which is the same dispatch and must
    stay the same code.

    The days/microseconds renderings come from `opteryx.expression.formatter`,
    the engine's own literal formatters, so a pushed bound is spelled exactly as
    the same literal is spelled everywhere else.
    """
    from opteryx.expression.formatter import _format_date_days
    from opteryx.expression.formatter import _format_timestamp_micros

    if not isinstance(column_type, ColumnType):
        # An unbound or synthetic literal carries no type, and an untagged
        # integer cannot be told from an epoch day count.
        return _UNRENDERABLE

    physical = column_type.physical
    if value is None or physical == DrakenType.NULL:
        return None

    # bool is a subclass of int - test it before the integer widths.
    if isinstance(value, bool):
        return "true" if value else "false"

    if physical == DrakenType.DATE32:
        if not isinstance(value, int):
            return _UNRENDERABLE
        return _pg_input_syntax(_format_date_days(value))

    if physical == DrakenType.TIMESTAMP64:
        if not isinstance(value, int):
            return _UNRENDERABLE
        return _pg_input_syntax(_format_timestamp_micros(value))

    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        # VARCHAR literals are bytes inside the plan. Only VARCHAR is pushable
        # (PUSHABLE_TYPES), so this is text, never an opaque VARBINARY value.
        return value.decode("utf-8")
    return _UNRENDERABLE


def _add_param(params: List[Optional[str]], text: Optional[str]) -> str:
    """Bind `text` as the next `$n` and return that placeholder."""
    params.append(text)
    return f"${len(params)}"


def _operand_sql(table: PostgresTable, node, params: List[Optional[str]]) -> Any:
    """An IDENTIFIER or LITERAL operand as SQL, or `_UNRENDERABLE`.

    No `char(n)` test here: this renders an operand whatever the surrounding
    predicate does with it, and `IS NULL` reads no value, so a blank-padded
    column is perfectly safe under it. The shapes that DO read the value go
    through `_value_operand_sql`.
    """
    if node.node_type == NodeType.IDENTIFIER:
        return _quote_identifier(table.pg_name(node.schema_column))
    if node.node_type == NodeType.LITERAL:
        text = _render_literal(node)
        if text is _UNRENDERABLE:
            return _UNRENDERABLE
        return _add_param(params, text)
    return _UNRENDERABLE


def _value_operand_sql(table: PostgresTable, node, params: List[Optional[str]]) -> Any:
    """`_operand_sql` for a position whose answer depends on the operand's VALUE
    (a comparison side, a BETWEEN bound). Declines a `char(n)` column."""
    if _blank_padded(table, node):
        return _UNRENDERABLE
    return _operand_sql(table, node, params)


def _blank_padded(table: PostgresTable, node) -> bool:
    """Is this operand a `char(n)` column?

    PostgreSQL reads a `char(n)`'s value with its trailing blanks REMOVED -
    `bpchareq` and friends ignore them, `~~` casts to text first, and so do
    ordering, grouping and DISTINCT - while the scan hands the engine the padded
    value the column actually holds. So `'ab  '::char(4) = 'ab'` is TRUE on the
    server and false in the engine, `LIKE '%b'` likewise, and a GROUP BY folds
    together two values the engine keeps apart.

    That makes every value-reading shape over such a column a wrong ANSWER, not a
    slow one, so all of them decline it. The scan still reads and returns the
    column; only pushing work about its value down is refused.
    """
    if node is None or node.node_type != NodeType.IDENTIFIER:
        return False
    schema_column = node.schema_column
    if schema_column is None or schema_column.name is None:
        return False
    if schema_column.name.lower() not in table._meta:
        return False
    return table.column_oid(schema_column) == _OID_BPCHAR


def _like_escape(text: str) -> str:
    """A literal string as a LIKE pattern body matching itself and nothing else.

    `%`, `_` and the escape character itself are the only characters LIKE reads,
    and backslash is LIKE's default escape (no `ESCAPE` clause needed, and none
    written: the pattern travels as a bind parameter, so `standard_conforming_
    strings` - which governs how the SERVER parses a string literal - never
    touches it).

    The bodies that reach here carry no `%` or `_` today: PredicateRewriteStrategy
    only lowers a LIKE to these shapes when the pattern has none besides its
    anchor. They are escaped anyway because the escaping has to be right for the
    body it is given, not for the body today's caller happens to pass.
    """
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _like_pattern_text(node) -> Any:
    """The pattern body of a lowered LIKE as `str`, or `_UNRENDERABLE`.

    The two lowerings spell it differently and both are read here: a
    `_STARTS_WITH`/`_ENDS_WITH` parameter is `bytes` tagged VARBINARY, an
    `InStr` operand is `str` tagged VARCHAR.
    """
    if node is None or node.node_type != NodeType.LITERAL:
        return _UNRENDERABLE
    value = node.value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    return _UNRENDERABLE


def _like_sql(table: PostgresTable, column_node, pattern_node, shape: str,
              params: List[Optional[str]]) -> Any:
    """`column LIKE $n` for one of the LIKE shapes the optimizer lowered.

    `shape` is 'prefix', 'suffix' or 'infix' - the three the rewriter produces
    from `'x%'`, `'%x'` and `'%x%'`. The pattern is rebuilt here rather than
    recovered, because by this point the original spelling is gone.
    """
    if not table._is_own_column(column_node):
        return _UNRENDERABLE
    body = _like_pattern_text(pattern_node)
    if body is _UNRENDERABLE:
        return _UNRENDERABLE
    escaped = _like_escape(body)
    pattern = {"prefix": escaped + "%", "suffix": "%" + escaped, "infix": "%" + escaped + "%"}[shape]
    column = _quote_identifier(table.pg_name(column_node.schema_column))
    return f"{column} LIKE {_add_param(params, pattern)}"


def _in_list_sql(table: PostgresTable, node, params: List[Optional[str]]) -> Any:
    """`column IN ($a, $b, ...)` for an InList/NotInList comparison.

    The members live in ONE literal node as a Python list under an
    `ARRAY<element>` type, so each is rendered from `element` - the same
    type-tag dispatch every other literal takes, which is what keeps a pushed
    `d IN (DATE '1998-09-01')` from shipping its epoch-day integer.
    """
    column_node, list_node = node.left, node.right
    if not table._is_own_column(column_node):
        return _UNRENDERABLE
    if list_node is None or list_node.node_type != NodeType.LITERAL:
        return _UNRENDERABLE
    array_type = list_node.type
    if not isinstance(array_type, ColumnType) or array_type.element is None:
        return _UNRENDERABLE
    values = list_node.value
    if not isinstance(values, (list, tuple, set)):
        return _UNRENDERABLE
    values = list(values)
    # An empty list has no SQL (`IN ()` is a syntax error), and a list past the
    # cap is declined so the gate cannot admit a statement the wire client then
    # refuses: its ceiling is 32767 bind parameters for the WHOLE statement, and
    # this leaves room for many such predicates in one.
    if not values or len(values) > _MAX_IN_LIST:
        return _UNRENDERABLE
    placeholders = []
    for value in values:
        text = _render_value(value, array_type.element)
        # A NULL member cannot be built today - the parser refuses an IN-list of
        # mixed types and NULL is its own - and it is declined rather than
        # spelled because `IN (NULL)` and `NOT IN (NULL)` are the two shapes
        # whose three-valued answer would have to be re-derived if it ever could.
        if text is _UNRENDERABLE or text is None:
            return _UNRENDERABLE
        placeholders.append(_add_param(params, text))
    column = _quote_identifier(table.pg_name(column_node.schema_column))
    operator = "NOT IN" if node.value == "NotInList" else "IN"
    return f"{column} {operator} ({', '.join(placeholders)})"


def _predicate_sql(table: PostgresTable, node, params: List[Optional[str]]) -> Any:
    """One pushed predicate as SQL text, or `_UNRENDERABLE`.

    This is BOTH the gate (`can_push` trial-renders through it) and the
    renderer, so the two cannot disagree. It returns a sentinel rather than
    raising because the gate needs an answer, not control flow - the same
    reason `_render_literal` does.
    """
    node_type = node.node_type
    if node_type == NodeType.NOT:
        # `col NOT LIKE 'x%'` lowers to NOT over a _STARTS_WITH; the negation is
        # spelled here and its operand renders as the positive form.
        inner = _predicate_sql(table, node.centre, params) if node.centre is not None else _UNRENDERABLE
        if inner is _UNRENDERABLE:
            return _UNRENDERABLE
        return f"NOT ({inner})"
    if node_type == NodeType.FUNCTION:
        # The anchored-LIKE lowerings. The case-insensitive twins (`_CI_*`) are
        # absent for the reason ILIKE is not pushed at all: the server folds case
        # by its locale and the engine folds it its own way.
        shape = _LIKE_FUNCTIONS.get(node.value)
        if shape is None:
            return _UNRENDERABLE
        parameters = list(node.parameters or [])
        if len(parameters) != 2:
            return _UNRENDERABLE
        return _like_sql(table, parameters[0], parameters[1], shape, params)
    if node_type == NodeType.COMPARISON_OPERATOR:
        if node.value in ("InList", "NotInList"):
            return _in_list_sql(table, node, params)
        if node.value in ("InStr", "NotInStr"):
            # The unanchored `LIKE '%x%'` lowering, spelled back as the LIKE it
            # came from.
            inner = _like_sql(table, node.left, node.right, "infix", params)
            if inner is _UNRENDERABLE:
                return _UNRENDERABLE
            return f"NOT ({inner})" if node.value == "NotInStr" else inner
        op = _COMPARISON_SQL.get(node.value)
        if op is None:
            return _UNRENDERABLE
        left = _value_operand_sql(table, node.left, params)
        right = _value_operand_sql(table, node.right, params)
        if left is _UNRENDERABLE or right is _UNRENDERABLE:
            return _UNRENDERABLE
        return f"{left} {op} {right}"
    if node_type == NodeType.UNARY_OPERATOR:
        op = _UNARY_SQL.get(node.value)
        if op is None:
            return _UNRENDERABLE
        centre = _operand_sql(table, node.centre, params)
        if centre is _UNRENDERABLE:
            return _UNRENDERABLE
        return f"{centre} {op}"
    if node_type == NodeType.BETWEEN:
        # Only the closed form; an open bound is a different SQL shape than
        # `BETWEEN` and is declined rather than mistranslated.
        if node.value not in (None, (True, True)):
            return _UNRENDERABLE
        subject = _value_operand_sql(table, node.left, params)
        low = _value_operand_sql(table, node.right, params)
        high = _value_operand_sql(table, node.centre, params)
        if _UNRENDERABLE in (subject, low, high):
            return _UNRENDERABLE
        return f"{subject} BETWEEN {low} AND {high}"
    return _UNRENDERABLE


def _emit_for_column(table: PostgresTable, schema_column: SchemaColumn) -> EmitColumn:
    """A relation column, emitted as the type the binder gave it, under the
    OID the describe reported for it."""
    return EmitColumn(
        identity=schema_column.identity,
        oid=table.column_oid(schema_column),
        physical=schema_column.column_type.physical.value,
        precision=table.column_decimal_precision(schema_column),
        scale=table.column_decimal_scale(schema_column),
    )


def _is_text(schema_column: SchemaColumn) -> bool:
    return schema_column.category == LogicalCategory.VARCHAR


def _key_sql(table: PostgresTable, schema_column: SchemaColumn) -> str:
    """A column as an ORDER BY / MIN / MAX operand. Text gets `COLLATE "C"` so
    the server orders bytes the way draken does; without it the server's
    top-N (or MIN/MAX) under the column's own collation is a different row."""
    sql = _quote_identifier(table.pg_name(schema_column))
    if _is_text(schema_column):
        sql += ' COLLATE "C"'
    return sql


def _order_by_sql(table: PostgresTable, order_by) -> str:
    """`order_by` is the scan's stamped spec: [(SchemaColumn, ascending), ...].

    draken: NULL sorts below every value (draken/morsels/sort.hpp), so ASC is
    NULLS FIRST and DESC is NULLS LAST. PostgreSQL's defaults are the inverse in
    BOTH directions, so the null placement is always written out."""
    parts = []
    for schema_column, ascending in order_by:
        key = _key_sql(table, schema_column)
        parts.append(f"{key} ASC NULLS FIRST" if ascending else f"{key} DESC NULLS LAST")
    return ", ".join(parts)


# Aggregate operand categories with a PostgreSQL overload whose semantics match
# the engine's. BOOL/DATE/TIMESTAMP SUM and AVG exist in the engine but not on
# the server; BOOL MIN/MAX exist in neither direction that agrees.
_MINMAX_CATEGORIES = {
    LogicalCategory.INTEGER,
    LogicalCategory.FLOAT,
    LogicalCategory.DECIMAL,
    LogicalCategory.DATE,
    LogicalCategory.TIMESTAMP,
    LogicalCategory.VARCHAR,
}
_SUM_CATEGORIES = {LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL}


def _remote_aggregate(table: PostgresTable, aggregate) -> Optional[Tuple[str, EmitColumn]]:
    """The remote spelling of one AGGREGATOR node and the column it returns, or
    None when it has no spelling with the engine's exact semantics.

    This is BOTH the gate (`can_push_aggregate`) and the renderer, so the two
    cannot disagree. The result type is never inferred here: the binder already
    bound one (`aggregate.schema_column.column_type`), and the spelling must
    produce EXACTLY that type on the wire — a cast is added where the server's
    natural result type differs (SUM over integers is int8 on the engine, but
    numeric on the server for int8 operands; AVG is FLOAT64 on the engine,
    numeric on the server for integer/numeric operands). A bound type the
    spelling cannot reproduce declines the aggregate rather than emitting a
    column the Source would refuse or a value with different precision.
    """
    function = aggregate.value
    bound = aggregate.schema_column
    if bound is None or bound.identity is None or bound.column_type is None:
        return None
    result_type: ColumnType = bound.column_type
    parameters = list(aggregate.parameters or [])
    distinct = aggregate.duplicate_treatment == "Distinct"

    def emit(oid: int, column_type: ColumnType) -> EmitColumn:
        return EmitColumn(
            identity=bound.identity,
            oid=oid,
            physical=column_type.physical.value,
            precision=table.column_decimal_precision(bound),
            scale=table.column_decimal_scale(bound),
        )

    if function == "COUNT":
        if result_type.physical != DrakenType.INT64 or len(parameters) != 1:
            return None
        operand = parameters[0]
        if operand.node_type == NodeType.WILDCARD:
            # COUNT(DISTINCT *) is a whole-row dedup count; not spelled here.
            if distinct:
                return None
            return "count(*)", emit(_OID_INT8, result_type)
        if not table._is_own_column(operand):
            return None
        column = _quote_identifier(table.pg_name(operand.schema_column))
        return (f"count(DISTINCT {column})" if distinct else f"count({column})"), emit(
            _OID_INT8, result_type
        )

    # Everything below is a single plain-column operand with no DISTINCT. A
    # filtered aggregate - `AGG(expr WHERE cond)` - reaches here folded into the
    # operand expression as an IIF, so it fails the plain-column test and is
    # declined with it.
    if distinct or len(parameters) != 1 or not table._is_own_column(parameters[0]):
        return None
    operand_column = parameters[0].schema_column
    operand_category = operand_column.category
    operand_oid = table.column_oid(operand_column)
    column = _quote_identifier(table.pg_name(operand_column))

    if function == "SUM":
        if operand_category not in _SUM_CATEGORIES:
            return None
        if operand_category == LogicalCategory.INTEGER:
            # The engine sums every integer width into INT64; the server returns
            # int8 for int2/int4 and numeric for int8. `::int8` pins the type and
            # turns an overflow into a server error rather than a widened value.
            if result_type.physical != DrakenType.INT64:
                return None
            return f"sum({column})::int8", emit(_OID_INT8, result_type)
        # FLOAT / DECIMAL pass the operand's type through on both sides; a
        # numeric sum keeps the operand's scale, and the decoder's tier check is
        # the overflow guard.
        if result_type.physical != operand_column.column_type.physical:
            return None
        return f"sum({column})", emit(operand_oid, result_type)

    if function == "AVG":
        if operand_category not in _SUM_CATEGORIES:
            return None
        # The binder types AVG as FLOAT64 for integer/decimal operands and passes
        # a float operand's width through. The server's avg is numeric for the
        # first group and float8/float4 for the second; cast to the bound type.
        if result_type.physical == DrakenType.FLOAT64:
            return f"avg({column})::float8", emit(_OID_FLOAT8, result_type)
        if result_type.physical == DrakenType.FLOAT32:
            return f"avg({column})::float4", emit(_OID_FLOAT4, result_type)
        return None

    if function in ("MIN", "MAX"):
        if operand_category not in _MINMAX_CATEGORIES:
            return None
        if result_type.physical != operand_column.column_type.physical:
            return None
        if _is_text(operand_column):
            # Text takes COLLATE "C": the server's min/max under the column's
            # collation is not the bytewise extreme draken would pick. The
            # collated expression's type is not always the column's own (a `name`
            # or a domain over it comes back as text), so the result is pinned to
            # text and the plan expects text — the OID must follow the spelling,
            # not the column.
            #
            # char(n) needed its own refusal here once (a `::text` cast strips the
            # blank padding the scan keeps); `_is_own_column` above now declines
            # every blank-padded operand, so a second test would only be a copy
            # waiting to fall out of step with it.
            return (
                f"{function.lower()}({_key_sql(table, operand_column)})::text",
                emit(_OID_TEXT, result_type),
            )
        return f"{function.lower()}({column})", emit(operand_oid, result_type)

    # ANY_VALUE (server >= 16 only), the STDDEV/VAR family (different
    # accumulation), MEDIAN/APPROX_*/CORR/ARRAY_AGG/CIDR_AGG (different
    # algorithms or unordered results): no spelling with matching semantics.
    return None


def build_scan_statement(
    table: PostgresTable,
    columns: list,
    predicates: Optional[list],
    limit: Optional[int],
    *,
    order_by: Optional[list] = None,
    topn_limit: Optional[int] = None,
    groups: Optional[list] = None,
    aggregates: Optional[list] = None,
    distinct: bool = False,
) -> ScanStatement:
    """The statement the native Source runs for one scan.

    `columns` are the plan's projected LogicalColumns (emit order); an empty
    projection (a bare `SELECT 1 FROM t`) selects a constant so the server still
    streams one row per matching row and the Source emits zero-column morsels
    carrying the count. `predicates` are the conditions the optimizer pushed —
    each one passed `can_push` — and become `$n` parameters. `limit` is a pushed
    LIMIT.

    The keyword shapes are the ones the remote-pushdown strategies absorb into
    the scan, each admitted by its `can_push_*` gate first:
      * `order_by` + `topn_limit`: a HeapSort's spec, rendered ORDER BY ... LIMIT;
      * `groups` + `aggregates`: an absorbed Aggregate — the select list becomes
        the group keys followed by the aggregate spellings, and `columns` is not
        consulted;
      * `distinct`: an absorbed DISTINCT over `columns`.
    The builder has NO fallback: a shape the gates admitted and this refuses is
    an internal error, not a missed pushdown.
    """
    params: List[Optional[str]] = []
    emit: List[EmitColumn] = []

    if aggregates is not None:
        if distinct:
            raise InvalidInternalStateError("a PostgreSQL scan cannot carry both DISTINCT and an aggregate")
        select_parts = []
        for group in groups or []:
            select_parts.append(_quote_identifier(table.pg_name(group.schema_column)))
            emit.append(_emit_for_column(table, group.schema_column))
        for aggregate in aggregates:
            remote = _remote_aggregate(table, aggregate)
            if remote is None:
                raise InvalidInternalStateError(
                    f"aggregate {aggregate.value} reached the PostgreSQL statement builder "
                    "but has no remote spelling — can_push_aggregate should have declined it"
                )
            select_parts.append(remote[0])
            emit.append(remote[1])
        if not select_parts:
            raise InvalidInternalStateError("an absorbed aggregate with no keys and no aggregates")
        select_list = ", ".join(select_parts)
    else:
        select_list = ", ".join(
            _quote_identifier(table.pg_name(column.schema_column)) for column in columns
        )
        emit = [_emit_for_column(table, column.schema_column) for column in columns]
        if distinct:
            if not columns:
                raise InvalidInternalStateError("an absorbed DISTINCT over no columns")
            select_list = "DISTINCT " + select_list

    sql = f"SELECT {select_list or '1'} FROM {table.qualified_name}"
    clauses = []
    for predicate in predicates or []:
        clause = _predicate_sql(table, predicate, params)
        # `can_push` trial-renders through the same function, so a predicate that
        # reaches here and cannot be spelled means the two disagreed - an engine
        # inconsistency, exactly as an aggregate with no remote spelling is. It is
        # never a missed pushdown at this point: the statement is already being
        # built on the promise that the server applies this predicate, and
        # dropping it would silently return unfiltered rows.
        if clause is _UNRENDERABLE:
            raise InvalidInternalStateError(
                f"a {predicate.node_type.name} predicate ({predicate.value}) reached the "
                "PostgreSQL statement builder but has no SQL spelling — can_push should "
                "have declined it"
            )
        clauses.append(clause)
    if clauses:
        sql += " WHERE " + " AND ".join(f"({clause})" for clause in clauses)
    if aggregates is not None and groups:
        sql += " GROUP BY " + ", ".join(
            _quote_identifier(table.pg_name(group.schema_column)) for group in groups
        )
    if order_by:
        if topn_limit is None:
            raise InvalidInternalStateError("a pushed ORDER BY without its LIMIT reached the PostgreSQL scan")
        sql += " ORDER BY " + _order_by_sql(table, order_by)
    if topn_limit is not None:
        if limit is not None:
            raise InvalidInternalStateError("a PostgreSQL scan cannot carry both a LIMIT and a top-N")
        limit = topn_limit
    if limit is not None:
        if int(limit) < 0:
            raise InvalidInternalStateError(f"negative LIMIT {limit} reached the PostgreSQL scan")
        sql += f" LIMIT {int(limit)}"
    return ScanStatement(
        sql=sql,
        params=params,
        zero_columns=not emit,
        emit=tuple(emit),
    )
