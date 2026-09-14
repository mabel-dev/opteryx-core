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
them to `ColumnType`, estimate the row count from `pg_class`, and translate the
optimizer's pushed predicates / LIMIT / top-N / aggregate / DISTINCT into the
scan statement with `$n` bind parameters (never interpolated literals).

Every pushed shape is rendered with the ENGINE's semantics spelled out where
PostgreSQL's defaults differ: NULLS FIRST under ASC and NULLS LAST under DESC
(draken sorts NULL below every value), `COLLATE "C"` on text sort keys and text
MIN/MAX (draken compares bytes; the server would use the column's collation),
and an explicit cast wherever the server's result type is not the type the
binder bound the aggregate to. A shape that cannot be spelled that way is
DECLINED by `can_push_*` and stays a local operator — never mistranslated.

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
from typing import Any, Dict, List, Optional, Tuple

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.connectors.capabilities.aggregate_pushable import AggregatePushable
from opteryx.connectors.capabilities.distinct_pushable import DistinctPushable
from opteryx.connectors.capabilities.limit_pushable import LimitPushable
from opteryx.connectors.capabilities.predicate_pushable import PredicatePushable
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


class PostgresConnector(BaseConnector):
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
        gcs_bucket: Optional[str] = None,
        telemetry: Optional[QueryTelemetry] = None,
        prefix: Optional[str] = None,
        **kwargs,
    ) -> None:
        if kwargs:
            raise ValueError(
                f"PostgresConnector: unknown configuration keys {sorted(kwargs)}; "
                "expected host, port, dbname, user, password, sslmode, schema, timeout_s, "
                "preserve_sql_case, gcs_bucket"
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

    def stats_manifest_path(self, schema_name: str, table_name: str) -> Optional[str]:
        """Where this relation's statistics manifest lives, or None if unconfigured.

        The SAME location an Opteryx-backed relation's manifests use - the
        formula in `OpteryxConnector._dataset_location` plus `metadata/` - so
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
    PUSHABLE_OPS = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
        "Like": True,
        "NotLike": True,
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

    @property
    def qualified_name(self) -> str:
        return f"{_quote_identifier(self.schema_name)}.{_quote_identifier(self.table_name)}"

    # ---- schema ---------------------------------------------------------------

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema is not None:
            return self.schema
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
        `Manifest.stats_are_authoritative`. A missing or unreadable manifest is
        not an error: the relation simply plans with the row estimate alone,
        exactly as it did before any of this existed.
        """
        schema = self.get_dataset_schema()
        return schema, self._read_stats_manifest(schema)

    def _read_stats_manifest(self, schema: RelationSchema) -> Optional["Manifest"]:
        path = self.gateway.stats_manifest_path(self.schema_name, self.table_name)
        if path is None:
            return None

        from opteryx.models.manifest import Manifest
        from opteryx.models.manifest_io import read_manifest_file_entries

        try:
            from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem

            filesystem = OpteryxGcsFileSystem(bucket=self.gateway.gcs_bucket)
            with filesystem.open_input_file(path) as handle:
                data = bytes(handle.memoryview)
            file_entries, _ = read_manifest_file_entries(data)
        except Exception:
            # A relation whose statistics have never been refreshed, or a
            # storage blip. Planning without them is the pre-existing behaviour,
            # not a degraded one, so this must not fail the query.
            return None

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
        """pg_class.reltuples — the planner's own estimate, free to read. -1 (never
        analysed) and 0 both mean 'unknown' here: a real zero would make the
        estimator treat the relation as empty, which an unanalysed table is not."""
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
        if not PredicatePushable.can_push(self, operator, types):
            return False
        condition = operator.condition
        # The base gate admits a boolean-rooted FUNCTION (LIKE lowers to one, e.g.
        # _STARTS_WITH). This connector translates predicates into SQL text and
        # has no translation for a function call, so those stay as a Filter above
        # the scan — a missed pushdown, never a failed query.
        if get_all_nodes_of_type(condition, (NodeType.FUNCTION,)):
            return False
        # BETWEEN is pushed only in its closed form; an open bound would need a
        # different SQL shape than `BETWEEN`, so decline rather than mistranslate.
        if condition.node_type == NodeType.BETWEEN and condition.value not in (None, (True, True)):
            return False
        # Every identifier must be one of this relation's own columns.
        for node in get_all_nodes_of_type(condition, (NodeType.IDENTIFIER,)):
            if node.schema_column.name.lower() not in self._meta:
                return False
        # Every literal must be spellable as a PostgreSQL bind parameter. The
        # builder has no fallback - a predicate admitted here and then refused by
        # `_literal_text` is a failed query, not a missed pushdown - so the gate
        # asks the renderer itself rather than restating what it can spell.
        for node in get_all_nodes_of_type(condition, (NodeType.LITERAL,)):
            if _render_literal(node) is _UNRENDERABLE:
                return False
        return True

    def _is_own_column(self, node) -> bool:
        """A plain IDENTIFIER bound to one of this relation's own columns, of a
        type the statement builder can render (PUSHABLE_TYPES)."""
        if node is None or node.node_type != NodeType.IDENTIFIER:
            return False
        schema_column = node.schema_column
        if schema_column is None or schema_column.name is None:
            return False
        if schema_column.name.lower() not in self._meta:
            return False
        return schema_column.category in self.PUSHABLE_TYPES

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
    `_UNRENDERABLE`.

    The server casts the parameter to the column's type, so this is the
    literal's input-syntax form, never SQL.

    Dispatch is on the literal's TYPE TAG, never on the Python type of its
    value. A temporal literal reaches the connector as its PHYSICAL storage
    integer - DATE32 is days since the epoch, TIMESTAMP64 microseconds - so the
    `10470` of `CAST('1998-09-01' AS DATE)` is indistinguishable from the
    integer 10470, and only `node.type` tells them apart. Rendering from the
    value alone sent '10470' to the server as a date.

    The days/microseconds renderings come from `opteryx.expression.formatter`,
    the engine's own literal formatters, so a pushed bound is spelled exactly as
    the same literal is spelled everywhere else.
    """
    from opteryx.expression.formatter import _format_date_days
    from opteryx.expression.formatter import _format_timestamp_micros

    column_type = node.type
    if not isinstance(column_type, ColumnType):
        # An unbound or synthetic literal carries no type, and an untagged
        # integer cannot be told from an epoch day count.
        return _UNRENDERABLE

    value = node.value
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


def _literal_text(node) -> Optional[str]:
    """`_render_literal`, raising rather than returning the sentinel."""
    text = _render_literal(node)
    if text is _UNRENDERABLE:
        raise NotSupportedError(
            f"cannot push a {node.type} literal ({node.value!r}) into a PostgreSQL scan"
        )
    return text


def _operand_sql(table: PostgresTable, node, params: List[Optional[str]]) -> str:
    if node.node_type == NodeType.IDENTIFIER:
        return _quote_identifier(table.pg_name(node.schema_column))
    if node.node_type == NodeType.LITERAL:
        params.append(_literal_text(node))
        return f"${len(params)}"
    raise NotSupportedError(f"cannot push a {node.node_type} operand into a PostgreSQL scan")


def _predicate_sql(table: PostgresTable, node, params: List[Optional[str]]) -> str:
    node_type = node.node_type
    if node_type == NodeType.COMPARISON_OPERATOR:
        op = _COMPARISON_SQL.get(node.value)
        if op is None:
            raise NotSupportedError(f"cannot push comparison '{node.value}' into a PostgreSQL scan")
        left = _operand_sql(table, node.left, params)
        right = _operand_sql(table, node.right, params)
        return f"{left} {op} {right}"
    if node_type == NodeType.UNARY_OPERATOR:
        op = _UNARY_SQL.get(node.value)
        if op is None:
            raise NotSupportedError(f"cannot push unary '{node.value}' into a PostgreSQL scan")
        return f"{_operand_sql(table, node.centre, params)} {op}"
    if node_type == NodeType.BETWEEN:
        if node.value not in (None, (True, True)):
            raise NotSupportedError("cannot push an open-bounded BETWEEN into a PostgreSQL scan")
        subject = _operand_sql(table, node.left, params)
        low = _operand_sql(table, node.right, params)
        high = _operand_sql(table, node.centre, params)
        return f"{subject} BETWEEN {low} AND {high}"
    raise NotSupportedError(f"cannot push a {node_type} predicate into a PostgreSQL scan")


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
    # FILTER (WHERE ...) clause reaches here folded into the operand expression,
    # so it fails the plain-column test and is declined with it.
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
            if operand_oid == _OID_BPCHAR:
                # char(n): a text cast strips the blank padding the scan keeps.
                return None
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
    clauses = [_predicate_sql(table, predicate, params) for predicate in (predicates or [])]
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
