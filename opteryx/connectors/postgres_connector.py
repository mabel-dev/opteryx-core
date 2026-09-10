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
optimizer's pushed predicates / LIMIT into the scan statement with `$n` bind
parameters (never interpolated literals).

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

import datetime
import decimal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.connectors.capabilities.limit_pushable import LimitPushable
from opteryx.connectors.capabilities.predicate_pushable import PredicatePushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import QueryTelemetry
from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import ColumnType, LogicalCategory, LogicalKind
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
        telemetry: Optional[QueryTelemetry] = None,
        prefix: Optional[str] = None,
        **kwargs,
    ) -> None:
        if kwargs:
            raise ValueError(
                f"PostgresConnector: unknown configuration keys {sorted(kwargs)}; "
                "expected host, port, dbname, user, password, sslmode, schema, timeout_s, "
                "preserve_sql_case"
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


class PostgresTable(BaseTable, PredicatePushable, LimitPushable):
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
        return True

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
class ScanStatement:
    sql: str
    params: List[Optional[str]]
    zero_columns: bool


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


def _literal_text(value: Any) -> Optional[str]:
    """A literal as a TEXT bind parameter. The server casts it to the column's
    type, so this is the literal's input-syntax form, never SQL."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        # VARCHAR literals are bytes inside the plan. Only VARCHAR is pushable
        # (PUSHABLE_TYPES), so this is text, never an opaque VARBINARY value.
        return value.decode("utf-8")
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, datetime.date):
        return value.isoformat()
    raise NotSupportedError(
        f"cannot push a {type(value).__name__} literal into a PostgreSQL scan"
    )


def _operand_sql(table: PostgresTable, node, params: List[Optional[str]]) -> str:
    if node.node_type == NodeType.IDENTIFIER:
        return _quote_identifier(table.pg_name(node.schema_column))
    if node.node_type == NodeType.LITERAL:
        params.append(_literal_text(node.value))
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


def build_scan_statement(
    table: PostgresTable, columns: list, predicates: Optional[list], limit: Optional[int]
) -> ScanStatement:
    """The statement the native Source runs for one scan.

    `columns` are the plan's projected LogicalColumns (emit order); an empty
    projection (a bare COUNT(*)) selects a constant so the server still streams
    one row per matching row and the Source emits zero-column morsels carrying
    the count. `predicates` are the conditions the optimizer pushed — each one
    passed `can_push` — and become `$n` parameters. `limit` is the pushed LIMIT.
    """
    params: List[Optional[str]] = []
    select_list = ", ".join(
        _quote_identifier(table.pg_name(column.schema_column)) for column in columns
    )
    sql = f"SELECT {select_list or '1'} FROM {table.qualified_name}"
    clauses = [_predicate_sql(table, predicate, params) for predicate in (predicates or [])]
    if clauses:
        sql += " WHERE " + " AND ".join(f"({clause})" for clause in clauses)
    if limit is not None:
        if int(limit) < 0:
            raise InvalidInternalStateError(f"negative LIMIT {limit} reached the PostgreSQL scan")
        sql += f" LIMIT {int(limit)}"
    return ScanStatement(sql=sql, params=params, zero_columns=not columns)
