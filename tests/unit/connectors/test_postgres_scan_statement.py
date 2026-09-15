"""PostgresConnector plan-time behaviour that needs no server: the scan
statement builder (projection, pushed predicates as bind parameters, LIMIT),
literal rendering, relation-name splitting, the OID -> ColumnType map and
config validation.

The server-facing half (describe, pushdown decisions on a real plan, the
native Source) is covered by tests/storage/test_postgres_connector.py.
"""

import decimal
import os
import sys
import types

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.postgres_connector import PostgresConnector
from opteryx.connectors.postgres_connector import PostgresTable
from opteryx.connectors.postgres_connector import _literal_text
from opteryx.connectors.postgres_connector import build_scan_statement
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import Node
from opteryx.expression import NodeType
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity


class _Node:
    """Attribute-shaped like an expression Node, for the builder only."""

    def __init__(self, node_type, value=None, left=None, right=None, centre=None, schema_column=None, type=None):
        self.node_type = node_type
        self.value = value
        self.left = left
        self.right = right
        self.centre = centre
        self.schema_column = schema_column
        # A bound LITERAL always carries its ColumnType, and the builder renders
        # temporal literals FROM that tag - their value is the physical storage
        # integer. A fixture that omits it is not a literal the planner can
        # produce, so every `_lit` here supplies one.
        self.type = type


def _typmod(precision: int, scale: int) -> int:
    return ((precision << 16) | scale) + 4


def _table(prefix="pg", **connector_kwargs) -> PostgresTable:
    gateway = PostgresConnector(host="h", dbname="d", user="u", password="p", **connector_kwargs)
    gateway._matched_prefix = prefix
    table = gateway.table_engine("pg.public.planets", telemetry=None)
    table._meta = {
        "id": ("id", 23, -1),
        "name": ("name", 25, -1),
        "mass": ("mass", 1700, _typmod(6, 1)),
        'weird"name': ('weird"name', 25, -1),
    }
    return table


def _schema_column(name):
    return SchemaColumn(name=name, column_type=_lt.INT64, identity=mint_column_identity("planets", name))


def _col(name):
    return _Node(NodeType.IDENTIFIER, value=name, schema_column=_schema_column(name))


def _lit(value, column_type=_lt.INT64):
    return _Node(NodeType.LITERAL, value=value, type=column_type)


def _projection(*names):
    return [types.SimpleNamespace(schema_column=_schema_column(name)) for name in names]


def _cmp(op, left, right):
    return _Node(NodeType.COMPARISON_OPERATOR, value=op, left=left, right=right)


# ---- statement builder -------------------------------------------------------


def test_projection_only():
    statement = build_scan_statement(_table(), _projection("id", "name"), None, None)
    assert statement.sql == 'SELECT "id", "name" FROM "public"."planets"'
    assert statement.params == []
    assert statement.zero_columns is False


def test_zero_projection_selects_a_constant():
    statement = build_scan_statement(_table(), [], None, None)
    assert statement.sql == 'SELECT 1 FROM "public"."planets"'
    assert statement.zero_columns is True


def test_predicates_become_bind_parameters_in_order():
    predicates = [_cmp("Eq", _col("name"), _lit(b"Earth", _lt.VARCHAR)), _cmp("Gt", _col("mass"), _lit(1))]
    statement = build_scan_statement(_table(), _projection("name"), predicates, None)
    assert statement.sql == (
        'SELECT "name" FROM "public"."planets" WHERE ("name" = $1) AND ("mass" > $2)'
    )
    assert statement.params == ["Earth", "1"]


def test_every_comparison_operator_translates():
    expected = {"Eq": "=", "NotEq": "<>", "Gt": ">", "GtEq": ">=", "Lt": "<", "LtEq": "<=", "Like": "LIKE", "NotLike": "NOT LIKE"}
    for op, sql in expected.items():
        statement = build_scan_statement(_table(), _projection("id"), [_cmp(op, _col("id"), _lit(1))], None)
        assert statement.sql.endswith(f'WHERE ("id" {sql} $1)'), op


def test_literal_on_the_left_is_kept_on_the_left():
    statement = build_scan_statement(_table(), _projection("id"), [_cmp("Lt", _lit(5), _col("id"))], None)
    assert statement.sql.endswith('WHERE ($1 < "id")')
    assert statement.params == ["5"]


def test_between_and_null_tests():
    between = _Node(NodeType.BETWEEN, value=(True, True), left=_col("id"), right=_lit(2), centre=_lit(4))
    not_null = _Node(NodeType.UNARY_OPERATOR, value="IsNotNull", centre=_col("name"))
    is_null = _Node(NodeType.UNARY_OPERATOR, value="IsNull", centre=_col("mass"))
    statement = build_scan_statement(_table(), _projection("id"), [between, not_null, is_null], None)
    assert statement.sql.endswith(
        'WHERE ("id" BETWEEN $1 AND $2) AND ("name" IS NOT NULL) AND ("mass" IS NULL)'
    )
    assert statement.params == ["2", "4"]


def test_limit_is_appended():
    statement = build_scan_statement(_table(), _projection("id"), None, 3)
    assert statement.sql.endswith(' FROM "public"."planets" LIMIT 3')


def test_negative_limit_is_an_internal_error():
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(_table(), _projection("id"), None, -1)


def test_identifiers_are_quoted_with_embedded_quotes_doubled():
    statement = build_scan_statement(_table(), _projection('weird"name'), None, None)
    assert statement.sql == 'SELECT "weird""name" FROM "public"."planets"'


def test_column_lookup_is_case_insensitive():
    statement = build_scan_statement(_table(), _projection("NAME"), None, None)
    assert statement.sql == 'SELECT "name" FROM "public"."planets"'


def test_unknown_column_fails_loud():
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(_table(), _projection("nope"), None, None)


def test_unsupported_predicate_shapes_fail_loud():
    table = _table()
    disjunction = _Node(NodeType.OR, left=_cmp("Eq", _col("id"), _lit(1)), right=_cmp("Eq", _col("id"), _lit(2)))
    with pytest.raises(NotSupportedError):
        build_scan_statement(table, _projection("id"), [disjunction], None)
    open_between = _Node(NodeType.BETWEEN, value=(False, True), left=_col("id"), right=_lit(2), centre=_lit(4))
    with pytest.raises(NotSupportedError):
        build_scan_statement(table, _projection("id"), [open_between], None)
    function_operand = _cmp("Eq", _Node(NodeType.FUNCTION, value="LENGTH"), _lit(3))
    with pytest.raises(NotSupportedError):
        build_scan_statement(table, _projection("id"), [function_operand], None)
    unknown_op = _cmp("InList", _col("id"), _lit([1, 2]))
    with pytest.raises(NotSupportedError):
        build_scan_statement(table, _projection("id"), [unknown_op], None)


# ---- literal rendering -------------------------------------------------------


def test_literal_rendering():
    assert _literal_text(_lit(None, _lt.INT64)) is None
    assert _literal_text(_lit(True, _lt.BOOLEAN)) == "true"
    assert _literal_text(_lit(False, _lt.BOOLEAN)) == "false"
    assert _literal_text(_lit(42)) == "42"
    assert _literal_text(_lit(1.5, _lt.FLOAT64)) == "1.5"
    assert _literal_text(_lit(decimal.Decimal("1.50"), _lt.DECIMAL(6, 2))) == "1.50"
    assert _literal_text(_lit("text", _lt.VARCHAR)) == "text"
    assert _literal_text(_lit(b"bytes", _lt.VARCHAR)) == "bytes"
    with pytest.raises(NotSupportedError):
        _literal_text(_lit([1, 2], _lt.VARCHAR))


def test_temporal_literals_render_from_the_type_tag_not_the_value():
    # The planner hands a temporal literal its PHYSICAL storage integer: a DATE
    # is days since the epoch, a TIMESTAMP microseconds. Dispatching on the
    # Python type sent '10470' to the server as a date ('invalid input syntax
    # for type date'), so the tag is what decides.
    assert _literal_text(_lit(10470, _lt.DATE)) == "1998-09-01"
    assert _literal_text(_lit(0, _lt.DATE)) == "1970-01-01"
    assert _literal_text(_lit(-1, _lt.DATE)) == "1969-12-31"
    assert _literal_text(_lit(904644672000000, _lt.TIMESTAMP())) == "1998-09-01T10:11:12.000000"
    # ... and the same integer under an INTEGER tag is still the integer.
    assert _literal_text(_lit(10470)) == "10470"


def test_a_literal_without_a_type_tag_is_not_pushable():
    # An untagged 10470 cannot be told from an epoch day count, so it renders as
    # nothing rather than as a guess.
    with pytest.raises(NotSupportedError):
        _literal_text(_Node(NodeType.LITERAL, value=10470))


def test_pre_common_era_temporal_literals_are_declined():
    # The formatters spell year 0 and earlier in a form PostgreSQL cannot read
    # back (it wants a `BC` suffix), so those decline rather than mis-bind.
    with pytest.raises(NotSupportedError):
        _literal_text(_lit(-800000, _lt.DATE))
    with pytest.raises(NotSupportedError):
        _literal_text(_lit(-800000 * 86400 * 1000000, _lt.TIMESTAMP()))


def test_can_push_declines_a_predicate_holding_an_unrenderable_literal():
    # The gate and the builder MUST agree: build_scan_statement has no fallback,
    # so a predicate can_push admits and _literal_text then refuses is a failed
    # query, not a missed pushdown. Real expression Nodes here - the gate walks
    # the tree with the engine's own traversal.
    def _predicate(literal):
        return types.SimpleNamespace(
            condition=Node(
                NodeType.COMPARISON_OPERATOR,
                value="Gt",
                left=Node(NodeType.IDENTIFIER, value="id", schema_column=_schema_column("id")),
                right=literal,
            )
        )

    table = _table()
    # A pre-1 CE DATE is the shape the builder cannot spell.
    assert table.can_push(_predicate(Node(NodeType.LITERAL, value=-800000, type=_lt.DATE))) is False
    assert table.can_push(_predicate(Node(NodeType.LITERAL, value=10470, type=_lt.DATE))) is True


# ---- relation names ----------------------------------------------------------


def test_split_relation_strips_the_workspace_and_defaults_the_schema():
    gateway = PostgresConnector(host="h", dbname="d", user="u", password="p", schema="app")
    gateway._matched_prefix = "pg"
    assert gateway.split_relation("pg.public.planets") == ("public", "planets")
    assert gateway.split_relation("pg.planets") == ("app", "planets")
    assert gateway.split_relation("planets") == ("app", "planets")
    with pytest.raises(UnsupportedSyntaxError):
        gateway.split_relation("pg.a.b.c")
    with pytest.raises(UnsupportedSyntaxError):
        gateway.split_relation("other.a.b")


def test_split_relation_case_handling():
    folded = PostgresConnector(host="h", dbname="d", user="u", password="p")
    folded._matched_prefix = "pg"
    assert folded.split_relation("pg.public.planets", "pg.Public.Planets") == ("public", "planets")
    preserved = PostgresConnector(host="h", dbname="d", user="u", password="p", preserve_sql_case=True)
    preserved._matched_prefix = "pg"
    assert preserved.split_relation("pg.public.planets", "pg.Public.Planets") == ("Public", "Planets")


# ---- type mapping --------------------------------------------------------------


def test_column_type_mapping():
    table = _table()
    assert table._column_type("c", 20, -1) is _lt.INT64
    assert table._column_type("c", 23, -1) is _lt.INT32
    assert table._column_type("c", 16, -1) is _lt.BOOLEAN
    assert table._column_type("c", 25, -1) is _lt.VARCHAR
    assert table._column_type("c", 17, -1) is _lt.VARBINARY
    assert table._column_type("c", 3802, -1) is _lt.VARIANT
    assert table._column_type("c", 1082, -1) is _lt.DATE
    assert table._column_type("c", 1184, -1) == _lt.TIMESTAMP()
    small = table._column_type("c", 1700, _typmod(6, 1))
    assert small == _lt.DECIMAL(6, 1) and small.physical == _lt.DECIMAL(6, 1).physical
    wide = table._column_type("c", 1700, _typmod(20, 2))
    assert wide == _lt.DECIMAL(20, 2)
    assert wide.physical != small.physical  # int128 tier above 18 digits


def test_numeric_without_declared_scale_is_refused():
    with pytest.raises(NotSupportedError, match="numeric\\(p, s\\)"):
        _table()._column_type("amount", 1700, -1)


def test_unsupported_type_is_refused_by_name():
    with pytest.raises(NotSupportedError, match="interval"):
        _table()._column_type("gap", 1186, -1)
    with pytest.raises(NotSupportedError, match="int4\\[\\]"):
        _table()._column_type("ids", 1007, -1)


def test_row_estimate_semantics():
    table = _table()
    assert table._row_estimate(lambda config, sql, params: [["12.0"]]) == 12
    assert table._row_estimate(lambda config, sql, params: [["-1"]]) is None
    assert table._row_estimate(lambda config, sql, params: [["0"]]) is None
    assert table._row_estimate(lambda config, sql, params: [[None]]) is None
    assert table._row_estimate(lambda config, sql, params: []) is None


def test_row_estimate_statement_is_chosen_by_dialect():
    """CockroachDB leaves pg_class.reltuples NULL for every relation, so the
    PostgreSQL statement answers nothing there and every leaf plans at the
    estimator's unknown-row-count stand-in. The binding declares which server
    it is; the statement follows that declaration and nothing else."""
    seen = []

    def _capture(config, sql, params):
        seen.append((sql, params))
        return [["5"]]

    postgres = _table()
    assert postgres.gateway.dialect == "postgres"
    assert postgres._row_estimate(_capture) == 5
    assert "pg_catalog.pg_class" in seen[-1][0]
    assert seen[-1][1] == ["public", "planets"]

    cockroach = _table(dialect="cockroach")
    assert cockroach._row_estimate(_capture) == 5
    # The relation is named inline (SHOW STATISTICS takes an identifier, not a
    # bind parameter), quoted the same way every other statement quotes it.
    assert "SHOW STATISTICS FOR TABLE \"public\".\"planets\"" in seen[-1][0]
    assert seen[-1][1] == []

    # A relation the server holds no statistics for stays unknown - never a guess.
    assert cockroach._row_estimate(lambda config, sql, params: []) is None


# ---- statistics manifest ---------------------------------------------------------


class _FakeHandle:
    def __init__(self, data):
        self._data = data
        self.closed = False

    @property
    def memoryview(self):
        return memoryview(self._data)

    def close(self):
        self.closed = True


class _FakeGcs:
    """Stands in for OpteryxGcsFileSystem. `stat` is what get_file_info does:
    either None (the object is there) or an HttpStatusError to raise."""

    last = None

    def __init__(self, bucket=None, stat=None, data=b""):
        self.bucket = bucket
        self._stat = stat
        self._data = data
        self.handle = None
        self.opened = None
        _FakeGcs.last = self

    def get_file_info(self, path):
        if self._stat is not None:
            raise self._stat
        return object()

    def open_input_file(self, path):
        self.opened = path
        self.handle = _FakeHandle(self._data)
        return self.handle


def _stats_table(monkeypatch, **fake_kwargs):
    from opteryx.connectors.io_systems import gcs_filesystem

    monkeypatch.setattr(
        gcs_filesystem,
        "OpteryxGcsFileSystem",
        lambda **kwargs: _FakeGcs(**{**kwargs, **fake_kwargs}),
    )
    return _table(gcs_bucket="metadata-bucket")


def test_no_manifest_location_reads_nothing():
    """No bucket configured is not a failure and is not a read - the deployment
    simply keeps no statistics for this binding."""
    table = _table()
    assert table.gateway.stats_manifest_path("public", "planets") is None
    assert table._read_stats_manifest(None) is None


def test_an_absent_manifest_is_not_an_error(monkeypatch):
    """404: this relation's statistics have never been refreshed. The ordinary
    state of a newly-bound relation - plan with the row estimate alone."""
    from opteryx.compiled.http_client import HttpStatusError

    table = _stats_table(monkeypatch, stat=HttpStatusError("HTTP 404: gone", 404))
    assert table._read_stats_manifest(None) is None
    assert _FakeGcs.last.opened is None  # never attempted the read


@pytest.mark.parametrize("status", [403, 500, 0])
def test_a_manifest_that_cannot_be_read_is_an_error(monkeypatch, status):
    """A refused credential, a broken bucket, a transport failure that never got
    a response (status 0) - none of these mean "no statistics". They used to be
    swallowed by a bare `except Exception`, which made them invisible."""
    from opteryx.compiled.http_client import HttpStatusError

    table = _stats_table(monkeypatch, stat=HttpStatusError("HTTP boom", status))
    with pytest.raises(HttpStatusError):
        table._read_stats_manifest(None)


def test_a_present_manifest_is_actually_read(monkeypatch):
    """The read path runs and releases the handle. It could not before: GcsFile
    is not a context manager, so the `with` that used to wrap this raised
    TypeError on EVERY call and the bare except turned it into "no manifest" -
    the manifest was unreadable for every relation, whatever storage held."""
    from opteryx.models import manifest_io

    seen = {}

    def _fake_read(data):
        seen["data"] = data
        return [], None

    monkeypatch.setattr(manifest_io, "read_manifest_file_entries", _fake_read)
    table = _stats_table(monkeypatch, data=b"MANIFEST-BYTES")

    assert table._read_stats_manifest(None) is None  # no entries -> no manifest
    assert seen["data"] == b"MANIFEST-BYTES"
    assert _FakeGcs.last.opened.endswith("/metadata/manifest-planets.parquet")
    assert _FakeGcs.last.handle.closed is True


# ---- connector config ------------------------------------------------------------


def test_connector_config_validation():
    with pytest.raises(ValueError, match="unknown configuration keys"):
        PostgresConnector(host="h", dbname="d", user="u", password="p", bogus=1)
    with pytest.raises(ValueError, match="sslmode"):
        PostgresConnector(host="h", dbname="d", user="u", password="p", sslmode="prefer")
    with pytest.raises(ValueError, match="dialect"):
        PostgresConnector(host="h", dbname="d", user="u", password="p", dialect="mysql")
    with pytest.raises(ValueError, match="'host'"):
        PostgresConnector(host="", dbname="d", user="u", password="p")
    connector = PostgresConnector(host="h", dbname="d", user="u", password="secret", port="5433")
    assert connector.connection_config["port"] == 5433
    assert "secret" not in repr(connector)


def test_table_declares_its_physical_reader_and_capabilities():
    table = _table()
    assert table.scan_reader == "Postgres Reader"
    assert table.supports_predicate_pushdown is True
    assert table.supports_limit_pushdown is True
    assert table.supports_filtered_limit_pushdown is True
    assert table.supports_topn_pushdown is True
    assert table.supports_aggregate_pushdown is True
    assert table.supports_distinct_pushdown is True
    assert table.supports_diachronic is False
    assert table.supports_version_travel is False
    assert table.PUSHABLE_SCALAR_FUNCTIONS is False
    with pytest.raises(InvalidInternalStateError):
        table.read_dataset()


# ---- remote pushdown shapes: top-N, aggregate, DISTINCT ---------------------------


def _agg(function, operand, result_type, name="agg", distinct=False):
    node = _Node(NodeType.AGGREGATOR, value=function)
    node.parameters = [operand]
    node.duplicate_treatment = "Distinct" if distinct else None
    node.schema_column = SchemaColumn(
        name=name, column_type=result_type, identity=mint_column_identity("planets", name)
    )
    return node


def _wild():
    return _Node(NodeType.WILDCARD)


def _key(name, column_type):
    """A stamped top-N key: the scan carries SchemaColumns, not expression nodes."""
    return _typed_col(name, column_type).schema_column


def _typed_col(name, column_type):
    return _Node(
        NodeType.IDENTIFIER,
        value=name,
        schema_column=SchemaColumn(
            name=name, column_type=column_type, identity=mint_column_identity("planets", name)
        ),
    )


def test_emit_describes_the_plain_projection():
    statement = build_scan_statement(_table(), _projection("id", "mass"), None, None)
    assert [(e.oid, e.physical) for e in statement.emit] == [
        (23, _lt.INT64.physical.value),
        (1700, _lt.INT64.physical.value),  # the fixture types every column INT64
    ]
    assert [e.precision for e in statement.emit] == [0, 0]


def test_topn_renders_explicit_null_order_and_c_collation_for_text():
    order_by = [(_key("mass", _lt.INT64), False), (_key("name", _lt.VARCHAR), True)]
    statement = build_scan_statement(
        _table(), _projection("id"), None, None, order_by=order_by, topn_limit=7
    )
    assert statement.sql == (
        'SELECT "id" FROM "public"."planets" '
        'ORDER BY "mass" DESC NULLS LAST, "name" COLLATE "C" ASC NULLS FIRST LIMIT 7'
    )


def test_topn_after_predicates_is_one_statement():
    statement = build_scan_statement(
        _table(), _projection("id"), [_cmp("Eq", _col("id"), _lit(1))], None,
        order_by=[(_key("id", _lt.INT64), True)], topn_limit=3,
    )
    assert statement.sql.endswith('WHERE ("id" = $1) ORDER BY "id" ASC NULLS FIRST LIMIT 3')


def test_topn_and_limit_together_is_an_internal_error():
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(
            _table(), _projection("id"), None, 5,
            order_by=[(_key("id", _lt.INT64), True)], topn_limit=3,
        )


def test_order_by_without_a_limit_is_an_internal_error():
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(
            _table(), _projection("id"), None, None, order_by=[(_key("id", _lt.INT64), True)]
        )


def test_can_push_topn_requires_own_pushable_columns():
    table = _table()
    assert table.can_push_topn([(_typed_col("id", _lt.INT64), True)]) is True
    assert table.can_push_topn([(_typed_col("id", _lt.INT64), True), (_typed_col("name", _lt.VARCHAR), False)]) is True
    assert table.can_push_topn([(_typed_col("nope", _lt.INT64), True)]) is False
    assert table.can_push_topn([(_lit(1), True)]) is False
    assert table.can_push_topn([]) is False


def test_grouped_aggregate_statement_and_emit():
    groups = [_typed_col("id", _lt.INT64)]
    aggregates = [
        _agg("COUNT", _wild(), _lt.INT64, "count_star"),
        _agg("SUM", _typed_col("mass", _lt.DECIMAL(6, 1)), _lt.DECIMAL(6, 1), "sum_mass"),
        _agg("MIN", _typed_col("name", _lt.VARCHAR), _lt.VARCHAR, "min_name"),
        _agg("AVG", _typed_col("id", _lt.INT32), _lt.FLOAT64, "avg_id"),
    ]
    table = _table()
    assert table.can_push_aggregate(groups, aggregates) is True
    statement = build_scan_statement(
        table, [], [_cmp("Gt", _col("mass"), _lit(1))], None, groups=groups, aggregates=aggregates
    )
    assert statement.sql == (
        'SELECT "id", count(*), sum("mass"), min("name" COLLATE "C")::text, avg("id")::float8 '
        'FROM "public"."planets" WHERE ("mass" > $1) GROUP BY "id"'
    )
    assert statement.zero_columns is False
    assert [e.oid for e in statement.emit] == [23, 20, 1700, 25, 701]
    assert [e.identity for e in statement.emit] == [
        groups[0].schema_column.identity,
        *(a.schema_column.identity for a in aggregates),
    ]
    sum_emit = statement.emit[2]
    assert (sum_emit.precision, sum_emit.scale) == (6, 1)


def test_ungrouped_count_star_is_a_single_column_statement():
    aggregates = [_agg("COUNT", _wild(), _lt.INT64)]
    statement = build_scan_statement(_table(), [], None, None, groups=[], aggregates=aggregates)
    assert statement.sql == 'SELECT count(*) FROM "public"."planets"'
    assert statement.zero_columns is False
    assert statement.emit[0].oid == 20


def test_sum_over_integers_is_pinned_to_int8():
    aggregates = [_agg("SUM", _typed_col("id", _lt.INT32), _lt.INT64)]
    statement = build_scan_statement(_table(), [], None, None, groups=[], aggregates=aggregates)
    assert statement.sql == 'SELECT sum("id")::int8 FROM "public"."planets"'


def test_count_distinct_and_count_column():
    aggregates = [
        _agg("COUNT", _typed_col("name", _lt.VARCHAR), _lt.INT64, "c", distinct=True),
        _agg("COUNT", _typed_col("name", _lt.VARCHAR), _lt.INT64, "d"),
    ]
    statement = build_scan_statement(_table(), [], None, None, groups=[], aggregates=aggregates)
    assert statement.sql == 'SELECT count(DISTINCT "name"), count("name") FROM "public"."planets"'


@pytest.mark.parametrize(
    "aggregate",
    [
        _agg("SUM", _typed_col("name", _lt.VARCHAR), _lt.VARCHAR),  # no server overload
        _agg("SUM", _typed_col("id", _lt.BOOLEAN), _lt.BOOLEAN),
        _agg("MIN", _typed_col("id", _lt.BOOLEAN), _lt.BOOLEAN),
        _agg("SUM", _typed_col("id", _lt.INT32), _lt.INT32),  # bound type is not what the wire returns
        _agg("AVG", _typed_col("id", _lt.INT32), _lt.INT32),
        _agg("SUM", _typed_col("id", _lt.INT64), _lt.INT64, distinct=True),  # SUM(DISTINCT)
        _agg("COUNT", _wild(), _lt.INT64, distinct=True),  # COUNT(DISTINCT *)
        _agg("STDDEV", _typed_col("id", _lt.INT64), _lt.FLOAT64),
        _agg("ANY_VALUE", _typed_col("id", _lt.INT64), _lt.INT64),
        _agg("MEDIAN", _typed_col("id", _lt.INT64), _lt.FLOAT64),
        _agg("MAX", _typed_col("nope", _lt.INT64), _lt.INT64),  # not this relation's column
        _agg("MAX", _lit(1), _lt.INT64),  # not a column at all
    ],
)
def test_aggregates_without_an_exact_remote_spelling_are_declined(aggregate):
    table = _table()
    assert table.can_push_aggregate([], [aggregate]) is False
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, [], None, None, groups=[], aggregates=[aggregate])


def test_text_min_max_over_char_n_is_declined():
    table = _table()
    table._meta["padded"] = ("padded", 1042, 14)
    padded = _typed_col("padded", _lt.VARCHAR)
    assert table.can_push_aggregate([], [_agg("MAX", padded, _lt.VARCHAR)]) is False
    # ...but the column is still a fine DISTINCT / GROUP BY key and top-N key.
    assert table.can_push_distinct([padded]) is True
    assert table.can_push_topn([(padded, True)]) is True


def test_group_key_must_be_an_own_column():
    table = _table()
    aggregates = [_agg("COUNT", _wild(), _lt.INT64)]
    assert table.can_push_aggregate([_typed_col("nope", _lt.INT64)], aggregates) is False
    assert table.can_push_aggregate([_lit(1)], aggregates) is False


def test_distinct_statement():
    table = _table()
    columns = [_typed_col("id", _lt.INT64), _typed_col("name", _lt.VARCHAR)]
    assert table.can_push_distinct(columns) is True
    assert table.can_push_distinct([]) is False
    statement = build_scan_statement(
        table, columns, [_cmp("Eq", _col("id"), _lit(1))], None, distinct=True
    )
    assert statement.sql == 'SELECT DISTINCT "id", "name" FROM "public"."planets" WHERE ("id" = $1)'
    assert [e.oid for e in statement.emit] == [23, 25]


def test_distinct_and_aggregate_together_is_an_internal_error():
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(
            _table(), _projection("id"), None, None,
            groups=[], aggregates=[_agg("COUNT", _wild(), _lt.INT64)], distinct=True,
        )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
