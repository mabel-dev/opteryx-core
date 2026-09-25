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
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Function
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import Not

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.postgres_connector import PostgresConnector
from opteryx.connectors.postgres_connector import PostgresTable
from opteryx.connectors.postgres_connector import _UNRENDERABLE
from opteryx.connectors.postgres_connector import _render_literal
from opteryx.connectors.postgres_connector import build_scan_statement
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity
from opteryx.compiled.structures.expressions import LogicalColumn


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


def _schema_column(name, column_type=_lt.INT64):
    return SchemaColumn(name=name, column_type=column_type, identity=mint_column_identity("planets", name))


def _col(name, column_type=_lt.INT64):
    return _Node(NodeType.IDENTIFIER, value=name, schema_column=_schema_column(name, column_type))


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
    # `can_push` trial-renders through the same function the builder uses, so a
    # shape that reaches the builder unspellable means the two disagreed: an
    # engine inconsistency, raised as one. (Called directly here, bypassing the
    # gate that would have declined each of these.)
    table = _table()
    disjunction = _Node(NodeType.OR, left=_cmp("Eq", _col("id"), _lit(1)), right=_cmp("Eq", _col("id"), _lit(2)))
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, _projection("id"), [disjunction], None)
    open_between = _Node(NodeType.BETWEEN, value=(False, True), left=_col("id"), right=_lit(2), centre=_lit(4))
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, _projection("id"), [open_between], None)
    function_operand = _cmp("Eq", _Node(NodeType.FUNCTION, value="LENGTH"), _lit(3))
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, _projection("id"), [function_operand], None)
    unknown_op = _cmp("Overlaps", _col("id"), _lit(1))
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, _projection("id"), [unknown_op], None)
    # An IN-list whose literal carries a scalar type tag rather than ARRAY<...>
    # has no element type to render its members from.
    untyped_list = _cmp("InList", _col("id"), _lit([1, 2]))
    with pytest.raises(InvalidInternalStateError):
        build_scan_statement(table, _projection("id"), [untyped_list], None)


# ---- literal rendering -------------------------------------------------------


def test_literal_rendering():
    assert _render_literal(_lit(None, _lt.INT64)) is None
    assert _render_literal(_lit(True, _lt.BOOLEAN)) == "true"
    assert _render_literal(_lit(False, _lt.BOOLEAN)) == "false"
    assert _render_literal(_lit(42)) == "42"
    assert _render_literal(_lit(1.5, _lt.FLOAT64)) == "1.5"
    assert _render_literal(_lit(decimal.Decimal("1.50"), _lt.DECIMAL(6, 2))) == "1.50"
    assert _render_literal(_lit("text", _lt.VARCHAR)) == "text"
    assert _render_literal(_lit(b"bytes", _lt.VARCHAR)) == "bytes"
    assert _render_literal(_lit([1, 2], _lt.VARCHAR)) is _UNRENDERABLE


def test_temporal_literals_render_from_the_type_tag_not_the_value():
    # The planner hands a temporal literal its PHYSICAL storage integer: a DATE
    # is days since the epoch, a TIMESTAMP microseconds. Dispatching on the
    # Python type sent '10470' to the server as a date ('invalid input syntax
    # for type date'), so the tag is what decides.
    assert _render_literal(_lit(10470, _lt.DATE)) == "1998-09-01"
    assert _render_literal(_lit(0, _lt.DATE)) == "1970-01-01"
    assert _render_literal(_lit(-1, _lt.DATE)) == "1969-12-31"
    assert _render_literal(_lit(904644672000000, _lt.TIMESTAMP())) == "1998-09-01T10:11:12.000000"
    # ... and the same integer under an INTEGER tag is still the integer.
    assert _render_literal(_lit(10470)) == "10470"


def test_a_literal_without_a_type_tag_is_not_pushable():
    # An untagged 10470 cannot be told from an epoch day count, so it renders as
    # nothing rather than as a guess.
    assert _render_literal(_Node(NodeType.LITERAL, value=10470)) is _UNRENDERABLE


def test_pre_common_era_temporal_literals_are_declined():
    # The formatters spell year 0 and earlier in a form PostgreSQL cannot read
    # back (it wants a `BC` suffix), so those decline rather than mis-bind.
    assert _render_literal(_lit(-800000, _lt.DATE)) is _UNRENDERABLE
    assert _render_literal(_lit(-800000 * 86400 * 1000000, _lt.TIMESTAMP())) is _UNRENDERABLE


def test_can_push_declines_a_predicate_holding_an_unrenderable_literal():
    # The gate and the builder MUST agree: build_scan_statement has no fallback,
    # so a predicate can_push admits and the renderer then refuses is a failed
    # query, not a missed pushdown. Real expression Nodes here - the gate walks
    # the tree with the engine's own traversal.
    def _predicate(literal):
        return types.SimpleNamespace(
            condition=Comparison(
                value="Gt",
                left=LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="id", schema_column=_schema_column("id")),
                right=literal,
            )
        )

    table = _table()
    # A pre-1 CE DATE is the shape the builder cannot spell.
    assert table.can_push(_predicate(Literal(value=-800000, type=_lt.DATE))) is False
    assert table.can_push(_predicate(Literal(value=10470, type=_lt.DATE))) is True


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


def test_nothing_about_a_char_n_value_is_pushed():
    # PostgreSQL reads a char(n) with its trailing blanks removed and the scan
    # hands the engine the padded value, so EVERY shape that reads the value
    # answers differently on the two sides — MIN/MAX and a LIKE match, but also a
    # GROUP BY or DISTINCT (which folds together two values the engine keeps
    # apart) and a top-N (which orders them differently). These used to be
    # allowed; a pushed `= 'ab'` matched 'ab  ' on the server and nothing in the
    # engine.
    table = _table()
    table._meta["padded"] = ("padded", 1042, 14)
    padded = _typed_col("padded", _lt.VARCHAR)
    assert table.can_push_aggregate([], [_agg("MAX", padded, _lt.VARCHAR)]) is False
    assert table.can_push_aggregate([padded], [_agg("COUNT", _wild(), _lt.INT64)]) is False
    assert table.can_push_distinct([padded]) is False
    assert table.can_push_topn([(padded, True)]) is False
    for predicate in (
        _cmp("Eq", _col("padded", _lt.VARCHAR), _lit(b"ab", _lt.VARCHAR)),
        _cmp("Like", _col("padded", _lt.VARCHAR), _lit(b"ab%", _lt.VARCHAR)),
        _Node(
            NodeType.BETWEEN,
            value=(True, True),
            left=_col("padded", _lt.VARCHAR),
            right=_lit(b"a", _lt.VARCHAR),
            centre=_lit(b"b", _lt.VARCHAR),
        ),
    ):
        with pytest.raises(InvalidInternalStateError):
            build_scan_statement(table, _projection("id"), [predicate], None)
    # The column is still read and returned; only pushing work about its value is
    # refused. A null test reads no value, so it still pushes.
    assert build_scan_statement(table, _projection("padded"), None, None).sql == (
        'SELECT "padded" FROM "public"."planets"'
    )
    null_test = _Node(NodeType.UNARY_OPERATOR, value="IsNull", centre=_col("padded", _lt.VARCHAR))
    statement = build_scan_statement(table, _projection("id"), [null_test], None)
    assert statement.sql.endswith('WHERE ("padded" IS NULL)')


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


# ---- LIKE and IN ------------------------------------------------------------
#
# A LIKE predicate does NOT reach a connector spelled as a LIKE:
# PredicateRewriteStrategy lowers the anchored patterns to `_STARTS_WITH` /
# `_ENDS_WITH` FUNCTION nodes (negated: a NOT wrapping one) and the unanchored
# `'%x%'` to an `InStr` comparison, all BEFORE predicate pushdown runs. These
# fixtures are those lowered shapes, verified against the optimizer's real
# output, and the renderer's job is to put the LIKE back.


def _fn(name, *parameters):
    node = _Node(NodeType.FUNCTION, value=name)
    node.parameters = list(parameters)
    return node


def _not(inner):
    return _Node(NodeType.NOT, centre=inner)


def _text_col(name="name"):
    return _col(name, _lt.VARCHAR)


def _where(table, predicate):
    """The WHERE clause and bind parameters a single predicate produces."""
    statement = build_scan_statement(table, _projection("id"), [predicate], None)
    return statement.sql.split(" WHERE ", 1)[1], statement.params


def test_anchored_like_is_spelled_back_as_a_like():
    table = _table()
    # `name LIKE 'Ea%'`
    clause, params = _where(table, _fn("_STARTS_WITH", _text_col(), _lit(b"Ea", _lt.VARBINARY)))
    assert clause == '("name" LIKE $1)'
    assert params == ["Ea%"]
    # `name LIKE '%th'`
    clause, params = _where(table, _fn("_ENDS_WITH", _text_col(), _lit(b"th", _lt.VARBINARY)))
    assert clause == '("name" LIKE $1)'
    assert params == ["%th"]


def test_unanchored_like_is_spelled_back_as_a_like():
    # `name LIKE '%art%'` lowers to an InStr comparison whose operand is a str.
    clause, params = _where(_table(), _cmp("InStr", _text_col(), _lit("art", _lt.VARCHAR)))
    assert clause == '("name" LIKE $1)'
    assert params == ["%art%"]


def test_negated_like_forms_are_pushed_as_not():
    table = _table()
    # `name NOT LIKE 'Ea%'` — a NOT wrapping the lowered function.
    clause, params = _where(table, _not(_fn("_STARTS_WITH", _text_col(), _lit(b"Ea", _lt.VARBINARY))))
    assert clause == '(NOT ("name" LIKE $1))'
    assert params == ["Ea%"]
    # `name NOT LIKE '%art%'`
    clause, params = _where(table, _cmp("NotInStr", _text_col(), _lit("art", _lt.VARCHAR)))
    assert clause == '(NOT ("name" LIKE $1))'
    assert params == ["%art%"]


def test_like_pattern_metacharacters_are_escaped():
    # The pattern body matched literally in the engine has to match literally on
    # the server: `%` and `_` are LIKE's wildcards and backslash is its default
    # escape, so all three are escaped. Anything else would turn `a_b` into a
    # single-character wildcard match.
    clause, params = _where(_table(), _fn("_STARTS_WITH", _text_col(), _lit(b"a_b%c\\d", _lt.VARBINARY)))
    assert clause == '("name" LIKE $1)'
    assert params == ["a\\_b\\%c\\\\d%"]


def test_case_insensitive_like_lowerings_are_not_pushed():
    # ILIKE folds case by the server's locale and by the engine's own rules; the
    # two disagree on non-ASCII text, so neither `ILike` nor anything it lowers
    # to is pushed. Declining is a missed pushdown, never a wrong answer.
    table = _table()
    for predicate in (
        _fn("_CI_STARTS_WITH", _text_col(), _lit(b"ea", _lt.VARBINARY)),
        _fn("_CI_ENDS_WITH", _text_col(), _lit(b"th", _lt.VARBINARY)),
        _cmp("IInStr", _text_col(), _lit("art", _lt.VARCHAR)),
        _cmp("NotIInStr", _text_col(), _lit("art", _lt.VARCHAR)),
    ):
        with pytest.raises(InvalidInternalStateError):
            build_scan_statement(table, _projection("id"), [predicate], None)


def test_in_list_becomes_one_bind_parameter_per_member():
    clause, params = _where(
        _table(), _cmp("InList", _text_col(), _lit([b"Earth", b"Mars"], _lt.ARRAY(_lt.VARCHAR)))
    )
    assert clause == '("name" IN ($1, $2))'
    assert params == ["Earth", "Mars"]


def test_not_in_list():
    clause, params = _where(
        _table(), _cmp("NotInList", _col("id"), _lit([1, 2, 3], _lt.ARRAY(_lt.INT64)))
    )
    assert clause == '("id" NOT IN ($1, $2, $3))'
    assert params == ["1", "2", "3"]


def test_in_list_members_render_from_the_element_type():
    # The members are physical storage integers under an ARRAY<DATE> tag, exactly
    # as a scalar DATE literal is — rendering them from the Python value would
    # send the epoch day count to the server.
    clause, params = _where(
        _table(), _cmp("InList", _col("id"), _lit([10470, 0], _lt.ARRAY(_lt.DATE)))
    )
    assert clause == '("id" IN ($1, $2))'
    assert params == ["1998-09-01", "1970-01-01"]


def test_in_list_edge_cases_are_declined():
    table = _table()
    # `IN ()` is a syntax error, and a member that cannot be spelled (a pre-1 CE
    # date) declines the whole list rather than dropping a member.
    empty = _cmp("InList", _col("id"), _lit([], _lt.ARRAY(_lt.INT64)))
    unspellable = _cmp("InList", _col("id"), _lit([10470, -800000], _lt.ARRAY(_lt.DATE)))
    # Past the cap the wire client would refuse the statement (32767 bind
    # parameters), so the gate has to decline rather than admit a failing query.
    too_long = _cmp("InList", _col("id"), _lit(list(range(1025)), _lt.ARRAY(_lt.INT64)))
    for predicate in (empty, unspellable, too_long):
        with pytest.raises(InvalidInternalStateError):
            build_scan_statement(table, _projection("id"), [predicate], None)
    # ... and one member under the cap still pushes.
    clause, _ = _where(table, _cmp("InList", _col("id"), _lit(list(range(1024)), _lt.ARRAY(_lt.INT64))))
    assert clause.startswith('("id" IN ($1, $2,')


def test_char_n_columns_decline_like_and_in():
    # PostgreSQL strips a char(n)'s trailing blanks before comparing or matching
    # it; the scan hands the engine the padded value. `'ab  '::char(4) LIKE '%b'`
    # is therefore true on the server and false in the engine, so these shapes
    # decline the column — the same reason a pushed MIN/MAX declines char(n).
    table = _table()
    table._meta["code"] = ("code", 1042, -1)  # bpchar
    padded = _col("code", _lt.VARCHAR)
    for predicate in (
        _fn("_STARTS_WITH", padded, _lit(b"ab", _lt.VARBINARY)),
        _cmp("InStr", padded, _lit("ab", _lt.VARCHAR)),
        _cmp("InList", padded, _lit([b"ab"], _lt.ARRAY(_lt.VARCHAR))),
    ):
        with pytest.raises(InvalidInternalStateError):
            build_scan_statement(table, _projection("id"), [predicate], None)


def test_the_gate_admits_exactly_what_the_builder_can_spell():
    # can_push and build_scan_statement MUST agree: the builder has no fallback,
    # so anything the gate admits and the builder then refuses is a failed query.
    # Real expression Nodes here — the gate walks the tree with the engine's own
    # traversal and asks the base capability first.
    table = _table()

    def _predicate(condition):
        return types.SimpleNamespace(condition=condition)

    def _rcol(name, column_type=_lt.VARCHAR):
        return LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=name, schema_column=_schema_column(name, column_type))

    def _rlit(value, column_type):
        return Literal(value=value, type=column_type)

    boolean = _schema_column("", _lt.BOOLEAN)
    starts_with = Function(
        value="_STARTS_WITH",
        parameters=[_rcol("name"), _rlit(b"Ea", _lt.VARBINARY)],
        schema_column=boolean,
    )
    ci_starts_with = Function(
        value="_CI_STARTS_WITH",
        parameters=[_rcol("name"), _rlit(b"ea", _lt.VARBINARY)],
        schema_column=boolean,
    )
    in_list = Comparison(
        value="InList",
        left=_rcol("name"),
        right=_rlit([b"Earth"], _lt.ARRAY(_lt.VARCHAR)),
    )
    instr = Comparison(
        value="InStr",
        left=_rcol("name"),
        right=_rlit("art", _lt.VARCHAR),
    )

    assert table.can_push(_predicate(starts_with)) is True
    assert table.can_push(_predicate(Not(centre=starts_with))) is True
    assert table.can_push(_predicate(instr)) is True
    assert table.can_push(_predicate(in_list)) is True
    # ... and the declines.
    assert table.can_push(_predicate(ci_starts_with)) is False
    assert table.can_push(_predicate(Not(centre=ci_starts_with))) is False
    assert (
        table.can_push(
            _predicate(
                Comparison(
                    value="InList",
                    left=_rcol("name"),
                    right=_rlit([10470, -800000], _lt.ARRAY(_lt.DATE)),
                )
            )
        )
        is False
    )
    # A function with no SQL spelling stays a local Filter.
    assert (
        table.can_push(
            _predicate(
                Function(
                    value="ARRAY_CONTAINS",
                    parameters=[_rcol("name"), _rlit(b"x", _lt.VARBINARY)],
                    schema_column=boolean,
                )
            )
        )
        is False
    )


def test_the_optimizer_still_lowers_like_into_the_shapes_the_renderer_spells(monkeypatch):
    """The one test here whose predicates are not hand-built.

    Everything above asserts that the renderer spells a shape correctly; this
    asserts that the shape is the one the optimizer actually produces. The two
    are different risks: `col LIKE 'x%'` reaches a connector as `_STARTS_WITH`
    only because PredicateRewriteStrategy puts it that way, and if that lowering
    changes spelling the renderer goes on passing its own fixtures while every
    real LIKE quietly stops being pushed and the server streams whole tables.

    So a real query is planned against the file connector, the conditions its
    scan gate is offered are captured, and THOSE nodes are replayed through the
    PostgreSQL gate and builder.
    """
    import opteryx
    from opteryx.connectors import DiskConnector
    from opteryx.connectors.filesystem_connector import FileSystemTable

    opteryx.register_workspace("testdata", DiskConnector)

    captured = []
    original = FileSystemTable.can_push

    def _spy(self, operator, types_=None):
        captured.append(operator.condition)
        return original(self, operator, types_)

    monkeypatch.setattr(FileSystemTable, "can_push", _spy)

    def _pushed(where):
        captured.clear()
        for _ in opteryx.session().execute_to_morsels(
            f"SELECT name FROM testdata.planets WHERE {where}"
        ):
            pass
        table = _table()
        table._meta = {"name": ("name", 25, -1), "id": ("id", 23, -1)}
        rendered = []
        for condition in captured:
            if not table.can_push(types.SimpleNamespace(condition=condition)):
                rendered.append(None)
                continue
            statement = build_scan_statement(table, _projection("name"), [condition], None)
            rendered.append((statement.sql.split(" WHERE ", 1)[1], statement.params))
        return rendered

    assert _pushed("name LIKE 'Ea%'") == [('("name" LIKE $1)', ["Ea%"])]
    assert _pushed("name LIKE '%th'") == [('("name" LIKE $1)', ["%th"])]
    assert _pushed("name LIKE '%art%'") == [('("name" LIKE $1)', ["%art%"])]
    assert _pushed("name NOT LIKE 'Ea%'") == [('(NOT ("name" LIKE $1))', ["Ea%"])]
    assert _pushed("name NOT LIKE '%th'") == [('(NOT ("name" LIKE $1))', ["%th"])]
    assert _pushed("name NOT LIKE '%art%'") == [('(NOT ("name" LIKE $1))', ["%art%"])]
    # A pattern with no single anchor is not lowered at all and pushes as the
    # LIKE it still is.
    assert _pushed("name LIKE 'E%r%h'") == [('("name" LIKE $1)', ["E%r%h"])]
    # ILIKE lowers to the case-insensitive twins, which are declined.
    assert _pushed("name ILIKE 'ea%'") == [None]
    assert _pushed("name IN ('Earth','Mars')") == [('("name" IN ($1, $2))', ["Earth", "Mars"])]
    assert _pushed("name NOT IN ('Earth','Mars')") == [
        ('("name" NOT IN ($1, $2))', ["Earth", "Mars"])
    ]
    # DisjunctiveDomainPushdownStrategy folds an OR of equalities into an
    # IN-list, so making IN pushable makes that shape pushable too.
    assert _pushed("id = 3 OR id = 4") == [('("id" IN ($1, $2))', ["3", "4"])]
