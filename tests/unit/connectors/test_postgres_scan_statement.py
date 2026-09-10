"""PostgresConnector plan-time behaviour that needs no server: the scan
statement builder (projection, pushed predicates as bind parameters, LIMIT),
literal rendering, relation-name splitting, the OID -> ColumnType map and
config validation.

The server-facing half (describe, pushdown decisions on a real plan, the
native Source) is covered by tests/storage/test_postgres_connector.py.
"""

import datetime
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
from opteryx.expression import NodeType
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity


class _Node:
    """Attribute-shaped like an expression Node, for the builder only."""

    def __init__(self, node_type, value=None, left=None, right=None, centre=None, schema_column=None):
        self.node_type = node_type
        self.value = value
        self.left = left
        self.right = right
        self.centre = centre
        self.schema_column = schema_column


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


def _lit(value):
    return _Node(NodeType.LITERAL, value=value)


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
    predicates = [_cmp("Eq", _col("name"), _lit(b"Earth")), _cmp("Gt", _col("mass"), _lit(1))]
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
    assert _literal_text(None) is None
    assert _literal_text(True) == "true"
    assert _literal_text(False) == "false"
    assert _literal_text(42) == "42"
    assert _literal_text(1.5) == "1.5"
    assert _literal_text(decimal.Decimal("1.50")) == "1.50"
    assert _literal_text("text") == "text"
    assert _literal_text(b"bytes") == "bytes"
    assert _literal_text(datetime.datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02 03:04:05"
    assert _literal_text(datetime.date(2024, 1, 2)) == "2024-01-02"
    with pytest.raises(NotSupportedError):
        _literal_text([1, 2])


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


# ---- connector config ------------------------------------------------------------


def test_connector_config_validation():
    with pytest.raises(ValueError, match="unknown configuration keys"):
        PostgresConnector(host="h", dbname="d", user="u", password="p", bogus=1)
    with pytest.raises(ValueError, match="sslmode"):
        PostgresConnector(host="h", dbname="d", user="u", password="p", sslmode="prefer")
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
    assert table.supports_diachronic is False
    assert table.supports_version_travel is False
    assert table.PUSHABLE_SCALAR_FUNCTIONS is False
    with pytest.raises(InvalidInternalStateError):
        table.read_dataset()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
