"""The type vocabulary a CREATE TABLE column may be declared in.

DDL used to carry its OWN sqlparser-key → type-name map, written by hand and
living only in the CREATE TABLE planner. It had drifted badly from what the rest
of the engine understands:

  * rejected outright — NVARCHAR, VARBINARY, DECIMAL, TIME, INTERVAL, IPV4,
    VECTOR, TIMESTAMP[unit], and every exact integer width;
  * silently WIDENED — TINYINT and SMALLINT became INT64, REAL became FLOAT64.

Declared types now resolve through the same two steps a CAST target does
(`column_type_from_ast` → `_extract_data_type` → `_normalize_cast_type`), and
fall back to the persisted-schema alias table for the spellings a stored schema
may use. §14: there is ONE type object, from schema through AST to kernels.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party.sqloxide import parse_sql


def _declared(decl: str):
    """The ColumnType a `CREATE TABLE t (c <decl>)` resolves to."""
    rewritten = do_sql_rewrite(f"CREATE TABLE t (c {decl})")
    if isinstance(rewritten, tuple):
        rewritten = rewritten[0]
    plan, _, _ = do_logical_planning_phase(parse_sql(rewritten, "mysql")[0])
    for _, node in plan.nodes(True):
        if getattr(node, "columns", None):
            return node.columns[0].column_type
    raise AssertionError(f"no columns planned for {decl}")


def test_the_reported_syslog_schema_plans():
    """The exact shape that failed: TIMESTAMP[us] parsed as a Custom type and was
    rejected, and NVARCHAR/VARBINARY would have been rejected right behind it."""
    sql = """CREATE TABLE personal.bastian.events (
        ingest_time TIMESTAMP[us],
        event_time TIMESTAMP[us],
        source_ip VARCHAR,
        facility INTEGER,
        message NVARCHAR,
        fields NVARCHAR,
        raw VARBINARY,
        parse_ok BOOLEAN
    );"""
    rewritten = do_sql_rewrite(sql)
    if isinstance(rewritten, tuple):
        rewritten = rewritten[0]
    plan, _, _ = do_logical_planning_phase(parse_sql(rewritten, "mysql")[0])
    resolved = None
    for _, node in plan.nodes(True):
        if getattr(node, "columns", None):
            resolved = {c.name: str(c.column_type) for c in node.columns}
    assert resolved == {
        "ingest_time": "TIMESTAMP[us]",
        "event_time": "TIMESTAMP[us]",
        "source_ip": "VARCHAR",
        "facility": "INT64",
        "message": "NVARCHAR",
        "fields": "NVARCHAR",
        "raw": "VARBINARY",
        "parse_ok": "BOOLEAN",
    }, resolved


@pytest.mark.parametrize(
    "decl, expected",
    [
        # …the types DDL used to reject outright
        ("NVARCHAR", "NVARCHAR"),
        ("VARBINARY", "VARBINARY"),
        ("DECIMAL(10,2)", "DECIMAL(10, 2)"),
        ("TIME", "TIME[us]"),
        ("INTERVAL", "INTERVAL"),
        ("IPV4", "IPV4"),
        ("VECTOR(384)", "VECTOR(384)"),
        ("INT8", "INT8"),
        ("INT16", "INT16"),
        ("INT32", "INT32"),
        ("UINT8", "UINT8"),
        ("UINT32", "UINT32"),
        ("UINT64", "UINT64"),
        ("FLOAT32", "FLOAT32"),
        # …and the ones it already accepted, which must not have moved
        ("VARCHAR", "VARCHAR"),
        ("BOOLEAN", "BOOLEAN"),
        ("INTEGER", "INT64"),
        ("DOUBLE", "FLOAT64"),
        ("DATE", "DATE"),
        ("TIMESTAMP", "TIMESTAMP[us]"),
        ("BLOB", "VARBINARY"),
    ],
)
def test_declared_column_types_resolve(decl, expected):
    assert str(_declared(decl)) == expected, decl


@pytest.mark.parametrize(
    "unit, expected",
    [("ns", "NANOSECONDS"), ("us", "MICROSECONDS"), ("ms", "MILLISECONDS"), ("s", "SECONDS")],
)
def test_timestamp_unit_is_part_of_the_declared_type(unit, expected):
    """TIMESTAMP[us] is what prompted all of this — sqlparser sees the rewriter's
    internal form as a Custom type, which the old map had no entry for. The unit
    must survive onto the declared type: a column stored at ms and declared at us
    reads every value 1000x off."""
    ct = _declared(f"TIMESTAMP[{unit}]")
    assert ct.logical.unit.name == expected, (unit, ct.logical.unit)
    # …and it SURVIVES serialization, which is the half that was losing it.
    assert str(ct) == f"TIMESTAMP[{unit}]", str(ct)


def test_timestamp_days_is_refused_as_a_declared_type():
    """There is no day resolution to STORE a timestamp at — TimestampUnit has no
    such member. It stays valid on a CAST, where it is a scaling instruction and
    the result is canonical microseconds."""
    with pytest.raises(UnsupportedSyntaxError) as err:
        _declared("TIMESTAMP[d]")
    assert "DATE" in str(err.value), str(err.value)


@pytest.mark.parametrize(
    "decl, expected",
    [
        ("BIGINT", "INT64"),
        ("INT", "INT64"),
        ("TEXT", "VARCHAR"),
        ("STRING", "VARCHAR"),
        ("BOOL", "BOOLEAN"),
    ],
)
def test_alias_spellings_a_stored_schema_may_use_still_declare(decl, expected):
    """A CAST target refuses these and points at the exact name. A DECLARED type
    is a different question — it says what the catalog will STORE — so it accepts
    what a stored schema may say. Rejecting BIGINT in DDL because CAST rejects it
    would break working schemas over a rule about cast targets."""
    assert str(_declared(decl)) == expected, decl


@pytest.mark.parametrize(
    "decl, expected",
    [("TINYINT", "INT8"), ("SMALLINT", "INT16"), ("REAL", "FLOAT32")],
)
def test_narrow_alias_spellings_no_longer_silently_widen(decl, expected):
    """BEHAVIOUR CHANGE: these declared an INT64/FLOAT64 column before, because
    the old DDL map pointed every one of them at INTEGER/DOUBLE. They now mean
    what they say — the same width the schema reader gives them."""
    assert str(_declared(decl)) == expected, decl


def test_an_unknown_type_still_fails_loud_and_names_the_column():
    with pytest.raises(UnsupportedSyntaxError) as err:
        _declared("NOT_A_TYPE")
    assert "'c'" in str(err.value), str(err.value)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
