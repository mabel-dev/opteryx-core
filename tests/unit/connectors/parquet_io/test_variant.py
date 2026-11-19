"""Parquet VARIANT columns read as one JSON value per row.

docs/parquet_variant_design.md, read path. DuckDB writes the files — unshredded
(metadata + value), shredded as it chooses, and shredded to a schema we give it so
objects are only PARTLY shredded — and the source JSON is the oracle: whatever the
shredding, a row reads back as the value that was written.
"""

import json
import os
import sys

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from draken.draken_native import DrakenType
from opteryx.exceptions import IncompatibleTypesError
from rugo import parquet

pytestmark = pytest.mark.usefixtures("allow_local_reads")

ROWS = 3000

SHREDDINGS = {
    "unshredded": ", SHREDDING {data: 'VARIANT'}",
    "auto": "",
    # only some members are shredded, so objects carry a residual `value` as well
    "partial": ", SHREDDING {data: 'STRUCT(kind VARCHAR, \"commit\" STRUCT(collection VARCHAR), tags VARCHAR[])'}",
}


def _document(i):
    shape = i % 10
    if shape == 0:
        return {"kind": "commit", "time_us": 1732000000000000 + i,
                "commit": {"collection": f"c{i % 7}", "operation": "create", "rev": i}}
    if shape == 1:
        return {"kind": "identity", "time_us": 1732000000000000 + i, "tags": ["a", None, f"b{i}"]}
    if shape == 2:
        return {"kind": 7, "commit": "not an object", "tags": "not an array"}
    if shape == 3:
        return None  # SQL NULL
    if shape == 4:
        return "JSON null"  # the Variant null, written from the JSON text `null`
    if shape == 5:
        return "a string"
    if shape == 6:
        return {}
    if shape == 7:
        return {"tags": [], "deep": {"a": {"b": [{"c": 1.5}, {"c": None}]}}}
    if shape == 8:
        return [1, "two", {"x": True}]
    return {"kind": "commit", f"extra_{i % 3}": i, "text": 'é"\\\n'}


def _json_text(i):
    document = _document(i)
    if document is None:
        return None
    if document == "JSON null":
        return "null"
    return json.dumps(document)


def _expected(i):
    document = _document(i)
    return None if document == "JSON null" else document


@pytest.fixture(scope="module", params=sorted(SHREDDINGS))
def variant_file(request, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("variant") / f"{request.param}.parquet")
    con = duckdb.connect()
    con.execute("CREATE TABLE src (id INTEGER, j VARCHAR)")
    con.executemany("INSERT INTO src VALUES (?, ?)", [(i, _json_text(i)) for i in range(ROWS)])
    con.execute(
        f"COPY (SELECT id, j::JSON::VARIANT AS data FROM src ORDER BY id) TO '{path}' "
        f"(FORMAT PARQUET, ROW_GROUP_SIZE 1024{SHREDDINGS[request.param]})"
    )
    return path


def _engine(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    assert "NativeParquetScanSource" in set(session.telemetry["scan_sources"].values())
    return rows, session


def test_footer_reports_the_variant_type(variant_file):
    column = [c for c in parquet.read_metadata(variant_file).schema_columns if c.name == "data"][0]
    assert (column.physical_type, column.logical_type) == ("struct", "variant")


def test_column_reads_as_variant(variant_file):
    session = opteryx.session()
    tags = {t for m in session.execute_to_morsels(f"SELECT data FROM READ_PARQUET('{variant_file}')") for t in m.column_types}
    assert tags == {DrakenType.VARIANT}
    assert session.description[0][1].name == "VARIANT"


def test_column_binds_as_variant(variant_file):
    # VARIANT is extraction-only (docs/json_variant_type_plan.md): comparing it is refused.
    with pytest.raises(IncompatibleTypesError, match="VARIANT"):
        _engine(f"SELECT id FROM READ_PARQUET('{variant_file}') WHERE data = 'x'")


def test_every_row_reads_back_as_the_value_written(variant_file):
    rows, _ = _engine(f"SELECT id, data FROM READ_PARQUET('{variant_file}')")
    assert len(rows) == ROWS
    for i, text in rows:
        got = None if text is None else json.loads(text)
        assert got == _expected(i), (i, text)


def test_a_filtered_read_keeps_rows_aligned(variant_file):
    rows, _ = _engine(f"SELECT id, data FROM READ_PARQUET('{variant_file}') WHERE id % 7 = 3")
    assert sorted(i for i, _ in rows) == [i for i in range(ROWS) if i % 7 == 3]
    for i, text in rows:
        assert (None if text is None else json.loads(text)) == _expected(i), (i, text)


def test_extraction_operators_read_through_the_variant(variant_file):
    rows, _ = _engine(
        f"SELECT id, data->>'kind', data->'commit'->>'collection' FROM READ_PARQUET('{variant_file}')"
    )
    for i, kind, collection in rows:
        document = _expected(i)
        members = document if isinstance(document, dict) else {}
        want_kind = members.get("kind")
        commit = members.get("commit")
        assert kind == (None if want_kind is None else str(want_kind)), i
        assert collection == (commit["collection"] if isinstance(commit, dict) else None), i


# Every Variant scalar type, as a typed (shredded) Parquet column and as Variant
# binary: the two must render alike, by the rules nested STRUCT columns already use.
SCALARS = {
    "d": ("DATE '2024-01-02'", "2024-01-02"),
    "ts": ("TIMESTAMP '2024-01-02 03:04:05.123456'", "2024-01-02T03:04:05.123456+00:00"),
    "tz": ("TIMESTAMPTZ '2024-01-02 03:04:05+00'", "2024-01-02T03:04:05+00:00"),
    "tm": ("TIME '01:02:03'", "01:02:03"),
    "dec": ("1.50::DECIMAL(10,2)", "1.50"),
    "big": ("12345678901234567890.12::DECIMAL(38,2)", "12345678901234567890.12"),
    "b": ("'ab'::BLOB", "YWI="),
    "u": ("'123e4567-e89b-12d3-a456-426614174000'::UUID", "123e4567-e89b-12d3-a456-426614174000"),
    "f": ("1.5::FLOAT", "1.5"),
    "dbl": ("2.5e10::DOUBLE", "25000000000.0"),
    "i8": ("1::TINYINT", "1"),
    "i16": ("300::SMALLINT", "300"),
    "i32": ("70000::INTEGER", "70000"),
    "i64": ("-5000000000::BIGINT", "-5000000000"),
    "bo": ("true", "true"),
    "long": ("repeat('x', 100)", "x" * 100),
    "short": ("'hi'", "hi"),
}


@pytest.mark.parametrize("shredding", ["unshredded", "auto"])
def test_every_scalar_type_renders_by_the_struct_rules(shredding, tmp_path):
    path = str(tmp_path / f"scalars_{shredding}.parquet")
    members = ", ".join(f"'{name}': {sql}" for name, (sql, _) in SCALARS.items())
    duckdb.sql(
        f"COPY (SELECT {{{members}}}::VARIANT AS data) TO '{path}' (FORMAT PARQUET{SHREDDINGS[shredding]})"
    )
    select = ", ".join(f"data->>'{name}'" for name in SCALARS)
    rows, _ = _engine(f"SELECT {select} FROM READ_PARQUET('{path}')")
    assert dict(zip(SCALARS, rows[0])) == {name: text for name, (_, text) in SCALARS.items()}


def test_a_malformed_value_is_an_error_not_a_null(tmp_path):
    path = str(tmp_path / "corrupt.parquet")
    duckdb.sql(
        f"COPY (SELECT '\"corrupt me\"'::JSON::VARIANT AS data) TO '{path}' "
        "(FORMAT PARQUET, COMPRESSION UNCOMPRESSED, SHREDDING {data: 'VARIANT'})"
    )
    good = bytes([(10 << 2) | 1]) + b"corrupt me"  # a short string: header byte, then its bytes
    raw = open(path, "rb").read()
    assert good in raw
    with open(path, "wb") as f:
        f.write(raw.replace(good, bytes([(31 << 2) | 0]) + b"corrupt me"))  # primitive type id 31
    with pytest.raises(Exception, match="unknown primitive type id 31"):
        _engine(f"SELECT data FROM READ_PARQUET('{path}')")
