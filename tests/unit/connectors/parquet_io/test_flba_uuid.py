"""Parquet UUID (FIXED_LEN_BYTE_ARRAY(16) + UUID logical type) reads as DRAKEN_UUID.

docs/UUID_TYPE_DESIGN.md phase 2. DuckDB writes the files and is the oracle — it
annotates UUID columns with the UUID logical type, PLAIN or dictionary-encoded.
Both the standalone rugo reader and the engine's native scan are checked, and the
sort order must match DuckDB's (byte order == canonical-text order).
"""

import os
import sys
import uuid

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from rugo import parquet

pytestmark = pytest.mark.usefixtures("allow_local_reads")


def _write(path, select):
    duckdb.sql(f"COPY ({select}) TO '{path}' (FORMAT PARQUET)")
    return path


def _engine(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    assert "NativeParquetScanSource" in set(session.telemetry["scan_sources"].values())
    return rows


@pytest.fixture
def plain(tmp_path):
    path = str(tmp_path / "plain.parquet")
    return _write(path, "SELECT CASE WHEN i % 5 = 0 THEN NULL ELSE uuid() END AS id, i FROM range(5000) t(i)")


@pytest.fixture
def dictionary(tmp_path):
    path = str(tmp_path / "dict.parquet")
    return _write(path, "SELECT ('00000000-0000-0000-0000-00000000000' || (i % 3))::UUID AS id, i FROM range(5000) t(i)")


def _duck(path):
    return {i: u for u, i in duckdb.sql(f"SELECT id, i FROM '{path}'").fetchall()}


def test_footer_reports_the_uuid_logical_type(plain):
    column = [c for c in parquet.read_metadata(plain).schema_columns if c.name == "id"][0]
    assert (column.physical_type, column.logical_type) == ("fixed_len_byte_array", "uuid")


@pytest.mark.parametrize("fixture", ["plain", "dictionary"])
def test_standalone_rugo_reads_uuid(fixture, request):
    path = request.getfixturevalue(fixture)
    got = {}
    with parquet.read_parquet(path) as reader:
        for morsel in reader:
            got.update(zip(morsel.column(b"i").to_pylist(), morsel.column(b"id").to_pylist()))
    assert got == _duck(path)


@pytest.mark.parametrize("fixture", ["plain", "dictionary"])
def test_engine_reads_uuid_natively(fixture, request):
    path = request.getfixturevalue(fixture)
    assert {i: u for u, i in _engine(f"SELECT id, i FROM READ_PARQUET('{path}')")} == _duck(path)


def test_engine_filters_groups_and_orders_like_duckdb(plain, dictionary):
    want = [u for (u,) in duckdb.sql(f"SELECT id FROM '{plain}' WHERE id IS NOT NULL ORDER BY id").fetchall()]
    got = [u for (u,) in _engine(f"SELECT id FROM READ_PARQUET('{plain}') WHERE id IS NOT NULL ORDER BY id")]
    assert got == want
    assert _engine(f"SELECT COUNT(*) FROM READ_PARQUET('{plain}') WHERE id = '{want[7]}'") == [(1,)]
    assert _engine(f"SELECT COUNT(*) FROM READ_PARQUET('{plain}') WHERE id IS NULL") == [(1000,)]
    groups = sorted(_engine(f"SELECT id, COUNT(*) FROM READ_PARQUET('{dictionary}') GROUP BY id"), key=str)
    assert groups == [
        (uuid.UUID("00000000-0000-0000-0000-000000000000"), 1667),
        (uuid.UUID("00000000-0000-0000-0000-000000000001"), 1667),
        (uuid.UUID("00000000-0000-0000-0000-000000000002"), 1666),
    ]


def test_uuid_inside_a_struct_renders_as_text(tmp_path):
    # Before the UUID logical type was parsed, a nested FLBA(16) uuid rendered as
    # base64; it is the canonical lowercase text now, like every UUID rendering.
    path = _write(str(tmp_path / "nested.parquet"),
                  "SELECT {'id': ('00000000-0000-0000-0000-00000000000' || (i % 2))::UUID, 'n': i} AS s "
                  "FROM range(10) t(i)")
    rendered = {r[0] for r in _engine(f"SELECT s FROM READ_PARQUET('{path}')")}
    assert rendered == {
        f'{{"id":"00000000-0000-0000-0000-00000000000{i % 2}","n":{i}}}' for i in range(10)
    }


def test_min_max_match_duckdb(plain):
    want = duckdb.sql(f"SELECT i % 7 AS k, MIN(id), MAX(id) FROM '{plain}' GROUP BY k").fetchall()
    got = _engine(f"SELECT i % 7 AS k, MIN(id), MAX(id) FROM READ_PARQUET('{plain}') GROUP BY k")
    assert sorted(got) == sorted(want)
    assert _engine(f"SELECT MIN(id), MAX(id) FROM READ_PARQUET('{plain}')") == \
        duckdb.sql(f"SELECT MIN(id), MAX(id) FROM '{plain}'").fetchall()
