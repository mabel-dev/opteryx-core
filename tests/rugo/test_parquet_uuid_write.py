"""Parquet writer: UUID columns (docs/UUID_TYPE_DESIGN.md phase 3).

A DRAKEN_UUID column is written as FIXED_LEN_BYTE_ARRAY(16) with the UUID logical
type, the 16 bytes verbatim; statistics are the byte-order min/max. DuckDB reads
the files independently and is the oracle; rugo reads them back too, and
patch_columns carries the annotation through a rename and an added column.
"""

import os
import sys
import uuid

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.draken_native import vector_int64_from_sequence, vector_uuid_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet

LOW = uuid.UUID(int=0)
HIGH = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")


def _ids(n=500):
    ids = [uuid.UUID(int=(i * 0x9E3779B97F4A7C15_D1B54A32D192ED03) % (1 << 128)) for i in range(n)]
    ids[3] = None
    ids[5] = HIGH
    ids[6] = LOW
    return ids


def _morsel(ids):
    return Morsel.from_vectors(
        ["id", "n"], [vector_uuid_from_sequence(ids), vector_int64_from_sequence(list(range(len(ids))))]
    )


@pytest.fixture
def written(tmp_path):
    ids = _ids()
    path = str(tmp_path / "ids.parquet")
    with open(path, "wb") as f:
        f.write(parquet.write_parquet(_morsel(ids)))
    return path, ids


def test_schema_is_flba16_with_the_uuid_logical_type(written):
    path, _ = written
    schema = dict((name, (t, lt)) for name, t, lt in duckdb.sql(
        f"SELECT name, type, logical_type FROM parquet_schema('{path}')").fetchall())
    assert schema["id"] == ("FIXED_LEN_BYTE_ARRAY", "UUIDType()")


def test_statistics_are_byte_order_min_max(written):
    path, _ = written
    (mn, mx), = duckdb.sql(
        f"SELECT stats_min_value, stats_max_value FROM parquet_metadata('{path}') "
        "WHERE path_in_schema = 'id'").fetchall()
    assert (mn, mx) == (str(LOW), str(HIGH))


def test_duckdb_and_rugo_read_back_the_values(written):
    path, ids = written
    assert dict((n, u) for u, n in duckdb.sql(f"SELECT id, n FROM '{path}'").fetchall()) == dict(enumerate(ids))
    got = {}
    with parquet.read_parquet(path) as reader:
        for morsel in reader:
            got.update(zip(morsel.column(b"n").to_pylist(), morsel.column(b"id").to_pylist()))
    assert got == dict(enumerate(ids))


def test_equality_lookup_through_the_bloom_filter(written):
    path, ids = written
    assert duckdb.sql(f"SELECT n FROM '{path}' WHERE id = '{ids[200]}'").fetchall() == [(200,)]
    assert duckdb.sql(
        f"SELECT COUNT(*) FROM '{path}' WHERE id = '12345678-1234-1234-1234-123456789abc'"
    ).fetchall() == [(0,)]


def test_patch_rename_keeps_the_uuid_column(written, tmp_path):
    path, ids = written
    out = str(tmp_path / "renamed.parquet")
    with open(path, "rb") as f, open(out, "wb") as g:
        g.write(parquet.patch_columns(f.read(), rename={"n": "m"}))
    assert duckdb.sql(f"SELECT logical_type FROM parquet_schema('{out}') WHERE name = 'id'").fetchall() == [
        ("UUIDType()",)
    ]
    assert dict((n, u) for u, n in duckdb.sql(f"SELECT id, m FROM '{out}'").fetchall()) == dict(enumerate(ids))


def test_patch_adds_a_uuid_column(written, tmp_path):
    path, _ = written
    fill = uuid.UUID("6b8c5d4e-0000-4000-8000-0000000000ff")
    donor = parquet.write_parquet(
        Morsel.from_vectors(["ref"], [vector_uuid_from_sequence([fill])]), compression="none", dictionary=False
    )
    out = str(tmp_path / "added.parquet")
    with open(path, "rb") as f, open(out, "wb") as g:
        g.write(parquet.patch_columns(f.read(), add=[donor]))
    assert duckdb.sql(f"SELECT DISTINCT ref FROM '{out}'").fetchall() == [(fill,)]
