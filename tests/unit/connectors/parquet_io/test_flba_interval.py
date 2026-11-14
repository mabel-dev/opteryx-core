"""Parquet INTERVAL (FIXED_LEN_BYTE_ARRAY(12) + INTERVAL converted type) reads as
DRAKEN_INTERVAL: the slot [int64 months][int64 us] from parquet's three little-endian
uint32 (months, days, millis).

DuckDB writes the files, PLAIN or dictionary-encoded, from known components, so the
expected (months, us) pair is exact. Both the standalone rugo reader and the engine's
native scan (READ_PARQUET) are checked.
"""

import os
import sys

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from rugo import parquet

pytestmark = pytest.mark.usefixtures("allow_local_reads")

# Components per row i: NULL every 5th row, otherwise months/days/millis derived from
# i (millis < one day, so DuckDB stores them exactly). `card` bounds the distinct
# values, which decides whether DuckDB dictionary-encodes the column.
_COMPONENTS = "(i % {card}) % 30 AS mo, (i % {card}) % 400 AS dd, ((i % {card}) * 7919) % 86400000 AS ms"


def _write(path, card):
    duckdb.sql(
        f"COPY (SELECT i, CASE WHEN i % 5 = 0 THEN NULL "
        f"ELSE to_months(mo) + to_days(dd) + to_milliseconds(ms) END AS d "
        f"FROM (SELECT i, {_COMPONENTS.format(card=card)} FROM range(5000) t(i))) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    return path


def _expected(card):
    want = {}
    for i in range(5000):
        k = i % card
        want[i] = None if i % 5 == 0 else (k % 30, (k % 400) * 86_400_000_000 + ((k * 7919) % 86_400_000) * 1000)
    return want


def _encodings(path):
    ((enc,),) = duckdb.sql(
        f"SELECT encodings FROM parquet_metadata('{path}') WHERE path_in_schema = 'd'"
    ).fetchall()
    return enc


@pytest.fixture
def plain(tmp_path):
    path = _write(str(tmp_path / "plain.parquet"), 5000)
    assert "DICTIONARY" not in _encodings(path)
    return path, 5000


@pytest.fixture
def dictionary(tmp_path):
    path = _write(str(tmp_path / "dict.parquet"), 7)
    assert "DICTIONARY" in _encodings(path)
    return path, 7


def test_footer_reports_the_interval_logical_type(plain):
    column = [c for c in parquet.read_metadata(plain[0]).schema_columns if c.name == "d"][0]
    assert (column.physical_type, column.logical_type) == ("fixed_len_byte_array", "interval")


@pytest.mark.parametrize("fixture", ["plain", "dictionary"])
def test_standalone_rugo_reads_interval(fixture, request):
    path, card = request.getfixturevalue(fixture)
    got = {}
    with parquet.read_parquet(path) as reader:
        for morsel in reader:
            got.update(zip(morsel.column(b"i").to_pylist(), morsel.column(b"d").to_pylist()))
    assert got == _expected(card)


@pytest.mark.parametrize("fixture", ["plain", "dictionary"])
def test_engine_reads_interval_natively(fixture, request):
    path, card = request.getfixturevalue(fixture)
    session = opteryx.session()
    got = {}
    for morsel in session.execute_to_morsels(f"SELECT i, d FROM READ_PARQUET('{path}')"):
        got.update(zip(morsel.column("i").to_pylist(), morsel.column("d").to_pylist()))
    assert "NativeParquetScanSource" in set(session.telemetry["scan_sources"].values())
    assert got == _expected(card)


def test_days_beyond_int64_microseconds_are_refused(tmp_path):
    """A uint32 day count past ~106.7 million days overflows the int64 microsecond
    slot: both readers refuse it rather than wrap. No writer here produces one, so
    the day field of an uncompressed file is patched to 0xFFFFFFFF."""
    morsel = list(
        opteryx.session().execute_to_morsels("SELECT INTERVAL '12345' DAY + INTERVAL '678' MILLISECOND AS d")
    )[0]
    data = parquet.write_parquet(morsel, compression="none")
    value = (0).to_bytes(4, "little") + (12345).to_bytes(4, "little") + (678).to_bytes(4, "little")
    assert data.count(value) == 1
    path = str(tmp_path / "overflow.parquet")
    with open(path, "wb") as f:
        f.write(data.replace(value, (0).to_bytes(4, "little") + b"\xff\xff\xff\xff" + (678).to_bytes(4, "little")))
    with pytest.raises(RuntimeError, match="interval out of representable range"):
        with parquet.read_parquet(path) as reader:
            list(reader)
    with pytest.raises(RuntimeError, match="interval out of representable range"):
        list(opteryx.session().execute_to_morsels(f"SELECT d FROM READ_PARQUET('{path}')"))


def test_engine_filters_on_interval(dictionary):
    path, card = dictionary
    want = sum(1 for v in _expected(card).values() if v == (1, 86_400_000_000 + 7919 * 1000))
    rows = list(
        opteryx.session().execute_to_morsels(
            f"SELECT COUNT(*) AS c FROM READ_PARQUET('{path}') "
            f"WHERE d = INTERVAL '1' MONTH + INTERVAL '1' DAY + INTERVAL '7919' MILLISECOND"
        )
    )
    assert rows[0].column("c").to_pylist() == [want]
