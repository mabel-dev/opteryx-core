# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""READ_AVRO executes only as the native Source (NativeAvroScanSource).

The binder reads the FIRST file's header for the relation schema; every file is then
read with that schema as its reader schema, so files whose schemas evolved resolve
onto it (docs/AVRO_READER_DESIGN.md §19.3) and one that cannot fails the query
mid-execution naming the file. Morsel order across files is not guaranteed, so every
assertion here is order-free.

Oracles: READ_PARQUET over the parquet file the Avro sample was built from
(dev/generate_avro_sample.py), and the values fastavro was given to write.
"""

import datetime
import decimal
import http.server
import os
import sys
import threading
from functools import partial

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import fastavro

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import NotSupportedError

SAMPLE = "testdata/avro/space_missions.avro"
PARQUET = "testdata/flat/space_missions/space_missions.parquet"
UTC = datetime.timezone.utc


def _rows(sql, source="NativeAvroScanSource"):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    if source is not None:
        assert source in set(session.telemetry["scan_sources"].values())
    return sorted(rows, key=repr)


def _write(path, schema, records, codec="null", sync_interval=16000):
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records, codec=codec, sync_interval=sync_interval)
    return path


def _record(fields, name="r"):
    return {"type": "record", "name": name, "fields": fields}


# ── the sample against its parquet source ──

def test_sample_matches_its_parquet_source():
    avro = _rows(
        f"SELECT Company, Location, Price, Lauched_at, Rocket->>'Name', Rocket->>'Status', "
        f"Mission, Mission_Status FROM READ_AVRO('{SAMPLE}')"
    )
    parquet = _rows(
        f"SELECT Company, Location, Price, Lauched_at, Rocket, Rocket_Status, Mission, "
        f"Mission_Status FROM READ_PARQUET('{PARQUET}')",
        source=None,
    )
    assert len(avro) == 4630
    assert avro == parquet


def test_aggregates_match_parquet():
    sql = "SELECT Company, COUNT(*), MIN(Lauched_at), MAX(Price) FROM {} GROUP BY Company"
    assert _rows(sql.format(f"READ_AVRO('{SAMPLE}')")) == _rows(
        sql.format(f"READ_PARQUET('{PARQUET}')"), source=None
    )


def test_count_star_reads_no_column():
    assert _rows(f"SELECT COUNT(*) FROM READ_AVRO('{SAMPLE}')") == [(4630,)]


def test_where_and_alias():
    assert _rows(
        f"SELECT m.Mission FROM READ_AVRO('{SAMPLE}') AS m WHERE m.Price > 400"
    ) == _rows(f"SELECT Mission FROM READ_PARQUET('{PARQUET}') WHERE Price > 400", source=None)


# ── types, codecs, batches ──

TYPES = _record([
    {"name": "i", "type": "int"},
    {"name": "l", "type": ["null", "long"]},
    {"name": "f", "type": "float"},
    {"name": "d", "type": "double"},
    {"name": "b", "type": "boolean"},
    {"name": "s", "type": ["string", "null"]},
    {"name": "by", "type": "bytes"},
    {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["red", "green"]}},
    {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "nested", "type": ["null", _record([{"name": "x", "type": "long"}], "N")]},
])


def _typed_records(n):
    base = datetime.datetime(2020, 1, 1, tzinfo=UTC)
    return [{
        "i": i, "l": None if i % 3 == 0 else i * 10, "f": i / 4, "d": i / 3, "b": i % 2 == 0,
        "s": None if i % 5 == 0 else "s" * (i % 20), "by": bytes([i % 256]),
        "e": ["red", "green"][i % 2], "dt": datetime.date(2020, 1, 1) + datetime.timedelta(days=i % 400),
        "ts": base + datetime.timedelta(milliseconds=i), "dec": decimal.Decimal(i).scaleb(-2),
        "tags": ["t"] * (i % 3), "nested": None if i % 4 == 0 else {"x": i},
    } for i in range(n)]


@pytest.mark.parametrize("codec", ["null", "deflate", "snappy", "zstandard"])
def test_every_type_and_codec(tmp_path, codec):
    recs = _typed_records(70_000)  # more than one 65,536-row batch
    _write(tmp_path / "t.avro", TYPES, recs, codec=codec)
    got = _rows(f"SELECT i, l, f, d, b, s, by, e, dt, ts, dec, tags, nested->>'x' FROM READ_AVRO('{tmp_path}/t.avro')")
    want = sorted(
        [(r["i"], r["l"], r["f"], r["d"], r["b"], r["s"], r["by"], r["e"], r["dt"], r["ts"], r["dec"],
          r["tags"], None if r["nested"] is None else str(r["nested"]["x"])) for r in recs],
        key=repr,
    )
    assert got == want


def test_empty_file_has_a_schema_and_no_rows(tmp_path):
    _write(tmp_path / "e.avro", TYPES, [])
    assert _rows(f"SELECT i, s FROM READ_AVRO('{tmp_path}/e.avro')") == []
    assert _rows(f"SELECT COUNT(*) FROM READ_AVRO('{tmp_path}/e.avro')") == [(0,)]


# ── a glob: the first file's schema, every file resolved onto it ──

def test_glob_resolves_evolved_files_onto_the_first(tmp_path):
    first = _record([
        {"name": "id", "type": "long"},
        {"name": "name", "type": ["null", "string"]},
        {"name": "added_at", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}]},
    ])
    _write(tmp_path / "a.avro", first, [
        {"id": 1, "name": "one", "added_at": datetime.datetime(2026, 1, 1, tzinfo=UTC), "price": decimal.Decimal("1.50")},
    ])
    # b: id written as int (promotes to long), an extra field (dropped), fields reordered
    _write(tmp_path / "b.avro", _record([
        {"name": "extra", "type": "string"},
        {"name": "name", "type": ["null", "string"]},
        {"name": "id", "type": "int"},
    ]), [{"extra": "x", "name": "two", "id": 2}])
    # c: lacks name, added_at and price — NULL, including the TIMESTAMP and DECIMAL
    # constants the engine builds natively with their logical types (docs §19.4).
    _write(tmp_path / "c.avro", _record([{"name": "id", "type": "long"}]), [{"id": 3}, {"id": 4}])
    rows = _rows(f"SELECT id, name, added_at, price FROM READ_AVRO('{tmp_path}/*.avro')")
    assert rows == sorted([
        (1, "one", datetime.datetime(2026, 1, 1, tzinfo=UTC), decimal.Decimal("1.50")),
        (2, "two", None, None),
        (3, None, None, None),
        (4, None, None, None),
    ], key=repr)


def test_glob_file_that_cannot_resolve_fails_naming_it(tmp_path):
    _write(tmp_path / "a.avro", _record([{"name": "id", "type": "long"}]), [{"id": 1}])
    _write(tmp_path / "b.avro", _record([{"name": "id", "type": "string"}]), [{"id": "x"}])
    with pytest.raises(DatasetReadError, match=r"b\.avro.*the file's string cannot be read as long"):
        _rows(f"SELECT id FROM READ_AVRO('{tmp_path}/*.avro')")


def test_corrupt_file_fails_naming_it(tmp_path):
    path = _write(tmp_path / "a.avro", _record([{"name": "id", "type": "long"}]), [{"id": i} for i in range(100)])
    data = bytearray(path.read_bytes())
    data[-1] ^= 0xFF  # the trailing sync marker
    path.write_bytes(bytes(data))
    with pytest.raises(DatasetReadError, match=r"a\.avro.*sync marker"):
        _rows(f"SELECT id FROM READ_AVRO('{path}')")


def test_unreadable_header_fails_at_bind(tmp_path):
    path = tmp_path / "x.avro"
    path.write_bytes(b"not avro at all")
    with pytest.raises(DatasetReadError, match="could not be read"):
        _rows(f"SELECT * FROM READ_AVRO('{path}')")


def test_glob_with_no_match():
    from opteryx.exceptions import DatasetNotFoundError

    with pytest.raises(DatasetNotFoundError):
        _rows("SELECT * FROM READ_AVRO('testdata/avro/nothing_*.avro')")


# ── arguments and options ──

@pytest.mark.parametrize("sql, error, match", [
    ("SELECT * FROM READ_AVRO(1)", InvalidFunctionParameterError, "single string literal path"),
    (f"SELECT * FROM READ_AVRO('{SAMPLE}', ignore_errors => true)", InvalidFunctionParameterError,
     "unrecognized option 'ignore_errors'"),
    (f"SELECT * FROM READ_AVRO('{SAMPLE}', credentials => 'a.b')", InvalidFunctionParameterError,
     "applies to gs:// and s3:// paths only"),
    ("SELECT * FROM READ_AVRO('gcs://bucket/f.avro')", InvalidFunctionParameterError, "'gcs://' is not a supported scheme"),
    (f"SELECT * FROM READ_AVRO('file://{SAMPLE}')", InvalidFunctionParameterError, "'file://' is not a supported scheme"),
    ("SELECT * FROM READ_AVRO('gs://bucket/*.avro')", NotSupportedError, "glob patterns are not supported for gs://"),
    ("SELECT * FROM READ_AVRO('s3://bucket/*.avro')", NotSupportedError, "glob patterns are not supported for s3://"),
    (f"SELECT * FROM READ_AVRO('{SAMPLE}') AS t(a, b)", NotSupportedError, r"AS alias\(\.\.\.\) is not supported"),
])
def test_argument_refusals(sql, error, match):
    with pytest.raises(error, match=match):
        _rows(sql)


def test_explain_names_the_reader():
    session = opteryx.session()
    lines = []
    for morsel in session.execute_to_morsels(f"EXPLAIN SELECT Company FROM READ_AVRO('{SAMPLE}')"):
        for name in morsel.column_names:
            lines.extend(str(v) for v in morsel.column(name).to_pylist())
    assert any("READ_AVRO" in line or "AVRO" in line.upper() for line in lines)


# ── remote: http(s) is fetched whole by the native Source, with no credentials ──

@pytest.fixture
def http_dir(tmp_path):
    """A local HTTP server over `tmp_path` — the native Source GETs from it."""
    handler = partial(http.server.SimpleHTTPRequestHandler, directory=str(tmp_path))
    handler.log_message = lambda *args, **kwargs: None
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield tmp_path, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_http_file_is_read_natively(http_dir):
    directory, base = http_dir
    _write(directory / "a.avro", _record([{"name": "id", "type": "long"}, {"name": "s", "type": "string"}]),
           [{"id": 1, "s": "x"}, {"id": 2, "s": "y"}], codec="deflate")
    assert _rows(f"SELECT id, s FROM READ_AVRO('{base}/a.avro')") == [(1, "x"), (2, "y")]
    assert _rows(f"SELECT COUNT(*) FROM READ_AVRO('{base}/a.avro')") == [(2,)]


def test_http_missing_file_fails_loud(http_dir):
    _, base = http_dir
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT * FROM READ_AVRO('{base}/missing.avro')")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
