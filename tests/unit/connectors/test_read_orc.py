# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""READ_ORC('path-or-glob'): the ORC sibling of READ_AVRO (docs/ANONYMOUS_REMOTE_READS_DESIGN.md §2.2).

The relation is the FIRST file's footer; every file is read as it, matched by name, by the native
source. Files are written by pyarrow.orc (an independent ORC writer; test-only, CLAUDE.md §4). The
gs:// / s3:// and credential behaviour is in test_read_functions_never_use_system_credentials.py."""

import datetime
import decimal
import http.server
import os
import sys
import threading
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pyarrow.orc as po

import opteryx
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import NotSupportedError

pytestmark = pytest.mark.usefixtures("allow_local_reads")

UTC = datetime.timezone.utc


def _table(ids):
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([None if i % 4 == 0 else f"n{i}" for i in ids], pa.string()),
            "score": pa.array([i / 2 for i in ids], pa.float64()),
            "ts": pa.array([datetime.datetime(2026, 1, 1) + datetime.timedelta(hours=i) for i in ids], pa.timestamp("us")),
            "price": pa.array([decimal.Decimal(f"{i}.50") for i in ids], pa.decimal128(9, 2)),
        }
    )


def _write(path, table, **options):
    with open(path, "wb") as f:
        po.write_table(table, f, **options)
    return path


def _rows(sql, source="NativeOrcScanSource"):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    if source is not None:
        assert source in set(session.telemetry["scan_sources"].values())
    return sorted(rows, key=repr)


def test_one_file_every_type(tmp_path):
    _write(tmp_path / "a.orc", _table([1, 2, 3]), compression="zlib")
    got = _rows(f"SELECT id, name, score, ts, price FROM READ_ORC('{tmp_path}/a.orc')")
    want = [
        (i, None if i % 4 == 0 else f"n{i}", i / 2, datetime.datetime(2026, 1, 1, tzinfo=UTC) + datetime.timedelta(hours=i), decimal.Decimal(f"{i}.50"))
        for i in (1, 2, 3)
    ]
    assert got == sorted(want, key=repr)


def test_a_plain_binary_column_stays_varbinary(tmp_path):
    # Only the Iceberg `iceberg.binary-type` attribute makes a BINARY column a UUID or fixed.
    values = [b"\x55\x0e" * 8, b"abcd", b""]
    _write(tmp_path / "a.orc", pa.table({"b": pa.array(values, pa.binary())}))
    assert _rows(f"SELECT b FROM READ_ORC('{tmp_path}/a.orc')") == sorted([(v,) for v in values], key=repr)


def test_projection_filter_count_and_limit(tmp_path):
    _write(tmp_path / "a.orc", _table(range(1, 7)))
    path = f"{tmp_path}/a.orc"
    assert _rows(f"SELECT COUNT(*) FROM READ_ORC('{path}')") == [(6,)]
    assert _rows(f"SELECT id FROM READ_ORC('{path}') WHERE score > 2.0") == [(5,), (6,)]
    assert len(_rows(f"SELECT id FROM READ_ORC('{path}') LIMIT 2")) == 2


def test_a_glob_reads_every_match_in_one_relation(tmp_path):
    _write(tmp_path / "a.orc", _table([1, 2]))
    _write(tmp_path / "b.orc", _table([3, 4, 5]), compression="zstd")
    (tmp_path / "notes.txt").write_text("not matched")
    assert _rows(f"SELECT COUNT(*), SUM(id) FROM READ_ORC('{tmp_path}/*.orc')") == [(5, 15)]


def test_a_later_file_is_read_as_the_first_widening_and_adding_nulls(tmp_path):
    _write(tmp_path / "a.orc", _table([1]))
    _write(tmp_path / "b.orc", pa.table({"id": pa.array([2], pa.int32()), "name": pa.array(["two"], pa.string())}))
    assert _rows(f"SELECT id, name, score FROM READ_ORC('{tmp_path}/*.orc')") == [(1, "n1", 0.5), (2, "two", None)]


def test_a_file_that_cannot_resolve_fails_the_query(tmp_path):
    _write(tmp_path / "a.orc", pa.table({"x": pa.array([1], pa.int64())}))
    _write(tmp_path / "b.orc", pa.table({"x": pa.array(["s"], pa.string())}))
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT x FROM READ_ORC('{tmp_path}/*.orc')")


def test_a_column_the_reader_cannot_surface_names_itself_at_bind_time(tmp_path):
    # A MAP surfaces as a JSON object, so its keys must be strings (UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1).
    table = pa.table({"ok": pa.array([1], pa.int64()), "m": pa.array([[(1, 2)]], pa.map_(pa.int64(), pa.int64()))})
    _write(tmp_path / "a.orc", table)
    with pytest.raises(NotSupportedError, match=r"column 'm'.*MAP has LONG keys"):
        _rows(f"SELECT ok FROM READ_ORC('{tmp_path}/a.orc')")


def test_nested_columns_read_as_json_and_arrays(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "st": pa.array([{"a": 1}, None], pa.struct([("a", pa.int64())])),
            "lst": pa.array([[1, 2], []], pa.list_(pa.int64())),
        }
    )
    _write(tmp_path / "a.orc", table)
    assert _rows(f"SELECT id, st, lst FROM READ_ORC('{tmp_path}/a.orc')") == [(1, '{"a":1}', [1, 2]), (2, None, [])]


def test_a_file_that_is_not_orc_names_itself(tmp_path):
    (tmp_path / "a.orc").write_bytes(b"not an orc file")
    with pytest.raises(DatasetReadError, match=r"a\.orc.*could not be read"):
        _rows(f"SELECT * FROM READ_ORC('{tmp_path}/a.orc')")


def test_a_glob_matching_nothing_is_not_found(tmp_path):
    with pytest.raises(DatasetNotFoundError):
        _rows(f"SELECT * FROM READ_ORC('{tmp_path}/*.orc')")


@pytest.mark.parametrize(
    "sql, error",
    [
        ("SELECT * FROM READ_ORC('file:///x.orc')", InvalidFunctionParameterError),
        ("SELECT * FROM READ_ORC('gcs://b/x.orc')", InvalidFunctionParameterError),
        ("SELECT * FROM READ_ORC(1)", InvalidFunctionParameterError),
        ("SELECT * FROM READ_ORC('x.orc', bogus => 1)", Exception),
    ],
)
def test_bad_arguments_are_refused(sql, error):
    with pytest.raises(error):
        _rows(sql, source=None)


def test_column_aliases_are_refused(tmp_path):
    _write(tmp_path / "a.orc", _table([1]))
    with pytest.raises(NotSupportedError, match="AS alias"):
        _rows(f"SELECT * FROM READ_ORC('{tmp_path}/a.orc') AS t(a, b, c, d, e)")


def test_an_http_url_is_fetched_with_no_credential(tmp_path):
    data = (tmp_path / "a.orc")
    _write(data, _table([1, 2, 3]))
    seen = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):  # pragma: no cover
            pass

        def do_GET(self):
            seen.append(dict(self.headers))
            body = data.read_bytes()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        assert _rows(f"SELECT COUNT(*) FROM READ_ORC('http://127.0.0.1:{server.server_address[1]}/a.orc')") == [(3,)]
    finally:
        server.shutdown()
    assert seen and all("Authorization" not in h for h in seen)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
