# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""READ_JSONL executes only as the native streaming Source (NativeJsonlScanSource).

Planning fixes the files, projection, pinned schema and predicates once; the
Source's own decode pool cuts each file into newline-aligned chunks and decodes
them while execution consumes them. Chunk order is not guaranteed, so every
assertion here is order-free. Errors in a LATER file (the binder only reads the
first) fail the query mid-execution with the message the old compile-time path
raised.
"""

import http.server
import os
import sys
import threading
from functools import partial

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    assert set(session.telemetry["scan_sources"].values()) == {"NativeJsonlScanSource"}
    return sorted(rows, key=repr)


def _write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    return path


def test_projection_predicates_and_count(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n{"a": 2, "b": "yy"}\n{"a": 3, "b": null}\n')
    _write(tmp_path, "b.jsonl", '{"a": 4, "b": "z"}\n{}\n')
    glob = f"READ_JSONL('{tmp_path}/*.jsonl')"
    assert _rows(f"SELECT a, b FROM {glob}") == sorted(
        [(1, "x"), (2, "yy"), (3, None), (4, "z"), (None, None)], key=repr
    )
    assert _rows(f"SELECT a FROM {glob} WHERE a > 2") == [(3,), (4,)]
    assert _rows(f"SELECT a FROM {glob} WHERE a > 100") == []
    assert _rows(f"SELECT COUNT(*) FROM {glob}") == [(5,)]


def test_one_physical_column_under_two_identities(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "c": {"k": "v1"}}\n{"a": 2, "c": {"k": "v2"}}\n')
    rows = _rows(
        f"SELECT a, c ->> 'k' AS k FROM READ_JSONL('{tmp_path}/a.jsonl') WHERE c ->> 'k' = 'v2'"
    )
    assert rows == [(2, "v2")]


def test_malformed_record_in_a_later_file_fails_mid_execution(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"a": 2, "b": "y"}\nnot json\n')
    with pytest.raises(DatasetReadError, match=r"b\.jsonl.*Malformed JSONL at line 2 \(byte offset 19\): 'not json'"):
        _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl')")
    with pytest.raises(DatasetReadError, match=r"Malformed JSONL at line 2"):
        _rows(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*.jsonl')")


def test_ignore_errors_drops_the_malformed_record(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"a": 2, "b": "y"}\nnot json\n')
    assert _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl', ignore_errors => true)") == [(1,), (2,)]


def test_declared_type_mismatch_in_a_later_file_fails_mid_execution(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"a": "oops", "b": "y"}\n')
    with pytest.raises(DatasetReadError, match=r"b\.jsonl.*does not fit the schema resolved at bind time.*'oops'"):
        _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl')")


def test_a_file_lacking_a_bound_column_is_drift(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"b": "y"}\n')
    with pytest.raises(DatasetReadError, match=r"b\.jsonl'\): the expected columns \['a'\]"):
        _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl')")


def test_a_nested_column_absent_from_a_file_is_not_drift(tmp_path):
    _write(tmp_path, "a.jsonl", '{"c": {"k": "v1"}}\n')
    _write(tmp_path, "b.jsonl", '{"c": {"other": 1}}\n')
    assert _rows(f"SELECT c ->> 'k' FROM READ_JSONL('{tmp_path}/*.jsonl')") == [("v1",), (None,)]


def test_many_files_and_chunks_lose_and_invent_nothing(tmp_path, monkeypatch):
    # Tiny chunks force many chunks per file across many files: every row arrives once.
    import opteryx.connectors.jsonl_io as jsonl_io

    monkeypatch.setattr(jsonl_io, "DEFAULT_CHUNK_SIZE", 64)
    expected = []
    for f in range(12):
        lines = []
        for r in range(50):
            value = f * 1000 + r
            lines.append(f'{{"a": {value}, "b": "{"s" * (r % 7)}"}}')
            expected.append((value,))
        _write(tmp_path, f"{f:02d}.jsonl", "\n".join(lines) + "\n")
    assert _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl')") == sorted(expected, key=repr)
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*.jsonl')") == [(600,)]


def test_file_scheme_is_refused(tmp_path):
    # `file://` is not an alias for a local path: the binder refuses it outright.
    path = _write(tmp_path, "a.jsonl", '{"a": 1}\n')
    with pytest.raises(InvalidFunctionParameterError, match="'file://' is not a supported scheme"):
        _rows(f"SELECT a FROM READ_JSONL('file://{path}')")


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
    _write(directory, "a.jsonl", '{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n')
    assert _rows(f"SELECT a, b FROM READ_JSONL('{base}/a.jsonl')") == [(1, "x"), (2, "y")]
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{base}/a.jsonl')") == [(2,)]


def test_http_missing_file_fails_loud(http_dir):
    _, base = http_dir
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT a FROM READ_JSONL('{base}/missing.jsonl')")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
