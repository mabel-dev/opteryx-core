"""READ_PARQUET over a MUTABLE object must never be answered from another version's
cached footer or chunks.

Every Parquet cache tier - the in-process parsed and bytes footer caches, the disk and
shared (cross-instance) footer tiers, and the C++ chunk cache - is keyed by file, on the
ruling that platform files are write-once. A READ_* path is a customer's object: it is
rewritten in place, and a bucket name can be deleted and recreated by someone else. Keyed
by path alone, a rewritten object was served its OLD footer and chunks: COUNT(*) and
SUM/MAX returned the old file's answers, a filter pruned every live row group on stale
min/max, and a column the new file added read as NULL. The same mechanism handed a
caller who could read only the NEW object the OLD object's statistics and bytes.

The binder records the version each object had at THIS caller's authorised HEAD
(`filesystem.object_versions`), and pool_reader keys every tier by (path, version) for
that scan (`_object_versions` / `_vkey`, io_pipeline.hpp `chunk_cache_key`).
"""

import http.server
import os
import sys
import threading
import urllib.parse

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa
import pyarrow.parquet as pq

import opteryx
from opteryx.connectors.io_systems._file_info import version_from_headers

pytestmark = pytest.mark.usefixtures("allow_local_reads")


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.to_arrow().to_pylist())
    return out


def _parquet_bytes(table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


V1 = pa.table({"x": [1, 2, 3]})
V2 = pa.table({"x": [100, 200, 300, 400, 500, 600, 700], "y": list("abcdefg")})


# ── a minimal object store: HEAD with a version header, ranged GETs, deny ──────


class _Store(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.objects = {}        # "/bucket/key" -> bytes
        self.generations = {}    # "/bucket/key" -> int
        self.denied = set()      # paths answering 403
        self.version_header = "etag"  # "etag" | "generation" | None
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def put(self, path, data):
        self.objects[path] = data
        self.generations[path] = self.generations.get(path, 0) + 1

    def url(self, path):
        return f"http://127.0.0.1:{self.server_address[1]}{path}"

    @staticmethod
    def gs(path):
        return "gs:/" + path


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # pragma: no cover
        pass

    def _headers_for(self, path, length):
        self.send_header("Content-Length", str(length))
        gen = self.server.generations.get(path)
        if gen is not None and self.server.version_header == "etag":
            self.send_header("ETag", f'"v{gen}"')
        elif gen is not None and self.server.version_header == "generation":
            self.send_header("x-goog-generation", str(1000 + gen))

    def _refuse(self, status):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        path = urllib.parse.urlsplit(self.path).path
        if path in self.server.denied:
            return self._refuse(403)
        data = self.server.objects.get(path)
        if data is None:
            return self._refuse(404)
        self.send_response(200)
        self._headers_for(path, len(data))
        self.end_headers()

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if path in self.server.denied:
            return self._refuse(403)
        data = self.server.objects.get(path)
        if data is None:
            return self._refuse(404)
        rng = self.headers.get("Range")
        if rng:
            spec = rng.split("=", 1)[1]
            if "," in spec:
                return self._refuse(416)
            lo, _, hi = spec.partition("-")
            if lo == "":
                start, end = len(data) - int(hi), len(data) - 1
            else:
                start = int(lo)
                end = int(hi) if hi else len(data) - 1
            end = min(end, len(data) - 1)
            body = data[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            body = data
            self.send_response(200)
        self._headers_for(path, len(body))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def store(monkeypatch):
    """A local object store standing in for storage.googleapis.com: a `gs://bucket/key`
    READ_PARQUET goes through the anonymous GCS filesystem (the real remote scan path -
    signed fetch URL, every footer tier, the chunk cache), with only its public host
    swapped for this server's."""
    from opteryx.connectors.io_systems.anonymous_gcs_filesystem import AnonymousGcsFileSystem

    s = _Store()

    def _local_url(path):
        stripped = path[5:] if path.startswith("gs://") else path
        return s.url("/" + stripped)

    monkeypatch.setattr(AnonymousGcsFileSystem, "_to_public_https_url", staticmethod(_local_url))
    yield s
    s.shutdown()
    s.server_close()


# ── version tokens ───────────────────────────────────────────────────────────


def test_version_from_headers_prefers_the_strongest_identifier():
    assert version_from_headers({"x-goog-generation": "17", "etag": '"a"'}) == "g:17"
    assert version_from_headers({"ETag": '"abc"'}) == 'e:"abc"'
    assert (
        version_from_headers({"last-modified": "Sat, 07 Nov 2026 10:00:00 GMT", "content-length": "9"})
        == "m:Sat, 07 Nov 2026 10:00:00 GMT:9"
    )
    assert version_from_headers({"content-length": "9"}) is None


def test_a_local_rewrite_changes_the_version(tmp_path):
    from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem

    path = str(tmp_path / "f.parquet")
    pq.write_table(V1, path)
    before = OpteryxLocalFileSystem().get_file_info(path).version
    pq.write_table(V2, path)
    after = OpteryxLocalFileSystem().get_file_info(path).version
    assert before and after and before != after


# ── the reported bug: a rewrite served the old file ─────────────────────────


def _assert_reads_v2(path):
    assert _rows(f"SELECT COUNT(*) AS n FROM READ_PARQUET('{path}')") == [{"n": 7}]
    assert _rows(f"SELECT SUM(x) AS s, MAX(x) AS m FROM READ_PARQUET('{path}')") == [{"s": 2800, "m": 700}]
    # Stale min/max (1..3) used to prune every row group of the new file.
    assert _rows(f"SELECT COUNT(*) AS n FROM READ_PARQUET('{path}') WHERE x > 50") == [{"n": 7}]
    # The binder reads the NEW schema; a stale footer lacks `y` and read it as NULL.
    assert [r["y"] for r in _rows(f"SELECT y FROM READ_PARQUET('{path}')")] == list("abcdefg")


def test_a_rewritten_local_file_is_read_as_rewritten(tmp_path):
    path = str(tmp_path / "f.parquet")
    pq.write_table(V1, path)
    assert _rows(f"SELECT COUNT(*) AS n, SUM(x) AS s FROM READ_PARQUET('{path}')") == [{"n": 3, "s": 6}]
    pq.write_table(V2, path)
    _assert_reads_v2(path)


@pytest.mark.parametrize("version_header", ["etag", "generation", None])
def test_a_rewritten_remote_object_is_read_as_rewritten(store, version_header):
    """Remote: footers go through `_acquire_remote_footers` (every footer tier) and the
    data through the C++ fetch path and its chunk cache. With no version header at all
    the HEAD's size stands in, which still separates these two objects."""
    store.version_header = version_header
    key = f"/bucket/rewrite-{version_header}.parquet"
    store.put(key, _parquet_bytes(V1))
    url = store.gs(key)
    assert _rows(f"SELECT COUNT(*) AS n, SUM(x) AS s, MAX(x) AS m FROM READ_PARQUET('{url}')") == [
        {"n": 3, "s": 6, "m": 3}
    ]
    store.put(key, _parquet_bytes(V2))
    _assert_reads_v2(url)


def test_the_original_version_is_still_served_from_cache(store):
    """Versioned keys must not turn the caches off: re-reading an unchanged object does
    not fetch its footer again."""
    key = "/bucket/unchanged.parquet"
    store.put(key, _parquet_bytes(V1))
    url = store.gs(key)
    assert _rows(f"SELECT SUM(x) AS s FROM READ_PARQUET('{url}')") == [{"s": 6}]
    assert _rows(f"SELECT SUM(x) AS s FROM READ_PARQUET('{url}')") == [{"s": 6}]


def test_a_warm_cache_never_answers_a_caller_the_store_refuses(store):
    """The authorisation is the store's, and it is asked before any cache is: a cached
    footer and cached chunks for this exact object do not let a refused caller read it."""
    key = "/bucket/private.parquet"
    store.put(key, _parquet_bytes(V1))
    url = store.gs(key)
    assert _rows(f"SELECT COUNT(*) AS n FROM READ_PARQUET('{url}')") == [{"n": 3}]
    store.denied.add(key)
    with pytest.raises(Exception):
        _rows(f"SELECT COUNT(*) AS n FROM READ_PARQUET('{url}')")


def test_a_path_without_a_recorded_version_is_headed_before_any_cache(tmp_path):
    """A filesystem that carries `object_versions` gets every scanned path HEADed (by
    itself) before a cache is consulted - a path the binder did not record is not read
    from cache unversioned."""
    from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats_many

    path = str(tmp_path / "f.parquet")
    pq.write_table(V1, path)

    class _Spy(OpteryxLocalFileSystem):
        def __init__(self):
            super().__init__()
            self.object_versions = {}
            self.headed = []

        def get_file_info(self, paths):
            self.headed.append(paths)
            return super().get_file_info(paths)

    first = _Spy()
    assert fetch_column_stats_many(first, [path])[0][0] == 3
    assert first.headed == [[path]] and path in first.object_versions

    pq.write_table(V2, path)
    second = _Spy()
    assert fetch_column_stats_many(second, [path])[0][0] == 7
    assert second.object_versions[path] != first.object_versions[path]
