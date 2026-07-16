"""Footer cache telemetry: per-scan hit/miss counts on the diagnostics surface.

The remote footer cache is invisible in production unless a query's own telemetry says
whether it paid for itself. `IpcRowGroupSource.diagnostics()` reports `footer_cache_hits`/
`footer_cache_misses` for THIS scan, which `compiler.py` rolls into the same
`io_scan_diagnostics` telemetry that already carries `bytes_fetched`/`http_request_count`
to the query's persisted stats. These tests drive the real compiled scan planner
(`open_ipc_source`) over two files served by a local HTTP server — one pre-seeded in a fake
remote store (simulating another instance's hit), one not (a genuine miss) — so both counts
are exercised in a single call, with no live Valkey or cloud dependency.
"""

import functools
import http.server
import os
import threading

import pyarrow.parquet as pq
import pytest

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io.footer_remote_cache import RemoteFooterCache
import opteryx.connectors.parquet_io.pool_reader as pool_reader

DATADIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "testdata", "generated")
)
FILE_HIT = "customers.parquet"  # pre-seeded in the fake remote store -> hit
FILE_MISS = "orders.parquet"  # not seeded -> genuine origin fetch -> write-back


class FakeStore:
    """Minimal BaseKeyValueStore-shaped store — no live Valkey needed."""

    def __init__(self):
        self.data = {}

    def get_many(self, keys):
        return {k: self.data[k] for k in keys if k in self.data}

    def set_many(self, items):
        self.data.update(items)

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value


@pytest.fixture
def http_server():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=DATADIR)
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    yield f"http://127.0.0.1:{port}"
    httpd.shutdown()


def _envelope_bytes(filename):
    """A stand-in for the footer envelope a cold fetch would capture.

    Both fixtures are far under any realistic footer-prefetch threshold (the native
    fetcher reads a 32-64KB tail), so the whole file IS exactly what a real cold fetch
    of a file this size would read — not an approximation."""
    path = os.path.join(DATADIR, filename)
    assert os.path.getsize(path) < 64 * 1024, "fixture must fit the prefetch window whole"
    with open(path, "rb") as f:
        return f.read()


def _first_column(filename):
    return pq.ParquetFile(os.path.join(DATADIR, filename)).schema.names[0]


@pytest.fixture
def patched_remote_cache(monkeypatch):
    """Swap the module-level `remote_footer_cache()` singleton for one backed by a
    FakeStore — proves the wiring without any live network cache."""
    cache = RemoteFooterCache(FakeStore(), max_value_bytes=4 * 1024 * 1024)
    monkeypatch.setattr(pool_reader, "remote_footer_cache", lambda: cache)
    return cache


def test_hit_and_miss_are_both_reported_in_one_scans_diagnostics(http_server, patched_remote_cache):
    cache = patched_remote_cache
    url_hit = f"{http_server}/{FILE_HIT}"
    url_miss = f"{http_server}/{FILE_MISS}"

    # Seed under the URL the scan will actually request — simulates another instance
    # having already populated the shared tier for this exact file.
    cache.put(url_hit, _envelope_bytes(FILE_HIT))
    assert RemoteFooterCache._key(url_miss) not in cache._store.data

    src = pool_reader.open_ipc_source(
        None,
        [url_hit, url_miss],
        [_first_column(FILE_HIT)],
        decode_workers=2,
        footer_bytes_cache=ParquetFooterBytesCache(64 * 1024 * 1024),
    )
    try:
        diag = src.diagnostics()
    finally:
        src.close()

    assert diag.get("footer_cache_hits") == 1, diag
    assert diag.get("footer_cache_misses") == 1, diag

    # The miss must have been written back — the whole point of the tier.
    assert cache._store.get(RemoteFooterCache._key(url_miss)) is not None


def test_all_local_files_omit_footer_cache_fields_entirely(patched_remote_cache):
    # Local paths are never remote-scheme, so the tier is never consulted — diagnostics
    # must OMIT the keys, not report a misleading 0/0 ("consulted, found nothing").
    local_path = os.path.join(DATADIR, FILE_MISS)

    src = pool_reader.open_ipc_source(
        None,
        [local_path],
        [_first_column(FILE_MISS)],
        decode_workers=2,
        footer_bytes_cache=ParquetFooterBytesCache(64 * 1024 * 1024),
    )
    try:
        diag = src.diagnostics()
    finally:
        src.close()

    assert "footer_cache_hits" not in diag, diag
    assert "footer_cache_misses" not in diag, diag


def test_no_remote_tier_configured_omits_footer_cache_fields(http_server, monkeypatch):
    # The disabled-by-default case: no tier at all. Must look identical to "never
    # consulted", not "consulted and got nothing".
    monkeypatch.setattr(pool_reader, "remote_footer_cache", lambda: None)
    url = f"{http_server}/{FILE_MISS}"

    src = pool_reader.open_ipc_source(
        None,
        [url],
        [_first_column(FILE_MISS)],
        decode_workers=2,
        footer_bytes_cache=ParquetFooterBytesCache(64 * 1024 * 1024),
    )
    try:
        diag = src.diagnostics()
    finally:
        src.close()

    assert "footer_cache_hits" not in diag, diag
    assert "footer_cache_misses" not in diag, diag


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
