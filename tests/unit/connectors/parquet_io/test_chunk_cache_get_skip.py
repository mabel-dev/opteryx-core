"""C7 chunk cache: a remote chunk already in the cache is never fetched.

docs/C7_GET_SKIP_AND_DECODED_CACHE_DESIGN.md, Part A. A cache entry is the
chunk re-laid as an UNCOMPRESSED column chunk (rewritten page headers +
decompressed bodies), so a hit decodes without the chunk's bytes; the IO planner
(plan_block) pins hits before it fetches and buys no range for them.

Pinned here on the production path — SQL through the compiler's native scan plan
into ParquetIOPipeline over HTTP (dev/throttle_server.py), where every byte is a
counted range GET:

  * a repeat of a fully cached scan issues ZERO data GETs, with the same answer
    as the local read of the same file, for a rugo (V1, zstd) file and a
    DATA_PAGE_V2 (snappy, dictionary + nulls) file;
  * a page-index-pruned scan (page jump plan) over cached chunks decodes from the
    image at the mapped page offsets and answers like the local read;
  * a local repeat scan is served from the image too, same answer as cache off,
    and a small chunk's pread is skipped on a hit;
  * a remote UNCOMPRESSED chunk is cached too (its fetched bytes ARE the
    image) so its repeat issues no GET, but only when fetched whole — a
    page-pruned fetch holds just the surviving runs and fills nothing — and a
    local uncompressed chunk is never cached (the OS page cache holds it);
  * the cached chunks are neither charged to the prefetch memory ledger nor
    counted by admission (coupled and fetch-ahead paths), and the skips are
    visible in io_scan_diagnostics.
"""

import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes, physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import (
    chunk_cache_stats,
    configure_chunk_cache,
    flush_chunk_cache,
)

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet as rp

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)
GIB = 1 << 30
R = 2000        # rows per row group
N_RG = 8
N = R * N_RG


def _rugo_file(path):
    m = Morsel.from_vectors(
        [b"a", b"s", b"f"],
        [vector_from_sequence(list(range(N)), DrakenType.INT64),
         vector_from_sequence([f"v{i % 97}" for i in range(N)], DrakenType.VARCHAR),
         vector_from_sequence([i * 0.5 for i in range(N)], DrakenType.FLOAT64)],
    )
    with open(path, "wb") as f:
        f.write(rp.write_parquet(m, max_rows_per_row_group=R))


def _plain_file(path):
    # UNCOMPRESSED (codec 0), several pages per chunk so a range prunes pages.
    m = Morsel.from_vectors(
        [b"a", b"s", b"f"],
        [vector_from_sequence(list(range(N)), DrakenType.INT64),
         vector_from_sequence([None if i % 11 == 0 else f"p{i % 89}" for i in range(N)],
                              DrakenType.VARCHAR),
         vector_from_sequence([i * 0.25 for i in range(N)], DrakenType.FLOAT64)],
    )
    with open(path, "wb") as f:
        f.write(rp.write_parquet(m, compression="none", max_rows_per_row_group=R,
                                 max_page_bytes=2048))


def _v2_file(path):
    # Test-data generation only (CLAUDE.md §4): pyarrow writes DATA_PAGE_V2 with
    # level bytes outside the compressed region, which rugo's writer does not.
    import pyarrow as pa
    import pyarrow.parquet as pq

    t = pa.table({
        "a": pa.array(list(range(N)), pa.int64()),
        "s": pa.array([None if i % 11 == 0 else f"w{i % 53}" for i in range(N)]),
        "g": pa.array([None if i % 7 == 0 else i % 13 for i in range(N)], pa.int32()),
    })
    pq.write_table(t, path, compression="snappy", data_page_version="2.0",
                   use_dictionary=True, row_group_size=4 * R, data_page_size=4096,
                   write_page_index=True)


def _server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0", "--rtt-ms", "0",
         "--error-rate", "0", "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def _register_http_workspace(name, local_dir, port, subdir):
    from opteryx.connectors import FileSystemConnector, register_workspace
    from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem

    files = sorted(f for f in os.listdir(os.path.join(local_dir, subdir)) if f.endswith(".parquet"))
    urls = [f"http://127.0.0.1:{port}/{subdir}/{f}" for f in files]
    sizes = {u: os.path.getsize(os.path.join(local_dir, subdir, f)) for u, f in zip(urls, files)}

    class ServedDirFileSystem(OpteryxHttpFileSystem):
        # Signing is the identity: it routes the query onto the native path
        # (memory: an unsignable remote path runs the trampoline instead).
        signs_urls = True

        def rewrite_to_signed_url(self, path, expiry_seconds=3600):
            return path

        def list_file_infos(self, base_dir, recursive=True):
            from opteryx.connectors.io_systems._file_info import FileInfoLike

            return [FileInfoLike(path=u, size=sizes[u], mtime=0) for u in urls]

        def get_file_size(self, path):
            return sizes[path]

    register_workspace(name, FileSystemConnector, filesystem=ServedDirFileSystem(),
                       storage_type="HTTP")


def _diags(sql, settings=()):
    # platform_admin: the fetch-ahead minimum is a RESTRICTED IO knob.
    session = opteryx.session(entitlements=["platform_admin"])
    for st in settings:
        for _ in session.execute_to_morsels(st):
            pass
    for _ in session.execute_to_morsels(sql):
        pass
    diags = session.telemetry.get("io_scan_diagnostics") or []
    session.close()
    assert len(diags) == 1, diags
    return diags[0]


def _run(sql):
    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    diags = session.telemetry.get("io_scan_diagnostics") or []
    # Data GETs: a remote page-index region read (a predicate's PageIndex
    # pruning) is its own GET, outside the chunk cache, and not counted here.
    gets = sum(int(d.get("http_request_count", 0)) for d in diags)
    if gets:
        gets -= sum(int(d.get("page_index_fetches", 0)) for d in diags)
    sources = set((session.telemetry.get("scan_sources") or {}).values())
    session.close()
    return sorted(rows, key=repr), gets, sources


@pytest.fixture
def cache_on():
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(12 * GIB, 100, 4 * GIB)
    flush_chunk_cache()
    yield
    flush_chunk_cache()
    configure_chunk_cache(container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES)


@pytest.fixture(scope="module")
def served():
    with tempfile.TemporaryDirectory() as tmp:
        for d in ("rugo", "v2", "plain"):
            os.makedirs(os.path.join(tmp, d))
        _rugo_file(os.path.join(tmp, "rugo", "part-0.parquet"))
        _v2_file(os.path.join(tmp, "v2", "part-0.parquet"))
        _plain_file(os.path.join(tmp, "plain", "part-0.parquet"))
        proc, port = _server(tmp)
        try:
            _register_http_workspace("ccgs_rugo", tmp, port, "rugo")
            _register_http_workspace("ccgs_v2", tmp, port, "v2")
            _register_http_workspace("ccgs_plain", tmp, port, "plain")
            yield {d: os.path.join(tmp, d) for d in ("rugo", "v2", "plain")}
        finally:
            proc.kill()
            proc.wait()


def _local_cache_off(sql, path):
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(container, 0, 0)   # budget 0 = off
    try:
        rows, gets, _ = _run(sql.format(rel=f"'{path}'"))
    finally:
        configure_chunk_cache(12 * GIB, 100, 4 * GIB)
    assert gets == 0
    return rows


@pytest.mark.parametrize("arm,sql", [
    ("rugo", "SELECT * FROM {rel}"),
    ("rugo", "SELECT s, COUNT(*) AS c FROM {rel} GROUP BY s"),
    ("v2", "SELECT * FROM {rel}"),
    ("v2", "SELECT s, g FROM {rel} WHERE g IS NULL OR s IS NULL"),
    ("plain", "SELECT * FROM {rel}"),
    ("plain", "SELECT s, COUNT(*) AS c FROM {rel} GROUP BY s"),
    ("plain", "SELECT a, f FROM {rel} WHERE s IS NULL"),
])
def test_repeat_remote_scan_issues_no_data_gets(cache_on, served, arm, sql):
    expected = _local_cache_off(sql, served[arm])
    flush_chunk_cache()
    rel = f"ccgs_{arm}.{arm}"
    first, gets1, src = _run(sql.format(rel=rel))
    assert src == {"NativeParquetScanSource"}, src
    assert first == expected
    assert gets1 > 0
    filled = chunk_cache_stats()
    assert filled["inserts"] > 0, "the first remote scan filled nothing"
    second, gets2, _ = _run(sql.format(rel=rel))
    assert second == expected
    assert gets2 == 0, f"a fully cached repeat scan still issued {gets2} range GETs"
    assert chunk_cache_stats()["hits"] > filled["hits"]


def test_page_jump_decodes_from_the_image(cache_on, served):
    # Fill with an unpruned scan, then a narrow range over the sorted column: the
    # page index prunes pages (jump plan), served at the image's mapped offsets.
    rel = "ccgs_v2.v2"
    sql = "SELECT a, s, g FROM {rel} WHERE a BETWEEN 3100 AND 3350"
    expected = _local_cache_off(sql, served["v2"])   # (turning the cache off flushes it)
    assert len(expected) == 251
    _run(f"SELECT * FROM {rel}")
    pruned_before = chunk_cache_stats()["hits"]
    rows, gets, _ = _run(sql.format(rel=rel))
    assert chunk_cache_stats()["hits"] > pruned_before
    assert rows == expected
    assert gets == 0


def test_local_repeat_scan_is_served_from_the_image(cache_on, served):
    sql = "SELECT s, g, COUNT(*) AS c FROM {rel} GROUP BY s, g"
    expected = _local_cache_off(sql, served["v2"])
    flush_chunk_cache()
    first, _, _ = _run(sql.format(rel=f"'{served['v2']}'"))
    filled = chunk_cache_stats()
    second, _, _ = _run(sql.format(rel=f"'{served['v2']}'"))
    assert chunk_cache_stats()["hits"] > filled["hits"]
    assert first == expected
    assert second == expected


@pytest.mark.parametrize("min_blocks,fetch_ahead", [(None, False), (0, True)])
def test_cached_chunks_cost_no_prefetch_memory(cache_on, served, min_blocks, fetch_ahead):
    """The prefetch ledger holds what a fetch BUYS: a fully cached repeat holds no
    compressed bytes, so its high watermark is the decoded bytes alone. Coupled
    (the small file stays under the fetch-ahead minimum) and fetch-ahead paths."""
    settings = () if min_blocks is None else (f"SET parquet_io_fetch_ahead_min_blocks = {min_blocks}",)
    sql = "SELECT * FROM ccgs_v2.v2"
    cold = _diags(sql, settings)
    hot = _diags(sql, settings)
    assert (cold["fetch_ahead_depth"] > 0) == fetch_ahead
    assert cold["memory_budget_bytes"] > 0
    assert cold["bytes_fetched"] > 0 and hot["bytes_fetched"] == 0
    assert hot["chunk_cache_get_skipped_bytes"] == cold["bytes_fetched"]
    assert hot["chunk_cache_get_skipped_chunks"] > 0
    assert cold["chunk_cache_get_skipped_chunks"] == 0
    assert hot["memory_held_high_watermark"] < cold["memory_held_high_watermark"]


def test_local_small_chunk_pread_is_skipped_on_a_hit(cache_on, served):
    sql = f"SELECT * FROM '{served['v2']}'"
    cold = _diags(sql)
    hot = _diags(sql)
    assert cold["chunk_cache_pread_skipped_chunks"] == 0
    assert hot["chunk_cache_pread_skipped_chunks"] > 0
    assert hot["chunk_cache_pread_skipped_bytes"] > 0
    assert hot["bytes_fetched"] == 0


def test_uncompressed_page_pruned_fetch_fills_nothing(cache_on, served):
    """A page-pruned remote fetch of an UNCOMPRESSED chunk holds only the
    surviving page runs, so it is never cached; once a whole fetch has filled the
    cache, the same pruned scan is served from the image with no data GET."""
    rel = "ccgs_plain.plain"
    sql = "SELECT a, s, f FROM {rel} WHERE a BETWEEN 3100 AND 3350"
    expected = _local_cache_off(sql, served["plain"])
    assert len(expected) == 251
    before = chunk_cache_stats()["inserts"]
    rows, gets, _ = _run(sql.format(rel=rel))
    assert rows == expected
    assert gets > 0
    pruned = _diags(sql.format(rel=rel))
    assert pruned["page_index_pages_pruned"] > 0, "the range pruned no pages"
    assert chunk_cache_stats()["inserts"] == before, "a page-pruned fetch filled the cache"
    _run(f"SELECT * FROM {rel}")
    filled = chunk_cache_stats()
    assert filled["inserts"] > before
    rows, gets, _ = _run(sql.format(rel=rel))
    assert rows == expected
    assert gets == 0
    assert chunk_cache_stats()["hits"] > filled["hits"]


def test_local_uncompressed_is_not_cached(cache_on, served):
    sql = "SELECT s, COUNT(*) AS c FROM {rel} GROUP BY s"
    expected = _local_cache_off(sql, served["plain"])
    flush_chunk_cache()
    before = chunk_cache_stats()
    for _ in range(2):
        rows, _, _ = _run(sql.format(rel=f"'{served['plain']}'"))
        assert rows == expected
    after = chunk_cache_stats()
    assert after["inserts"] == before["inserts"]
    assert after["hits"] == before["hits"]
