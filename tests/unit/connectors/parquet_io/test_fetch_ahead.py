"""Remote fetch-ahead: decoupling the range GET from the decode thread.

`ParquetIOPipeline::decode_row_group` fetches AND decodes on one thread, so with
the coupled path the number of concurrent fetches equals the decode worker count
by construction. `parquet_io_fetch_ahead` (config `PARQUET_IO_FETCH_AHEAD`, 0 =
off) adds a dedicated GET-only pool of that depth so requests in flight are no
longer pinned to the thread count. See docs/FETCH_DECODE_DECOUPLING_DESIGN.md.

What is pinned here:
  * the knob reaches the pipeline and reads back (`fetch_ahead_depth`) — the
    "prove the knob moves" gate before any production measurement;
  * fetch-ahead is byte- and row-identical to the coupled path;
  * a local-only scan never arms the fetch pool (nothing for it to do);
  * the two silently-inert combinations are REJECTED, not accepted;
  * a fetch-stage failure surfaces with the ORIGINAL error — it is not swallowed
    and not quietly re-fetched by the decode stage;
  * early abandonment stays clean and the bytes bought-then-discarded are reported.

These tests drive the real C++ pipeline against dev/throttle_server.py (Range
GETs, injected faults) like test_http_retry.py does.
"""

import json
import os
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)

ROW_GROUP_SIZE = 500
N_ROW_GROUPS = 24
N = ROW_GROUP_SIZE * N_ROW_GROUPS
COLUMNS = ["x", "s"]


def _write(tmp):
    # Unique name per process: the parsed-footer cache is process-global and keyed
    # by URL, and a fault test below must not be served a footer from a sibling run.
    name = f"fetch_ahead_{os.getpid()}.parquet"
    path = os.path.join(tmp, name)
    table = pa.table({
        "x": pa.array(list(range(N)), type=pa.int64()),
        "s": pa.array([f"v{i % 97}" for i in range(N)], type=pa.string()),
    })
    pq.write_table(table, path, row_group_size=ROW_GROUP_SIZE)
    assert pq.ParquetFile(path).num_row_groups == N_ROW_GROUPS
    return path, name


def _server(root, rtt_ms=0.0, error_rate=0.0):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0",
         "--rtt-ms", str(rtt_ms), "--error-rate", str(error_rate), "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def _footer_envelope(path):
    """PAR1 + thrift + len_le32 + PAR1 — what a cold footer fetch captures."""
    with open(path, "rb") as f:
        blob = f.read()
    footer_len = int.from_bytes(blob[-8:-4], "little")
    thrift = blob[-8 - footer_len:-8]
    return b"PAR1" + thrift + footer_len.to_bytes(4, "little") + b"PAR1"


def _scan(url, fetch_ahead, workers=2, take=None, **kwargs):
    """Drive the public wrapper; return (x values, pipeline diagnostics).

    The wrapper closes its source in `finally`, so the diagnostics are harvested
    through the same JSON hook the dev bench harness uses (one line per scan)."""
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    fd, diag_path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    os.environ["OPTERYX_IO_DIAG_JSON"] = diag_path
    seen = []
    try:
        gen = iter_row_groups_ipc(None, [url], COLUMNS, decode_workers=workers,
                                  fetch_ahead=fetch_ahead, **kwargs)
        try:
            for _scan_rg, rg in gen:
                seen.extend(rg[b"x"].to_pylist())
                if take is not None and len(seen) >= take:
                    break
        finally:
            gen.close()
        with open(diag_path) as f:
            lines = f.read().strip().splitlines()
        assert len(lines) == 1, lines
        return seen, json.loads(lines[-1])
    finally:
        del os.environ["OPTERYX_IO_DIAG_JSON"]
        os.remove(diag_path)


# ── the knob binds, and the answer does not change ───────────────────────────

def test_depth_reads_back_on_a_remote_scan_and_is_off_by_default():
    with tempfile.TemporaryDirectory() as tmp:
        path, name = _write(tmp)
        proc, port = _server(tmp)
        try:
            url = f"http://127.0.0.1:{port}/{name}"
            rows_off, diag_off = _scan(url, fetch_ahead=0)
            rows_on, diag_on = _scan(url, fetch_ahead=6)
        finally:
            proc.kill(); proc.wait()
    assert diag_off["fetch_ahead_depth"] == 0
    assert diag_on["fetch_ahead_depth"] == 6
    assert sorted(rows_off) == list(range(N))
    assert sorted(rows_on) == list(range(N))
    # Same bytes, same requests: fetch-ahead moves WHEN bytes are fetched, never
    # WHICH bytes — a difference here would mean the two stages derived
    # different extents for the same row group.
    assert diag_on["bytes_fetched"] == diag_off["bytes_fetched"] > 0
    assert diag_on["http_request_count"] == diag_off["http_request_count"]
    assert diag_on["http_request_count"] >= N_ROW_GROUPS
    assert diag_on["prefetch_discarded_bytes"] == 0  # nothing was abandoned


def test_local_only_scan_does_not_arm_the_fetch_pool():
    """Local files are mmap'd in decode and never enter the fetch stage, so a
    local scan with the knob set must not spin up idle fetch threads."""
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        rows, diag = _scan(path, fetch_ahead=6)
    assert sorted(rows) == list(range(N))
    assert diag["fetch_ahead_depth"] == 0


def test_native_plan_reports_depth_and_rejects_an_inert_one():
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    with tempfile.TemporaryDirectory() as tmp:
        path, name = _write(tmp)
        proc, port = _server(tmp)
        try:
            url = f"http://127.0.0.1:{port}/{name}"
            plan = open_native_scan_plan([url], ["x"], decode_workers=2, fetch_ahead=6)
            try:
                assert plan.diagnostics()["fetch_ahead_depth"] == 6
                assert plan.row_group_count == N_ROW_GROUPS
            finally:
                plan.close()
            with pytest.raises(ValueError, match="must exceed the decode worker count"):
                open_native_scan_plan([url], ["x"], decode_workers=2, fetch_ahead=2)
        finally:
            proc.kill(); proc.wait()


@pytest.mark.xfail(
    strict=True,
    reason="PRE-EXISTING, found 2026-09-12 while adding fetch-ahead: open_native_scan_plan's "
           "H5 local-footer pre-pass probes _PARSED_FOOTER_CACHE with try_get(path, "
           "&footer_map[path]), which default-constructs an EMPTY entry on a miss; with "
           "exactly ONE cold local file the `len(_local_miss_paths) > 1` guard skips the "
           "batch fill, the main loop sees the entry as parsed and enumerates ZERO row "
           "groups. Same poison-on-miss class as test_footer_cache_row_loss.py. "
           "test_wp02_predicate_relocation::test_pruning_matches_direct_source_plan fails "
           "the same way. Not fixed here (out of scope); flip this to a passing test "
           "when it is.",
)
def test_native_plan_local_only_is_validated_but_not_armed():
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        plan = open_native_scan_plan([path], ["x"], decode_workers=2, fetch_ahead=6)
        try:
            assert plan.row_group_count == N_ROW_GROUPS
            assert plan.diagnostics()["fetch_ahead_depth"] == 0
        finally:
            plan.close()


# ── the two combinations that would be silently inert are rejected ───────────

def test_depth_not_exceeding_the_decode_workers_is_rejected():
    from opteryx.connectors.parquet_io.pool_reader import open_ipc_source

    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        for depth in (4, 2):
            with pytest.raises(ValueError, match="must exceed the decode worker count"):
                open_ipc_source(None, [path], ["x"], decode_workers=4, fetch_ahead=depth)
        with pytest.raises(ValueError, match="must be >= 0"):
            open_ipc_source(None, [path], ["x"], decode_workers=4, fetch_ahead=-1)


def test_explicit_window_smaller_than_depth_is_rejected():
    from opteryx.connectors.parquet_io.pool_reader import open_ipc_source

    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        with pytest.raises(ValueError, match="caps fetch depth"):
            open_ipc_source(None, [path], ["x"], decode_workers=2, fetch_ahead=8,
                            in_flight_limit_override=4)
        # Equal is fine: the window covers the depth exactly.
        src = open_ipc_source(None, [path], ["x"], decode_workers=2, fetch_ahead=8,
                              in_flight_limit_override=8)
        src.close()


# ── failure and cancellation ─────────────────────────────────────────────────

def test_fetch_stage_failure_surfaces_with_the_original_error():
    """Every range GET 503s. The fetch stage's failure must reach the consumer
    as the SAME error the coupled path raises (HttpClient's own "exhausted N
    retries" message) — carried on the item and rethrown by decode, not
    swallowed, and not turned into a second, hidden fetch attempt."""
    from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache

    with tempfile.TemporaryDirectory() as tmp:
        path, name = _write(tmp)
        proc, port = _server(tmp, error_rate=1.0)
        try:
            url = f"http://127.0.0.1:{port}/{name}"
            # Footer served from the bytes cache + a known size, so the ONLY
            # network requests are the row-group range GETs that will fail.
            cache = ParquetFooterBytesCache()
            cache.put(url, _footer_envelope(path))
            sizes = {url: os.path.getsize(path)}
            with pytest.raises(RuntimeError) as ei:
                _scan(url, fetch_ahead=6, footer_bytes_cache=cache, file_sizes=sizes)
        finally:
            proc.kill(); proc.wait()
    msg = str(ei.value)
    assert "Parquet pipeline error" in msg, msg
    assert "exhausted" in msg and "retries" in msg, msg


def test_early_abandonment_is_clean_and_discarded_bytes_are_reported():
    """Consume one row group with a deep fetch-ahead against a slow link, then
    abandon. The generator's finally cancels + closes without hanging, the rows
    already read are correct, and the bytes fetch-ahead bought for row groups
    nobody read are reported (bounded by what was fetched at all). The exact
    count is timing-dependent, so only the invariants are asserted."""
    with tempfile.TemporaryDirectory() as tmp:
        path, name = _write(tmp)
        proc, port = _server(tmp, rtt_ms=20)
        try:
            url = f"http://127.0.0.1:{port}/{name}"
            rows, diag = _scan(url, fetch_ahead=8, take=1)
        finally:
            proc.kill(); proc.wait()
    assert len(rows) == ROW_GROUP_SIZE
    assert all(0 <= v < N for v in rows)
    assert diag["fetch_ahead_depth"] == 8
    assert diag["cancelled_skips"] >= 0
    assert 0 <= diag["prefetch_discarded_bytes"] <= diag["bytes_fetched"]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
