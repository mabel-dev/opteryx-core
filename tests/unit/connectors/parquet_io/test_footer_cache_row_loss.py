"""
Regression: a parsed-footer-cache miss must not poison the footer map.

`open_ipc_source`'s batch footer prefetch probed the parsed-footer cache with
`try_get(path, &footer_map[path])`. Evaluating that argument default-constructs
an *empty* FileStats in `footer_map` even when the probe misses. The next line
short-circuits on a footer-bytes-cache hit, so the footer is never parsed — and
the main loop then sees `footer_map.count(path) != 0`, treats the empty entry as
a parsed footer, iterates its zero row groups, and silently drops the file's rows.

The two caches evict independently, so "parsed miss + bytes hit" is reachable in
production. This test constructs exactly that state.
"""

import os
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)

N_ROWS = 5000


def _footer_envelope(path):
    """Rebuild what FetchParquetFooter returns: PAR1 + thrift + len_le32 + PAR1.

    Built here in Python so we can populate the bytes cache *without* going
    through the reader (which would also warm the parsed-footer cache and hide
    the bug).
    """
    with open(path, "rb") as f:
        blob = f.read()
    assert blob[:4] == b"PAR1" and blob[-4:] == b"PAR1", "not a parquet file"
    footer_len = int.from_bytes(blob[-8:-4], "little")
    thrift = blob[-8 - footer_len : -8]
    return b"PAR1" + thrift + footer_len.to_bytes(4, "little") + b"PAR1"


def _server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0"],
        stdout=subprocess.PIPE,
        text=True,
    )
    port = int(proc.stdout.readline().strip().split("port=")[1])
    return proc, port


def test_bytes_cache_hit_with_parsed_cache_miss_reads_all_rows():
    from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    table = pa.table({"id": pa.array(list(range(N_ROWS)), type=pa.int64())})

    with tempfile.TemporaryDirectory() as tmp:
        # A unique name keeps this path out of the process-global parsed-footer
        # cache, which has no public handle to clear.
        fname = f"footer_cache_row_loss_{os.getpid()}.parquet"
        path = os.path.join(tmp, fname)
        pq.write_table(table, path, row_group_size=1000)

        proc, port = _server(tmp)
        try:
            url = f"http://127.0.0.1:{port}/{fname}"

            # Bytes cache: HIT.  Parsed cache: MISS (never seen this URL).
            # This is the state the batch-prefetch short-circuit mishandled.
            bytes_cache = ParquetFooterBytesCache()
            bytes_cache.put(url, _footer_envelope(path))
            assert bytes_cache.get(url) is not None, "precondition: bytes cache must hit"

            got = []
            for _scan_rg, rg in iter_row_groups_ipc(
                None, [url], ["id"], footer_bytes_cache=bytes_cache
            ):
                got.extend(rg[b"id"].to_pylist())
        finally:
            proc.terminate()
            proc.wait(timeout=10)

    # Before the fix this returned zero rows: the file was silently skipped.
    assert sorted(got) == list(range(N_ROWS)), f"expected {N_ROWS} rows, got {len(got)}"
