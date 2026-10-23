"""The remote footer batch (rugo/src/parquet/filesystem.hpp FetchParquetFootersMany).

Binding a remote parquet dataset fetches every file's footer in one batch. Pinned
here over HTTP/1.1 against a server that counts requests and how many are in
flight at once:

  * a footer bigger than the old fixed 64 KiB suffix (rugo's 64k-row-group
    layout puts ~170 KiB in an 88 MB file) arrives in ONE suffix request per
    file, sized from the file (`footer_suffix_bytes`), not two;
  * the batch runs under its own connection cap, not the data path's 3 per host:
    with every request delayed, far more than 3 are in flight together;
  * the sizing rule itself: 1/256 of the file, between 64 KiB and 8 MiB, 256 KiB
    when the size is unknown.
"""

import os
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io.pool_reader import footer_suffix_bytes
from rugo import parquet as rp

from tests.unit.connectors.parquet_io.test_fetch_blocks import _register_http_workspace

N_FILES = 8
DELAY_S = 0.2
# The suffix rule assumes the footer is at most 1/256 of the file, which rugo's
# 64k-row-group layout gives (~1/500 on ClickBench). Per chunk a footer holds
# ~116 B and the data ~8 B x rows, so the ratio holds from ~3,700 rows per row
# group; 4,096 x 18 row groups x 40 columns is the smallest file whose footer is
# over the old 64 KiB suffix (~84 KB) at that ratio (~1/280).
RG_ROWS = 4096
N_RG = 18


def test_suffix_rule():
    KiB, MiB = 1024, 1024 * 1024
    assert footer_suffix_bytes(-1) == 256 * KiB
    assert footer_suffix_bytes(1 * MiB) == 64 * KiB            # floor
    assert footer_suffix_bytes(88 * MiB) == 88 * MiB // 256    # the ClickBench file
    assert footer_suffix_bytes(4 * 1024 * MiB) == 8 * MiB      # ceiling


class _Counting:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.ranges = []


def _server(root, counting):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _body(self):
            path = os.path.join(root, self.path.lstrip("/"))
            with open(path, "rb") as f:
                data = f.read()
            rng = self.headers.get("Range")
            if rng is None:
                return 200, data, None
            spec = rng.split("=", 1)[1]
            start, end = spec.split("-")
            if start == "":                       # suffix range: last N bytes
                n = min(int(end), len(data))
                start, end = len(data) - n, len(data) - 1
            else:
                start, end = int(start), min(int(end), len(data) - 1)
            return 206, data[start:end + 1], f"bytes {start}-{end}/{len(data)}"

        def do_HEAD(self):
            path = os.path.join(root, self.path.lstrip("/"))
            self.send_response(200)
            self.send_header("Content-Length", str(os.path.getsize(path)))
            self.end_headers()

        def do_GET(self):
            with counting.lock:
                counting.active += 1
                counting.peak = max(counting.peak, counting.active)
                counting.ranges.append((self.path, self.headers.get("Range")))
            try:
                time.sleep(DELAY_S)
                status, body, content_range = self._body()
                self.send_response(status)
                if content_range:
                    self.send_header("Content-Range", content_range)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                with counting.lock:
                    counting.active -= 1

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _wide_file():
    # Incompressible int64s, so the data is ~8 B a value (see RG_ROWS).
    import random

    rng = random.Random(4)
    rows = RG_ROWS * N_RG
    names = [f"c{i:02d}".encode() for i in range(40)]
    vectors = [vector_from_sequence([rng.getrandbits(63) for _ in range(rows)], DrakenType.INT64)
               for _ in range(40)]
    return rp.write_parquet(Morsel.from_vectors(names, vectors), max_rows_per_row_group=RG_ROWS,
                            bloom_filters=False)


@pytest.fixture(scope="module")
def served():
    with tempfile.TemporaryDirectory() as tmp:
        d = os.path.join(tmp, "wide")
        os.makedirs(d)
        data = _wide_file()
        for i in range(N_FILES):
            with open(os.path.join(d, f"part-{i:02d}.parquet"), "wb") as f:
                f.write(data)
        sizes = [(len(data), int.from_bytes(data[-8:-4], "little") + 8)] * N_FILES
        counting = _Counting()
        httpd = _server(tmp, counting)
        try:
            _register_http_workspace("fb_wide", tmp, httpd.server_address[1], "wide")
            yield counting, sizes
        finally:
            httpd.shutdown()


def test_each_footer_is_one_request_and_the_batch_is_not_capped_at_three(served):
    import opteryx

    counting, sizes = served
    size, footer = sizes[0]
    assert footer > 64 * 1024, "the fixture must exceed the old fixed suffix"
    assert footer <= min(size, footer_suffix_bytes(size)), "and fit the new one"

    session = opteryx.session()
    rows = [r for m in session.execute_to_morsels("SELECT COUNT(*) AS n FROM fb_wide.wide")
            for r in m.column("n").to_pylist()]
    session.close()
    assert rows == [N_FILES * RG_ROWS * N_RG]

    suffix_requests = [r for r in counting.ranges if r[1] and r[1].startswith("bytes=-")]
    per_file = {}
    for path, _ in suffix_requests:
        per_file[path] = per_file.get(path, 0) + 1
    assert len(per_file) == N_FILES, per_file
    assert set(per_file.values()) == {1}, per_file          # one round, not two
    assert counting.peak > 3, counting.peak                 # not the data path's cap


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
