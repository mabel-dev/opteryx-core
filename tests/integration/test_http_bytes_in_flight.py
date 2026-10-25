"""HttpTuning::max_bytes_in_flight: the process-wide cap on request bytes on the wire
(<0 off, 0 adaptive, >0 fixed). The adaptive cap's ramp and its behaviour on slow and fast
links are measured by scratch/repro_s3_shaped_scan.py --bench, not asserted here.

Eight threads each fetch a two-range batch (1 MiB) from a server that paces every response
(2 MB/s), so uncapped they overlap. The server records the peak bytes it had in flight, which
is the server-side truth the client cap must bound. The cap is read once per process into a
static, so each case runs in a child process.

The server is a small stdlib one defined here: this test needs exactly one thing from it - its
own peak - and no S3 behaviour, so it depends on no external server.
"""

import http.server
import os
import subprocess
import sys
import threading
import time

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, _ROOT)

RANGE = 512 * 1024
BATCH = 2 * RANGE
THREADS = 8

_CHILD = r"""
import sys, threading
from opteryx.compiled.http_client import HttpClient
url, threads, rng = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
sizes = []
def fetch():
    out = HttpClient().get_many([(url, {"Range": f"bytes=0-{rng - 1}"}),
                                 (url, {"Range": f"bytes={rng}-{2 * rng - 1}"})])
    sizes.append([len(b) for b in out])
ts = [threading.Thread(target=fetch) for _ in range(threads)]
[t.start() for t in ts]; [t.join() for t in ts]
assert sizes == [[rng, rng]] * threads, sizes
print("OK")
"""


BYTES_PER_SECOND = 2_000_000   # 16 Mbps per response
CHUNK = 64 * 1024


class _PacedBlobServer:
    """Serves one blob over HTTP range requests at BYTES_PER_SECOND per response, and
    records the most response bytes it ever had in flight at once."""

    def __init__(self, blob):
        self.peak_bytes_in_flight = 0
        in_flight = [0]
        lock = threading.Lock()
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                start, end = 0, len(blob) - 1
                spec = self.headers.get("Range")
                if spec:
                    start, end = (int(part) for part in spec.split("=")[1].split("-"))
                body = blob[start : end + 1]
                self.send_response(206 if spec else 200)
                self.send_header("Content-Length", str(len(body)))
                if spec:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(blob)}")
                self.end_headers()
                with lock:
                    in_flight[0] += len(body)
                    outer.peak_bytes_in_flight = max(outer.peak_bytes_in_flight, in_flight[0])
                try:
                    for offset in range(0, len(body), CHUNK):
                        piece = body[offset : offset + CHUNK]
                        # Pace BEFORE the write: the last write must be the last act, or the
                        # response stays "in flight" here after the client has all its bytes.
                        time.sleep(len(piece) / BYTES_PER_SECOND)
                        self.wfile.write(piece)
                finally:
                    with lock:
                        in_flight[0] -= len(body)

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.endpoint = f"http://127.0.0.1:{self._server.server_address[1]}"

    def __enter__(self):
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()


def _peak_bytes_in_flight(cap):
    with _PacedBlobServer(os.urandom(BATCH)) as server:
        env = dict(os.environ, OPTERYX_HTTP_MAX_BYTES_IN_FLIGHT=str(cap))
        result = subprocess.run(
            [sys.executable, "-c", _CHILD, f"{server.endpoint}/b/blob.bin", str(THREADS), str(RANGE)],
            env=env, cwd=_ROOT, capture_output=True, text=True, timeout=120,
        )
        assert result.stdout.strip() == "OK", result.stdout + result.stderr
        return server.peak_bytes_in_flight


def test_no_cap_lets_batches_overlap():
    assert _peak_bytes_in_flight(-1) > 3 * BATCH


def test_adaptive_default_does_not_throttle_within_the_assumed_minimum_link():
    # Adaptive (0) opens at the assumed minimum bandwidth x timeout floor (75 MB at 60 Mbps),
    # far above these 8 MiB of traffic, so batches overlap exactly as with no cap.
    assert _peak_bytes_in_flight(0) > 3 * BATCH


def test_cap_bounds_bytes_in_flight_and_every_batch_still_completes():
    # One batch (1 MiB) fits under 1.5 MiB; two do not, so batches are admitted one at a time.
    peak = _peak_bytes_in_flight(BATCH + BATCH // 2)
    assert peak <= BATCH + BATCH // 2, peak


def test_a_batch_larger_than_the_cap_is_admitted_alone_not_deadlocked():
    # Cap far below one batch: each batch runs alone, in turn, and all eight complete.
    peak = _peak_bytes_in_flight(1024)
    assert peak <= BATCH, peak
