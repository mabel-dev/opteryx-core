"""
Throttling HTTP file server for remote-blob-storage IO benchmarking (WP-2).

Serves files from a root directory with configurable request latency,
per-connection bandwidth, and fault injection, so the hostile remote
environment (GCS-like: ~50-200ms RTT, a few hundred Mbps, occasional 503s)
is reproducible on a dev machine against the real C++ IO pipeline.

Dev tooling only — never imported by production code.

Run standalone:
    python dev/throttle_server.py --root /path/to/files --port 9876 \
        --rtt-ms 100 --bandwidth-mbps 300 --error-rate 0.0 --seed 17

Or programmatically (from the bench harness, as a subprocess):
    proc = subprocess.Popen([sys.executable, "dev/throttle_server.py", ...])

Behaviour:
- HEAD: returns Content-Length (required by the C++ footer fetcher).
- GET with Range: returns 206 with the requested slice.
- GET without Range: returns the whole file.
- Latency: sleeps rtt_ms before the first response byte of every request.
- Bandwidth: writes in 64 KB chunks, sleeping per chunk to cap the
  per-connection rate. The cap is per connection, not global — with N
  concurrent connections aggregate throughput is N * bandwidth. Use a low
  per-connection cap to emulate per-stream object-store limits.
- Faults: with probability error_rate the request gets a 503 before any
  body bytes (deterministic per (seed, request_counter) for reproducibility).
"""

import argparse
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CHUNK_SIZE = 64 * 1024


class ThrottleConfig:
    def __init__(self, root, rtt_ms=0.0, bandwidth_mbps=0.0, error_rate=0.0, seed=0):
        self.root = os.path.abspath(root)
        self.rtt_s = rtt_ms / 1000.0
        # bytes/sec; 0 = unthrottled
        self.bandwidth_bps = bandwidth_mbps * 1_000_000 / 8.0
        self.error_rate = error_rate
        self.seed = seed
        self.request_counter = 0
        self.counter_lock = threading.Lock()

    def next_request_id(self):
        with self.counter_lock:
            self.request_counter += 1
            return self.request_counter

    def should_fault(self, request_id):
        if self.error_rate <= 0.0:
            return False
        # Deterministic per (seed, request_id); combine into a single int seed
        # because random.Random rejects tuple seeds.
        combined = (self.seed * 1_000_003) ^ (request_id * 2_654_435_761)
        return random.Random(combined).random() < self.error_rate


class ThrottleHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    config: ThrottleConfig = None  # set by serve()

    # Silence per-request logging — it distorts timing and floods stderr.
    def log_message(self, format, *args):
        pass

    def _resolve_path(self):
        rel = self.path.lstrip("/").split("?")[0]
        full = os.path.abspath(os.path.join(self.config.root, rel))
        if not full.startswith(self.config.root):
            return None  # path traversal attempt
        if not os.path.isfile(full):
            return None
        return full

    def _apply_rtt(self):
        if self.config.rtt_s > 0:
            time.sleep(self.config.rtt_s)

    def do_HEAD(self):
        self._apply_rtt()
        full = self._resolve_path()
        if full is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(full)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        request_id = self.config.next_request_id()
        self._apply_rtt()

        if self.config.should_fault(request_id):
            self.send_error(503, "Injected fault")
            return

        full = self._resolve_path()
        if full is None:
            self.send_error(404)
            return

        file_size = os.path.getsize(full)
        range_header = self.headers.get("Range")
        start, end = 0, file_size - 1
        status = 200
        if range_header and range_header.startswith("bytes="):
            spec = range_header[len("bytes="):].split("-")
            if spec[0]:
                start = int(spec[0])
                end = int(spec[1]) if len(spec) > 1 and spec[1] else file_size - 1
            else:
                # suffix range: bytes=-N
                start = max(0, file_size - int(spec[1]))
                end = file_size - 1
            end = min(end, file_size - 1)
            if start > end:
                self.send_error(416)
                return
            status = 206

        length = end - start + 1
        self.send_response(status)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.end_headers()

        bps = self.config.bandwidth_bps
        with open(full, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    break
                t0 = time.monotonic()
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    return  # client went away (e.g. LIMIT early-exit)
                remaining -= len(chunk)
                if bps > 0:
                    # Sleep off the remainder of this chunk's bandwidth budget.
                    budget = len(chunk) / bps
                    elapsed = time.monotonic() - t0
                    if budget > elapsed:
                        time.sleep(budget - elapsed)


def serve(root, port, rtt_ms=0.0, bandwidth_mbps=0.0, error_rate=0.0, seed=0):
    """Start the server (blocking). Returns only on shutdown."""
    config = ThrottleConfig(root, rtt_ms, bandwidth_mbps, error_rate, seed)
    handler = type("BoundThrottleHandler", (ThrottleHandler,), {"config": config})
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    # Announce readiness on stdout so a parent process can wait for it.
    sys.stdout.write(f"READY port={server.server_address[1]}\n")
    sys.stdout.flush()
    server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--root", required=True, help="directory of files to serve")
    parser.add_argument("--port", type=int, default=0, help="port (0 = ephemeral)")
    parser.add_argument("--rtt-ms", type=float, default=0.0)
    parser.add_argument("--bandwidth-mbps", type=float, default=0.0, help="per-connection cap; 0 = unthrottled")
    parser.add_argument("--error-rate", type=float, default=0.0, help="probability of an injected 503 per GET")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    serve(args.root, args.port, args.rtt_ms, args.bandwidth_mbps, args.error_rate, args.seed)


if __name__ == "__main__":
    main()
