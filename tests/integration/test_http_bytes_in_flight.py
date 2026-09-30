"""HttpTuning::max_bytes_in_flight: the process-wide cap on request bytes on the wire
(<0 off, 0 adaptive, >0 fixed). The adaptive cap's ramp and its behaviour on slow and fast
links are measured by scratch/repro_s3_shaped_scan.py --bench, not asserted here.

Eight threads each fetch a two-range batch (1 MiB) from a hadro that paces every response
(2 MB/s), so uncapped they overlap. hadro's own stats report the peak bytes it had in flight,
which is the server-side truth the client cap must bound. The cap is read once per process into
a static, so each case runs in a child process.

hadro is found as an installed package, else as the sibling checkout ../hadro/src.
"""

import json
import os
import subprocess
import sys

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_HADRO = os.path.abspath(os.path.join(_ROOT, "..", "hadro", "src"))
if os.path.isdir(_HADRO) and _HADRO not in sys.path:
    sys.path.insert(1, _HADRO)
sys.path.insert(1, _ROOT)

hadro = pytest.importorskip("hadro", reason="hadro (S3 test server) is not importable")

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


def _peak_bytes_in_flight(tmp_path, cap):
    import urllib.request

    bucket = tmp_path / "b"
    bucket.mkdir()
    (bucket / "blob.bin").write_bytes(os.urandom(BATCH))
    with hadro.Server(data=str(tmp_path), bandwidth_mbps=16, stats=True) as server:
        env = dict(os.environ, OPTERYX_HTTP_MAX_BYTES_IN_FLIGHT=str(cap))
        result = subprocess.run(
            [sys.executable, "-c", _CHILD, f"{server.endpoint}/b/blob.bin", str(THREADS), str(RANGE)],
            env=env, cwd=_ROOT, capture_output=True, text=True, timeout=120,
        )
        assert result.stdout.strip() == "OK", result.stdout + result.stderr
        with urllib.request.urlopen(f"{server.endpoint}/_shaping/stats") as response:
            return json.load(response)["peak_bytes_in_flight"]


def test_no_cap_lets_batches_overlap(tmp_path):
    assert _peak_bytes_in_flight(tmp_path, -1) > 3 * BATCH


def test_adaptive_default_does_not_throttle_within_the_assumed_minimum_link(tmp_path):
    # Adaptive (0) opens at the assumed minimum bandwidth x timeout floor (75 MB at 60 Mbps),
    # far above these 8 MiB of traffic, so batches overlap exactly as with no cap.
    assert _peak_bytes_in_flight(tmp_path, 0) > 3 * BATCH


def test_cap_bounds_bytes_in_flight_and_every_batch_still_completes(tmp_path):
    # One batch (1 MiB) fits under 1.5 MiB; two do not, so batches are admitted one at a time.
    peak = _peak_bytes_in_flight(tmp_path, BATCH + BATCH // 2)
    assert peak <= BATCH + BATCH // 2, peak


def test_a_batch_larger_than_the_cap_is_admitted_alone_not_deadlocked(tmp_path):
    # Cap far below one batch: each batch runs alone, in turn, and all eight complete.
    peak = _peak_bytes_in_flight(tmp_path, 1024)
    assert peak <= BATCH, peak
