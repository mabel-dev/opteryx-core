"""A retry after a transport failure must not ride the connection that failed.

Over HTTP/2 a stalled transfer is aborted by resetting its stream; the
connection stays in curl's cache, and with multiplexing (PIPEWAIT) the retry
joins it again - and stalls again, however many retries there are. Seen on
2026-11-14: ClickBench over OData on Cloud Run failed with
`get_many: exhausted 2 retries (... stalled (no bytes for 5000ms)) received=0`,
every attempt dead, while the same scans read through gRPC never stalled.

The server stalls every stream on its first connection and serves normally on
any later one, so a read succeeds only if its retry dialled a new connection.
Run in a child so OPTERYX_HTTP_* (read once per process) apply.
"""

import os
import subprocess
import sys

import pytest

pytest.importorskip("h2")
pytest.importorskip("cryptography")

sys.path.insert(1, os.path.dirname(__file__))
from _h2_range_server import H2RangeServer  # noqa: E402
from _h2_range_server import object_bytes  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))

_CHILD = r"""
import sys
from opteryx.compiled.http_client import HttpClient
mode, url, expected = sys.argv[1], sys.argv[2], bytes.fromhex(sys.argv[3])
client = HttpClient()
ranges = [(0, 4096), (65536, 4096), (131072, 4096)]
try:
    if mode == "get_many":
        out = client.get_many([(url, {"Range": f"bytes={o}-{o + n - 1}"}) for o, n in ranges])
        assert out[1] == expected, "wrong bytes"
    else:
        o, n = ranges[1]
        assert client.get(url, {"Range": f"bytes={o}-{o + n - 1}"}) == expected, "wrong bytes"
    print("OK")
except RuntimeError as e:
    print(f"ERR {e}")
"""


def _read(mode, stall_connections, retries):
    with H2RangeServer(stall_connections=stall_connections) as server:
        res = subprocess.run(
            [sys.executable, "-c", _CHILD, mode, f"https://localhost:{server.port}/object",
             object_bytes(65536, 4096).hex()],
            env=dict(os.environ, SSL_CERT_FILE=server.ca_file, OPTERYX_HTTP_STALL_MS="500",
                     OPTERYX_HTTP_MAX_RETRIES=str(retries)),
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        return (res.stdout.strip() or res.stderr.strip()), server.connections


@pytest.mark.parametrize("mode", ["get_many", "get"])
def test_reads_over_h2(mode):
    out, _ = _read(mode, stall_connections=0, retries=0)
    assert out == "OK", out


# get_many's three ranges dial three connections (PIPEWAIT is off by default); only
# the first stalls, so the two healthy ones are in the cache when the retry comes.
# Before the fix the retry joined the dead one and the count stayed at three.
@pytest.mark.parametrize("mode, dialled", [("get_many", 4), ("get", 2)])
def test_a_retry_after_a_stall_dials_a_new_connection(mode, dialled):
    out, connections = _read(mode, stall_connections=1, retries=1)
    assert out == "OK", out
    assert connections == dialled, f"{connections} connections accepted, expected {dialled}"


# Every connection stalls, so all three of get_many's ranges retry, each time on
# connections of their own: 3 x (1 + 2 retries) = 9. get: 1 + 2 = 3.
@pytest.mark.parametrize("mode, dialled", [("get_many", 9), ("get", 3)])
def test_a_dead_host_still_exhausts_its_retries(mode, dialled):
    """Fresh connections are not a free pass: when every one stalls, the read fails."""
    out, connections = _read(mode, stall_connections=99, retries=2)
    assert out.startswith("ERR"), out
    assert "stalled (no bytes for 500ms)" in out, out
    assert connections == dialled, f"{connections} connections accepted, expected {dialled}"
