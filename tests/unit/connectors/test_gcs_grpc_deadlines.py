"""The native gRPC reader's progress deadlines (docs/REMOTE_READ_FLOW_CONTROL_DESIGN.md
§4.1, test §6.6), against a local TLS + HTTP/2 ReadObject server: a stream that goes
`stall_ms` without bytes fails the read, retryably, naming the range; a read still
moving at `ceiling_ms` fails as the ceiling. The front-end transport (TLS) is used;
DirectPath shares the same read_many and stream code."""

import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.dirname(__file__))

from _grpc_read_object_server import ReadObjectServer
from _grpc_read_object_server import object_bytes

from opteryx.compiled.gcs_grpc import GcsGrpcClient
from opteryx.compiled.gcs_grpc import GcsGrpcError

RANGES = [(0, 16384), (65536, 16384), (131072, 16384)]


def _read(server, stall_ms, ceiling_ms):
    client = GcsGrpcClient(host="localhost", port=server.port, connections=1, ca_file=server.ca_file)
    t0 = time.monotonic()
    try:
        out, _ = client.read_many("bucket", "object", RANGES, "Bearer test",
                                  stall_ms=stall_ms, ceiling_ms=ceiling_ms)
        return out, time.monotonic() - t0
    except GcsGrpcError as e:
        return e, time.monotonic() - t0


def test_ranges_read_back_whole():
    with ReadObjectServer() as server:
        out, _ = _read(server, stall_ms=1000, ceiling_ms=10000)
    assert not isinstance(out, Exception), out
    assert out == [object_bytes(o, n) for o, n in RANGES]


def test_a_stalled_stream_fails_the_read_promptly():
    """Range 1 sends half its bytes and then nothing; the other two complete."""
    with ReadObjectServer(stall_offset=65536) as server:
        err, elapsed = _read(server, stall_ms=500, ceiling_ms=10000)
    assert isinstance(err, GcsGrpcError), err
    assert err.retryable
    assert "stalled (no bytes for 500 ms) on range 1 after 8192 of 16384 bytes" in str(err), err
    assert elapsed < 2.0, f"stall took {elapsed:.2f}s to fail"


def test_a_crawling_read_fails_at_the_ceiling():
    """A message every 50 ms is never a 500 ms stall, but 16 messages per range outlive 400 ms."""
    with ReadObjectServer(drip_s=0.05) as server:
        err, elapsed = _read(server, stall_ms=500, ceiling_ms=400)
    assert isinstance(err, GcsGrpcError), err
    assert err.retryable
    assert "exceeded the 400 ms ceiling" in str(err), err
    assert elapsed < 1.5, f"ceiling took {elapsed:.2f}s to fire"


def test_non_positive_deadlines_are_refused():
    with ReadObjectServer() as server:
        client = GcsGrpcClient(host="localhost", port=server.port, connections=1, ca_file=server.ca_file)
        with pytest.raises(ValueError, match="must be positive"):
            client.read_many("bucket", "object", RANGES, "Bearer test", stall_ms=0, ceiling_ms=1000)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
