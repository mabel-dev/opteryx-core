#!/usr/bin/env python3
"""
docs/io-process/test_iops.py
============================
Smoke-test / benchmark for the opteryx.iops package.

Exercises the same two-process ring-buffer pipeline as poc.py but uses the
public IOPSReader API rather than the hand-rolled internals.  Run this after
any change to opteryx/iops/ to confirm the package still works end-to-end.

Usage
-----
    python docs/io-process/test_iops.py

Environment variables
---------------------
    PROTOCOL      Storage protocol: "gs", "s3", or "file"  (default: "gs")
    BLOB_PREFIX   Object prefix to list blobs from for GCS/S3
                  (default: opteryx_store/test/tweets/data)
    BLOBS         Comma-separated explicit blob paths; overrides BLOB_PREFIX
    SUM_COLUMN    Column name to sum in each Parquet file  (default: followers)
    SLOT_SIZE_MB  Per-slot buffer size in MiB  (default: 64)
    SLOT_COUNT    Number of ring slots  (default: 16)
    MAX_INFLIGHT  Max concurrent downloads  (default: 8)
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import time
from dataclasses import dataclass
from dataclasses import field
from typing import List

os.environ["OPTERYX_DEBUG"] = "1"
os.environ["OPTERYX_TRACE"] = "1"
os.environ["OPTERYX_TRACE_FILE"] = "scratch/io_trace.jsonl"
os.environ["CONCURRENT_READS"] = "8"
os.environ["READ_BUFFER_NUM_PAGES"] = "4"
#os.environ.pop("OPTERYX_DEBUG", None)

os.remove("scratch/io_trace.jsonl") if os.path.exists("scratch/io_trace.jsonl") else None

#import opteryx
#import orso

sys.path.insert(1, os.path.join(sys.path[0], "../../../../mabel/orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../.."))
sys.path.insert(1, os.path.join(sys.path[0], "../../../pyiceberg-firestore-gcs"))


# ── Configuration ─────────────────────────────────────────────────────────────

PROTOCOL     = os.environ.get("PROTOCOL", "gs")
BLOB_PREFIX  = os.environ.get("BLOB_PREFIX", "opteryx_store/test/tweets/data")
BLOBS        = [p.strip() for p in os.environ.get("BLOBS", "").split(",") if p.strip()]
SUM_COLUMN   = os.environ.get("SUM_COLUMN", "followers")
SLOT_SIZE    = int(os.environ.get("SLOT_SIZE_MB", 64)) * 1024 * 1024
SLOT_COUNT   = int(os.environ.get("SLOT_COUNT", 16))
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT", 8))


# ── Blob discovery ────────────────────────────────────────────────────────────

def _list_blobs_gcs(prefix: str) -> List[str]:
    """List all objects under *prefix* using the GCS Storage SDK.

    *prefix* must start with the bucket name as the first path component,
    e.g. ``"my-bucket/path/to/data"``.  The full prefix (including the
    bucket component) is passed to ``list_blobs`` because the objects are
    stored with the bucket name as the leading path element inside the
    bucket — matching the convention used in poc.py.
    """
    from google.cloud import storage as gcs_storage

    bucket_name = prefix.split("/", 1)[0]
    client      = gcs_storage.Client()
    bucket      = client.bucket(bucket_name)
    # Return fully-qualified gs://bucket/object_name paths.
    # stream_to strips "gs://" then splits the first component as the bucket,
    # correctly resolving the GCS REST URL even when the object names inside
    # this bucket start with the bucket name as a leading path component.
    discovered  = [
        f"gs://{bucket_name}/{b.name}"
        for b in client.list_blobs(bucket, prefix=prefix)
        if not b.name.endswith("/")
    ]
    if not discovered:
        print(f"  [warn] No objects found under gs://{prefix}")
    return discovered


def _list_blobs_local(prefix: str) -> List[str]:
    """Walk *prefix* as a local directory and return all files."""
    results = []
    for root, _dirs, files in os.walk(prefix):
        for name in files:
            results.append(os.path.join(root, name))
    return results


def discover_blobs(protocol: str, prefix: str) -> List[str]:
    if protocol in ("gs", "gcs"):
        return _list_blobs_gcs(prefix)
    if protocol in ("s3",):
        raise NotImplementedError(
            "S3 blob listing not implemented in this script — set BLOBS explicitly."
        )
    return _list_blobs_local(prefix)


# ── Metrics ───────────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    blobs_ok:          int         = 0
    blobs_err:         int         = 0
    bytes_transferred: int         = 0
    gcs_latencies:     list[float] = field(default_factory=list)
    decode_times:      list[float] = field(default_factory=list)
    _t_start:          float       = field(default_factory=time.perf_counter, repr=False)

    def _pct(self, data: list[float]) -> tuple[float, float, float]:
        s = sorted(data)
        n = len(s)
        if n == 0:
            return 0.0, 0.0, 0.0
        return (
            s[int(n * 0.50)],
            s[min(int(n * 0.95), n - 1)],
            s[min(int(n * 0.99), n - 1)],
        )

    def summary(self) -> str:
        if not self.gcs_latencies:
            return "  No successful reads."
        gp50, gp95, gp99 = self._pct(self.gcs_latencies)
        dp50, dp95, dp99 = self._pct(self.decode_times)
        gcs_total    = sum(self.gcs_latencies)
        decode_total = sum(self.decode_times)
        grand_total  = gcs_total + decode_total or 1e-9
        wall_clock   = time.perf_counter() - self._t_start or 1e-9
        throughput   = self.bytes_transferred / wall_clock / (1024 * 1024)
        bottleneck   = "download" if gcs_total > decode_total else "Arrow decode"
        return (
            f"  blobs ok / err    : {self.blobs_ok} / {self.blobs_err}\n"
            f"  bytes transferred : {self.bytes_transferred:,}\n"
            f"  effective throughput: {throughput:.1f} MiB/s\n"
            f"\n"
            f"  download latency  : total={gcs_total:.3f}s  "
            f"p50={gp50:.3f}s  p95={gp95:.3f}s  p99={gp99:.3f}s\n"
            f"  Arrow decode time : total={decode_total:.3f}s  "
            f"p50={dp50:.3f}s  p95={dp95:.3f}s  p99={dp99:.3f}s\n"
            f"\n"
            f"  download share    : {100*gcs_total/grand_total:.1f}%\n"
            f"  decode share      : {100*decode_total/grand_total:.1f}%\n"
            f"  >> bottleneck     : {bottleneck}"
        )


# ── Main test ─────────────────────────────────────────────────────────────────

def run_test(protocol: str, blob_paths: List[str], column: str) -> float:
    """
    Spin up IOPSReader, stream every blob through the ring buffer, decode each
    Parquet payload with Arrow, sum *column*, and return the total.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from opteryx.iops import IOPSReader
    from opteryx.iops import RingConfig
    from opteryx.iops.ring import slot_state_counts

    cfg = RingConfig(
        slot_size    = SLOT_SIZE,
        slot_count   = SLOT_COUNT,
        max_inflight = MAX_INFLIGHT,
        shm_name     = "opteryx_iops_test_ring",
    )

    total_sum = 0.0
    metrics   = Metrics()

    print("=" * 62)
    print("opteryx.iops end-to-end test")
    print(f"  protocol    : {protocol}")
    print(f"  blobs       : {len(blob_paths)}")
    print(f"  column      : {column}")
    print(f"  slots       : {SLOT_COUNT} × {SLOT_SIZE >> 20} MiB")
    print(f"  max inflight: {MAX_INFLIGHT}")
    print(f"  ring_atomic : ", end="")
    try:
        from opteryx.iops.ring import _HAS_ATOMIC
        print("enabled (hardware CAS)" if _HAS_ATOMIC else "disabled (pure-Python fallback)")
    except Exception:
        print("unknown")
    print("=" * 62)

    t_overall = time.perf_counter()

    with IOPSReader(protocol=protocol, cfg=cfg) as reader:
        for payload in reader.iter_blobs(blob_paths):
            if payload.length == 0:
                # Propagated as an error payload — iter_blobs raises on errors,
                # but a length-0 success shouldn't happen; skip defensively.
                payload.release()
                metrics.blobs_err += 1
                continue

            metrics.gcs_latencies.append(payload.gcs_latency_s)
            metrics.bytes_transferred += payload.length

            print(
                f"  ← [{payload.request_id:04d}] slot={payload.slot_id}"
                f"  {payload.length:>12,} bytes"
                f"  download={payload.gcs_latency_s:.3f}s"
                f"  path=…{payload.blob_path[-40:]}"
            )

            # Zero-copy decode: Arrow reads directly from the shm memoryview.
            # pq.read_table() fully decodes into Arrow-owned memory before
            # returning — the slot can be released immediately after.
            t_decode = time.perf_counter()
            raw = payload.data
            try:
                table = pq.read_table(pa.BufferReader(raw), columns=[column])
            except Exception as exc:
                print(f"       [decode error] {exc}")
                metrics.blobs_err += 1
                payload.release()
                continue
            finally:
                del raw  # release exported shm pointer before payload.release()

            payload.release()   # FREE the slot as soon as Arrow has a copy

            decode_s = time.perf_counter() - t_decode
            metrics.decode_times.append(decode_s)

            col_result = pc.sum(table.column(column))
            if col_result.is_valid:
                total_sum += col_result.as_py()
            del table

            metrics.blobs_ok += 1
            print(
                f"       decode={decode_s:.4f}s"
                f"  running_sum={total_sum:,.0f}"
            )

    elapsed = time.perf_counter() - t_overall

    print()
    print("=" * 62)
    print(f"  Total wall time : {elapsed:.3f}s")
    print(f"  Sum of '{column}' : {total_sum:,.4f}")
    print()
    print("Metrics:")
    print(metrics.summary())
    print("=" * 62)

    return total_sum


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Ensure spawn start method so the worker process does not inherit any
    # open file descriptors or GCS client state from this process.
    multiprocessing.set_start_method("spawn", force=True)

    blob_paths = BLOBS or discover_blobs(PROTOCOL, BLOB_PREFIX)
    if not blob_paths:
        sys.exit(
            f"No blobs found.  Set BLOBS or BLOB_PREFIX correctly for protocol={PROTOCOL!r}."
        )
    print(f"  discovered {len(blob_paths)} blob(s)")

    run_test(PROTOCOL, blob_paths, SUM_COLUMN)
