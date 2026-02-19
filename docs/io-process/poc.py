#!/usr/bin/env python3
"""
POC: IO/EXEC Process Isolation with Shared Memory Ring Buffer
=============================================================
Tests the two-process architecture from design.md:

  IO Process  – downloads Parquet blobs from GCS into shared-memory slots
  EXEC Process – decodes Parquet from shared memory and sums a target column

Cut-down parameters vs. production design:
  - 4 slots  (production: 24)
  - 4 MB per slot  (production: 64 MB)
  - 2 in-flight reads  (production: 8)

Usage
-----
    # set required env vars, then:
    python poc.py

Environment variables
---------------------
    GCS_BUCKET        GCS bucket name  (required)
    GCS_PREFIX        object prefix to list blobs from
                      (default: opteryx_store/opteryx_store/test/tweets/data)
    GCS_BLOBS         comma-separated explicit blob paths; overrides GCS_PREFIX
    SUM_COLUMN        column name to sum  (default: "followers")
    SLOT_SIZE_MB      per-slot size in MB  (default: 64)
    SLOT_COUNT        number of ring slots  (default: 8)
    MAX_INFLIGHT      max simultaneous in-flight reads  (default: 2)
"""

from __future__ import annotations

import ctypes
import io
import multiprocessing
import os
import struct
import time
from dataclasses import dataclass
from dataclasses import field
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing.shared_memory import SharedMemory
from typing import Optional

# ── Configuration ──────────────────────────────────────────────────────────────

GCS_BUCKET   = "opteryx_store"
GCS_PREFIX   = os.environ.get("GCS_PREFIX", "opteryx_store/test/tweets/data")
GCS_BLOBS    = [p.strip() for p in os.environ.get("GCS_BLOBS", "").split(",") if p.strip()]
SUM_COLUMN   = os.environ.get("SUM_COLUMN", "followers")
SLOT_SIZE    = int(os.environ.get("SLOT_SIZE_MB", 64)) * 1024 * 1024
SLOT_COUNT   = int(os.environ.get("SLOT_COUNT", 16))
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT", 8))

SHM_NAME = "opteryx_poc_ring"


# ── Blob discovery ────────────────────────────────────────────────────────────

def list_gcs_blobs(bucket_name: str, prefix: str) -> list[str]:
    """
    List all object paths under *prefix* in *bucket_name*.
    Returns paths relative to the bucket root (i.e. suitable for blob.download_as_bytes).
    Only returns objects whose names end with .parquet (or no extension, treating
    directory-style prefixes as containers).
    """
    from google.cloud import storage as gcs_storage

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blobs  = client.list_blobs(bucket, prefix=prefix)
    paths  = [
        b.name for b in blobs
        if not b.name.endswith("/")          # skip folder placeholders
    ]
    if not paths:
        raise SystemExit(
            f"No objects found at gs://{bucket_name}/{prefix}\n"
            "Check GCS_BUCKET and GCS_PREFIX are set correctly."
        )
    return paths


# ── Slot state constants  (§6.2) ───────────────────────────────────────────────

FREE    = 0
WRITING = 1
READY   = 2
READING = 3

STATE_NAMES = {FREE: "FREE", WRITING: "WRITING", READY: "READY", READING: "READING"}


# ── Shared-memory layout  (§6.1) ───────────────────────────────────────────────
#
#  Each slot:
#    offset 0  : state      (uint32)   – governs ownership
#    offset 4  : length     (uint32)   – payload bytes written; valid once READY
#    offset 8  : request_id (uint64)   – echoes originating ReadRequest
#    offset 16 : pad        (48 bytes) – brings header to 64 bytes (cache-line)
#    offset 64 : payload    (SLOT_SIZE bytes)
#
#  slot stride = 64 + SLOT_SIZE
#  total size  = SLOT_COUNT * slot_stride

SLOT_HEADER_SIZE = 64
SLOT_STRIDE      = SLOT_HEADER_SIZE + SLOT_SIZE
TOTAL_SIZE       = SLOT_COUNT * SLOT_STRIDE

_HEADER_FMT      = "<IIQ"          # state, length, request_id (16 bytes)
_STATE_FMT       = "<I"            # state only (4 bytes)


def _slot_offset(slot_id: int) -> int:
    return slot_id * SLOT_STRIDE


def write_slot_header(buf, slot_id: int, state: int, length: int, request_id: int) -> None:
    struct.pack_into(_HEADER_FMT, buf, _slot_offset(slot_id), state, length, request_id)


def read_slot_header(buf, slot_id: int):
    state, length, request_id = struct.unpack_from(_HEADER_FMT, buf, _slot_offset(slot_id))
    return state, length, request_id


def write_slot_state(buf, slot_id: int, state: int) -> None:
    struct.pack_into(_STATE_FMT, buf, _slot_offset(slot_id), state)


def read_slot_state(buf, slot_id: int) -> int:
    (state,) = struct.unpack_from(_STATE_FMT, buf, _slot_offset(slot_id))
    return state


def write_payload(shm: SharedMemory, slot_id: int, data: bytes) -> None:
    off = _slot_offset(slot_id) + SLOT_HEADER_SIZE
    shm.buf[off : off + len(data)] = data


def read_payload(shm: SharedMemory, slot_id: int, length: int) -> memoryview:
    """Zero-copy view into the slot payload — no bytes() allocation."""
    off = _slot_offset(slot_id) + SLOT_HEADER_SIZE
    return shm.buf[off : off + length]


# ── Control-plane messages  (§7) ───────────────────────────────────────────────

@dataclass
class ReadRequest:
    request_id: int
    blob_path: str


@dataclass
class ReadComplete:
    request_id: int
    slot_id: int
    length: int
    gcs_latency_s: float = 0.0
    error: Optional[str] = None


SHUTDOWN = None   # sentinel sent by EXEC to stop IO worker


# ── IO Worker Process  (§8.1) ──────────────────────────────────────────────────

def io_worker(
    shm_name: str,
    pipe_conn,          # child end of Pipe
    bucket_name: str,
) -> None:
    """
    Runs in a separate spawned process.

    A ThreadPoolExecutor (max_workers=MAX_INFLIGHT) issues concurrent GCS
    downloads.  The main thread receives ReadRequests from EXEC and submits
    them to the pool; each worker thread owns its download, writes the payload
    into a shared-memory slot, and sends ReadComplete back via the Pipe.

    Locks:
      _slot_lock  – atomically claims a FREE slot (scan + WRITING transition)
      _send_lock  – serialises Pipe.send() calls from worker threads
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    # Import GCS client inside the worker (spawn: no inherited state)
    try:
        from google.cloud import storage as gcs_storage

        # Size the connection pool to match concurrency so threads never
        # queue waiting for a free HTTP connection.
        client  = gcs_storage.Client()
        client._connection.API_BASE_URL  # ensure client is initialised
        try:
            adapter = client._http.adapters.get("https://")
            if adapter:
                adapter._pool_maxsize     = MAX_INFLIGHT
                adapter._pool_connections = MAX_INFLIGHT
        except Exception:
            pass  # best-effort; not critical
        bucket  = client.bucket(bucket_name)
    except Exception as exc:
        pipe_conn.send(f"ERROR:{exc}")
        return

    shm  = SharedMemory(name=shm_name)
    buf  = shm.buf

    _slot_lock = threading.Lock()   # atomically claim FREE → WRITING
    _send_lock = threading.Lock()   # Pipe.send() is not thread-safe

    def claim_free_slot() -> int:
        """Spin until a FREE slot is found; atomically mark it WRITING."""
        while True:
            with _slot_lock:
                for i in range(SLOT_COUNT):
                    if read_slot_state(buf, i) == FREE:
                        write_slot_state(buf, i, WRITING)
                        return i
            time.sleep(0.001)

    def download_one(req: ReadRequest) -> None:
        slot_id = claim_free_slot()
        write_slot_header(buf, slot_id, WRITING, 0, req.request_id)

        # Write directly into the shared-memory slot to avoid the intermediate
        # bytes allocation that download_as_bytes() would create.
        off   = _slot_offset(slot_id) + SLOT_HEADER_SIZE
        slot_view = shm.buf[off : off + SLOT_SIZE]

        class _SlotWriter(io.RawIOBase):
            """File-like that streams GCS chunks straight into the shm slot."""
            __slots__ = ("pos",)
            def __init__(self):
                self.pos = 0
            def write(self, b: bytes) -> int:  # type: ignore[override]
                n = len(b)
                slot_view[self.pos : self.pos + n] = b
                self.pos += n
                return n

        writer = _SlotWriter()
        t0 = time.perf_counter()
        try:
            bucket.blob(req.blob_path).download_to_file(writer)
            gcs_latency = time.perf_counter() - t0
            length      = writer.pos
            del slot_view, writer  # release exported pointer on shm before state transition

            if length > SLOT_SIZE:
                raise ValueError(
                    f"Blob {req.blob_path!r} is {length:,} bytes; "
                    f"exceeds slot size {SLOT_SIZE:,}"
                )

            write_slot_header(buf, slot_id, READY, length, req.request_id)

            with _send_lock:
                pipe_conn.send(
                    ReadComplete(req.request_id, slot_id, length, gcs_latency)
                )

        except Exception as exc:
            del slot_view, writer  # release exported pointer before freeing slot
            write_slot_state(buf, slot_id, FREE)
            with _send_lock:
                pipe_conn.send(
                    ReadComplete(req.request_id, -1, 0, error=str(exc))
                )

    pipe_conn.send("READY")

    with ThreadPoolExecutor(max_workers=MAX_INFLIGHT, thread_name_prefix="gcs") as pool:
        while True:
            msg = pipe_conn.recv()
            if msg is SHUTDOWN:
                break
            pool.submit(download_one, msg)

    # Release buf (shm.buf memoryview) before closing; same rule as EXEC side —
    # any live reference to shm.buf blocks mmap.close() with "exported pointers exist".
    del buf
    shm.close()


# ── Metrics  (§13) ────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    blobs_ok: int = 0
    blobs_err: int = 0
    bytes_transferred: int = 0
    gcs_latencies: list[float] = field(default_factory=list)
    decode_times: list[float] = field(default_factory=list)      # parquet decode + compute
    exec_wait_times: list[float] = field(default_factory=list)   # time blocked waiting for READY

    def _percentiles(self, data: list[float]) -> tuple[float, float, float]:
        s = sorted(data)
        n = len(s)
        return s[int(n * 0.50)], s[min(int(n * 0.95), n-1)], s[min(int(n * 0.99), n-1)]

    def summary(self) -> str:
        if not self.gcs_latencies:
            return "No successful reads."
        gp50, gp95, gp99 = self._percentiles(self.gcs_latencies)
        dp50, dp95, dp99 = self._percentiles(self.decode_times)
        exec_avg = (
            sum(self.exec_wait_times) / len(self.exec_wait_times)
            if self.exec_wait_times else 0.0
        )
        gcs_total    = sum(self.gcs_latencies)
        decode_total = sum(self.decode_times)
        grand_total  = gcs_total + decode_total
        bottleneck   = "GCS download" if gcs_total > decode_total else "Arrow decode"
        return (
            f"  blobs ok/err      : {self.blobs_ok} / {self.blobs_err}\n"
            f"  bytes total        : {self.bytes_transferred:,}\n"
            f"\n"
            f"  GCS download time  : total={gcs_total:.3f}s   p50={gp50:.3f}s  p95={gp95:.3f}s  p99={gp99:.3f}s\n"
            f"  Arrow decode time  : total={decode_total:.3f}s   p50={dp50:.3f}s  p95={dp95:.3f}s  p99={dp99:.3f}s\n"
            f"\n"
            f"  GCS share          : {100*gcs_total/grand_total:.1f}%\n"
            f"  Decode share       : {100*decode_total/grand_total:.1f}%\n"
            f"  >> bottleneck      : {bottleneck}\n"
            f"\n"
            f"  EXEC wait (avg)    : {exec_avg:.4f}s  (blocked waiting for READY slot)"
        )


def _slot_state_counts(buf) -> dict[str, int]:
    counts = {FREE: 0, WRITING: 0, READY: 0, READING: 0}
    for i in range(SLOT_COUNT):
        counts[read_slot_state(buf, i)] += 1
    return {STATE_NAMES[k]: v for k, v in counts.items()}


# ── EXEC Process (parent / main)  (§8.2) ──────────────────────────────────────

def run_poc(bucket: str, blobs: list[str], column: str) -> float:
    """
    Allocates shared memory, spawns the IO worker, issues ReadRequests for
    every blob in *blobs*, decodes each Parquet payload, sums *column*, and
    returns the total sum.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if not bucket:
        raise SystemExit("GCS_BUCKET env var not set.")
    if not blobs:
        raise SystemExit("No blobs provided.")

    print("=" * 60)
    print("Opteryx IO/EXEC POC")
    print(f"  bucket      : {bucket}")
    print(f"  blobs       : {len(blobs)}")
    print(f"  column      : {column}")
    print(f"  slots       : {SLOT_COUNT} × {SLOT_SIZE // (1024*1024)} MB  ({TOTAL_SIZE // (1024*1024)} MB total)")
    print(f"  max inflight: {MAX_INFLIGHT}")
    print("=" * 60)

    # ── Dev-hygiene: unlink any leftover shm from a previous crash  (§4.1) ───
    try:
        stale = SharedMemory(name=SHM_NAME, create=False)
        stale.close()
        stale.unlink()
        print(f"  [cleanup] unlinked stale shared memory: {SHM_NAME}")
    except FileNotFoundError:
        pass

    # ── Allocate shared memory ────────────────────────────────────────────────
    shm = SharedMemory(name=SHM_NAME, create=True, size=TOTAL_SIZE)
    buf = shm.buf

    # Initialise all slots FREE
    for i in range(SLOT_COUNT):
        write_slot_state(buf, i, FREE)

    # Pre-fault all shared memory pages so the first GCS write into each slot
    # doesn't pay the page-fault cost during the hot path.  Touch the first
    # byte of every 4 KB page with a single memoryview assignment.
    PAGE = 4096
    for off in range(0, TOTAL_SIZE, PAGE):
        buf[off] = 0

    # ── Verify page alignment of slot 0 payload  (§6.1 open question) ────────
    payload_addr = ctypes.addressof(
        ctypes.c_char.from_buffer(shm.buf, SLOT_HEADER_SIZE)
    )
    aligned = payload_addr % 4096 == 0
    print(f"  page-aligned : {'YES' if aligned else 'NO (unexpected)'}  (slot 0 payload @ {payload_addr:#x})")

    # ── Spawn IO worker ───────────────────────────────────────────────────────
    parent_conn, child_conn = Pipe()
    io_proc = Process(
        target=io_worker,
        args=(SHM_NAME, child_conn, bucket),
        name="io-worker",
    )
    # Use spawn (default on macOS ≥ 3.8); explicit for documentation purposes
    io_proc.start()

    # Wait for IO worker to signal READY or ERROR
    startup_msg = parent_conn.recv()
    if isinstance(startup_msg, str) and startup_msg.startswith("ERROR:"):
        print(f"IO worker failed to start: {startup_msg}")
        io_proc.join()
        shm.close()
        shm.unlink()
        raise SystemExit(1)

    assert startup_msg == "READY"
    print(f"  IO worker PID: {io_proc.pid}  READY")
    print()

    # ── Issue reads and consume results ───────────────────────────────────────
    inflight_sem = multiprocessing.Semaphore(MAX_INFLIGHT)
    metrics      = Metrics()
    total_sum    = 0.0

    pending: dict[int, str] = {}   # request_id → blob_path
    request_id = 0
    blobs_sent = 0
    blobs_done = 0
    total_blobs = len(blobs)

    t_overall = time.perf_counter()

    while blobs_done < total_blobs:

        # Issue new requests while capacity and blobs remain
        while blobs_sent < total_blobs and inflight_sem.acquire(block=False):
            blob_path = blobs[blobs_sent]
            req = ReadRequest(request_id, blob_path)
            pending[request_id] = blob_path
            parent_conn.send(req)
            print(f"  → [{request_id:04d}] ReadRequest  {blob_path}")
            request_id  += 1
            blobs_sent  += 1

        # Wait for a completion (EXEC blocked here = I/O bound)
        t_exec_wait = time.perf_counter()
        if not parent_conn.poll(timeout=120.0):
            print("  ✗ Timed out waiting for ReadComplete")
            break

        metrics.exec_wait_times.append(time.perf_counter() - t_exec_wait)

        completion: ReadComplete = parent_conn.recv()

        if completion.error:
            print(f"  ✗ [{completion.request_id:04d}] ERROR: {completion.error}")
            metrics.blobs_err += 1
            blobs_done += 1
            inflight_sem.release()
            continue

        slot_id = completion.slot_id
        length  = completion.length
        metrics.blobs_ok += 1
        metrics.bytes_transferred += length
        metrics.gcs_latencies.append(completion.gcs_latency_s)
        print(
            f"  ← [{completion.request_id:04d}] ReadComplete  slot={slot_id}"
            f"  {length:,} bytes  gcs={completion.gcs_latency_s:.3f}s"
        )

        # ── READY → READING → FREE ───────────────────────────────────────────
        # Zero-copy path: pass a memoryview directly into pyarrow's BufferReader.
        # pq.read_table() fully decodes Parquet into Arrow-owned memory before
        # returning, so the slot can be freed immediately after — the Arrow table
        # holds no reference into shared memory.  No bytes() copy is needed.
        write_slot_state(buf, slot_id, READING)
        t_decode = time.perf_counter()
        raw      = read_payload(shm, slot_id, length)           # zero-copy memoryview into shm
        table    = pq.read_table(pa.BufferReader(raw), columns=[column])
        # Arrow has fully decoded into its own buffers; release the shm slot now.
        del raw
        write_slot_state(buf, slot_id, FREE)
        inflight_sem.release()

        col_result = pc.sum(table.column(column))
        decode_s   = time.perf_counter() - t_decode
        metrics.decode_times.append(decode_s)
        print(f"       decode={decode_s:.4f}s  slot states: {_slot_state_counts(buf)}")
        if col_result.is_valid:
            total_sum += col_result.as_py()
        del table

        blobs_done += 1
        del pending[completion.request_id]

    elapsed = time.perf_counter() - t_overall

    # ── Results ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"  Total time    : {elapsed:.3f}s")
    print(f"  Sum of '{column}': {total_sum:,.4f}")
    print()
    print("Metrics:")
    print(metrics.summary())
    print("=" * 60)

    # ── Shutdown IO worker  (§4.1 teardown) ──────────────────────────────────
    parent_conn.send(SHUTDOWN)
    io_proc.join(timeout=10)
    if io_proc.is_alive():
        print("  [warn] IO worker did not exit cleanly; terminating.")
        io_proc.terminate()
        io_proc.join()

    # ── Recovery scan: reset stranded WRITING slots  (§11.3) ─────────────────
    for i in range(SLOT_COUNT):
        if read_slot_state(buf, i) == WRITING:
            print(f"  [recovery] resetting stranded WRITING slot {i}")
            write_slot_state(buf, i, FREE)

    # Release the buf memoryview (shm.buf) before closing; any live reference
    # to shm.buf or a slice of it keeps an exported pointer on the mmap and
    # causes "cannot close exported pointers exist".
    del buf
    shm.close()
    shm.unlink()

    return total_sum


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # multiprocessing.set_start_method is only needed on Linux where the
    # default is fork; on macOS it is already spawn from Python 3.8 onwards.
    # Keeping it here makes the intent explicit and ensures correct behaviour
    # if this script is ever run on Linux.
    multiprocessing.set_start_method("spawn", force=True)

    blobs = GCS_BLOBS or list_gcs_blobs(GCS_BUCKET, GCS_PREFIX)
    print(f"  discovered {len(blobs)} blob(s) from gs://{GCS_BUCKET}/{GCS_PREFIX}")

    run_poc(GCS_BUCKET, blobs, SUM_COLUMN)
