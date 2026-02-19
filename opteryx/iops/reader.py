# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/reader.py
======================
EXEC-side driver for the IO process isolation subsystem (§1, §4 design.md).

Usage
-----
::

    from opteryx.iops import IOPSReader, RingConfig

    cfg = RingConfig(slot_size=64 << 20, slot_count=16, max_inflight=8)

    with IOPSReader(bucket="opteryx_store", cfg=cfg) as reader:
        for payload in reader.iter_blobs(blob_paths):
            # payload.data is a zero-copy memoryview of the shm slot.
            table = pyarrow.parquet.read_table(pyarrow.BufferReader(payload.data))
            # Tell the ring the slot is free again.
            payload.release()

``SlotPayload.release()`` is idempotent; it is also called automatically when
the ``with IOPSReader`` block exits, so leaking a payload across the context
boundary cleans up gracefully (though callers should release as soon as possible
to keep the ring moving).

Zero-copy guarantee
-------------------
``payload.data`` is a ``memoryview`` that points directly into the shared-memory
slot — no ``bytes`` copy is made.  Arrow's ``BufferReader`` accepts a
``memoryview`` and the Parquet decoder writes decoded column data into Arrow
heap memory.  Once ``read_table()`` returns the memoryview is no longer needed
and ``release()`` can be called.

Ordering
--------
``iter_blobs`` yields blobs in **completion order** — whichever blob the IO
worker finishes first is yielded first.  This eliminates head-of-line
blocking: a slow blob on a cold GCS node cannot stall faster blobs that
completed behind it, keeping all ``max_inflight`` slots active.  Callers
that require submission-order processing must sort or buffer results
themselves.

Backpressure
------------
``multiprocessing.Semaphore(cfg.max_inflight)`` limits the number of
``ReadRequest`` messages in flight at any one time.  EXEC acquires the semaphore
before sending each request; ``payload.release()`` releases it.
"""

from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from dataclasses import field
from typing import Iterator
from typing import List
from typing import Optional

from opteryx.iops.messages import SHUTDOWN
from opteryx.iops.messages import ReadComplete
from opteryx.iops.messages import decode_complete
from opteryx.iops.messages import decode_startup
from opteryx.iops.messages import encode_request
from opteryx.iops.ring import FREE
from opteryx.iops.ring import READING
from opteryx.iops.ring import READY
from opteryx.iops.ring import RingConfig
from opteryx.iops.ring import allocate_ring
from opteryx.iops.ring import read_payload
from opteryx.iops.ring import read_slot_header
from opteryx.iops.ring import release_ring
from opteryx.iops.ring import write_slot_state
from opteryx.iops.worker import io_worker

# Default ring configuration — driven by config.py, overridable by passing a RingConfig.
_DEFAULT_SHM_NAME = "opteryx_iops_ring"


def _iops_defaults() -> tuple[int, int, int, int]:
    """Read IOPS tuning values from config, with safe fallbacks."""
    try:
        from opteryx import config as _cfg

        return (
            _cfg.IOPS_SLOT_SIZE,
            _cfg.IOPS_MAX_INFLIGHT,
            _cfg.IOPS_SLOT_COUNT,
            _cfg.IOPS_CHUNK_SIZE,
        )
    except Exception:
        _inf = 8
        return 64 * 1024 * 1024, _inf, max(_inf * 2, 16), 4 * 1024 * 1024


_DEFAULT_SLOT_SIZE, _DEFAULT_MAX_INFLIGHT, _DEFAULT_SLOT_COUNT, _DEFAULT_CHUNK_SIZE = (
    _iops_defaults()
)


@dataclass
class SlotPayload:
    """A blob that has arrived in a ring slot and is ready for zero-copy decode.

    Attributes
    ----------
    slot_id:
        Index of the ring slot that holds the data.
    data:
        Zero-copy ``memoryview`` of the slot payload (exactly ``length`` bytes).
        Valid until ``release()`` is called.
    length:
        Byte length of the blob data.
    request_id:
        Opaque integer echoed from the original ``ReadRequest``.
    blob_path:
        GCS object path that was downloaded.
    gcs_latency_s:
        Wall-clock time consumed by the GCS download alone.
    """

    slot_id: int
    data: memoryview
    length: int
    request_id: int
    blob_path: str
    gcs_latency_s: float = 0.0
    error: Optional[str] = None  # set for failed blobs; slot_id will be -1

    # Internal back-reference set by IOPSReader — not part of the public API.
    _ring_buf: object = field(default=None, repr=False, compare=False)
    _cfg: object = field(default=None, repr=False, compare=False)
    _sem: object = field(default=None, repr=False, compare=False)
    _released: bool = field(default=False, repr=False, compare=False)

    def release(self) -> None:
        """Return the slot to FREE and release the back-pressure semaphore.

        Idempotent — safe to call more than once.
        """
        if self._released:
            return
        self._released = True
        # Drop the exported pointer on shm.buf before transitioning state.
        del self.data
        self.data = memoryview(b"")  # sentinel so attribute is still valid
        # slot_id == -1 for error payloads — no ring slot was allocated.
        if self.slot_id >= 0:
            write_slot_state(self._ring_buf, self._cfg, self.slot_id, FREE)
        if self._sem is not None:
            self._sem.release()


class IOPSReader:
    """Context manager that manages the IO worker process and ring buffer.

    Parameters
    ----------
    protocol:
        Storage protocol string passed to ``create_filesystem()`` — e.g.
        ``"gs"`` for GCS, ``"s3"`` for S3/MinIO, ``"file"`` for local disk.
        Blob paths supplied to ``iter_blobs`` must include the bucket/root
        as the first path component (e.g.
        ``"my-bucket/prefix/file.parquet"``).
    cfg:
        Ring configuration.  Defaults to ``RingConfig(slot_size=64 MiB,
        slot_count=16, max_inflight=8)``.
    """

    def __init__(
        self,
        protocol: str,
        cfg: Optional[RingConfig] = None,
    ) -> None:
        self._protocol = protocol
        self._cfg = cfg or RingConfig(
            slot_size=_DEFAULT_SLOT_SIZE,
            slot_count=_DEFAULT_SLOT_COUNT,
            max_inflight=_DEFAULT_MAX_INFLIGHT,
            shm_name=_DEFAULT_SHM_NAME,
            chunk_size=_DEFAULT_CHUNK_SIZE,
        )
        self._shm = None
        self._worker = None
        self._parent_conn = None
        self._sem = None
        # Track live SlotPayload objects so __exit__ can release them.
        self._live_payloads: List[SlotPayload] = []

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "IOPSReader":
        self._start()
        return self

    def __exit__(self, *_exc) -> None:
        self._stop()

    # ── Public API ────────────────────────────────────────────────────────────

    def iter_blobs(self, blob_paths: List[str]) -> Iterator[SlotPayload]:
        """Yield decoded slot payloads in the same order as *blob_paths*.

        Downloads happen concurrently in the IO worker; blobs are yielded in
        **completion order** (first finished, first yielded).  This avoids
        head-of-line blocking: a slow blob never stalls the semaphore slots
        waiting for faster blobs behind it.

        Yields
        ------
        SlotPayload
            The caller must call ``payload.release()`` as soon as the data
            is no longer needed to keep the ring flowing.  Check
            ``payload.error`` before use — failed blobs yield an error
            sentinel rather than raising.
        """
        if self._shm is None:
            raise RuntimeError("IOPSReader must be used as a context manager")

        conn = self._parent_conn
        sem = self._sem
        cfg = self._cfg
        shm = self._shm
        buf = shm.buf

        total = len(blob_paths)
        sent = 0  # how many ReadRequests have been sent
        received = 0  # how many ReadCompletes have been received

        # request_id → blob_path (for error reporting)
        id_to_path: dict[int, str] = {}

        while received < total or sent < total:
            # ── Send phase: push as many requests as semaphore allows ─────────
            while sent < total:
                if not sem.acquire(block=False):
                    break
                conn.send_bytes(encode_request(sent, blob_paths[sent]))
                id_to_path[sent] = blob_paths[sent]
                sent += 1

            # ── Receive + yield phase: emit each blob as soon as it arrives ───
            # No reordering — blobs are yielded in completion order, not
            # submission order.  This eliminates head-of-line blocking: a slow
            # blob cannot stall all semaphore slots and starve the pipeline.
            if received < total:
                if conn.poll(0):
                    rc: ReadComplete = decode_complete(conn.recv_bytes())
                    received += 1
                    blob_path = id_to_path.pop(rc.request_id)

                    if not rc.ok:
                        # Yield an error sentinel — no slot was allocated by the worker.
                        payload = SlotPayload(
                            slot_id=-1,
                            data=memoryview(b""),
                            length=0,
                            request_id=rc.request_id,
                            blob_path=blob_path,
                            error=rc.error,
                            _ring_buf=buf,
                            _cfg=cfg,
                            _sem=sem,
                        )
                    else:
                        # Transition slot READY → READING; hand zero-copy view
                        # to the caller.
                        _state, _length, _rid = read_slot_header(buf, cfg, rc.slot_id)
                        write_slot_state(buf, cfg, rc.slot_id, READING)

                        raw = read_payload(shm, cfg, rc.slot_id, rc.length)

                        payload = SlotPayload(
                            slot_id=rc.slot_id,
                            data=raw,
                            length=rc.length,
                            request_id=rc.request_id,
                            blob_path=blob_path,
                            gcs_latency_s=rc.gcs_latency_s,
                            _ring_buf=buf,
                            _cfg=cfg,
                            _sem=sem,
                        )
                    self._live_payloads.append(payload)
                    yield payload
                    try:
                        self._live_payloads.remove(payload)
                    except ValueError:
                        pass
                elif sent == total:
                    # Nothing left to send and nothing arrived — genuinely idle.
                    # Yield the CPU briefly rather than burning a core on polling.
                    import time as _time

                    _time.sleep(0.0005)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _start(self) -> None:
        """Allocate the ring buffer and start the IO worker process."""
        cfg = self._cfg

        self._shm = allocate_ring(cfg)
        self._sem = multiprocessing.Semaphore(cfg.max_inflight)

        parent_conn, child_conn = multiprocessing.Pipe()
        self._parent_conn = parent_conn

        self._worker = multiprocessing.Process(
            target=io_worker,
            args=(cfg.shm_name, child_conn, self._protocol, cfg),
            daemon=True,
            name="opteryx-iops-worker",
        )
        self._worker.start()
        child_conn.close()  # parent no longer needs the child end

        # Wait for the worker to signal readiness (or report an error).
        decode_startup(parent_conn.recv_bytes())

    def _stop(self) -> None:
        """Gracefully shut down the worker and release all resources."""
        # Force-release any payloads the caller forgot to release.
        for payload in list(self._live_payloads):
            payload.release()
        self._live_payloads.clear()

        # Tell the worker to exit and wait for it to drain its thread pool.
        if self._parent_conn is not None:
            try:
                self._parent_conn.send_bytes(SHUTDOWN)
            except Exception:
                pass

        if self._worker is not None and self._worker.is_alive():
            self._worker.join(timeout=10)
            if self._worker.is_alive():
                self._worker.terminate()
                self._worker.join(timeout=2)

        if self._parent_conn is not None:
            self._parent_conn.close()
            self._parent_conn = None

        # Release the shm.buf memoryview held inside ring helpers.
        # release_ring() calls shm.close() + shm.unlink().
        if self._shm is not None:
            release_ring(self._shm)
            self._shm = None
