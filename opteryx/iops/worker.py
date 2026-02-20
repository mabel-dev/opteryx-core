# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/worker.py
======================
IO worker process — runs in a **spawned** child process (§8.1 of design.md).

This module is imported fresh by the spawned process; it must not rely on any
state inherited from the parent.  The filesystem is constructed post-spawn via
``create_filesystem(protocol)`` so that no sockets or credentials are inherited
from the parent process.

Architecture
------------
A single ``asyncio`` event loop runs all concurrent downloads as coroutines.
``aiohttp`` is used for GCS/HTTP downloads; each ``await`` fully releases the
GIL, allowing the event loop to multiplex many downloads on one thread without
GIL contention.  S3 and local filesystem calls use ``asyncio.to_thread`` to
offload their blocking I/O without stalling the loop.

One shared ``aiohttp.ClientSession`` is created per worker invocation and
re-used across all downloads in the batch, giving connection-pool reuse with
no per-download handshake overhead.

Storage back-end
----------------
The worker is storage-agnostic.  It calls ``fs.async_stream_to(path, sink,
http_session)`` on the filesystem object returned by ``create_filesystem()``.
All three built-in backends implement this method.

Pipe protocol
-------------
Messages are binary-packed structs (no pickle).  See ``opteryx.iops.messages``
for the wire format.

Shared-memory pointer discipline
---------------------------------
Every ``shm.buf`` slice (memoryview) keeps an exported pointer on the mmap.
``mmap.close()`` raises ``BufferError`` if any exported pointer is still live.
Rules enforced here:
  - ``slot_view`` (write window into a slot): ``del``'d on both success and
    error paths before the slot state transition.
  - ``buf`` (the persistent ``shm.buf`` view): ``del``'d at the end of
    ``_async_main`` before control returns to ``io_worker`` which calls
    ``shm.close()``.
"""

from __future__ import annotations

from opteryx.iops.messages import READY_BYTES
from opteryx.iops.messages import SHUTDOWN
from opteryx.iops.messages import decode_request
from opteryx.iops.messages import encode_complete
from opteryx.iops.messages import encode_startup_error
from opteryx.iops.ring import FREE
from opteryx.iops.ring import READY
from opteryx.iops.ring import WRITING
from opteryx.iops.ring import RingConfig
from opteryx.iops.ring import cas_slot_state
from opteryx.iops.ring import payload_view
from opteryx.iops.ring import write_slot_header
from opteryx.iops.ring import write_slot_state


async def _async_main(pipe_conn, fs, cfg, shm) -> None:
    """Async core of the IO worker.

    Runs inside ``asyncio.run()`` in the spawned process.  Receives
    ``ReadRequest`` messages from EXEC, fires off ``download_one`` coroutines
    concurrently, and sends ``ReadComplete`` messages back when each finishes.
    """
    import asyncio
    import queue
    import threading

    import aiohttp

    loop = asyncio.get_running_loop()
    buf = shm.buf
    # GCS credential refresh lock: prevent concurrent refresh from multiple coroutines.
    refresh_lock = asyncio.Lock()
    request_queue: asyncio.Queue[bytes] = asyncio.Queue()
    send_queue: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()

    async def _ensure_fresh_token() -> None:
        """Refresh GCS OAuth token if it has expired (no-op for non-GCS backends)."""
        if not hasattr(fs, "client_credentials"):
            return
        if not fs.client_credentials.valid:
            async with refresh_lock:
                # Double-check after acquiring the lock.
                if not fs.client_credentials.valid:
                    await asyncio.to_thread(fs._refresh_credentials)

    async def _claim_free_slot_async() -> int:
        """Async-friendly slot claim: CAS(FREE→WRITING), yields CPU when full.

        Because the reader-side semaphore limits in-flight requests to
        ``max_inflight`` and the ring has ``slot_count >= max_inflight`` slots,
        a free slot is almost always available immediately.  The ``await`` only
        fires in the rare case where decode latency has temporarily exhausted
        all slots.
        """
        while True:
            for i in range(cfg.slot_count):
                if cas_slot_state(buf, cfg, i, FREE, WRITING):
                    return i
            await asyncio.sleep(0.0005)

    # One shared TCP connector + ClientSession for all GCS downloads.
    tcp_conn = aiohttp.TCPConnector(
        limit=cfg.max_inflight * 2,
        limit_per_host=cfg.max_inflight,
    )

    async def download_one(request_id: int, blob_path: str) -> None:
        """Download one blob into a ring slot, then notify EXEC via the pipe."""
        slot_id = await _claim_free_slot_async()
        write_slot_header(buf, cfg, slot_id, WRITING, 0, request_id)

        slot_view = payload_view(shm, cfg, slot_id)

        # Minimal file-like sink that writes directly into the shm slot.
        # No intermediate bytes() copy; each network chunk lands straight
        # into shared memory.
        class _SlotWriter:
            __slots__ = ("pos",)

            def __init__(self) -> None:
                self.pos = 0

            def write(self, b: bytes) -> int:
                n = len(b)
                if self.pos + n > cfg.slot_size:
                    raise ValueError(
                        f"Blob {blob_path!r} exceeds slot size {cfg.slot_size:,} bytes"
                    )
                slot_view[self.pos : self.pos + n] = b
                self.pos += n
                return n

        writer = _SlotWriter()
        t0 = loop.time()
        try:
            await _ensure_fresh_token()
            await fs.async_stream_to(blob_path, writer, http_session, cfg.chunk_size)
            gcs_latency = loop.time() - t0
            length = writer.pos

            # Release exported pointer before state transition.
            del slot_view, writer

            write_slot_header(buf, cfg, slot_id, READY, length, request_id)
            send_queue.put(encode_complete(request_id, slot_id, length, gcs_latency))

        except Exception as exc:
            del slot_view, writer
            write_slot_state(buf, cfg, slot_id, FREE)
            send_queue.put(encode_complete(request_id, -1, 0, error=str(exc)))

    def _recv_loop() -> None:
        """Blocking pipe receiver thread -> async request queue."""
        try:
            while True:
                data = pipe_conn.recv_bytes()
                loop.call_soon_threadsafe(request_queue.put_nowait, data)
                if data == SHUTDOWN:
                    break
        except (EOFError, OSError):
            # Treat pipe close as shutdown.
            loop.call_soon_threadsafe(request_queue.put_nowait, SHUTDOWN)

    def _send_loop() -> None:
        """Blocking pipe sender thread fed by send_queue."""
        while True:
            data = send_queue.get()
            if data is None:
                return
            try:
                pipe_conn.send_bytes(data)
            except (BrokenPipeError, EOFError, OSError):
                return

    recv_thread = threading.Thread(target=_recv_loop, name="opteryx-iops-recv", daemon=True)
    send_thread = threading.Thread(target=_send_loop, name="opteryx-iops-send", daemon=True)
    recv_thread.start()
    send_thread.start()

    async with aiohttp.ClientSession(connector=tcp_conn) as http_session:
        active: set[asyncio.Task] = set()

        # Main dispatch loop: requests are fed by a dedicated blocking recv thread.
        while True:
            data = await request_queue.get()

            if data == SHUTDOWN:
                break

            request_id, blob_path = decode_request(data)
            task = asyncio.create_task(download_one(request_id, blob_path))
            active.add(task)
            task.add_done_callback(active.discard)

        # Drain all in-flight downloads before exiting.
        if active:
            await asyncio.gather(*active, return_exceptions=True)

    send_queue.put(None)
    send_thread.join(timeout=2)
    recv_thread.join(timeout=2)

    # Release the persistent shm.buf memoryview.  Any live slice (slot_view)
    # from an in-flight download would keep an exported pointer alive and cause
    # shm.close() to raise BufferError — but we've drained all tasks above.
    del buf


def io_worker(
    shm_name: str,
    pipe_conn,
    protocol: str,
    cfg: RingConfig,
) -> None:
    """Entry point for the spawned IO worker process.

    Parameters
    ----------
    shm_name:
        POSIX shared-memory name (must match what EXEC allocated).
    pipe_conn:
        Child end of a ``multiprocessing.Pipe``.  Receives binary-packed
        ReadRequest messages; sends binary-packed ReadComplete messages (and
        single-byte startup signals).
    protocol:
        Storage protocol understood by ``create_filesystem()`` — e.g.
        ``"gs"`` for GCS, ``"s3"`` for S3/MinIO, ``"file"`` for local disk.
    cfg:
        Ring configuration — must equal the one used by EXEC to allocate the ring.
    """
    import asyncio
    from multiprocessing.shared_memory import SharedMemory

    # ── Initialise filesystem ─────────────────────────────────────────────────
    try:
        from opteryx.connectors.io_systems import create_filesystem

        fs = create_filesystem(protocol)
    except Exception as exc:
        pipe_conn.send_bytes(encode_startup_error(str(exc)))
        return

    # ── Attach to shared memory ───────────────────────────────────────────────
    shm = SharedMemory(name=shm_name)

    # Signal readiness *after* shm is attached so EXEC can immediately send.
    pipe_conn.send_bytes(READY_BYTES)

    # ── Run the async event loop ──────────────────────────────────────────────
    try:
        asyncio.run(_async_main(pipe_conn, fs, cfg, shm))
    finally:
        shm.close()
        # Intentionally no shm.unlink() — EXEC owns the lifetime of the region.
