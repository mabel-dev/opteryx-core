# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/ring.py
====================
Shared-memory ring buffer — slot layout, state machine, and low-level helpers.

This module is imported by **both** the EXEC process and the IO worker process,
so it must only use the standard library.

Layout (§6.1 of design.md)
---------------------------
Each slot:
    offset  0 : state      (uint32) — governs ownership; see constants below
    offset  4 : length     (uint32) — payload bytes written (valid once READY)
    offset  8 : request_id (uint64) — echoes originating ReadRequest
    offset 16 : _pad       (48 bytes) — pad header to 64 bytes (cache-line aligned)
    offset 64 : payload    (slot_size bytes)

slot stride = SLOT_HEADER_SIZE + slot_size
total size  = slot_count  × slot_stride

State machine (§6.2 of design.md)
-----------------------------------
    FREE (0) → WRITING (1) → READY (2) → READING (3) → FREE (0)

Ownership:
    IO worker : FREE → WRITING → READY
    EXEC      : READY → READING → FREE
"""

from __future__ import annotations

import ctypes
import struct
from dataclasses import dataclass
from multiprocessing.shared_memory import SharedMemory

# ── Optional Cython atomic back-end (Phase 2, §17 of design.md) ───────────────
# When ring_atomic has been compiled, state reads/writes use hardware-fenced
# __atomic_load_n / __atomic_store_n / __atomic_compare_exchange_n instead of
# struct.pack_into / struct.unpack_from.  The pure-Python fallback remains
# correct for development and platforms where the extension is unavailable.
try:
    from opteryx.iops.ring_atomic import cas_state as _cas_state
    from opteryx.iops.ring_atomic import load_state as _load_state
    from opteryx.iops.ring_atomic import store_state as _store_state

    _HAS_ATOMIC: bool = True
except ImportError:
    _HAS_ATOMIC = False


# ── Slot state constants ───────────────────────────────────────────────────────

FREE = 0
WRITING = 1
READY = 2
READING = 3

STATE_NAMES: dict[int, str] = {FREE: "FREE", WRITING: "WRITING", READY: "READY", READING: "READING"}


# ── Ring configuration ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RingConfig:
    """Immutable parameters that describe the shared-memory ring.

    Both processes receive *the same* ``RingConfig`` instance (passed as an
    argument to the IO worker at spawn time), so layout calculations are
    guaranteed to agree.
    """

    slot_size: int  # bytes per slot payload (e.g. 64 * 1024 * 1024)
    slot_count: int  # number of slots in the ring (e.g. 16)
    max_inflight: int  # max simultaneous in-flight reads (e.g. 8)
    shm_name: str  # POSIX shared-memory name
    chunk_size: int = 4 * 1024 * 1024  # HTTP streaming chunk size in bytes (default 4 MiB)
    prefault_mode: str = "adaptive"  # one of: adaptive, full, first-slot, none

    @property
    def slot_stride(self) -> int:
        return SLOT_HEADER_SIZE + self.slot_size

    @property
    def total_size(self) -> int:
        return self.slot_count * self.slot_stride


# ── Slot layout constants ──────────────────────────────────────────────────────

SLOT_HEADER_SIZE: int = 64  # bytes; cache-line aligned
_HEADER_FMT: str = "<IIQ"  # state (u32), length (u32), request_id (u64) = 16 bytes
_STATE_FMT: str = "<I"  # state only (u32)


# ── Low-level helpers ──────────────────────────────────────────────────────────


def slot_offset(cfg: RingConfig, slot_id: int) -> int:
    return slot_id * cfg.slot_stride


def write_slot_header(
    buf,
    cfg: RingConfig,
    slot_id: int,
    state: int,
    length: int,
    request_id: int,
) -> None:
    struct.pack_into(_HEADER_FMT, buf, slot_offset(cfg, slot_id), state, length, request_id)


def read_slot_header(buf, cfg: RingConfig, slot_id: int) -> tuple[int, int, int]:
    """Returns (state, length, request_id)."""
    return struct.unpack_from(_HEADER_FMT, buf, slot_offset(cfg, slot_id))


def write_slot_state(buf, cfg: RingConfig, slot_id: int, state: int) -> None:
    """Release-store the slot state (uses hardware fence when ring_atomic is compiled)."""
    off = slot_offset(cfg, slot_id)
    if _HAS_ATOMIC:
        _store_state(buf, off, state)
    else:
        struct.pack_into(_STATE_FMT, buf, off, state)


def read_slot_state(buf, cfg: RingConfig, slot_id: int) -> int:
    """Acquire-load the slot state (uses hardware fence when ring_atomic is compiled)."""
    off = slot_offset(cfg, slot_id)
    if _HAS_ATOMIC:
        return _load_state(buf, off)
    (state,) = struct.unpack_from(_STATE_FMT, buf, off)
    return state


def cas_slot_state(
    buf,
    cfg: RingConfig,
    slot_id: int,
    expected: int,
    desired: int,
) -> bool:
    """Atomic compare-and-swap on a slot's state word.

    Returns True iff the slot held ``expected`` and was atomically replaced by
    ``desired``.  When ring_atomic is compiled this uses
    ``__atomic_compare_exchange_n`` (ACQ_REL / ACQUIRE); otherwise falls back
    to a non-atomic read-modify-write that is safe under the Python GIL but
    requires callers to serialise access externally.
    """
    off = slot_offset(cfg, slot_id)
    if _HAS_ATOMIC:
        return _cas_state(buf, off, expected, desired)
    # Pure-Python fallback: GIL provides mutual exclusion within one process,
    # but this is not safe for cross-process use without an external lock.
    (cur,) = struct.unpack_from(_STATE_FMT, buf, off)
    if cur == expected:
        struct.pack_into(_STATE_FMT, buf, off, desired)
        return True
    return False


def read_payload(shm: SharedMemory, cfg: RingConfig, slot_id: int, length: int) -> memoryview:
    """Zero-copy view into the slot payload — no bytes() allocation.

    The caller **must** ``del`` the returned memoryview (and ensure no other
    references to it survive) before calling ``write_slot_state(..., FREE)``
    or ``shm.close()``, otherwise the mmap will raise
    ``BufferError: cannot close exported pointers exist``.
    """
    off = slot_offset(cfg, slot_id) + SLOT_HEADER_SIZE
    return shm.buf[off : off + length]


def payload_view(shm: SharedMemory, cfg: RingConfig, slot_id: int) -> memoryview:
    """Full-slot write window — used by IO worker to stream data in.

    Returns a view of exactly ``slot_size`` bytes starting at the payload
    offset.  The caller must ``del`` this view before transitioning the slot
    out of WRITING state.
    """
    off = slot_offset(cfg, slot_id) + SLOT_HEADER_SIZE
    return shm.buf[off : off + cfg.slot_size]


# ── Ring lifecycle ─────────────────────────────────────────────────────────────


def allocate_ring(cfg: RingConfig) -> SharedMemory:
    """Create and initialise the shared-memory region.

    Dev-hygiene: attempts to unlink any stale region with the same name before
    allocating (handles crashes on macOS where POSIX shm survives process death).
    All slots are initialised to FREE and every page is pre-faulted so IO worker
    threads never pay a page-fault penalty during the hot path.
    """
    # Unlink any stale shm from a previous crash (§4.1 of design.md)
    try:
        stale = SharedMemory(name=cfg.shm_name, create=False)
        stale.close()
        stale.unlink()
    except FileNotFoundError:
        pass

    shm = SharedMemory(name=cfg.shm_name, create=True, size=cfg.total_size)
    buf = shm.buf

    # Initialise all slots FREE
    for i in range(cfg.slot_count):
        write_slot_state(buf, cfg, i, FREE)

    # Pre-fault selected pages so first writes don't stall on page faults.
    mode = (cfg.prefault_mode or "adaptive").lower()
    if mode == "adaptive":
        # Full pre-fault is good for smaller rings; for larger rings it can
        # dominate startup latency, so touch only the first slot.
        mode = "full" if cfg.total_size <= (512 * 1024 * 1024) else "first-slot"

    if mode == "full":
        prefault_bytes = cfg.total_size
    elif mode == "first-slot":
        prefault_bytes = cfg.slot_stride
    elif mode == "none":
        prefault_bytes = 0
    else:
        prefault_bytes = cfg.slot_stride

    for off in range(0, prefault_bytes, 4096):
        buf[off] = 0

    del buf
    return shm


def reset_ring_states(buf, cfg: RingConfig) -> None:
    """Reset all slots to FREE (used when reusing an existing ring allocation)."""
    for i in range(cfg.slot_count):
        write_slot_state(buf, cfg, i, FREE)


def release_ring(shm: SharedMemory) -> None:
    """Close and unlink (the EXEC side owns the lifetime)."""
    shm.close()
    shm.unlink()


# ── Diagnostics ───────────────────────────────────────────────────────────────


def slot_state_counts(buf, cfg: RingConfig) -> dict[str, int]:
    counts = {FREE: 0, WRITING: 0, READY: 0, READING: 0}
    for i in range(cfg.slot_count):
        counts[read_slot_state(buf, cfg, i)] += 1
    return {STATE_NAMES[k]: v for k, v in counts.items()}


def check_page_alignment(shm: SharedMemory) -> bool:
    """Return True if the slot-0 payload starts on a 4 KB page boundary (§6.1)."""
    addr = ctypes.addressof(ctypes.c_char.from_buffer(shm.buf, SLOT_HEADER_SIZE))
    return addr % 4096 == 0
