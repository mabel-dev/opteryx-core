# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/ring_atomic.pyx
============================
Phase 2 of the IO process isolation design (§17, design.md):

  «Move slot metadata + state transitions into Cython/C. Use atomic state
   transitions (__atomic_compare_exchange via a Cython cdef wrapping
   GCC/Clang built-ins — Python-level atomics are not sufficient).»

This module uses GCC/Clang built-in atomics (__atomic_compare_exchange_n,
__atomic_load_n, __atomic_store_n) to provide formally sequenced state
transitions across two OS processes that share a single mmap region.

Why hardware atomics matter here
---------------------------------
Struct.pack_into / struct.unpack_from in pure Python compile to memory
reads/writes that are individually atomic on x86/ARM for 4-byte aligned
addresses (naturally-aligned word stores are single instructions).  But:

- There is **no memory fence** — the compiler (and CPU out-of-order engine)
  can reorder surrounding stores/loads.
- FREE → WRITING (slot claiming) races between worker threads require a CAS,
  not a plain write protected by a Python Lock.  The Lock serialises Python
  bytecode, not the memory-level operation; under spawn the shared memory is
  outside the GIL's visibility.

The functions below give:
  ``cas_state``    — ACQ_REL CAS (FREE→WRITING slot claim, lock-free)
  ``load_state``   — ACQUIRE load (EXEC reads READY slot)
  ``store_state``  — RELEASE store (IO worker WRITING→READY, EXEC READING→FREE)

Python-visible functions
------------------------
cas_state(buf, offset, expected, desired) -> bool
    Atomic compare-and-swap on the 4-byte state word at buf[offset].
    Returns True iff the word held ``expected`` and was replaced by ``desired``.

load_state(buf, offset) -> int
    Acquire-load on the 4-byte state word at buf[offset].

store_state(buf, offset, value) -> None
    Release-store ``value`` into the 4-byte state word at buf[offset].

cdef-level functions (zero-overhead, importable by other .pyx via ring_atomic.pxd)
------------------------------------------------------------------------------------
_cas_u32_at(base, offset, expected, desired) -> bint    nogil
_load_u32_at(base, offset) -> uint32_t                  nogil
_store_u32_at(base, offset, value)                      nogil
"""

from libc.stdint cimport uint32_t


# ── Inline C wrappers for GCC/Clang __atomic builtins ─────────────────────────
# Written as static inline helpers in a verbatim C block so Cython emits them
# directly into the generated .c file without a separate .h dependency.

cdef extern from *:
    """
    #include <stdint.h>

    static inline int _ring_cas_u32(uint32_t *ptr, uint32_t expected, uint32_t desired) {
        /* ACQ_REL on success, ACQUIRE on failure — correct for a spinlock-free CAS loop */
        return __atomic_compare_exchange_n(ptr, &expected, desired, /*weak=*/0,
                                           __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    }

    static inline uint32_t _ring_load_u32(const uint32_t *ptr) {
        return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
    }

    static inline void _ring_store_u32(uint32_t *ptr, uint32_t val) {
        __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
    }
    """
    int      _ring_cas_u32  (uint32_t *ptr, uint32_t expected, uint32_t desired) nogil
    uint32_t _ring_load_u32 (const uint32_t *ptr) nogil
    void     _ring_store_u32(uint32_t *ptr, uint32_t val) nogil


# ── cdef fast path — importable by other .pyx modules via ring_atomic.pxd ─────

cdef inline bint _cas_u32_at(
    unsigned char *base,
    Py_ssize_t offset,
    uint32_t expected,
    uint32_t desired,
) noexcept nogil:
    """CAS on the uint32 at base+offset.  Returns 1 if swap occurred."""
    return _ring_cas_u32(<uint32_t *>(base + offset), expected, desired) != 0


cdef inline uint32_t _load_u32_at(
    const unsigned char *base,
    Py_ssize_t offset,
) noexcept nogil:
    """Acquire-load the uint32 at base+offset."""
    return _ring_load_u32(<const uint32_t *>(base + offset))


cdef inline void _store_u32_at(
    unsigned char *base,
    Py_ssize_t offset,
    uint32_t value,
) noexcept nogil:
    """Release-store value into the uint32 at base+offset."""
    _ring_store_u32(<uint32_t *>(base + offset), value)


# ── Python-visible wrappers ───────────────────────────────────────────────────
# Accept shm.buf (a C-contiguous memoryview of unsigned char) and a byte offset.
# Callers must guarantee that offset is 4-byte aligned — ring.py always provides
# slot_offset(cfg, slot_id) which is a multiple of SLOT_HEADER_SIZE (64 bytes).

def cas_state(
    unsigned char[::1] buf not None,
    Py_ssize_t offset,
    unsigned int expected,
    unsigned int desired,
) -> bool:
    """Atomic compare-and-swap on the 4-byte state word at ``buf[offset]``.

    Returns ``True`` iff the word held ``expected`` and was atomically
    replaced by ``desired``.  Uses ACQ_REL / ACQUIRE memory ordering.
    """
    return _ring_cas_u32(
        <uint32_t *>(&buf[offset]),
        <uint32_t>expected,
        <uint32_t>desired,
    ) != 0


def load_state(
    unsigned char[::1] buf not None,
    Py_ssize_t offset,
) -> int:
    """Acquire-load the 4-byte state word at ``buf[offset]``."""
    return <int>_ring_load_u32(<const uint32_t *>(&buf[offset]))


def store_state(
    unsigned char[::1] buf not None,
    Py_ssize_t offset,
    unsigned int value,
) -> None:
    """Release-store ``value`` into the 4-byte state word at ``buf[offset]``."""
    _ring_store_u32(<uint32_t *>(&buf[offset]), <uint32_t>value)
