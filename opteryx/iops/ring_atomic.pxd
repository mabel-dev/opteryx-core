# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/ring_atomic.pxd
============================
Cython declaration file — exposes cdef-level atomic helpers to other .pyx
modules so they can call them at zero Python overhead.

Usage in another .pyx::

    from opteryx.iops.ring_atomic cimport _cas_u32_at, _load_u32_at, _store_u32_at

    cdef unsigned char *base = ...
    if _cas_u32_at(base, slot_offset, FREE, WRITING):
        # we own the slot
"""

from libc.stdint cimport uint32_t


cdef bint _cas_u32_at(
    unsigned char *base,
    Py_ssize_t offset,
    uint32_t expected,
    uint32_t desired,
) noexcept nogil

cdef uint32_t _load_u32_at(
    const unsigned char *base,
    Py_ssize_t offset,
) noexcept nogil

cdef void _store_u32_at(
    unsigned char *base,
    Py_ssize_t offset,
    uint32_t value,
) noexcept nogil
