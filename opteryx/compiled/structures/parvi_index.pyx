# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
opteryx/compiled/structures/parvi_index.pyx

Cython wrapper for opteryx::parvi::ParviMap — a fixed-capacity 16-slot inline
hash map optimized for small group-by results.

Design notes
------------
- ParviMap is stack-allocated inline: no heap allocation overhead.
- Single SIMD-group probe: entire table is one 16-byte control group.
- Overflow handling: insert_new() returns {slot=kCapacity, found=false} on full.
- Promotion: drain_into(CarcharIndex) copies live entries for seamless migration.
"""

from libc.stdint cimport int64_t, uint64_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector

cdef extern from "parvi.hpp" namespace "opteryx::parvi":
    cdef struct ParviResult:
        size_t slot
        bint   found

    cdef cppclass CarcharIndex:
        pass

    cdef cppclass ParviMap:
        ParviMap() except +
        size_t size() const
        bint   full() const
        bint   lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        ParviResult insert_new(uint64_t key, int64_t payload_ref)
        void   drain_into(CarcharIndex& target) const

    const size_t kCapacity


# Python wrapper for benchmarking
cdef class ParviMapWrapper:
    """
    Python-visible wrapper for ParviMap.

    Used for benchmarking and testing. Hot-path usage in GROUP BY goes
    through Cython directly (see _engine.pxi) for zero overhead.
    """
    cdef ParviMap* _ptr

    def __cinit__(self):
        self._ptr = new ParviMap()

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()

    def __repr__(self):
        return f"ParviMapWrapper(size={self._ptr.size()}, full={self._ptr.full()})"

    cpdef size_t size(self):
        """Return the number of keys currently in the map."""
        return self._ptr.size()

    cpdef bint full(self):
        """Return True if the map is at capacity (16 entries)."""
        return self._ptr.full()

    cpdef bint lookup_fast(self, uint64_t key):
        """Look up a key. Returns True if found."""
        cdef int64_t payload_out
        return self._ptr.lookup_fast(key, payload_out)

    cpdef tuple insert_new(self, uint64_t key, int64_t payload_ref):
        """Insert a new key. Returns (slot, inserted_bool, overflow_bool)."""
        cdef ParviResult result = self._ptr.insert_new(key, payload_ref)
        overflow = result.slot == kCapacity
        return (result.slot, result.found, overflow)

    @staticmethod
    cdef size_t get_capacity():
        """Return the fixed capacity of parvi (16)."""
        return kCapacity

    cpdef size_t capacity(self):
        """Return the fixed capacity of parvi (16)."""
        return 16  # kCapacity = 16
