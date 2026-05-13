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
opteryx/compiled/structures/parvi_set.pyx

Cython wrapper for opteryx::parvi::ParviSet — a fixed-capacity 16-slot inline
hash set optimized for small distinct/COUNT(DISTINCT) workloads.

Design notes
------------
- ParviSet is stack-allocated inline: no heap allocation overhead.
- Single SIMD-group probe: entire table is one 16-byte control group.
- Overflow handling: insert_or_ignore() returns {is_new: false} when full.
- Promotion: drain_into(CarcharSet) copies live entries for seamless migration.
- mark_new_indices(): bulk insert with overflow detection for DISTINCT pipelines.
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper

# ParviSet and CarcharSet are declared in parvi_set.pxd

# Noexcept C++ wrappers for bulk operations (mark_new_indices with overflow detection)
cdef extern from *:
    """
    #include "parvi.hpp"
    namespace opteryx_psw {
        template <typename IndexT>
        struct MarkResult {
            size_t count;
            bool overflow;
        };

        static inline opteryx_psw::MarkResult<int32_t> mark_new_idx32(
            opteryx::parvi::ParviSet* s,
            const uint64_t* keys,
            int32_t* out_indices,
            size_t length
        ) noexcept {
            auto result = s->mark_new_indices(keys, out_indices, length);
            return {result.first, result.second};
        }

        static inline opteryx_psw::MarkResult<int64_t> mark_new_idx64(
            opteryx::parvi::ParviSet* s,
            const uint64_t* keys,
            int64_t* out_indices,
            size_t length
        ) noexcept {
            auto result = s->mark_new_indices(keys, out_indices, length);
            return {result.first, result.second};
        }
    }
    """
    cdef struct MarkResult32 "opteryx_psw::MarkResult<int32_t>":
        size_t count
        bint overflow

    cdef struct MarkResult64 "opteryx_psw::MarkResult<int64_t>":
        size_t count
        bint overflow

    MarkResult32 _psw_mark_new_idx32 "opteryx_psw::mark_new_idx32"(
        ParviSet* s,
        const uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept nogil

    MarkResult64 _psw_mark_new_idx64 "opteryx_psw::mark_new_idx64"(
        ParviSet* s,
        const uint64_t* keys,
        int64_t* out_indices,
        size_t length,
    ) noexcept nogil


# Python wrapper for DISTINCT/COUNT(DISTINCT)
cdef class ParviSetWrapper:
    """
    Python-visible wrapper for ParviSet.

    Used for DISTINCT and COUNT(DISTINCT) operations. Hot-path usage goes
    through Cython directly for zero overhead.
    """

    def __cinit__(self):
        self._ptr = new ParviSet()

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()

    def __repr__(self):
        return f"ParviSetWrapper(size={self._ptr.size()}, full={self._ptr.full()})"

    # cpdef methods (callable from Python)
    cpdef size_t size(self):
        """Return the number of entries currently in the set."""
        return self._ptr.size()

    cpdef bint full(self):
        """Return True if the set is at capacity (16 entries)."""
        return self._ptr.full()

    cpdef bint contains(self, uint64_t key):
        """Check if a key is present."""
        return self._ptr.contains(key)

    cpdef bint insert(self, uint64_t key):
        """Insert a key. Returns True if newly inserted."""
        cdef ParviSetResult result = self._ptr.insert_or_ignore(key)
        return result.is_new

    cpdef tuple mark_new_indices_32_public(
        self,
        uint64_t[::1] keys_view,
        int32_t[::1] indices_view,
        size_t length,
    ):
        """
        Public cpdef wrapper for mark_new_indices_32 (callable from Python).

        Takes memoryviews instead of raw pointers.
        """
        cdef MarkResult32 result
        with nogil:
            result = _psw_mark_new_idx32(
                self._ptr, &keys_view[0], &indices_view[0], length
            )
        return (result.count, result.overflow)

    cpdef void drain_into_carchar(self, CarcharSetWrapper target):
        """Drain all current Parvi keys into the provided Carchar set."""
        if self._ptr is NULL or target is None or target._ptr is NULL:
            return
        self._ptr.drain_into(target._ptr[0])

    cpdef void clear(self):
        """Clear the set."""
        self._ptr.clear()

    # cdef methods (C-level only, for internal use)
    cdef tuple mark_new_indices_32(
        self,
        uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept:
        """
        Bulk insert; return (count_new, overflow_bool).

        Writes indices of newly-inserted entries into out_indices[0..count_new).
        Returns overflow=True only when an unseen key is encountered while
        already at capacity.
        """
        cdef MarkResult32 result = _psw_mark_new_idx32(
            self._ptr, keys, out_indices, length
        )
        return (result.count, result.overflow)

    cdef tuple mark_new_indices_64(
        self,
        uint64_t* keys,
        int64_t* out_indices,
        size_t length,
    ) noexcept:
        """Same as mark_new_indices_32 but for int64 indices."""
        cdef MarkResult64 result = _psw_mark_new_idx64(
            self._ptr, keys, out_indices, length
        )
        return (result.count, result.overflow)
