# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True
# distutils: language = c++

"""
opteryx/compiled/structures/carchar_index.pyx

Cython extension type wrapping opteryx::carchar::CarcharJoinIndex.

Replacement for hash_table.HashTable with improved memory layout and SIMD-optimized
probing. Maps uint64_t keys to lists of int64_t row IDs (multi-value per key).

Design notes
------------
- CarcharJoinIndex is heap-allocated via `new`/`delete` so Cython never tries to
  default-construct or copy the C++ object.
- Hot-path methods (insert_row) are declared `noexcept nogil` so callers can hold
  these loops inside `with nogil` blocks.
- Inline row storage (first 2 rows) avoids allocation for common case;
  overflow vector used for additional rows.
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector
from libcpp.pair cimport pair


# ---------------------------------------------------------------------------
# noexcept C++ wrappers
#
# CarcharJoinIndex::insert_row and reserve() can reallocate; we wrap them
# noexcept here so the hot-path cdef methods can carry the nogil qualifier.
# ---------------------------------------------------------------------------
cdef extern from *:
    """
    #include "carchar_join_index.hpp"
    namespace opteryx_cji {

        static inline void insert_row_wrapped(
            opteryx::carchar::CarcharJoinIndex* idx, uint64_t key, int64_t row_id
        ) noexcept {
            idx->insert_row(key, row_id);
        }

        static inline void pre_reserve(
            opteryx::carchar::CarcharJoinIndex* idx, size_t n
        ) noexcept {
            idx->reserve(n);
        }

    }  // namespace opteryx_cji
    """
    void _cji_insert "opteryx_cji::insert_row_wrapped"(
        CarcharJoinIndex* idx, uint64_t key, int64_t row_id
    ) noexcept nogil

    void _cji_reserve "opteryx_cji::pre_reserve"(
        CarcharJoinIndex* idx, size_t n
    ) noexcept nogil


# ---------------------------------------------------------------------------
# CarcharJoinIndexWrapper
# ---------------------------------------------------------------------------

cdef class CarcharJoinIndexWrapper:
    """
    Persistent Carchar hash map for join operations.

    Wraps a heap-allocated opteryx::carchar::CarcharJoinIndex and provides
    efficient multi-value-per-key storage with inline optimization for up to 2 rows.

    Python-visible constructor::

        index = CarcharJoinIndexWrapper()           # default 16-slot, 0.80 load
        index = CarcharJoinIndexWrapper(4096)       # pre-sized
        index = CarcharJoinIndexWrapper(4096, 0.75) # custom load factor

    All hot-path methods are ``cdef noexcept nogil`` and can be called from
    inside ``with nogil:`` blocks.
    """

    def __cinit__(self, size_t initial_capacity=16, double load_factor=0.80):
        self._ptr = new CarcharJoinIndex(initial_capacity, load_factor)

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()

    def __repr__(self):
        return f"CarcharJoinIndexWrapper(size={self._ptr.size()}, capacity={self._ptr.capacity()})"

    # -----------------------------------------------------------------------
    # Python-visible accessors
    # -----------------------------------------------------------------------

    cpdef size_t size(self):
        """Return the number of unique keys currently in the index."""
        return self._ptr.size()

    cpdef size_t capacity(self):
        """Return the current allocated capacity of the index."""
        return self._ptr.capacity()

    cpdef void reserve(self, size_t capacity):
        """Pre-allocate for at least `capacity` unique keys."""
        self._ptr.reserve(capacity)

    cpdef void insert_row(self, uint64_t key, int64_t row_id):
        """Insert a row ID under the given key. Multiple rows can share a key."""
        _cji_insert(self._ptr, key, row_id)

    cpdef vector[int64_t] rows_for(self, uint64_t key):
        """Get list of row IDs for a given key. Returns empty list if not found."""
        return self._ptr.rows_for(key)

    cpdef list items_py(self):
        """
        Return list of (key, [row_ids]) tuples for all entries.

        Converts the C++ items() output (key, payload_ref pairs) into
        Python-friendly tuples with actual row ID lists.
        """
        cdef vector[pair[uint64_t, int64_t]] c_items = self._ptr.items()
        result = []
        for key, payload_ref in c_items:
            row_ids = self._ptr.rows_from_payload(payload_ref)
            result.append((key, list(row_ids)))
        return result
