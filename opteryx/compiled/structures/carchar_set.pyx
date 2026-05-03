# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True
# distutils: language = c++

"""
opteryx/compiled/structures/carchar_set.pyx

Cython extension type wrapping opteryx::carchar::CarcharSet.

Drop-in replacement for FlatHashSet with the same cdef API surface so all
call sites (joins, distinct operations, aggregation kernels) can be migrated
without restructuring hot-path code.

Design notes
------------
- CarcharSet is heap-allocated via `new`/`delete` so Cython never tries to
  default-construct or copy the C++ object.
- Hot-path methods (insert, contains, reserve, find_new_indices_out*) are all
  declared `noexcept nogil` so callers can hold these loops inside `with nogil`
  blocks.  If CarcharSet::insert_or_ignore triggers a resize that throws
  std::bad_alloc, std::terminate() is called — the same behaviour as the
  FlatHashSet predecessor, and correct for an out-of-memory situation inside a
  query engine.
- The C++ inline wrappers in the `cdef extern from *` block give each hot
  operation a single-instruction call boundary with no exception-table overhead.
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free


# ---------------------------------------------------------------------------
# noexcept C++ wrappers
#
# CarcharSet::insert_or_ignore and reserve() can reallocate; we wrap them
# noexcept here so the hot-path cdef methods can carry the nogil qualifier.
# ---------------------------------------------------------------------------
cdef extern from *:
    """
    #include "carchar_set.hpp"
    namespace opteryx_csw {

        static inline bool insert_new(
            opteryx::carchar::CarcharSet* s, uint64_t v
        ) noexcept {
            return s->insert_or_ignore(v);
        }

        static inline bool probe(
            const opteryx::carchar::CarcharSet* s, uint64_t v
        ) noexcept {
            return s->contains(v);
        }

        static inline void pre_reserve(
            opteryx::carchar::CarcharSet* s, size_t n
        ) noexcept {
            s->reserve(n);
        }

        static inline size_t mark_new_idx32(
            opteryx::carchar::CarcharSet* s,
            const uint64_t* keys,
            int32_t* out_indices,
            size_t length
        ) noexcept {
            return s->mark_new_indices_32(keys, out_indices, length);
        }

        static inline size_t mark_new_idx64(
            opteryx::carchar::CarcharSet* s,
            const uint64_t* keys,
            int64_t* out_indices,
            size_t length
        ) noexcept {
            return s->mark_new_indices_64(keys, out_indices, length);
         }

        static inline size_t insert_many(
            opteryx::carchar::CarcharSet* s,
            const uint64_t* keys,
            size_t length
        ) noexcept {
            return s->insert_many(keys, length);
         }

        static inline void tighten(
            opteryx::carchar::CarcharSet* s
        ) noexcept {
            s->tighten();
         }

        static inline size_t probe_found_32(
            const opteryx::carchar::CarcharSet* s,
            const uint64_t* keys,
            int32_t* out_indices,
            size_t length
        ) noexcept {
            return s->probe_found_32(keys, out_indices, length);
        }

        static inline size_t probe_not_found_32(
            const opteryx::carchar::CarcharSet* s,
            const uint64_t* keys,
            int32_t* out_indices,
            size_t length
        ) noexcept {
            return s->probe_not_found_32(keys, out_indices, length);
        }

    }   // namespace opteryx_csw
    """
    bint _csw_insert "opteryx_csw::insert_new"(
        CarcharSet* s, uint64_t v
    ) noexcept nogil

    bint _csw_contains "opteryx_csw::probe"(
        const CarcharSet* s, uint64_t v
    ) noexcept nogil

    void _csw_reserve "opteryx_csw::pre_reserve"(
        CarcharSet* s, size_t n
    ) noexcept nogil

    size_t _csw_mark_new_idx32 "opteryx_csw::mark_new_idx32"(
        CarcharSet* s,
        const uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept nogil

    size_t _csw_mark_new_idx64 "opteryx_csw::mark_new_idx64"(
        CarcharSet* s,
        const uint64_t* keys,
        int64_t* out_indices,
        size_t length,
    ) noexcept nogil

    size_t _csw_insert_many "opteryx_csw::insert_many"(
        CarcharSet* s,
        const uint64_t* keys,
        size_t length,
    ) noexcept nogil

    void _csw_tighten "opteryx_csw::tighten"(
        CarcharSet* s,
    ) noexcept nogil

    size_t _csw_probe_found_32 "opteryx_csw::probe_found_32"(
        const CarcharSet* s,
        const uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept nogil

    size_t _csw_probe_not_found_32 "opteryx_csw::probe_not_found_32"(
        const CarcharSet* s,
        const uint64_t* keys,
        int32_t* out_indices,
        size_t length,
    ) noexcept nogil


# ---------------------------------------------------------------------------
# CarcharSetWrapper
# ---------------------------------------------------------------------------

cdef class CarcharSetWrapper:
    """
    Persistent Carchar hash set for set-membership and distinct workloads.

    Wraps a heap-allocated opteryx::carchar::CarcharSet and exposes the same
    cdef API as the old FlatHashSet so all existing call sites compile without
    structural changes.

    Python-visible constructor::

        seen = CarcharSetWrapper()            # default 16-slot, 0.80 load
        seen = CarcharSetWrapper(4096)        # pre-sized
        seen = CarcharSetWrapper(4096, 0.75)  # custom load factor

    All hot-path methods are ``cdef noexcept nogil`` and can be called from
    inside ``with nogil:`` blocks.
    """

    # C-level prototypes for methods callable without the GIL
    cdef:
        size_t _insert_many_nogil(self, uint64_t* keys, size_t length) noexcept nogil
        void _tighten_nogil(self) noexcept nogil

    def __cinit__(self, size_t initial_capacity=16, double load_factor=0.80):
        self._ptr = new CarcharSet(initial_capacity, load_factor)

    def __dealloc__(self):
        if self._ptr is not NULL:
            del self._ptr
            self._ptr = NULL

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()

    def __repr__(self):
        return f"CarcharSetWrapper(size={self._ptr.size()}, capacity={self._ptr.capacity()})"

    # -----------------------------------------------------------------------
    # Python-visible accessors
    # -----------------------------------------------------------------------

    cpdef size_t size(self):
        """Return the number of entries currently in the set."""
        return self._ptr.size()

    cpdef size_t capacity(self):
        """Return the current allocated capacity of the set."""
        return self._ptr.capacity()

     # -----------------------------------------------------------------------
     # cdef hot-path methods — noexcept nogil
     # -----------------------------------------------------------------------

    cdef inline bint insert(self, uint64_t value) noexcept nogil:
        """Insert value; return True if newly inserted."""
        return _csw_insert(self._ptr, value)

    cdef inline bint contains(self, uint64_t value) noexcept nogil:
        """Return True if value is present in the set."""
        return _csw_contains(self._ptr, value)

    cdef inline void reserve(self, size_t capacity) noexcept nogil:
        """Pre-allocate for at least `capacity` entries."""
        _csw_reserve(self._ptr, capacity)

    cdef Py_ssize_t find_new_indices_out(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int64_t* out_indices,
     ) noexcept nogil:
        """
        Insert hashes[0..length); write index i into out_indices for each
        newly-inserted entry.  Returns the count of newly-inserted entries.

        Equivalent to FlatHashSet.find_new_indices_out — used by
        table_ops/distinct for large (>= 2^31 row) datasets.
         """
        return <Py_ssize_t>_csw_mark_new_idx64(
            self._ptr, hashes, out_indices, <size_t>length
         )

    cdef Py_ssize_t find_new_indices_out_32(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int32_t* out_indices,
     ) noexcept nogil:
        """
        Same as find_new_indices_out but writes int32 row indices.

        Used when num_rows < 2^31 (the common case).  Equivalent to
        FlatHashSet.find_new_indices_out_32.
         """
        return <Py_ssize_t>_csw_mark_new_idx32(
            self._ptr, hashes, out_indices, <size_t>length
          )

    cdef Py_ssize_t probe_found_32_nogil(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int32_t* out_indices,
    ) noexcept nogil:
        """
        Read-only batch probe: write row indices of hashes FOUND in the set
        into out_indices.  Returns the count written.  Never modifies the set.
        Used by the semi-join (IN subquery) probe phase.
        """
        return <Py_ssize_t>_csw_probe_found_32(
            self._ptr, hashes, out_indices, <size_t>length
        )

    cdef Py_ssize_t probe_not_found_32_nogil(
        self,
        uint64_t* hashes,
        Py_ssize_t length,
        int32_t* out_indices,
    ) noexcept nogil:
        """
        Read-only batch probe: write row indices of hashes NOT FOUND in the set
        into out_indices.  Returns the count written.  Never modifies the set.
        Used by the anti-join (NOT IN / EXCEPT) probe phase.
        """
        return <Py_ssize_t>_csw_probe_not_found_32(
            self._ptr, hashes, out_indices, <size_t>length
        )

    # -----------------------------------------------------------------------
    # C-level and Python-visible methods
    # -----------------------------------------------------------------------

    # C-level bulk insert usable without the GIL from other cdef code
    cdef size_t _insert_many_nogil(self, uint64_t* keys, size_t length) noexcept nogil:
        return _csw_insert_many(self._ptr, keys, length)

    def insert_many(self, keys):
        """
        Python-friendly bulk insert; accepts buffer-like object.
        """
        cdef uint64_t[:] kv = memoryview(keys)
        cdef size_t res
        if kv.shape[0] == 0:
            return 0
        with nogil:
            res = self._insert_many_nogil(&kv[0], <size_t>kv.shape[0])
        return res

    # C-level tighten usable without the GIL
    cdef void _tighten_nogil(self) noexcept nogil:
        _csw_tighten(self._ptr)

    def tighten(self):
        """Python-visible tighten"""
        with nogil:
            self._tighten_nogil()

    cpdef bint add(self, uint64_t value):
        """Insert value; return True if newly inserted (Python-visible alias)."""
        return _csw_insert(self._ptr, value)

    cpdef bint has(self, uint64_t value):
        """Return True if value is present (Python-visible alias)."""
        return _csw_contains(self._ptr, value)

    cpdef void reserve_py(self, size_t capacity):
        """Pre-allocate for at least `capacity` entries (Python-visible)."""
        _csw_reserve(self._ptr, capacity)

    def probe_found(self, keys):
        """
        Python-visible read-only probe: return a list of indices into `keys`
        where keys[i] IS present in the set.  Used for testing.
        """
        cdef uint64_t[:] kv = memoryview(keys)
        cdef Py_ssize_t n = kv.shape[0]
        if n == 0:
            return []
        cdef int32_t* out = <int32_t*>PyMem_Malloc(n * sizeof(int32_t))
        if out == NULL:
            raise MemoryError()
        cdef Py_ssize_t found
        with nogil:
            found = self.probe_found_32_nogil(&kv[0], n, out)
        result = [out[i] for i in range(found)]
        PyMem_Free(out)
        return result

    def probe_not_found(self, keys):
        """
        Python-visible read-only probe: return a list of indices into `keys`
        where keys[i] is NOT present in the set.  Used for testing.
        """
        cdef uint64_t[:] kv = memoryview(keys)
        cdef Py_ssize_t n = kv.shape[0]
        if n == 0:
            return []
        cdef int32_t* out = <int32_t*>PyMem_Malloc(n * sizeof(int32_t))
        if out == NULL:
            raise MemoryError()
        cdef Py_ssize_t not_found
        with nogil:
            not_found = self.probe_not_found_32_nogil(&kv[0], n, out)
        result = [out[i] for i in range(not_found)]
        PyMem_Free(out)
        return result
