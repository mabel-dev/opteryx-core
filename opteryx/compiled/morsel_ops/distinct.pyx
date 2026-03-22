# cython: language_level=3
# distutils: language = c++
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""opteryx.compiled.morsel_ops.distinct

Carchar-backed DISTINCT for Draken Morsels.

    distinct(morsel, seen_hashes, columns=None)

Filters the morsel IN PLACE.  For column types that support nogil hashing
(all fixed-width numeric, bool, date/time, string), the entire pipeline —
hash → CarcharSet probe → branchless index scatter — runs in a single
nogil block.  ArrayVector columns (rare) fall back to the Python hash()
path, after which mark_new and scatter still run nogil.
"""

from libc.stdlib cimport calloc, malloc, free
from libc.string cimport memset, memcpy
from libc.stdint cimport int32_t, uint8_t, uint64_t
from libc.stddef cimport size_t

from opteryx.draken.morsels.morsel cimport Morsel


# ── CarcharSet C++ binding ────────────────────────────────────────────────────

cdef extern from "carchar_set.hpp" namespace "opteryx::carchar" nogil:
    cdef cppclass CarcharSet:
        CarcharSet(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries)
        size_t mark_new(const uint64_t* keys, uint8_t* out_is_new, size_t length) noexcept
        size_t size() noexcept


# ── Python-visible wrapper ────────────────────────────────────────────────────

cdef class CarcharSetWrapper:
    """Persistent Carchar hash set for streaming DISTINCT.

    Create once, pass to every distinct() call; the set is mutated in place
    so duplicates are tracked across morsel boundaries.
    """
    cdef CarcharSet* _ptr

    def __cinit__(self, size_t initial_capacity=2048):
        self._ptr = new CarcharSet(initial_capacity, 0.80)

    def __dealloc__(self):
        del self._ptr

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()


# ── Public API ────────────────────────────────────────────────────────────────

def distinct(Morsel morsel, CarcharSetWrapper seen_hashes, list columns=None):
    """
    Filter a Draken Morsel to distinct rows, in place.

    Parameters
    ----------
    morsel : Morsel
        Modified in place; duplicate rows are removed.
    seen_hashes : CarcharSetWrapper
        Accumulates row hashes across calls for streaming DISTINCT.
    columns : list of bytes, optional
        Column names to hash; all columns used when None.
    """
    cdef Py_ssize_t n = morsel.ptr.num_rows
    cdef CarcharSet* cs = seen_hashes._ptr
    cdef uint64_t* hashes_ptr = NULL
    cdef uint8_t*  mask      = NULL
    cdef int32_t*  idx_buf   = NULL
    cdef int32_t*  col_indices = NULL
    cdef int32_t   n_cols = 0
    cdef size_t    count
    cdef Py_ssize_t i, j
    cdef bint had_fallback
    cdef uint64_t[::1] py_hashes

    if n == 0:
        return

    # ── Resolve column names → C int array (WITH GIL, once) ──────────────────
    col_indices = morsel._resolve_columns_to_indices(columns, &n_cols)
    if col_indices == NULL:
        raise MemoryError()

    # ── Allocate hash buffer (calloc = zeroed, nogil-safe allocator) ──────────
    hashes_ptr = <uint64_t*>calloc(<size_t>n, sizeof(uint64_t))
    if hashes_ptr == NULL:
        free(col_indices)
        raise MemoryError()

    mask = <uint8_t*>malloc(<size_t>n)
    if mask == NULL:
        free(col_indices)
        free(hashes_ptr)
        raise MemoryError()

    try:
        # ── Fast path: hash + probe in one nogil block ────────────────────────
        with nogil:
            had_fallback = morsel.c_hash(hashes_ptr, col_indices, n_cols, n)

        if had_fallback:
            # At least one column (e.g. ArrayVector) couldn't hash without GIL.
            # Re-zero and redo via the Python hash() path, then continue nogil.
            memset(hashes_ptr, 0, <size_t>n * sizeof(uint64_t))
            if columns is None:
                py_hashes = morsel.hash()
            else:
                py_hashes = morsel.hash(columns=columns)
            memcpy(hashes_ptr, &py_hashes[0], <size_t>n * sizeof(uint64_t))

        with nogil:
            count = cs.mark_new(hashes_ptr, mask, <size_t>n)

        if count == 0:
            morsel._empty_inplace()
            return

        # Allocate count+1 int32 slots: branchless scatter may write one slot
        # past position count-1 when j == count and mask[i] == 0.
        idx_buf = <int32_t*>malloc((<size_t>count + 1) * sizeof(int32_t))
        if idx_buf == NULL:
            raise MemoryError()

        j = 0
        with nogil:
            for i in range(n):
                idx_buf[j] = <int32_t>i
                j += <Py_ssize_t>mask[i]

        # cdef method: no Python dispatch; typed memoryview hits the int32
        # fast path in _take_inplace directly — no copy, no extra allocation.
        morsel._take_inplace(<int32_t[:<Py_ssize_t>count]>idx_buf)

    finally:
        free(col_indices)
        free(hashes_ptr)
        free(mask)
        free(idx_buf)
