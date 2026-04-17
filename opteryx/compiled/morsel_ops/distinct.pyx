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

Filters the morsel IN PLACE.  The entire pipeline — hash → CarcharSet probe →
branchless index scatter — runs in a single nogil block. All column types MUST
support nogil hashing via c_hash_into(). No Python fallback is permitted.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from libc.stdint cimport int32_t, uint64_t
from libc.stddef cimport size_t

from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.structures.carchar_set cimport CarcharSet, CarcharSetWrapper


# ── Public API ────────────────────────────────────────────────────────────────

def distinct(Morsel morsel, object seen_hashes, list columns=None):
    """
    Filter a Draken Morsel to distinct rows, in place.

    Parameters
    ----------
    morsel : Morsel
        Modified in place; duplicate rows are removed.
    seen_hashes : CarcharSetWrapper or ParviSetWrapper
        Accumulates row hashes across calls for streaming DISTINCT.
        Supports both carchar (dynamic) and parvi (fixed 16-slot) implementations.
    columns : list of bytes, optional
        Column names to hash; all columns used when None.
    """
    from opteryx.compiled.structures.parvi_set import ParviSetWrapper

    cdef Py_ssize_t n = morsel.ptr.num_rows
    cdef uint64_t* hashes_ptr = NULL
    cdef int32_t* idx_buf = NULL
    cdef int32_t* col_indices = NULL
    cdef int32_t n_cols = 0
    cdef size_t count
    cdef bint hash_requires_gil
    cdef uint64_t[::1] py_hashes
    cdef bint is_parvi
    cdef CarcharSetWrapper carchar_set
    cdef CarcharSet* cs
    cdef uint64_t[::1] hashes_memview
    cdef int32_t[::1] idx_memview

    if n == 0:
        return

    # ── Check set variant (WITH GIL) ──────────────────────────────────────────
    from opteryx.compiled.structures.parvi_set import ParviSetWrapper
    is_parvi = isinstance(seen_hashes, ParviSetWrapper)

    # ── Resolve column names → C int array (WITH GIL, once) ──────────────────
    col_indices = morsel._resolve_columns_to_indices(columns, &n_cols)
    if col_indices == NULL:
        raise MemoryError()

    # ── Allocate hash buffer — c_hash requires pre-zeroed buffer for simd_mix_hash ─
    hashes_ptr = <uint64_t*>malloc(<size_t>n * sizeof(uint64_t))
    if hashes_ptr == NULL:
        free(col_indices)
        raise MemoryError()

    # ── Zero the buffer before hashing (required by c_hash_into which uses simd_mix_hash) ─
    memset(hashes_ptr, 0, <size_t>n * sizeof(uint64_t))

    # ── Pre-allocate index buffer (worst case: all rows are new) ─────────────
    idx_buf = <int32_t*>malloc(<size_t>n * sizeof(int32_t))
    if idx_buf == NULL:
        free(col_indices)
        free(hashes_ptr)
        raise MemoryError()

    try:
        # ── Fast path: hash + probe in one nogil block ────────────────────────
        with nogil:
            hash_requires_gil = morsel.c_hash(hashes_ptr, col_indices, n_cols, n)

        if hash_requires_gil:
            # At least one column (e.g. ArrayVector) couldn't hash without GIL.
            # Re-zero and redo via the Python hash() path, then continue nogil.
            memset(hashes_ptr, 0, <size_t>n * sizeof(uint64_t))
            if columns is None:
                py_hashes = morsel.hash()
            else:
                py_hashes = morsel.hash(columns=columns)
            memcpy(hashes_ptr, &py_hashes[0], <size_t>n * sizeof(uint64_t))

        # Call mark_new_indices on the appropriate set type
        if is_parvi:
            # ParviSet path: create memoryviews from pointers and call cpdef wrapper
            hashes_memview = <uint64_t[:n]>hashes_ptr
            idx_memview = <int32_t[:n]>idx_buf
            count, _ = seen_hashes.mark_new_indices_32_public(hashes_memview, idx_memview, <size_t>n)
        else:
            # CarcharSet path
            carchar_set = <CarcharSetWrapper>seen_hashes
            cs = carchar_set._ptr
            with nogil:
                count = cs.mark_new_indices_32(hashes_ptr, idx_buf, <size_t>n)

        if count == 0:
            morsel._empty_inplace()
            return

        # cdef method: no Python dispatch; typed memoryview hits the int32
        # fast path in _take_inplace directly — no copy, no extra allocation.
        morsel._take_inplace(<int32_t[:<Py_ssize_t>count]>idx_buf)

    finally:
        free(col_indices)
        free(hashes_ptr)
        free(idx_buf)
