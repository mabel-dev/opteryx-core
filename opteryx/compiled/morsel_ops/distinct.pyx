# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

# distutils: language = c++
"""opteryx.compiled.morsel_ops.distinct

Carchar-backed DISTINCT for Draken Morsels.

    distinct(morsel, seen_hashes, columns=None)

Filters the morsel IN PLACE.  The entire pipeline — hash → CarcharSet probe →
branchless index scatter — runs in a single nogil block. All column types MUST
support nogil hashing via c_hash_into(). No Python fallback is permitted.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libcpp cimport bool as cpp_bool
from libcpp.pair cimport pair
from libcpp.vector cimport vector

from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector
from opteryx.compiled.structures.carchar_set cimport CarcharSet, CarcharSetWrapper
from opteryx.compiled.structures.parvi_set cimport ParviSet, ParviSetWrapper

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

cdef extern from "core/buffers.h" nogil:
    int draken_is_compressed(const DrakenVector* v)


# WP-13 Stage 2 — single-column compressed-key shaped distinct. For a dict/constant
# key, probe each of the k unique values ONCE (mark_new over the unique hashes) and
# keep the first-occurrence row of each globally-new value, instead of probing all
# n rows. Null semantics are UNCHANGED: the shaped hash bakes every null row into a
# single null slot, so nulls collapse to ONE group exactly as the per-row path does.
# Toggle for differential testing / kill-switch.
cdef bint _WP13_DISTINCT_KPROBE = True


cpdef void set_distinct_kprobe_enabled(bint enabled):
    global _WP13_DISTINCT_KPROBE
    _WP13_DISTINCT_KPROBE = enabled


cpdef bint get_distinct_kprobe_enabled():
    return _WP13_DISTINCT_KPROBE


cdef object _distinct_compressed_single(
    Morsel morsel,
    object seen_hashes,
    object columns,
    bint is_parvi,
):
    """Shaped k-probe DISTINCT for a single COMPRESSED key column. Returns one of:
      - None  → not applicable (dense key); caller uses the per-row path.
      - True  → Parvi overflow; morsel left UNCHANGED for promote-and-replay.
      - False → done; morsel filtered in place.
    """
    cdef Vector hv = <Vector>morsel.hash_keys(columns)
    cdef DrakenVector* huv = hv.unified()
    if draken_is_compressed(huv) == 0:
        return None

    cdef const uint64_t* khashes = <const uint64_t*>huv.data
    cdef const uint32_t* codes = huv.selection
    cdef Py_ssize_t k_out = <Py_ssize_t>huv.data_length
    cdef Py_ssize_t n = morsel.ptr.num_rows

    # First occurrence (row index) of each code that appears in this morsel, in
    # scan order — so kept rows come out already first-occurrence-ordered.
    cdef vector[uint8_t] seen_code
    seen_code.assign(k_out, 0)
    cdef uint64_t* uniq = <uint64_t*>malloc(<size_t>(k_out if k_out > 0 else 1) * sizeof(uint64_t))
    cdef int32_t* first_rows = <int32_t*>malloc(<size_t>(k_out if k_out > 0 else 1) * sizeof(int32_t))
    cdef int32_t* uniq_new = <int32_t*>malloc(<size_t>(k_out if k_out > 0 else 1) * sizeof(int32_t))
    cdef int32_t* keep = <int32_t*>malloc(<size_t>(k_out if k_out > 0 else 1) * sizeof(int32_t))
    if uniq == NULL or first_rows == NULL or uniq_new == NULL or keep == NULL:
        free(uniq); free(first_rows); free(uniq_new); free(keep)
        raise MemoryError()

    cdef Py_ssize_t m = 0
    cdef Py_ssize_t i, j
    cdef uint32_t c
    cdef size_t new_count = 0
    cdef cpp_bool overflow = False
    cdef pair[size_t, cpp_bool] parvi_result
    cdef CarcharSet* cs
    cdef ParviSet* ps

    try:
        with nogil:
            for i in range(n):
                c = codes[i]
                if seen_code[c] == 0:
                    seen_code[c] = 1
                    uniq[m] = khashes[c]
                    first_rows[m] = <int32_t>i
                    m += 1

            # Probe the m unique hashes once each; mark_new returns the positions
            # (into uniq) of the globally-new values.
            if is_parvi:
                ps = (<ParviSetWrapper>seen_hashes)._ptr
                parvi_result = ps.mark_new_indices[int32_t](uniq, uniq_new, <size_t>m)
                new_count = parvi_result.first
                overflow = parvi_result.second
            else:
                cs = (<CarcharSetWrapper>seen_hashes)._ptr
                new_count = cs.mark_new_indices_32(uniq, uniq_new, <size_t>m)

            if not overflow:
                for j in range(<Py_ssize_t>new_count):
                    keep[j] = first_rows[uniq_new[j]]

        if overflow:
            # Parvi overflow: caller promotes and replays the UNCHANGED morsel.
            return True

        if new_count == 0:
            morsel._empty_inplace()
            return False

        morsel._take_inplace(<int32_t[:<Py_ssize_t>new_count]>keep)
        return False
    finally:
        free(uniq)
        free(first_rows)
        free(uniq_new)
        free(keep)


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
        Supports both carchar (dynamic) and parvi (fixed 64-slot) implementations.
    columns : list of bytes, optional
        Column names to hash; all columns used when None.

    Returns
    -------
    bool
        True when Parvi overflow occurred (unseen key while at capacity).
        In that case the morsel is left unchanged so the caller can promote
        and replay safely.
    """
    cdef Py_ssize_t n = morsel.ptr.num_rows
    cdef uint64_t* hashes_ptr = NULL
    cdef int32_t* idx_buf = NULL
    cdef int32_t* col_indices = NULL
    cdef const DrakenVector** dvs = NULL
    cdef int32_t n_cols = 0
    cdef size_t count
    cdef bint hash_requires_gil
    cdef bint is_parvi
    cdef CarcharSet* cs
    cdef ParviSet* ps
    cdef pair[size_t, cpp_bool] parvi_result
    cdef bint overflow = False

    if n == 0:
        return

    # ── Check set variant (WITH GIL) ──────────────────────────────────────────
    is_parvi = isinstance(seen_hashes, ParviSetWrapper)

    # ── Resolve column names → C int array (WITH GIL, once) ──────────────────
    col_indices = morsel._resolve_columns_to_indices(columns, &n_cols)
    if col_indices == NULL:
        raise MemoryError()

    # ── WP-13: single-column compressed key → shaped k-probe (probe k uniques,
    #    not n rows). Returns None to fall through to the per-row path. ─────────
    cdef object kprobe_result
    if _WP13_DISTINCT_KPROBE and n_cols == 1:
        free(col_indices)
        col_indices = NULL
        kprobe_result = _distinct_compressed_single(
            morsel, seen_hashes, columns, is_parvi)
        if kprobe_result is not None:
            return kprobe_result
        # Dense key — re-resolve columns for the per-row path below.
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
        # ── Resolve column pointers up front so c_hash runs fully nogil ──────
        dvs = morsel._columns_to_pointers(col_indices, n_cols)

        # ── Hash + probe in one nogil block ───────────────────────────────────
        with nogil:
            hash_requires_gil = morsel.c_hash(hashes_ptr, dvs, n_cols, n)

        if hash_requires_gil:
            # A column type (e.g. ArrayVector) cannot be row-hashed nogil. Fail
            # loudly rather than degrading to a per-row Python hash path.
            raise TypeError(
                "DISTINCT: one or more key columns have a type that cannot be "
                "hashed (array/null/fp16)")

        # Extract the C++ pointer (WITH GIL: typed cast over the Python handle),
        # then call the native hot-path method directly under nogil. No Cython
        # method wraps either set — the wrapper is just an owning handle.
        if is_parvi:
            ps = (<ParviSetWrapper>seen_hashes)._ptr
            with nogil:
                parvi_result = ps.mark_new_indices[int32_t](hashes_ptr, idx_buf, <size_t>n)
                count = parvi_result.first
                overflow = parvi_result.second
        else:
            cs = (<CarcharSetWrapper>seen_hashes)._ptr
            with nogil:
                count = cs.mark_new_indices_32(hashes_ptr, idx_buf, <size_t>n)

        if overflow:
            # Parvi overflow: caller will promote and replay this unchanged morsel.
            return True

        if count == 0:
            morsel._empty_inplace()
            return overflow

        # cdef method: no Python dispatch; typed memoryview hits the int32
        # fast path in _take_inplace directly — no copy, no extra allocation.
        morsel._take_inplace(<int32_t[:<Py_ssize_t>count]>idx_buf)
        return overflow

    finally:
        free(col_indices)
        free(hashes_ptr)
        free(idx_buf)
        draken_free(dvs)
