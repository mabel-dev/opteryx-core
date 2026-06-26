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

"""opteryx.compiled.morsel_ops.sort

Permutation-based sort for Draken Morsels.

    perm = morsel_sort(morsel, column_names, ascending)

Returns a uint32 array where perm[i] is the original row index for sorted
position i.  Multi-column sort uses LSD (Least Significant Digit) stable
radix sort, processing columns from least-significant to most-significant.

String columns are materialized into a contiguous byte buffer (resolving the
unified selection through the arena slots) and sorted by full-content memcmp
via std::sort.

A sorted-dictionary fast-path applies when a string dictionary carries the
DRAKEN_DICT_KEYS_SORTED flag: its codes are assigned in the same unsigned-byte
order this routine sorts by, so sorting by CODE (an integer radix pass on the
selection) is byte-for-byte equivalent to the materialize + string-compare
path, and skips the O(total_bytes) copy entirely.

Numeric, temporal, boolean and dictionary-encoded numeric columns are sorted
via vec.compress(), which returns a sortable signed int64 per logical row for
every encoding shape; the keys are sign-flipped and fed to the radix pass.

Nulls are represented as INT64_MIN by compress_into, which maps to the
smallest unsigned key after the sign-bit flip, giving NULLS FIRST for ASC.
"""

from array import array

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.string cimport memset, memcpy

from libc.stdlib cimport malloc, free

from draken.core.buffers cimport (
    DrakenVector,
    DrakenStringArena,
    str_length,
    str_data,
    DRAKEN_VARCHAR,
    DRAKEN_NVARCHAR,
    DRAKEN_DICT_KEYS_SORTED,
)
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector



# ── Inline C++: std::sort helpers ────────────────────────────────────────────

cdef extern from * nogil:
    """
    #include <algorithm>
    #include <cstdint>
    #include <cstring>
    #include "core/string_slot.h"

    static void _sort_strings(
        uint32_t* perm,
        uint32_t n,
        const char* data,
        const int32_t* offsets,
        bool ascending
    ) {
        // STABLE string sort — std::stable_sort, NOT std::sort.
        //   Multi-column LSD: morsel_sort runs columns least-significant first;
        //   each pass MUST preserve the order the prior (less-significant) passes
        //   established for equal keys. std::sort is unstable, so a string pass
        //   destroyed the prior passes' order — multi-key ORDER BY with a string
        //   key was producing wrong results (the secondary key was ignored
        //   within equal-string groups). stable_sort preserves `perm`'s incoming
        //   order on equal strings → correct LSD.
        std::stable_sort(perm, perm + n,
            [data, offsets, ascending](uint32_t a, uint32_t b) {
                int32_t sa = offsets[a], la = offsets[a + 1] - sa;
                int32_t sb = offsets[b], lb = offsets[b + 1] - sb;
                int32_t cm = (la < lb) ? la : lb;
                int r = cm ? std::memcmp(data + sa, data + sb, (size_t)cm) : 0;
                if (r == 0) r = la - lb;
                return ascending ? (r < 0) : (r > 0);
            });
    }
    """
    void _sort_strings(
        uint32_t* perm,
        uint32_t n,
        const char* data,
        const int32_t* offsets,
        bint ascending,
    ) nogil


# ── Vergesort run-detection pre-pass ─────────────────────────────────────────

cdef extern from "vergesort.h" nogil:
    bint vergesort_u64(
        uint32_t* perm,
        uint32_t* tmp,
        const uint64_t* keys,
        size_t n,
    ) nogil
    void vergesort_reset_stats() nogil
    void vergesort_get_stats(uint64_t* hits, uint64_t* misses) nogil


def vergesort_stats():
    """Return (hits, misses) counters. hits = sorted by vergesort, misses = fell through to radix."""
    cdef uint64_t hits, misses
    vergesort_get_stats(&hits, &misses)
    return int(hits), int(misses)


def vergesort_reset():
    """Reset hit/miss counters to zero."""
    vergesort_reset_stats()


# ── LSD radix sort ────────────────────────────────────────────────────────────

cdef void _radix_sort(
    uint32_t* perm,
    uint32_t* tmp,
    const uint64_t* keys,
    Py_ssize_t n,
    int n_passes,
) noexcept nogil:
    """
    LSD radix sort that updates perm in-place.

    perm    current permutation (modified in-place)
    tmp     scratch buffer of the same length as perm
    keys    sort keys indexed by original row index (keys[row])
    n_passes  number of byte passes: 8 for int64, 1/2/4 for dict codes
    """
    cdef uint64_t count[256]
    cdef Py_ssize_t i
    cdef int b, bv
    cdef uint8_t byte_val
    cdef uint64_t total, old
    cdef uint32_t* src = perm
    cdef uint32_t* dst = tmp
    cdef uint32_t* sw

    for b in range(n_passes):
        memset(count, 0, 256 * sizeof(uint64_t))

        for i in range(n):
            count[(keys[src[i]] >> (b * 8)) & 0xFF] += 1

        total = 0
        for bv in range(256):
            old = count[bv]
            count[bv] = total
            total += old

        for i in range(n):
            byte_val = <uint8_t>((keys[src[i]] >> (b * 8)) & 0xFF)
            dst[count[byte_val]] = src[i]
            count[byte_val] += 1

        sw = src
        src = dst
        dst = sw

    # If an odd number of passes ran, the result ended up in tmp; copy back.
    if src != perm:
        memcpy(perm, src, <size_t>n * sizeof(uint32_t))


# ── Key transform helpers ─────────────────────────────────────────────────────
#
# compress_into returns signed int64.  To radix-sort correctly:
#   ASC:  flip sign bit only          key ^ 0x8000_0000_0000_0000
#         → INT64_MIN (nulls) becomes 0 (smallest unsigned) — NULLS FIRST
#         → negatives sort before non-negatives
#   DESC: flip sign bit then all bits  key ^ 0x7FFF_FFFF_FFFF_FFFF
#         → same effect but inverted order

cdef inline uint64_t _asc_xor = <uint64_t>0x8000000000000000ULL
cdef inline uint64_t _desc_xor = <uint64_t>0x7FFFFFFFFFFFFFFFULL


# ── Public API ────────────────────────────────────────────────────────────────

cpdef morsel_sort(Morsel morsel, list column_names, list ascending):
    """
    Compute a sort permutation for a Draken Morsel.

    Parameters
    ----------
    morsel : Morsel
        The morsel whose rows are to be sorted.
    column_names : list[bytes]
        Column names in sort-priority order, most significant first.
    ascending : list[bool]
        Sort direction per column; True = ascending, False = descending.

    Returns
    -------
    array('i')
        int32 permutation: result[i] is the original row index for sorted
        position i.  Apply with ``morsel.take(perm)``.
    """
    if len(column_names) != len(ascending):
        raise ValueError("column_names and ascending must have the same length")
    if not column_names:
        raise ValueError("at least one sort column is required")

    cdef Py_ssize_t n = morsel.num_rows
    if n == 0:
        return array("i")

    # Allocate all three C buffers once; reuse keys across every column.
    # perm and tmp swap roles each radix pass; keys is overwritten per column.
    cdef uint32_t* perm_buf = <uint32_t*> PyMem_Malloc(n * sizeof(uint32_t))
    cdef uint32_t* tmp_buf = <uint32_t*> PyMem_Malloc(n * sizeof(uint32_t))
    cdef uint64_t* keys = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
    if perm_buf == NULL or tmp_buf == NULL or keys == NULL:
        PyMem_Free(perm_buf)
        PyMem_Free(tmp_buf)
        PyMem_Free(keys)
        raise MemoryError()

    cdef Py_ssize_t i
    for i in range(n):
        perm_buf[i] = <uint32_t>i

    cdef int64_t[::1] signed_mv
    cdef Vector sv
    cdef uint64_t key_xor
    cdef bint asc
    cdef uint64_t flip
    cdef int[::1] rv
    cdef DrakenVector* sort_uv
    cdef DrakenStringArena* sort_arena
    cdef uint32_t* sort_sel
    cdef int64_t sort_total_bytes
    cdef Py_ssize_t sort_di
    cdef int32_t* sort_offsets
    cdef uint8_t* sort_buf
    cdef int64_t sort_fill
    cdef int64_t sort_slen

    try:
        # LSD: iterate columns from least-significant to most-significant.
        for col_idx in range(len(column_names) - 1, -1, -1):
            col_name = column_names[col_idx]
            asc = bool(ascending[col_idx])
            key_xor = _asc_xor if asc else _desc_xor

            vec = morsel._cxx_column(col_name)

            if (<Vector>vec).unified().type == DRAKEN_VARCHAR or (<Vector>vec).unified().type == DRAKEN_NVARCHAR:
                sv = <Vector>vec
                # Build a temporary contiguous buffer from arena slots via unified sel[i].
                sort_uv = sv.unified()
                sort_arena = <DrakenStringArena*>sort_uv.data
                sort_sel = <uint32_t*>sort_uv.selection

                # Sorted-dictionary fast-path: a dict whose values are ascending
                # (DRAKEN_DICT_KEYS_SORTED) assigns codes in the SAME unsigned-byte
                # order this routine sorts by — so sorting by CODE is byte-for-byte
                # equivalent to the materialize+string-compare path below (null rows
                # included: both order by slots[code]). Replaces an O(total_bytes)
                # copy + string compares with an integer radix sort on the codes.
                if (sort_uv.data_length > 1 and sort_uv.data_length < <uint32_t>n
                        and (sort_uv.flags & DRAKEN_DICT_KEYS_SORTED)):
                    with nogil:
                        if asc:
                            for i in range(n):
                                keys[i] = <uint64_t>sort_sel[i]
                        else:
                            for i in range(n):
                                keys[i] = ~(<uint64_t>sort_sel[i])
                        if not vergesort_u64(perm_buf, tmp_buf, keys, n):
                            _radix_sort(perm_buf, tmp_buf, keys, n, 8)
                    continue
                sort_total_bytes = 0
                for sort_di in range(n):
                    sort_total_bytes += <int64_t>str_length(&sort_arena.slots[sort_sel[sort_di]])
                sort_offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))
                sort_buf = <uint8_t*>malloc(sort_total_bytes if sort_total_bytes > 0 else 1)
                if sort_offsets == NULL or sort_buf == NULL:
                    if sort_offsets != NULL:
                        free(sort_offsets)
                    if sort_buf != NULL:
                        free(sort_buf)
                    raise MemoryError()
                sort_offsets[0] = 0
                sort_fill = 0
                for sort_di in range(n):
                    sort_slen = <int64_t>str_length(&sort_arena.slots[sort_sel[sort_di]])
                    if sort_slen > 0:
                        memcpy(sort_buf + sort_fill, str_data(&sort_arena.slots[sort_sel[sort_di]], sort_arena.arena), sort_slen)
                    sort_fill += sort_slen
                    sort_offsets[sort_di + 1] = <int32_t>sort_fill
                try:
                    with nogil:
                        _sort_strings(
                            perm_buf, <uint32_t>n,
                            <const char*>sort_buf,
                            sort_offsets,
                            asc,
                        )
                finally:
                    free(sort_offsets)
                    free(sort_buf)
            else:
                # ── Numeric / timestamp / date / bool / other (includes dict-encoded) ──
                # compress() returns a sortable signed int64 for all shapes.
                signed_mv = vec.compress()
                with nogil:
                    for i in range(n):
                        keys[i] = <uint64_t>signed_mv[i] ^ key_xor
                    if not vergesort_u64(perm_buf, tmp_buf, keys, n):
                        _radix_sort(perm_buf, tmp_buf, keys, n, 8)

        result = array("i", b"\x00" * (n * sizeof(uint32_t)))
        rv = result
        memcpy(&rv[0], perm_buf, n * sizeof(uint32_t))
        return result

    finally:
        PyMem_Free(perm_buf)
        PyMem_Free(tmp_buf)
        PyMem_Free(keys)
