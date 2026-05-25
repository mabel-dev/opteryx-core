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

Dense string columns use a 7-byte prefix radix sort (via compress_into) with
a full-content memcmp tiebreak applied inside any bin whose strings share
their first 7 bytes.

Dictionary-encoded columns are sorted by semantic value order, making the
result ORDER BY-correct.  A remap table is built by sorting the D dictionary
entries and assigning rank 0..D-1; codes are replaced with ranks before the
radix pass.  The remap table is small (D ≤ 256 for uint8 codes, ≤ 65536 for
uint16 codes) and fits in L1/L2 cache, so scalar lookup is fast.  The
SIMD-accelerated simd_remap_u8 / simd_remap_u16 functions in simd_remap.cpp
are available for external callers that apply remaps across many morsels.

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
    DrakenVarBuffer,
    DrakenStringArena,
    DrakenStringSlot,
    str_length,
    str_data,
    DrakenType,
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_BOOL,
    DRAKEN_VARCHAR,
    DRAKEN_NVARCHAR,
    DRAKEN_DATE32,
    DRAKEN_TIMESTAMP64,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
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
        std::sort(perm, perm + n,
            [data, offsets, ascending](uint32_t a, uint32_t b) {
                int32_t sa = offsets[a], la = offsets[a + 1] - sa;
                int32_t sb = offsets[b], lb = offsets[b + 1] - sb;
                int32_t cm = (la < lb) ? la : lb;
                int r = cm ? std::memcmp(data + sa, data + sb, (size_t)cm) : 0;
                if (r == 0) r = la - lb;
                return ascending ? (r < 0) : (r > 0);
            });
    }

    // O(D log D) sort for numeric dictionary remap building.
    // Sorts the order[] array by sort_keys[order[i]] ascending.
    static void _sort_numeric_remap(
        uint32_t* order,
        uint32_t D,
        const int64_t* sort_keys
    ) {
        std::sort(order, order + D,
            [sort_keys](uint32_t a, uint32_t b) {
                return sort_keys[a] < sort_keys[b];
            });
    }

    // O(D log D) sort for string dictionary remap building.
    // Sorts the order[] array by lexicographic value of string at order[i].
    static void _sort_string_remap(
        uint32_t* order,
        uint32_t D,
        const char* data,
        const int32_t* offsets
    ) {
        std::sort(order, order + D,
            [data, offsets](uint32_t a, uint32_t b) {
                int32_t sa = offsets[a], la = offsets[a + 1] - sa;
                int32_t sb = offsets[b], lb = offsets[b + 1] - sb;
                int32_t cm = (la < lb) ? la : lb;
                int r = cm ? std::memcmp(data + sa, data + sb, (size_t)cm) : 0;
                if (r == 0) r = la - lb;
                return r < 0;
            });
    }

    // O(D log D) sort for German-string dictionary remap building.
    // Slots are 16-byte DrakenStringSlot entries backed by an arena for extern strings.
    static void _sort_german_string_remap(
        const DrakenStringSlot* slots,
        const uint8_t* arena,
        uint32_t D,
        uint32_t* order
    ) {
        std::sort(order, order + D,
            [slots, arena](uint32_t a, uint32_t b) {
                uint32_t la = str_length(&slots[a]);
                uint32_t lb = str_length(&slots[b]);
                const uint8_t* pa = str_data(&slots[a], arena);
                const uint8_t* pb = str_data(&slots[b], arena);
                uint32_t cm = (la < lb) ? la : lb;
                int r = cm ? std::memcmp(pa, pb, (size_t)cm) : 0;
                if (r == 0) r = (int)la - (int)lb;
                return r < 0;
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

    void _sort_numeric_remap(
        uint32_t* order,
        uint32_t D,
        const int64_t* sort_keys,
    ) nogil

    void _sort_string_remap(
        uint32_t* order,
        uint32_t D,
        const char* data,
        const int32_t* offsets,
    ) nogil

    void _sort_german_string_remap(
        const DrakenStringSlot* slots,
        const uint8_t* arena,
        uint32_t D,
        uint32_t* order,
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


# ── Dictionary remap helpers ──────────────────────────────────────────────────

cdef inline int64_t _dict_value_as_int64(
    const DrakenVarBuffer* dv,
    DrakenType value_type,
    uint32_t code,
) noexcept nogil:
    """
    Read the dictionary value at position 'code' as a sortable int64.
    Returns INT64_MIN for null entries.  Handles fixed-width numeric types.
    For DRAKEN_VARCHAR, the caller must handle string comparison separately.
    """
    cdef uint8_t* nulls = <uint8_t*>dv.null_bitmap
    cdef int64_t bits
    cdef double d
    cdef int32_t fbits
    cdef float f

    if nulls != NULL and ((nulls[code >> 3] >> (code & 7)) & 1) == 0:
        return <int64_t>-9223372036854775808LL  # INT64_MIN

    if value_type == DRAKEN_INT64 or value_type == DRAKEN_TIMESTAMP64 or value_type == DRAKEN_TIME64:
        return (<int64_t*>dv.data)[code]
    if value_type == DRAKEN_INT32 or value_type == DRAKEN_DATE32 or value_type == DRAKEN_TIME32:
        return <int64_t>((<int32_t*>dv.data)[code])
    if value_type == DRAKEN_INT16:
        return <int64_t>((<int16_t*>dv.data)[code])
    if value_type == DRAKEN_INT8:
        return <int64_t>((<int8_t*>dv.data)[code])
    if value_type == DRAKEN_FLOAT64:
        # Reinterpret double bits as int64, apply IEEE 754 sort fix.
        # Positive doubles: bit-pattern order == value order.
        # Negative doubles: bit-patterns are reversed → XOR all bits.
        d = (<double*>dv.data)[code]
        memcpy(&bits, &d, 8)
        if bits < 0:
            bits = bits ^ <int64_t>0x7FFFFFFFFFFFFFFFLL
        return bits
    if value_type == DRAKEN_FLOAT32:
        f = (<float*>dv.data)[code]
        memcpy(&fbits, &f, 4)
        if fbits < 0:
            fbits = fbits ^ <int32_t>0x7FFFFFFF
        return <int64_t>fbits
    if value_type == DRAKEN_BOOL:
        return <int64_t>((<uint8_t*>dv.data)[code] != 0)
    # Unknown type: return code as key (preserves existing GROUP BY behaviour).
    return <int64_t>code


cdef uint32_t* _build_numeric_dict_remap(
    const DrakenVector* uv,
) noexcept:
    """
    Build a remap table for a numeric-valued dictionary column.

    remap[old_code] = rank   where rank 0 is the semantically smallest value.
    The caller inverts order for descending sort by XOR-ing keys with the
    all-ones mask for the code width.

    Returns a heap-allocated uint32[D] array, or NULL on malloc failure.
    The caller is responsible for freeing it with PyMem_Free.
    """
    cdef DrakenVarBuffer* dv = <DrakenVarBuffer*>uv.data
    if dv == NULL:
        return NULL

    cdef Py_ssize_t D = <Py_ssize_t>dv.length
    if D == 0:
        return NULL

    # Build an array of (sortable_int64_key, original_code) pairs.
    cdef int64_t* sort_keys = <int64_t*>PyMem_Malloc(D * sizeof(int64_t))
    cdef uint32_t* order = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))
    cdef uint32_t* remap = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))

    if sort_keys == NULL or order == NULL or remap == NULL:
        PyMem_Free(sort_keys)
        PyMem_Free(order)
        PyMem_Free(remap)
        return NULL

    cdef Py_ssize_t i
    for i in range(D):
        sort_keys[i] = _dict_value_as_int64(dv, dv.type, <uint32_t>i)
        order[i] = <uint32_t>i

    # O(D log D) sort via std::sort — correct for all dictionary sizes,
    # including uint16 codes where D can reach 65536.
    _sort_numeric_remap(order, <uint32_t>D, sort_keys)

    # Invert permutation: remap[old_code] = rank.
    for i in range(D):
        remap[order[i]] = <uint32_t>i

    PyMem_Free(sort_keys)
    PyMem_Free(order)
    return remap


cdef uint32_t* _build_string_dict_remap(
    const DrakenVector* uv,
) noexcept:
    """
    Build a remap table for a string-valued dictionary column.

    The dictionary backing store is DrakenStringArena (German-string slots +
    byte arena).  Sorts D slots lexicographically ascending; assigns rank 0 to
    the lexicographically smallest string.  The caller handles descending sort
    by XOR-ing keys with the all-ones mask for the code width.

    Returns a heap-allocated uint32[D] array, or NULL on malloc failure.
    """
    cdef DrakenStringArena* ga = <DrakenStringArena*>uv.data
    if ga == NULL:
        return NULL

    cdef Py_ssize_t D = <Py_ssize_t>ga.length
    if D == 0:
        return NULL

    cdef uint32_t* order = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))
    cdef uint32_t* remap = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))

    if order == NULL or remap == NULL:
        PyMem_Free(order)
        PyMem_Free(remap)
        return NULL

    cdef Py_ssize_t i
    for i in range(D):
        order[i] = <uint32_t>i

    # O(D log D) lexicographic sort via std::sort on German-string slots.
    _sort_german_string_remap(ga.slots, ga.arena, <uint32_t>D, order)

    # Invert permutation: remap[old_code] = rank.
    for i in range(D):
        remap[order[i]] = <uint32_t>i

    PyMem_Free(order)
    return remap


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

    cdef Py_ssize_t n = len(morsel)
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
    cdef uint32_t* remap = NULL
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

            vec = morsel.column(col_name)

            if (<Vector>vec).unified().type == DRAKEN_VARCHAR or (<Vector>vec).unified().type == DRAKEN_NVARCHAR:
                sv = <Vector>vec
                # Build a temporary contiguous buffer from arena slots via unified sel[i].
                sort_uv = sv.unified()
                sort_arena = <DrakenStringArena*>sort_uv.data
                sort_sel = <uint32_t*>sort_uv.selection
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
        PyMem_Free(remap)   # NULL-safe
