# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""opteryx.compiled.morsel_ops.sort

Permutation-based sort for Draken Morsels.

    perm = morsel_sort(morsel, column_names, ascending)

Returns a uint32 array where perm[i] is the original row index for sorted
position i.  Multi-column sort uses LSD (Least Significant Digit) stable
radix sort, processing columns from least-significant to most-significant.

Dense string columns use a 7-byte prefix radix sort (via compress_into) with
a full-content memcmp tiebreak applied inside any bin whose strings share
their first 7 bytes.

Dictionary-encoded columns are sorted on their raw dictionary codes — correct
for GROUP BY (same code == same value == same group).  Semantic ORDER BY on
dictionary strings is a follow-on.

Nulls are represented as INT64_MIN by compress_into, which maps to the
smallest unsigned key after the sign-bit flip, giving NULLS FIRST for ASC.
"""

from array import array

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.string cimport memset, memcpy

from opteryx.draken.core.buffers cimport DictAccessor, DrakenVarBuffer
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.aggregations.vector_readers cimport (
    _vector_dict_accessor,
    _dict_read_code,
)


# ── Inline C++: memcmp tiebreak via std::stable_sort ─────────────────────────

cdef extern from * nogil:
    """
    #include <algorithm>
    #include <cstdint>
    #include <cstring>

    static void _do_tiebreak_sort(
        uint32_t* begin,
        uint32_t* end,
        const char* data,
        const int32_t* offsets,
        bool ascending
    ) {
        std::stable_sort(begin, end,
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
    void _do_tiebreak_sort(
        uint32_t* begin,
        uint32_t* end,
        const char* data,
        const int32_t* offsets,
        bint ascending,
    ) nogil


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

        sw = src; src = dst; dst = sw

    # If an odd number of passes ran, the result ended up in tmp; copy back.
    if src != perm:
        memcpy(perm, src, <size_t>n * sizeof(uint32_t))


# ── String prefix tiebreak ────────────────────────────────────────────────────

cdef void _tiebreak_strings(
    uint32_t* perm,
    Py_ssize_t n,
    const uint64_t* prefix_keys,
    const char* str_data,
    const int32_t* str_offsets,
    bint ascending,
) noexcept nogil:
    """
    After a prefix radix sort, stable-sort any sub-run whose strings share
    the same 7-byte prefix AND contains at least one string longer than 7
    bytes (shorter strings are fully resolved by the prefix key).
    """
    cdef Py_ssize_t lo = 0, hi, i
    cdef uint64_t cur_key
    cdef bint needs_tiebreak
    cdef uint32_t row_idx

    while lo < n:
        cur_key = prefix_keys[perm[lo]]
        hi = lo + 1
        while hi < n and prefix_keys[perm[hi]] == cur_key:
            hi += 1

        if hi > lo + 1:
            needs_tiebreak = False
            for i in range(lo, hi):
                row_idx = perm[i]
                if str_offsets[row_idx + 1] - str_offsets[row_idx] > 7:
                    needs_tiebreak = True
                    break
            if needs_tiebreak:
                _do_tiebreak_sort(
                    perm + lo, perm + hi, str_data, str_offsets, ascending
                )

        lo = hi


# ── Key transform helpers ─────────────────────────────────────────────────────
#
# compress_into returns signed int64.  To radix-sort correctly:
#   ASC:  flip sign bit only          key ^ 0x8000_0000_0000_0000
#         → INT64_MIN (nulls) becomes 0 (smallest unsigned) — NULLS FIRST
#         → negatives sort before non-negatives
#   DESC: flip sign bit then all bits  key ^ 0x7FFF_FFFF_FFFF_FFFF
#         → same effect but inverted order

cdef inline uint64_t _asc_xor  = <uint64_t>0x8000000000000000ULL
cdef inline uint64_t _desc_xor = <uint64_t>0x7FFFFFFFFFFFFFFFULL


# ── Public API ────────────────────────────────────────────────────────────────

def morsel_sort(morsel, list column_names, list ascending):
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
    array('I')
        uint32 permutation: result[i] is the original row index for sorted
        position i.  Apply with ``morsel.take(perm)``.
    """
    if len(column_names) != len(ascending):
        raise ValueError("column_names and ascending must have the same length")
    if not column_names:
        raise ValueError("at least one sort column is required")

    cdef Py_ssize_t n = len(morsel)
    if n == 0:
        return array("I")

    cdef uint32_t* perm_buf = <uint32_t*> PyMem_Malloc(n * sizeof(uint32_t))
    cdef uint32_t* tmp_buf  = <uint32_t*> PyMem_Malloc(n * sizeof(uint32_t))
    if perm_buf == NULL or tmp_buf == NULL:
        PyMem_Free(perm_buf)
        PyMem_Free(tmp_buf)
        raise MemoryError()

    cdef Py_ssize_t i
    for i in range(n):
        perm_buf[i] = <uint32_t>i

    cdef uint64_t* keys = NULL
    cdef int64_t[::1] signed_mv
    cdef DictAccessor* acc
    cdef StringVector sv
    cdef DrakenVarBuffer* sv_ptr
    cdef uint64_t key_xor
    cdef bint asc
    cdef int n_passes

    try:
        # LSD: iterate columns from least-significant to most-significant.
        for col_idx in range(len(column_names) - 1, -1, -1):
            col_name = column_names[col_idx]
            asc = bool(ascending[col_idx])
            key_xor = _asc_xor if asc else _desc_xor

            vec = morsel.column(col_name)

            keys = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
            if keys == NULL:
                raise MemoryError()

            acc = _vector_dict_accessor(vec)

            if acc != NULL:
                # ── Dictionary-encoded: sort on raw codes ────────────────────
                # code_width is 1, 2, or 4 bytes → same number of radix passes.
                n_passes = acc.code_width
                if asc:
                    for i in range(n):
                        keys[i] = <uint64_t>_dict_read_code(acc, i)
                else:
                    for i in range(n):
                        keys[i] = <uint64_t>_dict_read_code(acc, i) ^ <uint64_t>0xFFFFFFFF
                _radix_sort(perm_buf, tmp_buf, keys, n, n_passes)

            elif isinstance(vec, StringVector):
                # ── String column ────────────────────────────────────────────
                # compress_into packs the first 7 bytes as a big-endian int64.
                # Constant StringVectors (all rows identical) need no tiebreak;
                # dense StringVectors need a memcmp pass for strings > 7 bytes.
                sv = <StringVector>vec
                signed_mv = sv.compress()
                for i in range(n):
                    keys[i] = <uint64_t>signed_mv[i] ^ key_xor
                _radix_sort(perm_buf, tmp_buf, keys, n, 8)

                if not sv._has_const:
                    sv_ptr = sv.ptr
                    _tiebreak_strings(
                        perm_buf, n, keys,
                        <const char*>sv_ptr.data,
                        sv_ptr.offsets,
                        asc,
                    )

            else:
                # ── Numeric / timestamp / date / bool / other ────────────────
                # compress_into returns a sortable signed int64 for all these.
                signed_mv = vec.compress()
                for i in range(n):
                    keys[i] = <uint64_t>signed_mv[i] ^ key_xor
                _radix_sort(perm_buf, tmp_buf, keys, n, 8)

            PyMem_Free(keys)
            keys = NULL

        # Copy perm into a Python array and return.
        result = array("I", bytes(n * sizeof(uint32_t)))
        cdef unsigned int[::1] rv = result
        memcpy(&rv[0], perm_buf, n * sizeof(uint32_t))
        return result

    finally:
        PyMem_Free(perm_buf)
        PyMem_Free(tmp_buf)
        PyMem_Free(keys)   # NULL-safe; guards against early MemoryError exit
