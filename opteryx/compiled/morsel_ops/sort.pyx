# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# distutils: sources = src/cpp/simd_remap.cpp src/cpp/cpu_features.cpp
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

from draken.core.buffers cimport (
    DictAccessor,
    DrakenVarBuffer,
    DrakenType,
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DRAKEN_DATE32,
    DRAKEN_TIMESTAMP64,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
)
from draken.morsels.morsel cimport Morsel
from draken.vectors.string_vector cimport StringVector
from draken.vectors.vector cimport Vector


# ── Inline helpers (replaces phantom vector_readers cimport) ──────────────────

cdef inline DictAccessor* _vector_dict_accessor(object vec) noexcept:
    """Return the DictAccessor* for a dictionary-encoded Vector, or NULL."""
    return (<Vector>vec).dict_accessor()


cdef inline uint32_t _dict_read_code(const DictAccessor* acc, Py_ssize_t i) noexcept nogil:
    """Read the dictionary code at row i, handling 1/2/4-byte code widths."""
    if acc.code_width == 1:
        return (<uint8_t*>acc.codes)[i]
    if acc.code_width == 2:
        return (<uint16_t*>acc.codes)[i]
    return (<uint32_t*>acc.codes)[i]


# ── Inline C++: std::sort helpers ────────────────────────────────────────────

cdef extern from * nogil:
    """
    #include <algorithm>
    #include <cstdint>
    #include <cstring>

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
    For DRAKEN_STRING, the caller must handle string comparison separately.
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
    const DictAccessor* acc,
) noexcept:
    """
    Build a remap table for a numeric-valued dictionary column.

    remap[old_code] = rank   where rank 0 is the semantically smallest value.
    The caller inverts order for descending sort by XOR-ing keys with the
    all-ones mask for the code width.

    Returns a heap-allocated uint32[D] array, or NULL on malloc failure.
    The caller is responsible for freeing it with PyMem_Free.
    """
    cdef DrakenVarBuffer* dv = acc.dict_values
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
        sort_keys[i] = _dict_value_as_int64(dv, acc.value_type, <uint32_t>i)
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
    const DictAccessor* acc,
) noexcept:
    """
    Build a remap table for a string-valued dictionary column.

    Sorts D strings lexicographically ascending using memcmp; assigns rank 0
    to the lexicographically smallest string.  The caller handles descending
    sort by XOR-ing keys with the all-ones mask for the code width.

    Returns a heap-allocated uint32[D] array, or NULL on malloc failure.
    """
    cdef DrakenVarBuffer* dv = acc.dict_values
    if dv == NULL or dv.offsets == NULL:
        return NULL

    cdef Py_ssize_t D = <Py_ssize_t>dv.length
    if D == 0:
        return NULL

    cdef const char* data = <const char*>dv.data
    cdef int32_t* offsets = dv.offsets
    cdef uint32_t* order = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))
    cdef uint32_t* remap = <uint32_t*>PyMem_Malloc(D * sizeof(uint32_t))

    if order == NULL or remap == NULL:
        PyMem_Free(order)
        PyMem_Free(remap)
        return NULL

    cdef Py_ssize_t i
    for i in range(D):
        order[i] = <uint32_t>i

    # O(D log D) lexicographic sort via std::sort — correct for all sizes.
    _sort_string_remap(order, <uint32_t>D, data, offsets)

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
    cdef DictAccessor* acc
    cdef StringVector sv
    cdef DrakenVarBuffer* sv_ptr
    cdef uint64_t key_xor
    cdef bint asc
    cdef int n_passes
    cdef uint32_t* remap = NULL
    cdef uint64_t flip
    cdef int[::1] rv

    try:
        # LSD: iterate columns from least-significant to most-significant.
        for col_idx in range(len(column_names) - 1, -1, -1):
            col_name = column_names[col_idx]
            asc = bool(ascending[col_idx])
            key_xor = _asc_xor if asc else _desc_xor

            vec = morsel.column(col_name)

            acc = _vector_dict_accessor(vec)

            if acc != NULL:
                # ── Dictionary-encoded: semantic ORDER BY via code remap ──────
                # Build a remap[old_code] = semantic_rank table (O(D log D)),
                # then radix-sort on ranks.  This is ORDER BY-correct unlike
                # raw code order which is insertion-ordered.
                n_passes = acc.code_width

                if acc.value_type == DRAKEN_STRING:
                    remap = _build_string_dict_remap(acc)
                else:
                    remap = _build_numeric_dict_remap(acc)

                if remap != NULL:
                    # Fill uint64 keys from remapped ranks, then radix sort.
                    # Remap table fits in L1/L2 (≤256 entries for u8 codes,
                    # ≤65536 for u16).  Release GIL: only raw C pointer work.
                    if n_passes == 1:
                        with nogil:
                            for i in range(n):
                                keys[i] = <uint64_t>remap[(<uint8_t*>acc.codes)[i]]
                            if not asc:
                                flip = (<uint64_t>1 << (8 * n_passes)) - 1
                                for i in range(n):
                                    keys[i] ^= flip
                            _radix_sort(perm_buf, tmp_buf, keys, n, n_passes)
                    elif n_passes == 2:
                        with nogil:
                            for i in range(n):
                                keys[i] = <uint64_t>remap[(<uint16_t*>acc.codes)[i]]
                            if not asc:
                                flip = (<uint64_t>1 << (8 * n_passes)) - 1
                                for i in range(n):
                                    keys[i] ^= flip
                            _radix_sort(perm_buf, tmp_buf, keys, n, n_passes)
                    else:  # n_passes == 4
                        with nogil:
                            for i in range(n):
                                keys[i] = <uint64_t>remap[(<uint32_t*>acc.codes)[i]]
                            if not asc:
                                flip = (<uint64_t>1 << (8 * n_passes)) - 1
                                for i in range(n):
                                    keys[i] ^= flip
                            _radix_sort(perm_buf, tmp_buf, keys, n, n_passes)

                    PyMem_Free(remap)
                    remap = NULL
                else:
                    # Remap build failed (malloc); fall back to raw code order.
                    # This is GROUP BY-correct but not ORDER BY-correct.
                    if asc:
                        for i in range(n):
                            keys[i] = <uint64_t>_dict_read_code(acc, i)
                    else:
                        for i in range(n):
                            keys[i] = <uint64_t>_dict_read_code(acc, i) ^ <uint64_t>0xFFFFFFFF
                    _radix_sort(perm_buf, tmp_buf, keys, n, n_passes)

            elif isinstance(vec, StringVector):
                # ── String column ────────────────────────────────────────────
                # std::sort with memcmp: correct for all byte values including
                # multibyte UTF-8. The previous prefix-radix approach cast the
                # 7-byte prefix to signed int64, which caused strings with
                # leading bytes >= 0x80 (Cyrillic, CJK, etc.) to sort before
                # ASCII — a correctness bug.
                sv = <StringVector>vec
                if not sv._has_const:
                    sv_ptr = sv.ptr
                    with nogil:
                        _sort_strings(
                            perm_buf, <uint32_t>n,
                            <const char*>sv_ptr.data,
                            sv_ptr.offsets,
                            asc,
                        )

            else:
                # ── Numeric / timestamp / date / bool / other ────────────────
                # compress_into returns a sortable signed int64 for all these.
                signed_mv = vec.compress()
                with nogil:
                    for i in range(n):
                        keys[i] = <uint64_t>signed_mv[i] ^ key_xor
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
