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

"""
StringVector: Cython implementation of a variable-width byte column for Draken.

This module provides:
- The StringVector class for efficient byte/variable-length storage
- Integration with DrakenVarBuffer and helpers for memory management
- Arrow interoperability (zero-copy wrapping)
- Fast equality, null handling, and hashing
"""

from cpython.buffer cimport PyBUF_READ
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int32_t, intptr_t, uint8_t, uint64_t, int64_t, uint32_t, uint16_t, uintptr_t
from libc.string cimport memcpy, memset, memcmp
from libc.stdlib cimport malloc, realloc, free

from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenConstantStringPayload
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant, draken_vector_from_dict
from draken.core.buffers cimport DrakenStringArena
from draken.core.buffers cimport DrakenStringSlot
from draken.core.buffers cimport STR_INLINE_MAX
from draken.core.buffers cimport str_data
from draken.core.buffers cimport str_is_inline
from draken.core.buffers cimport str_length
from draken.core.buffers cimport str_equals
from draken.core.buffers cimport str_compare
from draken.core.buffers cimport str_init_inline
from draken.core.buffers cimport str_init_extern
from draken.core.buffers cimport str_init_null
from draken.core.var_vector cimport alloc_var_buffer, buf_dtype, free_var_buffer
from draken.core.string_arena cimport alloc_string_arena
from draken.core.string_arena cimport free_string_arena
from draken.vectors.array_vector cimport ArrayVector, DrakenArrayBuffer

cdef extern from "xxhash.h":
    uint64_t XXH3_64bits(const void* input, size_t length) nogil

cdef extern from "simd_string_ops.h":
    void simd_to_upper(char* data, size_t length)
    void simd_to_lower(char* data, size_t length)

cdef extern from "<vector>" namespace "std":
    cdef cppclass vector[T]:
        size_t size()
        T& operator[](size_t)

cdef extern from "simd_search.h":
    vector[size_t] simd_find_all(const char* data, size_t length, char target)

cdef extern from "volnitsky.h":
    cppclass VolnitskyTable:
        pass
    VolnitskyTable* volnitsky_alloc() noexcept nogil
    void volnitsky_free(VolnitskyTable* t) noexcept nogil
    void volnitsky_build(VolnitskyTable* t, const uint8_t* pat, size_t len) nogil
    bint volnitsky_contains_cs(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil
    bint volnitsky_contains_ci(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat_lower, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil

cdef extern from *:
    """
    #if defined(__GNUC__) || defined(__clang__)
    #define BSWAP64(x) __builtin_bswap64(x)
    #elif defined(_MSC_VER)
    #include <intrin.h>
    #define BSWAP64(x) _byteswap_uint64(x)
    #else
    static inline uint64_t BSWAP64(uint64_t x) {
        x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
        x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
        return (x << 32) | (x >> 32);
    }
    #endif
    """
    uint64_t BSWAP64(uint64_t x) nogil

from draken.vectors.vector cimport (
    MIX_HASH_CONSTANT,
    NULL_HASH,
    Vector,
    mix_hash,
    simd_mix_hash,
    simd_mix_hash_from_dict_cw4,
    simd_mix_hash_from_dict_nullable_cw4,
    simd_popcount,
)
from draken.vectors.bool_vector cimport BoolVector

DEF STRING_HASH_CHUNK = 256


# Phase 4 helper: typed accessor for the StringArena under _unified_view.data.
# DrakenVector.data is void* (heterogeneous across vector types). For string
# columns it is always a DrakenStringArena*. Wrapping the cast here means every
# internal reader uses the same pattern and the cast lives in exactly one
# place. External readers in vector_ops/ and operators/ still cast inline —
# this helper is intentionally module-private (StringVector-internal only).
cdef inline DrakenStringArena* _string_arena(StringVector vec) noexcept nogil:
    return <DrakenStringArena*>vec._unified_view.data


# _ConstView and _const_view are declared/defined in string_vector.pxd so they
# can be cimported by external modules (vector_ops/ readers, operators/, etc.).


# Phase 5: allocate a 1-slot DrakenStringArena holding a single constant value.
# Caller owns the returned arena (must free via free_string_arena, typically
# by transferring ownership to a StringVector via _owns_dict_arena).
# Phase 6 helper: build a 1-slot-per-row DrakenStringArena from a DrakenVarBuffer.
# Used at every dense-construction site to produce dual-alive vectors (both
# VarBuffer-style ptr.data/offsets AND a StringArena under _unified_view.data)
# during the migration. Phase 7 will free the VarBuffer half.
cdef DrakenStringArena* _varbuffer_to_string_arena(
    const uint8_t* src_data, const int32_t* src_offsets, const uint8_t* src_nulls,
    Py_ssize_t row_count,
) except NULL:
    cdef Py_ssize_t i
    cdef Py_ssize_t off
    cdef Py_ssize_t slen
    cdef Py_ssize_t total_extern_bytes = 0
    cdef bint row_is_null

    for i in range(row_count):
        slen = src_offsets[i + 1] - src_offsets[i]
        if slen > STR_INLINE_MAX:
            total_extern_bytes += slen

    cdef DrakenStringArena* arena = alloc_string_arena(
        DRAKEN_STRING, <size_t>row_count, <size_t>total_extern_bytes,
    )
    if arena == NULL:
        raise MemoryError()

    cdef DrakenStringSlot* slots = arena.slots
    cdef uint8_t* arena_bytes = arena.arena
    cdef size_t arena_used = 0

    for i in range(row_count):
        off = src_offsets[i]
        slen = src_offsets[i + 1] - off
        row_is_null = (
            src_nulls != NULL
            and ((src_nulls[i >> 3] >> (i & 7)) & 1) == 0
        )
        if row_is_null:
            str_init_null(&slots[i])
        elif slen <= STR_INLINE_MAX:
            str_init_inline(&slots[i], src_data + off, <uint32_t>slen)
        else:
            memcpy(arena_bytes + arena_used, src_data + off, <size_t>slen)
            str_init_extern(
                &slots[i], src_data + off, <uint32_t>slen,
                <uint64_t>arena_used,
            )
            arena_used += <size_t>slen
    arena.arena_used = arena_used
    arena.length = <size_t>row_count
    return arena


cdef inline DrakenStringArena* _alloc_constant_string_arena(
    const uint8_t* src, Py_ssize_t length,
) except NULL:
    cdef Py_ssize_t arena_cap = length if length > STR_INLINE_MAX else 0
    cdef DrakenStringArena* arena = alloc_string_arena(
        DRAKEN_STRING, 1, <size_t>arena_cap,
    )
    if arena == NULL:
        raise MemoryError()
    if length <= STR_INLINE_MAX:
        str_init_inline(&arena.slots[0], src, <uint32_t>length)
    else:
        memcpy(arena.arena, src, <size_t>length)
        str_init_extern(&arena.slots[0], src, <uint32_t>length, 0)
        arena.arena_used = <size_t>length
    arena.length = 1
    return arena


cdef inline object _coerce_literal_bytes(object literal):
    if literal is None:
        return None
    if hasattr(literal, "as_py"):
        try:
            literal = literal.as_py()
        except Exception:
            return None
    if isinstance(literal, (bytes, bytearray, memoryview)):
        try:
            return bytes(literal)
        except Exception:
            return None
    if isinstance(literal, str):
        try:
            return literal.encode("utf8")
        except Exception:
            return None
    return None


cdef inline int _compare_bytes_lex(
    const uint8_t* left,
    Py_ssize_t left_len,
    const uint8_t* right,
    Py_ssize_t right_len,
) noexcept nogil:
    cdef Py_ssize_t min_len = left_len if left_len < right_len else right_len
    cdef int cmp_res = 0

    if min_len > 0:
        cmp_res = memcmp(left, right, <size_t>min_len)
    if cmp_res < 0:
        return -1
    if cmp_res > 0:
        return 1
    if left_len < right_len:
        return -1
    if left_len > right_len:
        return 1
    return 0


cdef BoolVector _constant_bool_result(Py_ssize_t n, bint matched, bint is_null) except *:
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask

    if nbytes > 0:
        memset(dst, 0, nbytes)

    if is_null:
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL
        return out

    if matched and nbytes > 0:
        memset(dst, 0xFF, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            dst[nbytes - 1] &= mask

    out.ptr.null_bitmap = NULL
    return out


cdef uint8_t _CONST_NULL_BYTE = 0


cdef void _populate_dense_min_max(StringVector vec) except *:
    """Scan a dense StringVector once and populate min/max metadata.

    Track A: invoked at construction time (builder.finish, parquet readers,
    dict-materialization output) so subsequent min()/max() calls are O(1).

    Tie-breaking follows the existing min()/max() semantics:
      - min: equal common prefix → shorter string is lex-smaller
      - max: equal common prefix → longer string is lex-larger

    Caller must guarantee vec has a populated DrakenStringArena (_owns_dict_arena).
    For constant/dict vectors, use the dedicated populators.
    """
    cdef DrakenStringArena* arena = <DrakenStringArena*>vec._unified_view.data
    if arena == NULL:
        return
    cdef Py_ssize_t n = <Py_ssize_t>arena.length
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef const uint8_t* cur_data
    cdef Py_ssize_t cur_len
    cdef Py_ssize_t min_idx = -1, max_idx = -1
    cdef const uint8_t* min_data = NULL
    cdef Py_ssize_t min_len = 0
    cdef const uint8_t* max_data = NULL
    cdef Py_ssize_t max_len = 0
    cdef int32_t best_len, common_len
    cdef int cmp
    cdef uint8_t byte, bit
    cdef uint8_t* nulls = vec.ptr.null_bitmap

    for i in range(n):
        if nulls != NULL:
            byte = nulls[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                continue
        slot = &arena.slots[i]
        cur_data = str_data(slot, arena.arena)
        cur_len = <Py_ssize_t>str_length(slot)

        if min_idx == -1:
            min_idx = i
            min_data = cur_data
            min_len = cur_len
            max_idx = i
            max_data = cur_data
            max_len = cur_len
            continue

        # Update min
        common_len = <int32_t>(cur_len if cur_len < min_len else min_len)
        cmp = memcmp(cur_data, min_data, common_len)
        if cmp == 0 and cur_len < min_len:
            cmp = -1
        if cmp < 0:
            min_idx = i
            min_data = cur_data
            min_len = cur_len

        # Update max
        common_len = <int32_t>(cur_len if cur_len < max_len else max_len)
        cmp = memcmp(cur_data, max_data, common_len)
        if cmp == 0 and cur_len > max_len:
            cmp = 1
        if cmp > 0:
            max_idx = i
            max_data = cur_data
            max_len = cur_len

    if min_idx == -1:
        # All-null or empty vector.
        vec._cached_min_ptr = NULL
        vec._cached_min_len = 0
        vec._cached_max_ptr = NULL
        vec._cached_max_len = 0
        vec._min_max_all_null = True
    else:
        vec._cached_min_ptr = min_data
        vec._cached_min_len = min_len
        vec._cached_max_ptr = max_data
        vec._cached_max_len = max_len
        vec._min_max_all_null = False
    vec._min_max_valid = True


cdef void _populate_dict_min_max(StringVector vec) except *:
    """Compute min/max from the dictionary entries of a dict-encoded vector.

    Track A: dict size is bounded (typically << row count) so this is much
    cheaper than scanning rows. Only entries that are actually referenced
    by codes would be strictly correct, but in practice Parquet dictionaries
    contain only referenced entries, so we scan all dict entries.

    If any row is null (validity bitmap set), this is unobservable in min/max
    semantics: we ignore nulls.
    """
    if vec._unified_view.data_length >= vec._unified_view.length:
        return
    cdef DrakenStringArena* gdv = _string_arena(vec)
    cdef Py_ssize_t n = <Py_ssize_t>gdv.length
    # If the entire vector is all-null (no live codes), we'd produce a wrong
    # answer. Detect: scan validity bitmap of the unified view. If every row
    # is null, set both to None. This is rare; skip the metadata when present.
    cdef Py_ssize_t logical_len = <Py_ssize_t>vec._unified_view.length
    cdef uint8_t* row_nulls = vec._unified_view.validity
    cdef Py_ssize_t live_rows = logical_len
    cdef Py_ssize_t i
    cdef uint8_t byte, bit
    if row_nulls != NULL:
        live_rows = 0
        for i in range(logical_len):
            byte = row_nulls[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if bit:
                live_rows += 1
                break  # at least one non-null is enough
    if live_rows == 0 and logical_len > 0:
        vec._cached_min_ptr = NULL
        vec._cached_min_len = 0
        vec._cached_max_ptr = NULL
        vec._cached_max_len = 0
        vec._min_max_all_null = True
        vec._min_max_valid = True
        return

    cdef DrakenStringSlot* slot
    cdef DrakenStringSlot* min_slot = NULL
    cdef DrakenStringSlot* max_slot = NULL
    cdef uint8_t* dict_nulls = gdv.null_bitmap

    for i in range(n):
        if dict_nulls != NULL:
            byte = dict_nulls[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                continue
        slot = &gdv.slots[i]
        if min_slot == NULL:
            min_slot = slot
            max_slot = slot
            continue
        if str_compare(slot, gdv.arena, min_slot, gdv.arena) < 0:
            min_slot = slot
        if str_compare(slot, gdv.arena, max_slot, gdv.arena) > 0:
            max_slot = slot

    if min_slot == NULL:
        vec._cached_min_ptr = NULL
        vec._cached_min_len = 0
        vec._cached_max_ptr = NULL
        vec._cached_max_len = 0
        vec._min_max_all_null = True
    else:
        vec._cached_min_ptr = str_data(min_slot, gdv.arena)
        vec._cached_min_len = <Py_ssize_t>str_length(min_slot)
        vec._cached_max_ptr = str_data(max_slot, gdv.arena)
        vec._cached_max_len = <Py_ssize_t>str_length(max_slot)
        vec._min_max_all_null = False
    vec._min_max_valid = True


cdef void _release_dict_storage(StringVector vec) noexcept:
    """Free dict-encoded storage. Codes and arena have independent ownership.

    Views and slices can own one without the other (e.g. a take() that
    allocates fresh codes while borrowing the parent's arena).
    """
    if vec._owns_codes and vec._unified_view.selection != NULL:
        free(<void*>vec._unified_view.selection)
        vec._unified_view.selection = NULL
    vec._owns_codes = False
    if vec._unified_view.data != NULL:
        free_string_arena(_string_arena(vec))
        vec._unified_view.data = NULL
    if vec._dict_code_counts != NULL:
        free(vec._dict_code_counts)
        vec._dict_code_counts = NULL
    vec._dict_code_counts_valid = False



cdef void _attach_dictionary_storage_from_buffers(
    StringVector vec,
    const int32_t[::1] codes,
    const int32_t[::1] dict_offsets,
    const int32_t[::1] dict_lengths,
    const uint8_t[::1] arena_bytes,
    bint ordered,
    const uint8_t* dict_entry_null_bitmap=NULL,
) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dict_lengths.shape[0]
    cdef Py_ssize_t code_bytes = row_count * sizeof(uint32_t)
    cdef Py_ssize_t bitmap_bytes
    cdef Py_ssize_t arena_size = arena_bytes.shape[0]
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t entry_start, entry_len
    cdef DrakenStringArena* german_dict
    cdef uint32_t* codes_ptr

    _release_dict_storage(vec)

    if code_bytes > 0:
        codes_ptr = <uint32_t*>malloc(code_bytes)
        if codes_ptr == NULL:
            raise MemoryError()
    else:
        codes_ptr = NULL

    # Build German-string arena for the dictionary values
    german_dict = alloc_string_arena(DRAKEN_STRING, <size_t>dict_size, <size_t>arena_size)
    if german_dict == NULL:
        raise MemoryError()
    for i in range(dict_size):
        entry_start = <Py_ssize_t>dict_offsets[i]
        entry_len = <Py_ssize_t>dict_lengths[i]
        if entry_len <= STR_INLINE_MAX:
            str_init_inline(
                &german_dict.slots[i],
                &arena_bytes[entry_start] if entry_len > 0 else NULL,
                <uint32_t>entry_len,
            )
        else:
            memcpy(german_dict.arena + german_dict.arena_used,
                   &arena_bytes[entry_start], <size_t>entry_len)
            str_init_extern(
                &german_dict.slots[i],
                &arena_bytes[entry_start],
                <uint32_t>entry_len,
                <uint64_t>german_dict.arena_used,
            )
            german_dict.arena_used += <size_t>entry_len

    if dict_entry_null_bitmap != NULL:
        bitmap_bytes = (dict_size + 7) >> 3
        german_dict.null_bitmap = <uint8_t*>malloc(<size_t>bitmap_bytes)
        if german_dict.null_bitmap == NULL:
            raise MemoryError()
        memcpy(german_dict.null_bitmap, dict_entry_null_bitmap, <size_t>bitmap_bytes)

    for i in range(row_count):
        codes_ptr[i] = <uint32_t>codes[i]

    vec._owns_codes = (codes_ptr != NULL)

    vec._unified_view = draken_vector_from_dict(
        <void*>german_dict, <uint32_t>dict_size,
        codes_ptr, <uint32_t>row_count,
        DRAKEN_STRING, vec.ptr.null_bitmap,
    )


cdef inline uint64_t _load_le_u64_partial(const uint8_t* ptr, size_t n) nogil:
    cdef uint64_t value = 0
    cdef size_t i
    for i in range(n):
        value |= (<uint64_t>ptr[i]) << (i * 8)
    return value


cdef inline uint64_t _load_u64(const uint8_t* ptr) noexcept nogil:
    cdef uint64_t value
    memcpy(&value, ptr, 8)
    return value


cdef inline uint64_t _short_string_hash(const uint8_t* ptr, size_t n) nogil:
    cdef uint64_t h, w0, w1, w2, w3

    if n <= 8:
        return mix_hash(<uint64_t>n, _load_le_u64_partial(ptr, n))
    if n <= 16:
        w0 = _load_u64(ptr)
        w1 = _load_u64(ptr + n - 8)
        h = mix_hash(<uint64_t>n, w0)
        return mix_hash(h, w1)
    if n <= 24:
        w0 = _load_u64(ptr)
        w1 = _load_u64(ptr + 8)
        w2 = _load_u64(ptr + n - 8)
        h = mix_hash(<uint64_t>n, w0)
        h = mix_hash(h, w1)
        return mix_hash(h, w2)
    w0 = _load_u64(ptr)
    w1 = _load_u64(ptr + 8)
    w2 = _load_u64(ptr + 16)
    w3 = _load_u64(ptr + n - 8)
    h = mix_hash(<uint64_t>n, w0)
    h = mix_hash(h, w1)
    h = mix_hash(h, w2)
    return mix_hash(h, w3)


# ---------------------------------------------------------------------------
# Scalar-comparison helpers (nogil, used by StringVector kernels)
# ---------------------------------------------------------------------------

cdef inline uint8_t _sv_ascii_lower(uint8_t b) noexcept nogil:
    # Unsigned arithmetic: for b < 65, (b - 65) wraps around to a large number > 25.
    # For 65 <= b <= 90, (b - 65) is 0..25.
    return b + (32 * ((b - 65U) <= 25U))


cdef inline bint _sv_byte_equals(uint8_t left, uint8_t right, bint ignore_case) noexcept nogil:
    if ignore_case:
        return _sv_ascii_lower(left) == _sv_ascii_lower(right)
    return left == right


cdef bint _sv_sql_like_match(
    const uint8_t* text,
    Py_ssize_t text_len,
    const uint8_t* pattern,
    Py_ssize_t pattern_len,
    bint ignore_case,
) noexcept nogil:
    """SQL LIKE matcher supporting % and _ wildcards and backslash escaping."""
    cdef Py_ssize_t ti = 0
    cdef Py_ssize_t pi = 0
    cdef Py_ssize_t last_pct = -1
    cdef Py_ssize_t last_match = 0
    cdef uint8_t pc

    while ti < text_len:
        if pi < pattern_len:
            pc = pattern[pi]
            if pc == 92 and (pi + 1) < pattern_len:  # backslash escape
                if _sv_byte_equals(text[ti], pattern[pi + 1], ignore_case):
                    ti += 1
                    pi += 2
                    continue
            elif pc == 95:  # "_" wildcard
                ti += 1
                pi += 1
                continue
            elif pc == 37:  # "%" wildcard
                last_pct = pi
                pi += 1
                last_match = ti
                continue
            elif _sv_byte_equals(text[ti], pc, ignore_case):
                ti += 1
                pi += 1
                continue

        if last_pct != -1:
            last_match += 1
            ti = last_match
            pi = last_pct + 1
            continue
        return False

    while pi < pattern_len and pattern[pi] == 37:
        pi += 1

    return pi == pattern_len


cdef bint _sv_contains_cs(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    return volnitsky_contains_cs(haystack, <size_t>hay_len, needle, <size_t>ndl_len, tbl)


cdef bint _sv_contains_ci(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle_lower,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    return volnitsky_contains_ci(haystack, <size_t>hay_len, needle_lower, <size_t>ndl_len, tbl)


cdef class StringVector(Vector):
    # Re-Cythonize this implementation when the pxd layout changes.

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, list):
            dictionary = list(dictionary)

        codes_view = codes
        if row_validity is None:
            return from_dict(codes_view, dictionary)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary, validity_view)

    @classmethod
    def from_dict_buffers(cls, codes, dict_offsets, dict_lengths, arena_bytes, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef int32_t[::1] offsets_view
        cdef int32_t[::1] lengths_view
        cdef uint8_t[::1] arena_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dict_offsets, memoryview):
            dict_offsets = pyarray("i", dict_offsets)
        if not isinstance(dict_lengths, memoryview):
            dict_lengths = pyarray("i", dict_lengths)
        if not isinstance(arena_bytes, memoryview):
            arena_bytes = bytearray(arena_bytes)

        codes_view = codes
        offsets_view = dict_offsets
        lengths_view = dict_lengths
        arena_view = arena_bytes

        if row_validity is None:
            return from_dict_buffers(codes_view, offsets_view, lengths_view, arena_view)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_buffers(codes_view, offsets_view, lengths_view, arena_view, validity_view)

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        """Construct a constant StringVector: one value broadcast to `length` rows.

        Storage: a 1-slot DrakenStringArena under _unified_view.data, with
        selection = draken_zero_sel(length) (set by draken_vector_from_constant).
        Every row index resolves to slot 0 through the selection lookup.
        """
        cdef StringVector vec
        cdef DrakenStringArena* arena
        cdef DrakenStringSlot* slot
        cdef bytes value_bytes
        cdef const char* src
        cdef Py_ssize_t src_len = 0

        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")

        vec = StringVector(0, 0, True)
        vec.ptr = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
        if vec.ptr == NULL:
            raise MemoryError()
        vec.owns_data = False
        vec.ptr.data = NULL
        vec.ptr.offsets = NULL
        vec.ptr.null_bitmap = NULL
        vec.ptr.length = <size_t>length
        vec.ptr.type = DRAKEN_STRING

        if is_null:
            arena = _alloc_constant_string_arena(NULL, 0)
        else:
            value_bytes = _coerce_literal_bytes(value)
            if value_bytes is None:
                raise TypeError("StringVector.from_constant expects bytes-like or str value")
            src_len = len(value_bytes)
            src = PyBytes_AS_STRING(value_bytes) if src_len > 0 else NULL
            arena = _alloc_constant_string_arena(<const uint8_t*>src, src_len)


        vec._unified_view = draken_vector_from_constant(
            <void*>arena, <uint32_t>length, DRAKEN_STRING,
            &_CONST_NULL_BYTE if is_null else NULL,
        )
        # Track A: constant vector — min == max == the slot's bytes.
        # The slot's data pointer is stable for the vector's lifetime (arena
        # is freed when the vector is dealloc'd via _owns_dict_arena).
        slot = &arena.slots[0]
        if is_null or length == 0 or src_len == 0:
            vec._cached_min_ptr = NULL
            vec._cached_min_len = 0
            vec._cached_max_ptr = NULL
            vec._cached_max_len = 0
            vec._min_max_all_null = is_null or length == 0
        else:
            vec._cached_min_ptr = str_data(slot, arena.arena)
            vec._cached_min_len = <Py_ssize_t>str_length(slot)
            vec._cached_max_ptr = vec._cached_min_ptr
            vec._cached_max_len = vec._cached_min_len
            vec._min_max_all_null = False
        vec._min_max_valid = True
        return vec

    def __cinit__(self, size_t length=0, size_t bytes_cap=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> no allocation; caller will set ptr
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_var_buffer(DRAKEN_STRING, length, bytes_cap)
            self.owns_data = True
        self._owns_codes = False
        self._dict_code_counts = NULL
        self._dict_code_counts_valid = False
        self._cached_min_ptr = NULL
        self._cached_min_len = 0
        self._cached_max_ptr = NULL
        self._cached_max_len = 0
        self._min_max_all_null = False
        self._min_max_valid = False
        if wrap:
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_STRING, NULL)
        else:
            self._unified_view = draken_vector_from_dense(
                NULL, <uint32_t>self.ptr.length, DRAKEN_STRING, NULL)

    def __dealloc__(self):
        # _release_dict_storage handles both the selection codes and the
        # DrakenStringArena under _unified_view.data, gated on the two
        # ownership flags. After Phase 5 the constant case also uses a
        # DrakenStringArena (1-slot, _owns_dict_arena=True), so the same
        # release path covers it — no separate constant-payload free.
        _release_dict_storage(self)

        if self.ptr != NULL:
            if self.owns_data:
                if self.ptr.data != NULL:
                    free(self.ptr.data)
                if self.ptr.offsets != NULL:
                    free(self.ptr.offsets)
                if self.ptr.null_bitmap != NULL:
                    free(self.ptr.null_bitmap)
            free(self.ptr)
            self.ptr = NULL

    cdef DrakenVector* unified(self) noexcept:
        # Arena invariant: _unified_view.data is always a non-NULL DrakenStringArena*
        # for any StringVector that has escaped a producer (finish(), from_arrow(),
        # from_constant(), from_dict*(), _attach_dictionary_storage_from_buffers()).
        # _unified_view.validity == ptr.null_bitmap for all current producers.
        # Exception: morsel_io.pyx legacy reader sets .data/.data_length but not .validity;
        # that site is tracked for Chunk 5 correction.
        return &self._unified_view

    cdef void _set_null_bitmap(self, uint8_t* bm) noexcept:
        self.ptr.null_bitmap = bm
        self._unified_view.validity = bm

    # ------------------------------------------------------------------
    # Encoded-form accessors (dict and RLE) for aggregation kernels.
    # The caller must check `self._unified_view.data_length < self._unified_view.length` before calling these;
    # they do not validate the encoding.
    # ------------------------------------------------------------------
    cdef Py_ssize_t c_length(self) noexcept nogil:
        if self.ptr == NULL:
            return 0
        return <Py_ssize_t>self.ptr.length

    cdef Py_ssize_t c_dict_size(self) noexcept nogil:
        if self._unified_view.data_length >= self._unified_view.length:
            return 0
        return <Py_ssize_t>self._unified_view.data_length

    cdef uint8_t c_dict_code_width(self) noexcept nogil:
        return 4

    cdef const uint8_t* c_dict_codes_ptr(self) noexcept nogil:
        return <const uint8_t*><const void*>self._unified_view.selection

    cdef const uint8_t* c_dict_value_ptr(
        self, Py_ssize_t i, Py_ssize_t* out_len
    ) noexcept nogil:
        cdef DrakenStringArena* gdv = _string_arena(self)
        cdef DrakenStringSlot* slot
        if gdv == NULL or i < 0 or <size_t>i >= gdv.length:
            out_len[0] = 0
            return NULL
        slot = &gdv.slots[i]
        out_len[0] = <Py_ssize_t>str_length(slot)
        return str_data(slot, gdv.arena)

    cdef bint c_dict_value_is_null(self, Py_ssize_t i) noexcept nogil:
        cdef DrakenStringArena* gdv = _string_arena(self)
        if gdv == NULL or gdv.null_bitmap == NULL:
            return False
        return not ((gdv.null_bitmap[i >> 3] >> (i & 7)) & 1)

    cdef const uint8_t* c_row_null_bitmap(self) noexcept nogil:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

    cdef const int64_t* c_dict_code_counts_ptr(self) except NULL:
        """Return a pointer to a length-`dict_size` int64 array of per-code
        occurrence counts.  Computed once on first access and cached on the
        vector.  Counts only include rows that are *valid* (non-null in the
        row null bitmap)."""
        cdef DrakenStringArena* gdv = _string_arena(self)
        cdef Py_ssize_t dict_size
        cdef Py_ssize_t n
        cdef Py_ssize_t i
        cdef uint32_t code
        cdef const uint32_t* codes
        cdef const uint8_t* row_nulls
        cdef int64_t* counts

        if self._unified_view.data_length >= self._unified_view.length:
            raise ValueError("c_dict_code_counts_ptr: vector is not dict-encoded")

        if self._dict_code_counts_valid and self._dict_code_counts != NULL:
            return self._dict_code_counts

        dict_size = <Py_ssize_t>gdv.length
        # Allocate (calloc) and zero-fill; even for dict_size==0 we keep a
        # 1-byte allocation so the returned pointer is never NULL.
        if self._dict_code_counts != NULL:
            free(self._dict_code_counts)
            self._dict_code_counts = NULL

        counts = <int64_t*>malloc(<size_t>(dict_size if dict_size > 0 else 1) * sizeof(int64_t))
        if counts == NULL:
            raise MemoryError()
        memset(counts, 0, <size_t>(dict_size if dict_size > 0 else 1) * sizeof(int64_t))

        if dict_size > 0 and self.ptr != NULL:
            n = <Py_ssize_t>self.ptr.length
            codes = self._unified_view.selection
            row_nulls = self.ptr.null_bitmap
            with nogil:
                for i in range(n):
                    if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
                        continue
                    code = codes[i]
                    if <Py_ssize_t>code >= dict_size:
                        # Fail fast — this indicates a corrupted dict vector.
                        with gil:
                            free(counts)
                            raise ValueError(
                                f"dictionary index out of bounds at row {i}: {code}"
                            )
                    counts[<Py_ssize_t>code] += 1

        self._dict_code_counts = counts
        self._dict_code_counts_valid = True
        return self._dict_code_counts

    cdef uint64_t c_dict_value_hash(self, Py_ssize_t i) noexcept nogil:
        """Final mixed hash for dict entry i, matching the value c_hash_into
        writes for a row pointing to entry i when the destination is zeroed."""
        cdef DrakenStringArena* gdv = _string_arena(self)
        cdef DrakenStringSlot* slot
        cdef size_t str_len
        cdef const uint8_t* slot_data
        cdef uint64_t per_string_hash
        cdef uint64_t scratch
        if gdv == NULL or i < 0 or <size_t>i >= gdv.length:
            return NULL_HASH
        if gdv.null_bitmap != NULL and not ((gdv.null_bitmap[i >> 3] >> (i & 7)) & 1):
            return NULL_HASH
        slot = &gdv.slots[i]
        str_len = <size_t>str_length(slot)
        slot_data = str_data(slot, gdv.arena)
        if str_len <= 32:
            per_string_hash = _short_string_hash(slot_data, str_len)
        else:
            per_string_hash = XXH3_64bits(<const void*>slot_data, str_len)
        # simd_mix_hash with dst=0: dst[0] = mix(0, per_string_hash).
        scratch = 0
        simd_mix_hash(&scratch, &per_string_hash, 1)
        return scratch

    # Python-callable wrappers around the encoded-form accessors.  Used by
    # tests and any non-hot-path consumers; the cdef methods above remain
    # the supported entry point for nogil kernels.
    def dict_value_at(self, Py_ssize_t i):
        cdef Py_ssize_t length
        cdef const uint8_t* p
        if self._unified_view.data_length >= self._unified_view.length:
            raise ValueError("dict_value_at: vector is not dict-encoded")
        if i < 0 or i >= self.c_dict_size():
            raise IndexError("dict index out of range")
        if self.c_dict_value_is_null(i):
            return None
        p = self.c_dict_value_ptr(i, &length)
        return PyBytes_FromStringAndSize(<const char*>p, length)

    def dict_code_at(self, Py_ssize_t i):
        if self._unified_view.data_length >= self._unified_view.length:
            raise ValueError("dict_code_at: vector is not dict-encoded")
        if i < 0 or i >= <Py_ssize_t>self.ptr.length:
            raise IndexError("row index out of range")
        return <Py_ssize_t>self._unified_view.selection[i]

    def dict_code_counts(self):
        cdef const int64_t* counts
        cdef Py_ssize_t dict_size
        cdef Py_ssize_t i
        if self._unified_view.data_length >= self._unified_view.length:
            raise ValueError("dict_code_counts: vector is not dict-encoded")
        counts = self.c_dict_code_counts_ptr()
        dict_size = self.c_dict_size()
        return [<int>counts[i] for i in range(dict_size)]

    @property
    def length(self):
        """Number of values currently stored in the vector."""
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_value_type(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return None
        return self._unified_view.type

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_size(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return 0
        return self._unified_view.data_length

    # Producer-layer introspection only — not for dispatch.
    @property
    def code_width(self):
        return 4 if self._unified_view.data_length < self._unified_view.length else None

    @property
    def ordered(self):
        return False

    def to_arrow(self):
        """
        Zero-copy conversion to Arrow StringArray (bytes-based).
        Keeps a reference to this vector to prevent premature garbage collection.
        """
        import pyarrow as pa

        cdef DrakenVector* uv = self.unified()
        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().to_arrow()

        if (self.ptr.offsets == NULL
                and self._unified_view.data_length >= self._unified_view.length):
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.binary())
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return pa.array(
                [PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)] * self.ptr.length,
                type=pa.binary(),
            )

        # Dense path: materialize from StringArena into Python bytes list.
        # Arrow zero-copy is not available for arena-encoded vectors.
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenVarBuffer* ptr = self.ptr
        if dense_arena == NULL or ptr == NULL:
            return pa.array([], type=pa.binary())
        cdef size_t n = ptr.length
        cdef uint8_t* nb = ptr.null_bitmap
        result_list = []
        cdef size_t idx
        cdef DrakenStringSlot* slot
        for idx in range(n):
            if nb != NULL and ((nb[idx >> 3] >> (idx & 7)) & 1) == 0:
                result_list.append(None)
            else:
                slot = &dense_arena.slots[idx]
                result_list.append(PyBytes_FromStringAndSize(
                    <char*>str_data(slot, dense_arena.arena),
                    str_length(slot),
                ))
        return pa.array(result_list, type=pa.binary())

    cdef object item_at(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint8_t byte, bit
        cdef _ConstView _cp
        cdef DrakenStringArena* dense_arena
        cdef DrakenStringSlot* dense_slot
        cdef Py_ssize_t start, end

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize()[i]

        if self._unified_view.data_length == 1:
            if i < 0 or i >= ptr.length:
                raise IndexError("Index out of range")
            if uv.validity != NULL:
                return None
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)

        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")

        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None

        # Arena-backed dense
        dense_arena = <DrakenStringArena*>self._unified_view.data
        dense_slot = &dense_arena.slots[i]
        return PyBytes_FromStringAndSize(
            <const char*>str_data(dense_slot, dense_arena.arena),
            <Py_ssize_t>str_length(dense_slot),
        )

    def __getitem__(self, Py_ssize_t i):
        """Return entry i as raw bytes, or None if null."""
        return self.item_at(i)

    def __iter__(self):
        if self._unified_view.data_length == 1:
            return iter(self.to_pylist())
        return _StringVectorIterator(self)

    def c_iter(self):
        """Return a C-level iterator for high-performance kernel operations."""
        if self._unified_view.data_length == 1:
            raise NotImplementedError("StringVector.c_iter() is not available for constant encoding")
        return _StringVectorCIterator._from_arena(
            <DrakenStringArena*>self._unified_view.data,
            <Py_ssize_t>self.ptr.length,
            self.ptr.null_bitmap,
        )

    cpdef Py_ssize_t byte_length(self, Py_ssize_t i):
        """Return the number of bytes for row ``i`` without materializing the value."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef _ConstView _cp
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().byte_length(i)
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")
        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return 0
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return _cp.length
        # Dense: read from the arena (Phase 6)
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        return <Py_ssize_t>str_length(&dense_arena.slots[i])

    cpdef object buffers(self):
        """Expose data, offsets, and null bitmap buffers as zero-copy views."""
        raise NotImplementedError(
            "StringVector.buffers() is not implemented for arena-backed vectors. "
            "Use to_arrow() for Arrow-format output."
        )

    cpdef object null_bitmap(self):
        """Return the null bitmap as a Python ``memoryview``, or ``None`` if all values are valid."""
        if self._unified_view.data_length == 1:
            return None
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t nb_size
        if ptr.null_bitmap == NULL:
            return None
        nb_size = (ptr.length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        return PyMemoryView_FromMemory(<char*>ptr.null_bitmap, nb_size, PyBUF_READ)

    cpdef int32_t[::1] lengths(self):
        """Return a direct view over the offsets buffer for fast length computations."""
        cdef DrakenVector* uv = self.unified()
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().lengths()
        if self._unified_view.data_length == 1:
            raise NotImplementedError("StringVector.lengths() is not available for constant encoding")
        return <int32_t[: self.ptr.length + 1]> self.ptr.offsets

    cpdef object view(self):
        """Return a per-row accessor reading through the unified view."""
        return _StringVectorView(self)

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVarBuffer* ptr
        cdef DrakenVector* uv
        cdef Py_ssize_t n
        cdef Py_ssize_t nb_size
        cdef Py_ssize_t bits_in_last
        cdef Py_ssize_t valid_count
        cdef uint8_t last_byte_mask
        cdef uint8_t byte_val

        uv = self.unified()
        ptr = self.ptr
        n = ptr.length
        if self._unified_view.data_length == 1:
            return n if uv.validity != NULL else 0
        if ptr.null_bitmap == NULL:
            return 0

        nb_size = (n + 7) >> 3
        bits_in_last = n & 7

        if bits_in_last == 0:
            # All bytes are fully used
            valid_count = <Py_ssize_t>simd_popcount(ptr.null_bitmap, <size_t>nb_size)
        else:
            # Mask last byte to only count valid bits
            if nb_size > 1:
                valid_count = <Py_ssize_t>simd_popcount(ptr.null_bitmap, <size_t>(nb_size - 1))
            else:
                valid_count = 0

            # Count only the valid bits in the last byte
            last_byte_mask = ptr.null_bitmap[nb_size - 1] & ((1 << bits_in_last) - 1)
            # Count 1 bits manually for the last byte
            byte_val = last_byte_mask
            while byte_val:
                valid_count += (byte_val & 1)
                byte_val >>= 1

        return n - valid_count

    def _unified_validity_is_set_for_test(self):
        """Test-only: True if the unified view carries a non-NULL validity bitmap."""
        return self._unified_view.validity != NULL

    cpdef Vector materialize(self):
        """Return a dense StringVector, expanding dict/const/RLE encodings if needed."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef StringVectorBuilder builder
        cdef Py_ssize_t i, val_len, data_bytes, off_start, off_end
        cdef const char* data_ptr
        cdef _ConstView _cp
        cdef DrakenStringArena* _gdict
        cdef DrakenStringSlot* _gslot
        cdef const uint32_t* _codes
        cdef uint8_t* _null_bm
        cdef Py_ssize_t _total_bytes
        cdef uint32_t _code
        if self._unified_view.data_length < self._unified_view.length:
            # dict-only path (make_string_dict_only): codes in selection, expand via dict
            _gdict = <DrakenStringArena*>uv.data
            _codes = uv.selection
            _null_bm = uv.validity
            _total_bytes = 0
            for i in range(n):
                if _null_bm != NULL and not ((_null_bm[i >> 3] >> (i & 7)) & 1):
                    continue
                _code = _codes[i]
                _total_bytes += <Py_ssize_t>str_length(&_gdict.slots[_code])
            builder = StringVectorBuilder(n, _total_bytes, resizable=False)
            for i in range(n):
                if _null_bm != NULL and not ((_null_bm[i >> 3] >> (i & 7)) & 1):
                    builder.append_null()
                else:
                    _code = _codes[i]
                    _gslot = &_gdict.slots[_code]
                    builder.append_bytes(
                        <const char*>str_data(_gslot, _gdict.arena),
                        <Py_ssize_t>str_length(_gslot),
                    )
            return builder.finish()
        if self._unified_view.data_length >= self._unified_view.length and self._unified_view.data_length > 1:
            # Dense — Arena-backed (Phase 6). Already in materialized form.
            return self
        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                builder = StringVectorBuilder(n, 0)
                for i in range(n):
                    builder.append_null()
            else:
                _cp = _const_view(<DrakenStringArena*>uv.data)
                val_len = <Py_ssize_t>_cp.length
                builder = StringVectorBuilder(n, n * val_len)
                for i in range(n):
                    builder.append_bytes(<char*>_cp.data, val_len)
            return builder.finish()
        return self

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef DrakenStringArena* _ga
        cdef _ConstView _cp
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, offset_bytes, null_bytes
        cdef uint64_t dict_data_bytes, dict_offset_bytes, code_bytes
        if self._unified_view.data_length == 1:
            if uv.data != NULL:
                _cp = _const_view(<DrakenStringArena*>uv.data)
                return <uint64_t>_cp.length
            return 0
        if self._unified_view.data_length < self._unified_view.length:
            code_bytes = n * sizeof(uint32_t)
            _ga = <DrakenStringArena*>uv.data
            if _ga != NULL:
                dict_data_bytes = <uint64_t>_ga.arena_used + <uint64_t>_ga.length * sizeof(DrakenStringSlot)
                dict_offset_bytes = 0
            else:
                dict_data_bytes = 0
                dict_offset_bytes = 0
            null_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
            return code_bytes + dict_data_bytes + dict_offset_bytes + null_bytes
        # Dense — Arena-backed (Phase 6)
        _ga = <DrakenStringArena*>self._unified_view.data
        if _ga != NULL:
            data_bytes = <uint64_t>_ga.arena_used + <uint64_t>_ga.length * sizeof(DrakenStringSlot)
        else:
            data_bytes = 0
        null_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + null_bytes

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if self._unified_view.data_length == 1:
            for i in range(n):
                buf[i] = 1 if uv.validity != NULL else 0
            return <int8_t[:n]> buf

        if ptr.null_bitmap == NULL:
            # No nulls — fill with 0
            for i in range(n):
                buf[i] = 0
        else:
            # Extract null bits — 1 means valid, so invert for null
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

        return <int8_t[:n]> buf

    # Optimized equality check using SIMD-friendly operations
    cpdef BoolVector equals(self, bytes value):
        """
        Return mask: 1 if equal to value, else 0.
        Optimized version with reduced branching and better cache locality.
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int cmp_res

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            _eq_code = _dict_find_code(self, PyBytes_AS_STRING(value), len(value))
            if _eq_code < 0:
                return BoolVector(<size_t>self.ptr.length)
            return _codes_to_boolvector_eq(self, _eq_code)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>PyBytes_AS_STRING(value),
                len(value),
            )
            return _constant_bool_result(n, cmp_res == 0, False)
        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef Py_ssize_t str_len
        cdef Py_ssize_t i
        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot

        # Process in chunks for better cache performance
        for i in range(n):
            # Check null first (most likely to fail)
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue

            dense_slot = &dense_arena.slots[i]
            str_len = <Py_ssize_t>str_length(dense_slot)

            # Length check before expensive memcmp
            if str_len != val_len:
                continue

            if memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, str_len) == 0:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector not_equals(self, bytes value):
        """Return mask: 1 if not equal to value, else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i
        cdef int cmp_res

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            _neq_code = _dict_find_code(self, PyBytes_AS_STRING(value), len(value))
            if _neq_code < 0:
                return _codes_to_boolvector_neq(self, <Py_ssize_t>self._unified_view.data_length)
            return _codes_to_boolvector_neq(self, _neq_code)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>val_ptr,
                val_len,
            )
            return _constant_bool_result(n, cmp_res != 0, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            if str_len != <int32_t>val_len:
                dst[i >> 3] |= (1 << (i & 7))
            elif memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, <size_t>str_len) != 0:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector less_than(self, bytes value):
        """Return mask: 1 if element < value (lexicographic bytes), else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len, min_len
        cdef int cmp_res
        cdef Py_ssize_t i

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return _dict_ordered_scalar(self, value, 0)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>val_ptr,
                val_len,
            )
            return _constant_bool_result(n, cmp_res < 0, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, <size_t>min_len)
            if cmp_res < 0 or (cmp_res == 0 and str_len < <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector greater_than(self, bytes value):
        """Return mask: 1 if element > value (lexicographic bytes), else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len, min_len
        cdef int cmp_res
        cdef Py_ssize_t i

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return _dict_ordered_scalar(self, value, 1)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>val_ptr,
                val_len,
            )
            return _constant_bool_result(n, cmp_res > 0, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, <size_t>min_len)
            if cmp_res > 0 or (cmp_res == 0 and str_len > <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector less_than_or_equals(self, bytes value):
        """Return mask: 1 if element <= value (lexicographic bytes), else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len, min_len
        cdef int cmp_res
        cdef Py_ssize_t i

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return _dict_ordered_scalar(self, value, 2)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>val_ptr,
                val_len,
            )
            return _constant_bool_result(n, cmp_res <= 0, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, <size_t>min_len)
            if cmp_res < 0 or (cmp_res == 0 and str_len <= <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector greater_than_or_equals(self, bytes value):
        """Return mask: 1 if element >= value (lexicographic bytes), else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len, min_len
        cdef int cmp_res
        cdef Py_ssize_t i

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return _dict_ordered_scalar(self, value, 3)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>_cp.data,
                _cp.length,
                <const uint8_t*>val_ptr,
                val_len,
            )
            return _constant_bool_result(n, cmp_res >= 0, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6 — produced by StringVectorBuilder.finish())
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(str_data(dense_slot, dense_arena.arena), val_ptr, <size_t>min_len)
            if cmp_res > 0 or (cmp_res == 0 and str_len >= <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector _compare_scalar(self, bytes value, int op):
        """Scalar compare using Draken standard op codes: 0=Eq 1=Ne 2=Gt 3=Ge 4=Lt 5=Le."""
        if op == 0:
            return self.equals(value)
        if op == 1:
            return self.not_equals(value)
        if op == 2:
            return self.greater_than(value)
        if op == 3:
            return self.greater_than_or_equals(value)
        if op == 4:
            return self.less_than(value)
        if op == 5:
            return self.less_than_or_equals(value)
        raise ValueError(f"StringVector._compare_scalar: unknown op {op}")

    cdef inline int _string_compare_pair(
        self,
        const uint8_t* d1, int32_t s1, int32_t l1,
        const uint8_t* d2, int32_t s2, int32_t l2,
        int op,
    ) nogil:
        # op: 0=eq, 1=neq, 2=lt, 3=lte, 4=gt, 5=gte
        cdef int32_t min_len = l1 if l1 < l2 else l2
        cdef int cmp_res
        if op == 0:
            if l1 != l2:
                return 0
            if l1 == 0:
                return 1
            return 1 if memcmp(<char*>d1 + s1, <char*>d2 + s2, <size_t>l1) == 0 else 0
        if op == 1:
            if l1 != l2:
                return 1
            if l1 == 0:
                return 0
            return 1 if memcmp(<char*>d1 + s1, <char*>d2 + s2, <size_t>l1) != 0 else 0
        cmp_res = memcmp(<char*>d1 + s1, <char*>d2 + s2, <size_t>min_len) if min_len > 0 else 0
        if op == 2:
            return 1 if (cmp_res < 0 or (cmp_res == 0 and l1 < l2)) else 0
        if op == 3:
            return 1 if (cmp_res < 0 or (cmp_res == 0 and l1 <= l2)) else 0
        if op == 4:
            return 1 if (cmp_res > 0 or (cmp_res == 0 and l1 > l2)) else 0
        return 1 if (cmp_res > 0 or (cmp_res == 0 and l1 >= l2)) else 0

    cdef BoolVector _compare_vector_op(self, StringVector other, int op):
        # Consolidated branchless+gate vector-vector string comparison
        # (per docs/null_representation_optimizations.md Change 2).
        # Note: branchless evaluates memcmp on null lanes (within allocated buffers).
        # The gate diverts to the original branching loop above 70% null density.
        cdef DrakenVarBuffer* ptr1 = self.ptr
        cdef DrakenVarBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef int32_t s1, l1, s2, l2
        cdef uint8_t v1, v2, v, m
        cdef Py_ssize_t i
        memset(dst, 0, nbytes)
        if (null1 != NULL or null2 != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)

        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False
        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        if null1 == NULL and null2 == NULL:
            for i in range(n):
                s1 = ptr1.offsets[i]; l1 = ptr1.offsets[i + 1] - s1
                s2 = ptr2.offsets[i]; l2 = ptr2.offsets[i + 1] - s2
                m = <uint8_t>self._string_compare_pair(<const uint8_t*>ptr1.data, s1, l1, <const uint8_t*>ptr2.data, s2, l2, op)
                dst[i >> 3] |= <uint8_t>(m << (i & 7))
        elif use_branching:
            for i in range(n):
                v1 = 1 if null1 == NULL else (null1[i >> 3] >> (i & 7)) & 1
                v2 = 1 if null2 == NULL else (null2[i >> 3] >> (i & 7)) & 1
                if v1 & v2:
                    out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    s1 = ptr1.offsets[i]; l1 = ptr1.offsets[i + 1] - s1
                    s2 = ptr2.offsets[i]; l2 = ptr2.offsets[i + 1] - s2
                    if self._string_compare_pair(<const uint8_t*>ptr1.data, s1, l1, <const uint8_t*>ptr2.data, s2, l2, op):
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            for i in range(n):
                v1 = 1 if null1 == NULL else (null1[i >> 3] >> (i & 7)) & 1
                v2 = 1 if null2 == NULL else (null2[i >> 3] >> (i & 7)) & 1
                v = v1 & v2
                s1 = ptr1.offsets[i]; l1 = ptr1.offsets[i + 1] - s1
                s2 = ptr2.offsets[i]; l2 = ptr2.offsets[i + 1] - s2
                m = <uint8_t>self._string_compare_pair(<const uint8_t*>ptr1.data, s1, l1, <const uint8_t*>ptr2.data, s2, l2, op)
                dst[i >> 3] |= <uint8_t>((v & m) << (i & 7))
                out_null[i >> 3] |= <uint8_t>(v << (i & 7))
        out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector equals_vector(self, StringVector other):
        """Element-wise op 0 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().equals_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.equals_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).equals_vector(other)
        if other._unified_view.data_length == 1:
            return self.equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 0)

    cpdef BoolVector not_equals_vector(self, StringVector other):
        """Element-wise op 1 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().not_equals_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.not_equals_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).not_equals_vector(other)
        if other._unified_view.data_length == 1:
            return self.not_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 1)

    cpdef BoolVector less_than_vector(self, StringVector other):
        """Element-wise op 2 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().less_than_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.less_than_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).less_than_vector(other)
        if other._unified_view.data_length == 1:
            return self.less_than_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 2)

    cpdef BoolVector less_than_or_equals_vector(self, StringVector other):
        """Element-wise op 3 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().less_than_or_equals_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.less_than_or_equals_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).less_than_or_equals_vector(other)
        if other._unified_view.data_length == 1:
            return self.less_than_or_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 3)

    cpdef BoolVector greater_than_vector(self, StringVector other):
        """Element-wise op 4 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().greater_than_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.greater_than_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).greater_than_vector(other)
        if other._unified_view.data_length == 1:
            return self.greater_than_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 4)

    cpdef BoolVector greater_than_or_equals_vector(self, StringVector other):
        """Element-wise op 5 between two StringVectors with null propagation."""
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().greater_than_or_equals_vector(other)
        if other._unified_view.data_length < other._unified_view.length:
            return self.greater_than_or_equals_vector(other.materialize())
        if self._unified_view.data_length == 1:
            return _materialize_const_string(self).greater_than_or_equals_vector(other)
        if other._unified_view.data_length == 1:
            return self.greater_than_or_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 5)

    cpdef BoolVector in_list(self, object value_set):
        """
        Return mask: 1 if element is a member of value_set, else 0. Propagates NULLs.
        value_set must be a set or frozenset of bytes.
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i
        cdef bytes cell_bytes

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().in_list(value_set)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cell_bytes = PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)
            return _constant_bool_result(n, cell_bytes in value_set, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Arena-backed dense (Phase 6)
        cdef DrakenStringArena* dense_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* dense_slot
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            dense_slot = &dense_arena.slots[i]
            str_len = <int32_t>str_length(dense_slot)
            cell_bytes = PyBytes_FromStringAndSize(
                <const char*>str_data(dense_slot, dense_arena.arena),
                <Py_ssize_t>str_len,
            )
            if cell_bytes in value_set:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector like(self, bytes pattern, bint ignore_case=False):
        """Return mask: 1 if element matches SQL LIKE pattern, else 0. Propagates NULLs.

        Optimized for dictionary-encoded vectors: tests each unique value once.
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* pat_ptr = PyBytes_AS_STRING(pattern)
        cdef Py_ssize_t pat_len = len(pattern)
        cdef Py_ssize_t str_len
        cdef int32_t start_l, end_l
        cdef Py_ssize_t i, dict_idx, dict_size
        cdef uint32_t code
        cdef DrakenStringArena* like_gdict
        cdef DrakenStringSlot* like_slot
        cdef const uint8_t* like_sdata
        cdef uint8_t* dict_like_results = NULL
        cdef const uint8_t* dict_codes
        cdef uint8_t* dict_row_nulls
        cdef _ConstView _cp
        cdef DrakenStringArena* dense_arena
        cdef DrakenStringSlot* dense_slot

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return _constant_bool_result(
                n,
                _sv_sql_like_match(
                    <const uint8_t*>_cp.data,
                    _cp.length,
                    <const uint8_t*>pat_ptr,
                    pat_len,
                    ignore_case,
                ),
                False,
            )

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        try:
            # Dictionary-encoded path: check each unique value once
            if self._unified_view.data_length < self._unified_view.length:
                like_gdict = _string_arena(self)
                if like_gdict == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>like_gdict.length
                dict_codes = <const uint8_t*><const void*>self._unified_view.selection
                if dict_codes == NULL or dict_size == 0:
                    return out  # Fallback to empty result

                dict_row_nulls = self.ptr.null_bitmap

                # Allocate results array for each dictionary entry
                dict_like_results = <uint8_t*>malloc(dict_size)
                if dict_like_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    like_slot = &like_gdict.slots[dict_idx]
                    str_len = <Py_ssize_t>str_length(like_slot)
                    like_sdata = str_data(like_slot, like_gdict.arena)

                    if _sv_sql_like_match(
                        like_sdata, str_len,
                        <const uint8_t*>pat_ptr, pat_len, ignore_case,
                    ):
                        dict_like_results[dict_idx] = 1
                    else:
                        dict_like_results[dict_idx] = 0

                # Scatter results by code index
                for i in range(n):
                    if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    code = self._unified_view.selection[i]
                    if dict_like_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant) — Arena-backed (Phase 6)
            else:
                dense_arena = <DrakenStringArena*>self._unified_view.data
                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    dense_slot = &dense_arena.slots[i]
                    str_len = <int32_t>str_length(dense_slot)
                    if _sv_sql_like_match(
                        str_data(dense_slot, dense_arena.arena), str_len,
                        <const uint8_t*>pat_ptr, pat_len, ignore_case,
                    ):
                        dst[i >> 3] |= (1 << (i & 7))
        finally:
            if dict_like_results != NULL:
                free(dict_like_results)

        return out

    cpdef BoolVector rlike(self, bytes pattern):
        """Return mask: 1 if element matches regex pattern, else 0. Propagates NULLs.

        Optimized for dictionary-encoded vectors: tests each unique value once.
        """
        import re
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t start, end
        cdef Py_ssize_t str_len
        cdef Py_ssize_t i, dict_idx, dict_size
        cdef uint32_t code
        cdef bytes cell_bytes
        cdef DrakenStringArena* rlike_gdict
        cdef DrakenStringArena* dense_arena_rl
        cdef DrakenStringSlot* dense_slot_rl
        cdef DrakenStringSlot* rlike_slot
        cdef const uint8_t* rlike_sdata
        cdef uint8_t* dict_rlike_results = NULL
        cdef uint8_t* dict_row_nulls
        cdef _ConstView _cp

        compiled = re.compile(pattern)

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            cell_bytes = PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)
            return _constant_bool_result(n, compiled.search(cell_bytes) is not None, False)

        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        try:
            # Dictionary-encoded path: check each unique value once
            if self._unified_view.data_length < self._unified_view.length:
                rlike_gdict = _string_arena(self)
                if rlike_gdict == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>rlike_gdict.length
                if dict_size == 0:
                    return out  # Fallback to empty result

                dict_row_nulls = self.ptr.null_bitmap

                # Allocate results array for each dictionary entry
                dict_rlike_results = <uint8_t*>malloc(dict_size)
                if dict_rlike_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    rlike_slot = &rlike_gdict.slots[dict_idx]
                    str_len = <Py_ssize_t>str_length(rlike_slot)
                    rlike_sdata = str_data(rlike_slot, rlike_gdict.arena)
                    cell_bytes = PyBytes_FromStringAndSize(<const char*>rlike_sdata, str_len)
                    if compiled.search(cell_bytes) is not None:
                        dict_rlike_results[dict_idx] = 1
                    else:
                        dict_rlike_results[dict_idx] = 0

                # Scatter results by code index
                for i in range(n):
                    if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    code = self._unified_view.selection[i]
                    if dict_rlike_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path — Arena-backed (Phase 6)
            else:
                dense_arena_rl = <DrakenStringArena*>self._unified_view.data
                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    dense_slot_rl = &dense_arena_rl.slots[i]
                    str_len = <Py_ssize_t>str_length(dense_slot_rl)
                    cell_bytes = PyBytes_FromStringAndSize(
                        <const char*>str_data(dense_slot_rl, dense_arena_rl.arena), str_len,
                    )
                    if compiled.search(cell_bytes):
                        dst[i >> 3] |= (1 << (i & 7))
        finally:
            if dict_rlike_results != NULL:
                free(dict_rlike_results)

        return out

    cpdef BoolVector contains(self, bytes substr, bint ignore_case=False):
        """Return mask: 1 if element contains substr, else 0. Propagates NULLs.

        Optimized for:
        - Dictionary-encoded vectors: tests each unique value once
        - Case-insensitive: pre-lowers entire buffer before comparison
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef char* ndl_ptr_char = PyBytes_AS_STRING(substr)
        cdef Py_ssize_t ndl_len = len(substr)
        cdef uint8_t* ndl_lower = NULL
        cdef int32_t start, end
        cdef Py_ssize_t str_len
        cdef Py_ssize_t i, j, dict_idx, dict_size
        cdef uint32_t code
        cdef uint8_t byte
        cdef DrakenStringArena* contains_gdict
        cdef DrakenStringSlot* contains_slot
        cdef const uint8_t* contains_sdata
        cdef uint8_t* dict_contains_results = NULL
        cdef uint8_t* dict_row_nulls
        cdef uint8_t* data_lower = NULL
        cdef Py_ssize_t data_len
        cdef VolnitskyTable* tbl = NULL
        cdef _ConstView _cp
        cdef DrakenStringArena* ctn_arena
        cdef DrakenStringSlot* ctn_slot
        cdef const uint8_t* ctn_sdata
        cdef Py_ssize_t ctn_len

        # Constant vector case
        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return _constant_bool_result(n, False, True)
            _cp = _const_view(<DrakenStringArena*>uv.data)
            if ignore_case and ndl_len > 0:
                ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
                if ndl_lower == NULL:
                    raise MemoryError()
                for j in range(ndl_len):
                    ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])
            tbl = volnitsky_alloc()
            if tbl == NULL:
                if ndl_lower != NULL:
                    free(ndl_lower)
                raise MemoryError()
            if ignore_case and ndl_lower != NULL:
                volnitsky_build(tbl, ndl_lower, <size_t>ndl_len)
            else:
                volnitsky_build(tbl, <const uint8_t*>ndl_ptr_char, <size_t>ndl_len)
            try:
                if ignore_case:
                    return _constant_bool_result(
                        n,
                        _sv_contains_ci(
                            <const uint8_t*>_cp.data,
                            _cp.length,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ),
                        False,
                    )
                return _constant_bool_result(
                    n,
                    _sv_contains_cs(
                        <const uint8_t*>_cp.data,
                        _cp.length,
                        <const uint8_t*>ndl_ptr_char,
                        ndl_len,
                        tbl,
                    ),
                    False,
                )
            finally:
                volnitsky_free(tbl)
                tbl = NULL
                if ndl_lower != NULL:
                    free(ndl_lower)

        # Setup output null bitmap
        memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # Pre-lowercase needle once
        if ignore_case and ndl_len > 0:
            ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
            if ndl_lower == NULL:
                raise MemoryError()
            for j in range(ndl_len):
                ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])

        # Build Volnitsky table once for all elements in this morsel
        tbl = volnitsky_alloc()
        if tbl == NULL:
            if ndl_lower != NULL:
                free(ndl_lower)
            raise MemoryError()
        if ignore_case and ndl_lower != NULL:
            volnitsky_build(tbl, ndl_lower, <size_t>ndl_len)
        else:
            volnitsky_build(tbl, <const uint8_t*>ndl_ptr_char, <size_t>ndl_len)

        try:
            # Dictionary-encoded path: check each unique value once
            if self._unified_view.data_length < self._unified_view.length:
                contains_gdict = _string_arena(self)
                if contains_gdict == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>contains_gdict.length
                if dict_size == 0:
                    return out  # Fallback to empty result

                dict_row_nulls = self.ptr.null_bitmap

                # Allocate results array for each dictionary entry
                dict_contains_results = <uint8_t*>malloc(dict_size)
                if dict_contains_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    contains_slot = &contains_gdict.slots[dict_idx]
                    str_len = <Py_ssize_t>str_length(contains_slot)
                    contains_sdata = str_data(contains_slot, contains_gdict.arena)

                    if ignore_case:
                        if _sv_contains_ci(
                            contains_sdata, str_len,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ):
                            dict_contains_results[dict_idx] = 1
                        else:
                            dict_contains_results[dict_idx] = 0
                    else:
                        if _sv_contains_cs(
                            contains_sdata, str_len,
                            <const uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ):
                            dict_contains_results[dict_idx] = 1
                        else:
                            dict_contains_results[dict_idx] = 0

                # Scatter results by code index
                for i in range(n):
                    if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    code = self._unified_view.selection[i]
                    if dict_contains_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant) — arena-backed
            else:
                ctn_arena = <DrakenStringArena*>self._unified_view.data

                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    ctn_slot = &ctn_arena.slots[i]
                    ctn_sdata = str_data(ctn_slot, ctn_arena.arena)
                    ctn_len = <Py_ssize_t>str_length(ctn_slot)

                    if ignore_case:
                        if _sv_contains_ci(
                            ctn_sdata, ctn_len,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ):
                            dst[i >> 3] |= (1 << (i & 7))
                    else:
                        if _sv_contains_cs(
                            ctn_sdata, ctn_len,
                            <const uint8_t*>ndl_ptr_char, ndl_len,
                            tbl,
                        ):
                            dst[i >> 3] |= (1 << (i & 7))

        finally:
            volnitsky_free(tbl)
            tbl = NULL
            if ndl_lower != NULL:
                free(ndl_lower)
            if data_lower != NULL:
                free(data_lower)
            if dict_contains_results != NULL:
                free(dict_contains_results)

        return out

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr
        cdef Py_ssize_t n
        cdef list out
        cdef Py_ssize_t i
        cdef uint8_t byte, bit
        cdef _ConstView _cp
        cdef DrakenStringArena* dense_arena
        cdef DrakenStringSlot* dense_slot
        cdef Py_ssize_t start, end
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().to_pylist()
        ptr = self.ptr
        n = ptr.length
        out = []

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                for i in range(n):
                    out.append(None)
            else:
                _cp = _const_view(<DrakenStringArena*>uv.data)
                for i in range(n):
                    out.append(PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length))
            return out

        # Arena-backed dense
        dense_arena = <DrakenStringArena*>self._unified_view.data
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue
            dense_slot = &dense_arena.slots[i]
            out.append(PyBytes_FromStringAndSize(
                <const char*>str_data(dense_slot, dense_arena.arena),
                <Py_ssize_t>str_length(dense_slot),
            ))

        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t value
        cdef Py_ssize_t i, j, block
        cdef uint8_t byte
        cdef size_t str_len
        cdef int32_t start, end
        cdef Py_ssize_t idx
        cdef Py_ssize_t dict_idx
        cdef uint64_t[STRING_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint64_t* dst
        cdef DrakenStringArena* hash_gdict
        cdef DrakenStringSlot* hash_slot
        cdef const uint8_t* hash_sdata
        cdef Py_ssize_t dict_size
        cdef uint8_t* dict_row_nulls
        cdef uint64_t* dict_hashes_ptr = NULL
        cdef uint32_t code
        cdef const uint8_t* dense_data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.hash_into: output buffer too small")

        cdef _ConstView _cv
        if self._unified_view.data_length == 1:
            if self._unified_view.validity != NULL:
                value = NULL_HASH
            else:
                _cv = _const_view(<DrakenStringArena*>self._unified_view.data)
                if _cv.length <= 32:
                    value = _short_string_hash(<const uint8_t*>_cv.data, <size_t>_cv.length)
                else:
                    value = XXH3_64bits(<const void*>_cv.data, <size_t>_cv.length)
            for j in range(STRING_HASH_CHUNK):
                scratch[j] = value
            dst = &out_buf[offset]
            i = 0
            while i < n:
                block = n - i
                if block > STRING_HASH_CHUNK:
                    block = STRING_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if self._unified_view.data_length < self._unified_view.length:
            # Dictionary-encoded path
            hash_gdict = _string_arena(self)
            dict_size = <Py_ssize_t>hash_gdict.length
            dict_row_nulls = self.ptr.null_bitmap
            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                raise MemoryError("StringVector.hash_into: cannot allocate dict hash buffer")
            try:
                # Hash each dictionary entry
                for dict_idx in range(dict_size):
                    hash_slot = &hash_gdict.slots[dict_idx]
                    str_len = <size_t>str_length(hash_slot)
                    hash_sdata = str_data(hash_slot, hash_gdict.arena)
                    if str_len <= 32:
                        dict_hashes_ptr[dict_idx] = _short_string_hash(hash_sdata, str_len)
                    else:
                        dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>hash_sdata, str_len)

                # Scatter hashes by code index
                dst = &out_buf[offset]
                i = 0
                while i < n:
                    block = n - i
                    if block > STRING_HASH_CHUNK:
                        block = STRING_HASH_CHUNK

                    if dict_row_nulls != NULL:
                        for j in range(block):
                            idx = i + j
                            byte = dict_row_nulls[idx >> 3]
                            if ((byte >> (idx & 7)) & 1) == 0:
                                scratch[j] = NULL_HASH
                            else:
                                code = self._unified_view.selection[idx]
                                scratch[j] = dict_hashes_ptr[code]
                    else:
                        for j in range(block):
                            code = self._unified_view.selection[i + j]
                            scratch[j] = dict_hashes_ptr[code]

                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            finally:
                free(dict_hashes_ptr)
            return

        # Dense (non-dictionary, non-constant) path
        cdef DrakenStringArena* dense_arena
        cdef DrakenStringSlot* dense_slot
        cdef const uint8_t* dense_bytes
        cdef const uint8_t* vb_data
        cdef int32_t* vb_offsets
        nb_ptr = ptr.null_bitmap
        dst = &out_buf[offset]

        if ptr.data != NULL:
            # VarBuffer-backed dense path: ptr.data/offsets are row-indexed and always
            # authoritative for row access (even when _unified_view.data holds a dict arena).
            vb_data = ptr.data
            vb_offsets = ptr.offsets
            i = 0
            with nogil:
                while i < n:
                    block = n - i
                    if block > STRING_HASH_CHUNK:
                        block = STRING_HASH_CHUNK
                    if nb_ptr != NULL:
                        for j in range(block):
                            idx = i + j
                            byte = nb_ptr[idx >> 3]
                            if ((byte >> (idx & 7)) & 1) == 0:
                                scratch[j] = NULL_HASH
                                continue
                            str_len = <size_t>(vb_offsets[idx + 1] - vb_offsets[idx])
                            dense_bytes = vb_data + vb_offsets[idx]
                            if str_len <= 32:
                                scratch[j] = _short_string_hash(dense_bytes, str_len)
                            else:
                                scratch[j] = XXH3_64bits(<const void*>dense_bytes, str_len)
                    else:
                        for j in range(block):
                            idx = i + j
                            str_len = <size_t>(vb_offsets[idx + 1] - vb_offsets[idx])
                            dense_bytes = vb_data + vb_offsets[idx]
                            if str_len <= 32:
                                scratch[j] = _short_string_hash(dense_bytes, str_len)
                            else:
                                scratch[j] = XXH3_64bits(<const void*>dense_bytes, str_len)
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            return

        # Pure arena-backed dense (no VarBuffer, arena is row-indexed)
        dense_arena = <DrakenStringArena*>self._unified_view.data
        i = 0
        with nogil:
            while i < n:
                block = n - i
                if block > STRING_HASH_CHUNK:
                    block = STRING_HASH_CHUNK

                if nb_ptr != NULL:
                    for j in range(block):
                        idx = i + j
                        byte = nb_ptr[idx >> 3]
                        if ((byte >> (idx & 7)) & 1) == 0:
                            scratch[j] = NULL_HASH
                            continue
                        dense_slot = &dense_arena.slots[idx]
                        str_len = <size_t>str_length(dense_slot)
                        dense_bytes = str_data(dense_slot, dense_arena.arena)
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(dense_bytes, str_len)
                        else:
                            scratch[j] = XXH3_64bits(<const void*>dense_bytes, str_len)
                else:
                    for j in range(block):
                        dense_slot = &dense_arena.slots[i + j]
                        str_len = <size_t>str_length(dense_slot)
                        dense_bytes = str_data(dense_slot, dense_arena.arena)
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(dense_bytes, str_len)
                        else:
                            scratch[j] = XXH3_64bits(<const void*>dense_bytes, str_len)

                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint64_t value
        cdef Py_ssize_t i, j, block
        cdef uint8_t byte
        cdef size_t str_len
        cdef int32_t start, end
        cdef Py_ssize_t idx
        cdef uint64_t[STRING_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint32_t code
        cdef Py_ssize_t dict_size, dict_idx
        cdef uint64_t* dict_hashes_ptr = NULL
        cdef uint8_t* dict_row_nulls
        cdef DrakenStringArena* cl_gdict
        cdef DrakenStringSlot* cl_slot
        cdef const uint8_t* cl_sdata
        cdef const uint8_t* data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr

        if n == 0:
            return 0

        cdef _ConstView _cv
        if self._unified_view.data_length == 1:
            if self._unified_view.validity != NULL:
                value = NULL_HASH
            else:
                _cv = _const_view(<DrakenStringArena*>self._unified_view.data)
                if _cv.length <= 32:
                    value = _short_string_hash(<const uint8_t*>_cv.data, <size_t>_cv.length)
                else:
                    value = XXH3_64bits(<const void*>_cv.data, <size_t>_cv.length)
            for i in range(STRING_HASH_CHUNK):
                scratch[i] = value
            i = 0
            while i < n:
                block = n - i
                if block > STRING_HASH_CHUNK:
                    block = STRING_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # Check encoding first before accessing any structures
        if self._unified_view.data_length < self._unified_view.length:
            # Dictionary-encoded path: access member variables directly (no GIL needed)
            cl_gdict = _string_arena(self)
            if cl_gdict == NULL:
                return 1  # Fall back to Python hash
            dict_size = <Py_ssize_t>cl_gdict.length
            dict_row_nulls = self.ptr.null_bitmap

            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                return 1  # OOM, fall back to Python hash

            # Hash each dictionary entry
            for dict_idx in range(dict_size):
                cl_slot = &cl_gdict.slots[dict_idx]
                str_len = <size_t>str_length(cl_slot)
                cl_sdata = str_data(cl_slot, cl_gdict.arena)
                if str_len <= 32:
                    dict_hashes_ptr[dict_idx] = _short_string_hash(cl_sdata, str_len)
                else:
                    dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>cl_sdata, str_len)

            # Fused gather-and-mix: uint32 codes only
            if dict_row_nulls != NULL:
                simd_mix_hash_from_dict_nullable_cw4(
                    out, dict_hashes_ptr, self._unified_view.selection, dict_row_nulls, 0, <size_t>n)
            else:
                simd_mix_hash_from_dict_cw4(
                    out, dict_hashes_ptr, self._unified_view.selection, <size_t>n)
            free(dict_hashes_ptr)
            return 0

        # Dense (non-dictionary, non-constant) path
        cdef const uint8_t* chi_vb_data
        cdef int32_t* chi_vb_offsets
        nb_ptr = ptr.null_bitmap

        if ptr.data != NULL:
            # VarBuffer-backed dense path: ptr.data/offsets are row-indexed and always
            # authoritative (even when _unified_view.data holds a dict arena).
            chi_vb_data = ptr.data
            chi_vb_offsets = ptr.offsets
            i = 0
            while i < n:
                block = n - i
                if block > STRING_HASH_CHUNK:
                    block = STRING_HASH_CHUNK
                if nb_ptr != NULL:
                    for j in range(block):
                        idx = i + j
                        byte = nb_ptr[idx >> 3]
                        if ((byte >> (idx & 7)) & 1) == 0:
                            scratch[j] = NULL_HASH
                            continue
                        str_len = <size_t>(chi_vb_offsets[idx + 1] - chi_vb_offsets[idx])
                        cl_sdata = chi_vb_data + chi_vb_offsets[idx]
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(cl_sdata, str_len)
                        else:
                            scratch[j] = XXH3_64bits(<const void*>cl_sdata, str_len)
                else:
                    for j in range(block):
                        idx = i + j
                        str_len = <size_t>(chi_vb_offsets[idx + 1] - chi_vb_offsets[idx])
                        cl_sdata = chi_vb_data + chi_vb_offsets[idx]
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(cl_sdata, str_len)
                        else:
                            scratch[j] = XXH3_64bits(<const void*>cl_sdata, str_len)
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # Pure arena-backed dense path (no VarBuffer; arena is row-indexed)
        cdef DrakenStringArena* ch_arena = <DrakenStringArena*>self._unified_view.data
        if ch_arena == NULL:
            return 1  # Fall back to Python hash

        i = 0
        while i < n:
            block = n - i
            if block > STRING_HASH_CHUNK:
                block = STRING_HASH_CHUNK

            if nb_ptr != NULL:
                for j in range(block):
                    idx = i + j
                    byte = nb_ptr[idx >> 3]
                    if ((byte >> (idx & 7)) & 1) == 0:
                        scratch[j] = NULL_HASH
                        continue
                    cl_slot = &ch_arena.slots[idx]
                    str_len = <size_t>str_length(cl_slot)
                    cl_sdata = str_data(cl_slot, ch_arena.arena)
                    if str_len <= 32:
                        scratch[j] = _short_string_hash(cl_sdata, str_len)
                    else:
                        scratch[j] = XXH3_64bits(<const void*>cl_sdata, str_len)
            else:
                for j in range(block):
                    cl_slot = &ch_arena.slots[i + j]
                    str_len = <size_t>str_length(cl_slot)
                    cl_sdata = str_data(cl_slot, ch_arena.arena)
                    if str_len <= 32:
                        scratch[j] = _short_string_hash(cl_sdata, str_len)
                    else:
                        scratch[j] = XXH3_64bits(<const void*>cl_sdata, str_len)

            simd_mix_hash(out + i, scratch_ptr, <size_t> block)
            i += block
        return 0

    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Single-column hash: out[i] = xxhash(row_i). No mix step, no memset.

        Null sentinel matches mix_hash(0, NULL_HASH) — same as c_hash_into —
        so callers can use the same null_marker for compaction.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t i, dict_idx, dict_size
        cdef uint64_t value
        cdef uint64_t null_sentinel = NULL_HASH * MIX_HASH_CONSTANT + 1
        null_sentinel ^= null_sentinel >> 32
        cdef int32_t start, end
        cdef size_t str_len
        cdef uint8_t byte
        cdef uint64_t* dict_hashes_ptr = NULL
        cdef DrakenStringArena* hs_gdict
        cdef DrakenStringSlot* hs_slot
        cdef const uint8_t* hs_sdata
        cdef const uint8_t* data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr
        cdef uint8_t* dict_row_nulls
        cdef uint32_t code

        if n == 0:
            return 0

        cdef _ConstView _cv2
        if self._unified_view.data_length == 1:
            if self._unified_view.validity != NULL:
                value = null_sentinel
            else:
                _cv2 = _const_view(<DrakenStringArena*>self._unified_view.data)
                if _cv2.length <= 32:
                    value = _short_string_hash(<const uint8_t*>_cv2.data, <size_t>_cv2.length)
                else:
                    value = XXH3_64bits(<const void*>_cv2.data, <size_t>_cv2.length)
            for i in range(n):
                out[i] = value
            return 0

        if self._unified_view.data_length < self._unified_view.length:
            hs_gdict = _string_arena(self)
            if hs_gdict == NULL:
                return 1
            dict_size = <Py_ssize_t>hs_gdict.length
            dict_row_nulls = self.ptr.null_bitmap

            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                return 1

            for dict_idx in range(dict_size):
                hs_slot = &hs_gdict.slots[dict_idx]
                str_len = <size_t>str_length(hs_slot)
                hs_sdata = str_data(hs_slot, hs_gdict.arena)
                if str_len <= 32:
                    dict_hashes_ptr[dict_idx] = _short_string_hash(hs_sdata, str_len)
                else:
                    dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>hs_sdata, str_len)

            # Scatter directly — no mix step, uint32 codes only
            if dict_row_nulls != NULL:
                for i in range(n):
                    out[i] = null_sentinel if not ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) else dict_hashes_ptr[self._unified_view.selection[i]]
            else:
                for i in range(n):
                    out[i] = dict_hashes_ptr[self._unified_view.selection[i]]

            free(dict_hashes_ptr)
            return 0

        # Dense (non-dictionary, non-constant) path
        cdef const uint8_t* chs_vb_data
        cdef int32_t* chs_vb_offsets
        nb_ptr = ptr.null_bitmap

        if ptr.data != NULL:
            # VarBuffer-backed dense path: ptr.data/offsets are row-indexed and always
            # authoritative (even when _unified_view.data holds a dict arena).
            chs_vb_data = ptr.data
            chs_vb_offsets = ptr.offsets
            if nb_ptr != NULL:
                for i in range(n):
                    byte = nb_ptr[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        out[i] = null_sentinel
                        continue
                    str_len = <size_t>(chs_vb_offsets[i + 1] - chs_vb_offsets[i])
                    hs_sdata = chs_vb_data + chs_vb_offsets[i]
                    if str_len <= 32:
                        out[i] = _short_string_hash(hs_sdata, str_len)
                    else:
                        out[i] = XXH3_64bits(<const void*>hs_sdata, str_len)
            else:
                for i in range(n):
                    str_len = <size_t>(chs_vb_offsets[i + 1] - chs_vb_offsets[i])
                    hs_sdata = chs_vb_data + chs_vb_offsets[i]
                    if str_len <= 32:
                        out[i] = _short_string_hash(hs_sdata, str_len)
                    else:
                        out[i] = XXH3_64bits(<const void*>hs_sdata, str_len)
            return 0

        # Pure arena-backed dense path (no VarBuffer; arena is row-indexed)
        cdef DrakenStringArena* cs_arena = <DrakenStringArena*>self._unified_view.data
        if cs_arena == NULL:
            return 1

        if nb_ptr != NULL:
            for i in range(n):
                byte = nb_ptr[i >> 3]
                if ((byte >> (i & 7)) & 1) == 0:
                    out[i] = null_sentinel
                    continue
                hs_slot = &cs_arena.slots[i]
                str_len = <size_t>str_length(hs_slot)
                hs_sdata = str_data(hs_slot, cs_arena.arena)
                if str_len <= 32:
                    out[i] = _short_string_hash(hs_sdata, str_len)
                else:
                    out[i] = XXH3_64bits(<const void*>hs_sdata, str_len)
        else:
            for i in range(n):
                hs_slot = &cs_arena.slots[i]
                str_len = <size_t>str_length(hs_slot)
                hs_sdata = str_data(hs_slot, cs_arena.arena)
                if str_len <= 32:
                    out[i] = _short_string_hash(hs_sdata, str_len)
                else:
                    out[i] = XXH3_64bits(<const void*>hs_sdata, str_len)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for StringVector: pack first 7 bytes into big-endian int64."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i, copy_len
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t acc
        cdef DrakenStringArena* ci_arena
        cdef DrakenStringSlot* ci_slot
        cdef const uint8_t* ci_data

        if self._unified_view.data_length < self._unified_view.length:
            self.materialize().compress_into(out_buf, offset)
            return

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.compress: output buffer too small")

        cdef _ConstView _cv_compress
        if self._unified_view.data_length == 1:
            if self._unified_view.validity != NULL:
                for i in range(n):
                    out_buf[offset + i] = <int64_t>(-(1 << 63))
            else:
                _cv_compress = _const_view(<DrakenStringArena*>self._unified_view.data)
                copy_len = _cv_compress.length
                if copy_len > 7:
                    copy_len = 7
                acc = <uint64_t>0
                memcpy(&acc, <const void*>_cv_compress.data, <size_t>copy_len)
                acc = BSWAP64(acc)
                for i in range(n):
                    out_buf[offset + i] = <int64_t>acc
            return

        cdef const uint8_t* ci_vb_data
        cdef int32_t* ci_vb_offsets
        if ptr.data != NULL:
            # VarBuffer-backed dense path: ptr.data/offsets are row-indexed and always
            # authoritative (even when _unified_view.data holds a dict arena).
            ci_vb_data = ptr.data
            ci_vb_offsets = ptr.offsets
            for i in range(n):
                if has_nulls and ((null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    out_buf[offset + i] = <int64_t>(-(1 << 63))
                    continue
                copy_len = ci_vb_offsets[i + 1] - ci_vb_offsets[i]
                if copy_len > 7:
                    copy_len = 7
                acc = <uint64_t>0
                if copy_len > 0:
                    memcpy(&acc, <const void*>(ci_vb_data + ci_vb_offsets[i]), <size_t>copy_len)
                acc = BSWAP64(acc)
                out_buf[offset + i] = <int64_t>acc
            return

        # Arena-backed dense
        ci_arena = <DrakenStringArena*>self._unified_view.data
        for i in range(n):
            if has_nulls and ((null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                out_buf[offset + i] = <int64_t>(-(1 << 63))
                continue

            ci_slot = &ci_arena.slots[i]
            ci_data = str_data(ci_slot, ci_arena.arena)
            copy_len = <Py_ssize_t>str_length(ci_slot)
            if copy_len > 7:
                copy_len = 7
            acc = <uint64_t>0
            memcpy(&acc, <const void*>ci_data, <size_t>copy_len)
            acc = BSWAP64(acc)
            out_buf[offset + i] = <int64_t>acc

    cpdef StringVector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = indices.shape[0]
        cdef Py_ssize_t i
        cdef int32_t src_idx
        cdef DrakenStringArena* tk_arena = <DrakenStringArena*>uv.data
        cdef DrakenStringSlot* tk_slot
        cdef const uint8_t* tk_sdata
        cdef Py_ssize_t tk_len
        cdef bint has_nulls = uv.validity != NULL
        cdef uint8_t src_bit

        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= <Py_ssize_t>uv.length:
                raise IndexError(
                    f"Index {src_idx} out of bounds for length {uv.length}"
                )

        cdef StringVectorBuilder tk_builder = StringVectorBuilder(n, 0, True, False)
        for i in range(n):
            src_idx = indices[i]
            if has_nulls:
                src_bit = (uv.validity[src_idx >> 3] >> (src_idx & 7)) & 1
                if not src_bit:
                    tk_builder.append_null()
                    continue
            tk_slot = &tk_arena.slots[<Py_ssize_t>uv.selection[<Py_ssize_t>src_idx]]
            tk_sdata = str_data(tk_slot, tk_arena.arena)
            tk_len = <Py_ssize_t>str_length(tk_slot)
            tk_builder.append_bytes(<const char*>tk_sdata, tk_len)
        return tk_builder.finish()

    cpdef object min(self):
        """Return lexicographically smallest non-null string value, or None if all null or empty."""
        # Track A: metadata short-circuit. When valid, build PyBytes from the
        # cached ptr+len which points into a buffer the vector owns.
        if self._min_max_valid:
            if self._min_max_all_null:
                return None
            return PyBytes_FromStringAndSize(<const char*>self._cached_min_ptr, self._cached_min_len)

        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef int cmp
        cdef Py_ssize_t cur_len, best_len, common_len
        cdef const uint8_t* cur_data
        cdef const uint8_t* best_data = NULL
        cdef Py_ssize_t best_len_val = 0
        cdef bint found = False
        cdef uint8_t byte, bit
        cdef DrakenStringArena* mn_arena
        cdef DrakenStringSlot* mn_slot

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().min()

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return None
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)

        # Arena-backed dense
        mn_arena = <DrakenStringArena*>self._unified_view.data
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    continue

            mn_slot = &mn_arena.slots[i]
            cur_data = str_data(mn_slot, mn_arena.arena)
            cur_len = <Py_ssize_t>str_length(mn_slot)

            if not found:
                best_data = cur_data
                best_len_val = cur_len
                found = True
                continue

            common_len = cur_len if cur_len < best_len_val else best_len_val
            cmp = memcmp(cur_data, best_data, common_len)
            # Tie on the shared prefix: shorter string is lex-smaller.
            if cmp == 0 and cur_len < best_len_val:
                cmp = -1
            if cmp < 0:
                best_data = cur_data
                best_len_val = cur_len

        if not found:
            return None
        return PyBytes_FromStringAndSize(<const char*>best_data, best_len_val)

    cpdef object max(self):
        """Return lexicographically largest non-null string value, or None if all null or empty."""
        # Track A: metadata short-circuit. When valid, build PyBytes from the
        # cached ptr+len which points into a buffer the vector owns.
        if self._min_max_valid:
            if self._min_max_all_null:
                return None
            return PyBytes_FromStringAndSize(<const char*>self._cached_max_ptr, self._cached_max_len)

        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef int cmp
        cdef Py_ssize_t cur_len, best_len_val, common_len
        cdef const uint8_t* cur_data
        cdef const uint8_t* best_data = NULL
        cdef bint found = False
        cdef uint8_t byte, bit
        cdef DrakenStringArena* mx_arena
        cdef DrakenStringSlot* mx_slot
        best_len_val = 0

        cdef _ConstView _cp

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().max()

        if self._unified_view.data_length == 1:
            if uv.validity != NULL:
                return None
            _cp = _const_view(<DrakenStringArena*>uv.data)
            return PyBytes_FromStringAndSize(<char*>_cp.data, _cp.length)

        # Arena-backed dense
        mx_arena = <DrakenStringArena*>self._unified_view.data
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    continue

            mx_slot = &mx_arena.slots[i]
            cur_data = str_data(mx_slot, mx_arena.arena)
            cur_len = <Py_ssize_t>str_length(mx_slot)

            if not found:
                best_data = cur_data
                best_len_val = cur_len
                found = True
                continue

            common_len = cur_len if cur_len < best_len_val else best_len_val
            cmp = memcmp(cur_data, best_data, common_len)
            # Tie on the shared prefix: longer string is lex-larger.
            if cmp == 0 and cur_len > best_len_val:
                cmp = 1
            if cmp > 0:
                best_data = cur_data
                best_len_val = cur_len

        if not found:
            return None
        return PyBytes_FromStringAndSize(<const char*>best_data, best_len_val)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two strings at given indices lexicographically. Returns -1, 0, 1. Assumes non-null."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr
        cdef char* data
        cdef int32_t left_start, left_end, right_start, right_end
        cdef Py_ssize_t left_len, right_len, common_len
        cdef int cmp_result
        cdef uint32_t lc, rc
        cdef const uint8_t* lp
        cdef const uint8_t* rp

        if self._unified_view.data_length < self._unified_view.length:
            # Dict-aware path: compare via codes without materializing the
            # dictionary. For an ordered dict, code order matches value order, so
            # a single integer compare suffices. Otherwise dereference into the
            # dict's varbuffer and memcmp the underlying bytes.
            lc = self._unified_view.selection[left_idx]
            rc = self._unified_view.selection[right_idx]
            if lc == rc:
                return 0
            # Ordered-dict shortcut is gone; the dict-ordered flag is no longer
            # tracked under the unified format. Fall through to value compare.
            lp = self.c_dict_value_ptr(<Py_ssize_t>lc, &left_len)
            rp = self.c_dict_value_ptr(<Py_ssize_t>rc, &right_len)
            common_len = left_len if left_len < right_len else right_len
            cmp_result = memcmp(lp, rp, common_len)
            if cmp_result != 0:
                return -1 if cmp_result < 0 else 1
            if left_len < right_len:
                return -1
            elif left_len > right_len:
                return 1
            return 0

        if self._unified_view.data_length == 1:
            return 0

        # Arena-backed dense
        cdef DrakenStringArena* ca_arena = <DrakenStringArena*>self._unified_view.data
        cdef DrakenStringSlot* ca_left_slot = &ca_arena.slots[left_idx]
        cdef DrakenStringSlot* ca_right_slot = &ca_arena.slots[right_idx]
        lp = str_data(ca_left_slot, ca_arena.arena)
        rp = str_data(ca_right_slot, ca_arena.arena)
        left_len = <Py_ssize_t>str_length(ca_left_slot)
        right_len = <Py_ssize_t>str_length(ca_right_slot)

        common_len = left_len if left_len < right_len else right_len
        cmp_result = memcmp(lp, rp, common_len)

        if cmp_result != 0:
            return -1 if cmp_result < 0 else 1

        if left_len < right_len:
            return -1
        elif left_len > right_len:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint8_t byte

        if self._unified_view.data_length == 1:
            return uv.validity != NULL

        if ptr.null_bitmap == NULL:
            return False

        byte = ptr.null_bitmap[idx >> 3]
        return ((byte >> (idx & 7)) & 1) == 0

    cpdef sum(self):
        """Sum is not defined for string vectors."""
        raise NotImplementedError("sum() is not supported for StringVector")

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        cdef _ConstView _cp_str
        k = min(<Py_ssize_t>self.ptr.length, 5)
        if self._unified_view.data_length == 1:
            if self._unified_view.validity != NULL:
                vals = [None] * k
            else:
                _cp_str = _const_view(<DrakenStringArena*>self._unified_view.data)
                vals = [PyBytes_FromStringAndSize(<char*>_cp_str.data, _cp_str.length)] * k
            return f"<StringVector len={self.ptr.length} values={vals}>"
        for i in range(k):
            vals.append(self[i])
        return f"<StringVector len={self.ptr.length} values={vals}>"


# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    bint is_null


cdef class _StringVectorIterator:
    """Efficient iterator that avoids repeated attribute lookups during scans."""

    cdef DrakenStringArena* _arena
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    def __cinit__(self, StringVector vec):
        self._arena = <DrakenStringArena*>vec._unified_view.data
        self._pos = 0
        self._length = vec.ptr.length
        self._nulls = vec.ptr.null_bitmap
        self._has_nulls = (self._nulls != NULL)

    def __iter__(self):
        return self

    def __next__(self):
        if self._pos >= self._length:
            raise StopIteration()

        cdef Py_ssize_t i = self._pos
        self._pos += 1

        # Check for null value
        if self._has_nulls and ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0:
            return None

        cdef DrakenStringSlot* slot = &self._arena.slots[i]
        cdef Py_ssize_t slen = str_length(slot)
        return PyBytes_FromStringAndSize(<char*>str_data(slot, self._arena.arena), slen)


cdef class _StringVectorCIterator:
    """
    Highly optimized C-level iterator with minimal overhead.
    """

    def __cinit__(self):
        # Initialize with NULL; must use _from_arena factory method
        self._arena = NULL
        self._pos = 0
        self._length = 0
        self._nulls = NULL
        self._has_nulls = False

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr):
        """Factory method - kept for API compatibility; delegates to _from_arena."""
        raise RuntimeError("_StringVectorCIterator._from_ptr: use StringVector.c_iter() instead")

    @staticmethod
    cdef _StringVectorCIterator _from_arena(DrakenStringArena* arena, Py_ssize_t length, uint8_t* nulls):
        """Factory method to create iterator from a StringArena."""
        cdef _StringVectorCIterator it = _StringVectorCIterator.__new__(_StringVectorCIterator)
        it._arena = arena
        it._pos = 0
        it._length = length
        it._nulls = nulls
        it._has_nulls = (nulls != NULL)
        return it

    cdef inline bint next(self, StringElement* elem) nogil:
        """
        Ultra-fast inline method for C-level iteration.
        """
        if self._pos >= self._length:
            return False

        cdef Py_ssize_t i = self._pos
        cdef DrakenStringSlot* slot
        self._pos += 1

        # Check for null
        if self._has_nulls and ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            slot = &self._arena.slots[i]
            elem.ptr = <char*>str_data(slot, self._arena.arena)
            elem.length = str_length(slot)
            elem.is_null = False

        return True

    cpdef void reset(self):
        """Reset iterator to beginning."""
        self._pos = 0

    @property
    def position(self):
        """Current position in iteration."""
        return self._pos

    cpdef StringElement get_at(self, Py_ssize_t index):
        """
        Get element at specific index without advancing iterator.
        Useful for random access patterns.
        """
        if index < 0 or index >= self._length:
            raise IndexError("Index out of range")

        cdef StringElement elem
        cdef DrakenStringSlot* slot

        if self._nulls != NULL and ((self._nulls[index >> 3] >> (index & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            slot = &self._arena.slots[index]
            elem.ptr = <char*>str_data(slot, self._arena.arena)
            elem.length = str_length(slot)
            elem.is_null = False

        return elem


cdef class _StringVectorView:
    """Per-row accessor reading through the unified view.

    Resolves each row as ``arena.slots[selection[i]]`` — uniform across
    every layout. Holds direct pointers captured at construction so the
    inner loop is a single indirect read per row.
    """

    def __cinit__(self, StringVector vec):
        cdef DrakenVector* uv = vec.unified()
        self._arena = <DrakenStringArena*>uv.data
        self._selection = uv.selection
        self._nulls = uv.validity
        self._length = <Py_ssize_t>uv.length

    cpdef intptr_t value_ptr(self, Py_ssize_t i):
        if i < 0 or i >= self._length:
            raise IndexError("Index out of range")
        cdef DrakenStringSlot* slot = &self._arena.slots[self._selection[i]]
        return <intptr_t>str_data(slot, self._arena.arena)

    cpdef Py_ssize_t value_len(self, Py_ssize_t i):
        if i < 0 or i >= self._length:
            raise IndexError("Index out of range")
        cdef DrakenStringSlot* slot = &self._arena.slots[self._selection[i]]
        return <Py_ssize_t>str_length(slot)

    cpdef bint is_null(self, Py_ssize_t i):
        if i < 0 or i >= self._length:
            raise IndexError("Index out of range")
        if self._nulls == NULL:
            return False
        return ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0


cdef class StringVectorBuilder:
    """Utility for constructing ``StringVector`` instances with controlled preallocation."""

    def __cinit__(self, Py_ssize_t length, Py_ssize_t bytes_capacity,
                  bint resizable=False, bint strict_capacity=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if bytes_capacity < 0:
            raise ValueError("bytes_capacity must be non-negative")

        self._vec = StringVector(length, bytes_capacity)
        self._ptr = self._vec.ptr
        self._length = length
        self._next_index = 0
        self._bytes_cap = bytes_capacity
        self._offset = 0
        self._finished = False
        self._resizable = resizable
        self._strict_capacity = strict_capacity
        self._mask_user_provided = False

        # Cache frequently accessed pointers
        self._data = <char*>self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap

        if self._offsets != NULL:
            self._offsets[0] = 0

    def __dealloc__(self):
        # Allow the vector to GC naturally; nothing special to do.
        pass

    @classmethod
    def with_counts(cls, Py_ssize_t length, Py_ssize_t total_bytes):
        """Create a builder with an exact byte budget that must be fully consumed."""
        return cls(length, total_bytes, False, True)

    @classmethod
    def with_estimate(cls, Py_ssize_t length, Py_ssize_t est_avg_bytes):
        """Create a resizable builder using an average byte estimate per row."""
        if length < 0:
            raise ValueError("length must be non-negative")
        if est_avg_bytes < 0:
            raise ValueError("est_avg_bytes must be non-negative")
        initial = length * est_avg_bytes
        if initial <= 0:
            initial = max(length, 64)
        return cls(length, initial, True, False)

    def __len__(self):
        return self._length

    property bytes_capacity:
        def __get__(self):
            return self._bytes_cap

    property bytes_used:
        def __get__(self):
            return self._offset

    property remaining_bytes:
        def __get__(self):
            return self._bytes_cap - self._offset

    cpdef void append(self, bytes value):
        """Append a value at the next position, copying bytes into the backing buffer."""
        self._append_with_ptr(self._next_index, PyBytes_AS_STRING(value), len(value))

    cpdef void append_bulk(self, list values):
        """
        Append multiple values at once for better performance.
        """
        cdef Py_ssize_t i, n = len(values)
        cdef bytes value
        cdef char* val_ptr
        cdef Py_ssize_t val_len

        for i in range(n):
            if self._next_index >= self._length:
                raise IndexError("Cannot append beyond builder length")

            value = values[i]
            if value is None:
                self._set_null(self._next_index)
            else:
                val_ptr = PyBytes_AS_STRING(value)
                val_len = len(value)
                self._append_with_ptr(self._next_index, val_ptr, val_len)

    cdef void append_bytes_bulk(self, const char** ptrs, Py_ssize_t* lengths, Py_ssize_t n):
        """
        Append multiple raw byte sequences at once.
        """
        cdef Py_ssize_t i
        for i in range(n):
            if self._next_index >= self._length:
                raise IndexError("Cannot append beyond builder length")
            self._append_with_ptr(self._next_index, ptrs[i], lengths[i])

    cdef inline void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *:
        self._require_index(index)
        if length < 0:
            raise ValueError("length must be non-negative")

        self._ensure_capacity(length)

        if length > 0 and src != NULL:
            memcpy(self._data + self._offset, src, length)

        if self._nulls != NULL and not self._mask_user_provided:
            self._nulls[index >> 3] |= (1 << (index & 7))

        self._offset += length
        self._next_index += 1
        self._offsets[self._next_index] = <int32_t>self._offset

    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length):
        """
        Append from raw pointer + length without Python bytes intermediary.

        Zero-copy-friendly: avoids creating a Python bytes object, though
        data is still copied into the builder's internal buffer.

        Args:
            ptr: Pointer to byte data (can be NULL if length is 0)
            length: Number of bytes to copy

        Example:
            cdef char* data = get_string_data()
            builder.append_bytes(data, strlen(data))
        """
        self._append_with_ptr(self._next_index, ptr, length)

    cpdef void append_view(self, const uint8_t[::1] value):
        """Append from a read-only memoryview without creating an intermediate bytes object."""
        cdef Py_ssize_t size = value.shape[0]
        cdef const uint8_t* ptr = NULL
        if size == 0:
            self._append_with_ptr(self._next_index, NULL, 0)
        else:
            ptr = &value[0]
            self._append_with_ptr(self._next_index, <const char*>ptr, size)

    cpdef void append_null(self):
        """Append a null entry without advancing the byte offset."""
        self._set_null(self._next_index)

    cpdef void set(self, Py_ssize_t index, bytes value):
        """Set ``index`` to ``value`` (must be the next slot)."""
        self._append_with_ptr(index, PyBytes_AS_STRING(value), len(value))

    cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length):
        """
        Set value at index from raw pointer + length.

        Args:
            index: Index to set (must be the next available slot)
            ptr: Pointer to byte data (can be NULL if length is 0)
            length: Number of bytes to copy
        """
        self._append_with_ptr(index, ptr, length)

    cpdef void set_view(self, Py_ssize_t index, const uint8_t[::1] value):
        cdef Py_ssize_t size = value.shape[0]
        cdef const uint8_t* ptr = NULL
        if size > 0:
            ptr = &value[0]
        self._append_with_ptr(index, <const char*>ptr, size)

    cpdef void set_null(self, Py_ssize_t index):
        self._set_null(index)

    cpdef void set_validity_mask(self, const uint8_t[::1] mask):
        """Install a user-supplied validity bitmap for the entire builder."""
        cdef Py_ssize_t nb_size = (self._length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        if mask.shape[0] < nb_size:
            raise ValueError("validity mask is too small for declared length")
        if self._ptr.null_bitmap == NULL:
            self._ptr.null_bitmap = <uint8_t*> malloc(nb_size)
            if self._ptr.null_bitmap == NULL:
                raise MemoryError()
        memcpy(self._ptr.null_bitmap, &mask[0], nb_size)
        self._mask_user_provided = True

    cpdef StringVector finish(self):
        """Finalize construction and hand off the built vector.

        Phase 6: the builder accumulates values into the legacy DrakenVarBuffer
        (`self._vec.ptr`), then converts to a DrakenStringArena (one slot per
        row) at finish time. _unified_view.data points at the arena, not the
        VarBuffer. `vec.ptr` is kept allocated as a transitional shell with
        NULL'd data/offsets; its null_bitmap remains the canonical row-level
        null bitmap. Phase 7 will retire `vec.ptr` entirely.
        """
        if self._finished:
            return self._vec
        if self._next_index != self._length:
            raise ValueError(
                f"builder incomplete: appended {self._next_index} of {self._length} entries"
            )
        if self._offsets[self._length] != self._offset:
            self._offsets[self._length] = <int32_t>self._offset
        if self._strict_capacity and self._offset != self._bytes_cap:
            raise ValueError(
                f"builder consumed {self._offset} bytes but expected {self._bytes_cap}"
            )
        self._finished = True

        cdef Py_ssize_t row_count = self._length
        cdef DrakenStringArena* arena = _varbuffer_to_string_arena(
            <const uint8_t*>self._vec.ptr.data,
            self._vec.ptr.offsets,
            self._vec.ptr.null_bitmap,
            row_count,
        )

        # Phase 6 dual-alive: keep VarBuffer intact alongside the arena until
        # morsel.pyx and other callers have been migrated away from ptr.offsets/ptr.data.
        # Phase 7 will free data and offsets once all callers use arena exclusively.

        # Wire the StringArena as the unified-view payload.

        self._vec._unified_view = draken_vector_from_dense(
            <void*>arena, <uint32_t>row_count, DRAKEN_STRING,
            self._vec.ptr.null_bitmap,
        )
        # Track A: populate min/max metadata over the new arena.
        _populate_dense_min_max(self._vec)
        return self._vec

    cdef inline void _set_null(self, Py_ssize_t index) except *:
        self._require_index(index)
        self._ensure_capacity(0)
        self._initialize_null_bitmap()
        self._nulls[index >> 3] &= ~(1 << (index & 7))
        self._next_index += 1
        self._offsets[self._next_index] = <int32_t>self._offset

    cdef inline void _ensure_capacity(self, Py_ssize_t to_add) except *:
        if to_add <= 0:
            return
        if self._offset + to_add <= self._bytes_cap:
            return
        if not self._resizable:
            raise ValueError("not enough remaining capacity for value")

        cdef Py_ssize_t new_cap = self._bytes_cap
        if new_cap == 0:
            new_cap = max(to_add, 64)
        else:
            while new_cap < self._offset + to_add:
                new_cap = new_cap * 2

        cdef uint8_t* new_data
        if self._data == NULL:
            new_data = <uint8_t*> malloc(new_cap)
        else:
            new_data = <uint8_t*> realloc(self._ptr.data, new_cap)
        if new_data == NULL:
            raise MemoryError()

        # Update cached pointers
        self._ptr.data = new_data
        self._data = <char*>new_data
        self._bytes_cap = new_cap

    cdef inline void _initialize_null_bitmap(self) except *:
        cdef Py_ssize_t nb_size
        if self._nulls == NULL:
            nb_size = (self._length + 7) // 8
            if nb_size == 0:
                nb_size = 1
            self._ptr.null_bitmap = <uint8_t*> malloc(nb_size)
            if self._ptr.null_bitmap == NULL:
                raise MemoryError()
            # Update cached pointer
            self._nulls = self._ptr.null_bitmap
            memset(self._nulls, 0xFF, nb_size)
            self._mask_user_provided = False

    cdef inline void _require_index(self, Py_ssize_t index) except *:
        if self._finished:
            raise ValueError("builder already finished")
        if index < 0 or index >= self._length:
            raise IndexError("index out of bounds")
        if index != self._next_index:
            raise IndexError(f"builder expects index {self._next_index}, got {index}")
        if self._offsets == NULL:
            raise ValueError("builder offsets buffer is missing")


cdef StringVector from_arrow(object array):
    """
    Wrap an Arrow StringArray without copying.
    Keeps references to Arrow buffers to prevent GC from freeing memory.
    """
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "StringVector.from_arrow expects a dense string/binary Arrow array; "
            "use StringVector.from_dict for dictionary input"
        )

    if pa.types.is_large_string(array.type):
        array = array.cast(pa.string())
    elif pa.types.is_large_binary(array.type):
        array = array.cast(pa.binary())

    cdef StringVector vec = StringVector(0, 0, True)
    vec.ptr = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_offs_buf = bufs[1]
    vec._arrow_data_buf = bufs[2]

    vec.ptr.length = <size_t> len(array)
    cdef Py_ssize_t offset = array.offset

    # Data buffer (bytes)
    cdef intptr_t data_addr = bufs[2].address
    vec.ptr.data = <uint8_t*> data_addr

    # Offsets buffer (int32_t[length+1])
    cdef intptr_t offs_addr = bufs[1].address
    vec.ptr.offsets = (<int32_t*> offs_addr) + offset

    # Null bitmap (optional)
    cdef intptr_t nb_addr
    cdef Py_ssize_t nb_size
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef Py_ssize_t i
    cdef object new_bitmap_bytes

    if bufs[0] is not None:
        nb_addr = bufs[0].address

        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            # Unaligned offset: must copy and shift
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            # memset(dst_bitmap, 0, nb_size) # Not needed as we overwrite

            src_bitmap = <uint8_t*> nb_addr

            copy_bitmap_shifted(src_bitmap, dst_bitmap, offset, len(array))

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    vec.ptr.type = DRAKEN_STRING
    # Phase 6 dual-alive: build a StringArena alongside the VarBuffer.
    cdef DrakenStringArena* fa_arena = _varbuffer_to_string_arena(
        <const uint8_t*>vec.ptr.data,
        vec.ptr.offsets,
        vec.ptr.null_bitmap,
        <Py_ssize_t>vec.ptr.length,
    )
    # Phase 6: Keep VarBuffer pointer intact (Arrow-borrowed, not freed, owns_data=False).
    # Phase 7 will NULL vec.ptr.data / vec.ptr.offsets once morsel.pyx is migrated.

    vec._unified_view = draken_vector_from_dense(
        <void*>fa_arena, <uint32_t>vec.ptr.length, DRAKEN_STRING,
        vec.ptr.null_bitmap,
    )
    # Track A: dense Arrow ingest — compute min/max in one pass.
    _populate_dense_min_max(vec)
    return vec


cdef StringVector from_dict(const int32_t[::1] codes, list dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = len(dictionary)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t arena_bytes_count = 0
    cdef object value
    cdef bytes encoded
    cdef StringVectorBuilder builder
    cdef StringVector vec
    cdef int32_t* dict_offsets_buf = NULL
    cdef int32_t* dict_lengths_buf = NULL
    cdef uint8_t* arena_buf = NULL
    cdef int32_t[::1] dict_offsets_view
    cdef int32_t[::1] dict_lengths_view
    cdef uint8_t[::1] arena_view

    if dict_size == 0:
        raise ValueError("StringVector.from_dict requires a non-empty dictionary")

    if dict_size > 0:
        dict_offsets_buf = <int32_t*>malloc((dict_size + 1) * sizeof(int32_t))
        dict_lengths_buf = <int32_t*>malloc(dict_size * sizeof(int32_t))
        if dict_offsets_buf == NULL or dict_lengths_buf == NULL:
            if dict_offsets_buf != NULL:
                free(dict_offsets_buf)
            if dict_lengths_buf != NULL:
                free(dict_lengths_buf)
            raise MemoryError()

    dict_offsets_buf[0] = 0
    for i in range(dict_size):
        value = dictionary[i]
        if isinstance(value, str):
            encoded = (<str>value).encode("utf-8")
        else:
            encoded = <bytes>value
        arena_bytes_count += len(encoded)
        dict_lengths_buf[i] = <int32_t>len(encoded)
        dict_offsets_buf[i + 1] = <int32_t>arena_bytes_count

    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        value = dictionary[code]
        if isinstance(value, str):
            total_bytes += len(<str>value)
        else:
            total_bytes += len(<bytes>value)

    if arena_bytes_count > 0:
        arena_buf = <uint8_t*>malloc(arena_bytes_count)
        if arena_buf == NULL:
            free(dict_offsets_buf)
            free(dict_lengths_buf)
            raise MemoryError()
        for i in range(dict_size):
            value = dictionary[i]
            if isinstance(value, str):
                encoded = (<str>value).encode("utf-8")
            else:
                encoded = <bytes>value
            if len(encoded) > 0:
                memcpy(arena_buf + dict_offsets_buf[i], PyBytes_AS_STRING(encoded), len(encoded))

    try:
        builder = StringVectorBuilder.with_counts(row_count, total_bytes)
        for i in range(row_count):
            code = <Py_ssize_t>codes[i]
            if code < 0 or code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            value = dictionary[code]
            if isinstance(value, str):
                encoded = (<str>value).encode("utf-8")
            else:
                encoded = <bytes>value
            builder.append(encoded)

        vec = builder.finish()
        dict_offsets_view = <int32_t[:dict_size + 1]>dict_offsets_buf
        dict_lengths_view = <int32_t[:dict_size]>dict_lengths_buf
        arena_view = <uint8_t[:arena_bytes_count]>arena_buf
        _attach_dictionary_storage_from_buffers(vec, codes, dict_offsets_view, dict_lengths_view, arena_view, False)
        return vec
    finally:
        if dict_offsets_buf != NULL:
            free(dict_offsets_buf)
        if dict_lengths_buf != NULL:
            free(dict_lengths_buf)
        if arena_buf != NULL:
            free(arena_buf)


cdef StringVector from_dict_nullable(
    const int32_t[::1] codes,
    list dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = len(dictionary)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t arena_bytes_count = 0
    cdef object value
    cdef bytes encoded
    cdef StringVectorBuilder builder
    cdef StringVector vec
    cdef int32_t* dict_offsets_buf = NULL
    cdef int32_t* dict_lengths_buf = NULL
    cdef uint8_t* arena_buf = NULL
    cdef int32_t[::1] dict_offsets_view
    cdef int32_t[::1] dict_lengths_view
    cdef uint8_t[::1] arena_view

    if dict_size == 0:
        raise ValueError("StringVector.from_dict requires a non-empty dictionary")
    if row_validity.shape[0] != row_count:
        raise ValueError("row_validity length must match codes length")

    if dict_size > 0:
        dict_offsets_buf = <int32_t*>malloc((dict_size + 1) * sizeof(int32_t))
        dict_lengths_buf = <int32_t*>malloc(dict_size * sizeof(int32_t))
        if dict_offsets_buf == NULL or dict_lengths_buf == NULL:
            if dict_offsets_buf != NULL:
                free(dict_offsets_buf)
            if dict_lengths_buf != NULL:
                free(dict_lengths_buf)
            raise MemoryError()

    dict_offsets_buf[0] = 0
    for i in range(dict_size):
        value = dictionary[i]
        if isinstance(value, str):
            encoded = (<str>value).encode("utf-8")
        else:
            encoded = <bytes>value
        arena_bytes_count += len(encoded)
        dict_lengths_buf[i] = <int32_t>len(encoded)
        dict_offsets_buf[i + 1] = <int32_t>arena_bytes_count

    for i in range(row_count):
        if row_validity[i] != 0:
            code = <Py_ssize_t>codes[i]
            if code < 0 or code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            value = dictionary[code]
            if isinstance(value, str):
                total_bytes += len(<str>value)
            else:
                total_bytes += len(<bytes>value)

    if arena_bytes_count > 0:
        arena_buf = <uint8_t*>malloc(arena_bytes_count)
        if arena_buf == NULL:
            free(dict_offsets_buf)
            free(dict_lengths_buf)
            raise MemoryError()
        for i in range(dict_size):
            value = dictionary[i]
            if isinstance(value, str):
                encoded = (<str>value).encode("utf-8")
            else:
                encoded = <bytes>value
            if len(encoded) > 0:
                memcpy(arena_buf + dict_offsets_buf[i], PyBytes_AS_STRING(encoded), len(encoded))

    try:
        builder = StringVectorBuilder.with_counts(row_count, total_bytes)
        for i in range(row_count):
            if row_validity[i] != 0:
                code = <Py_ssize_t>codes[i]
                if code < 0 or code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                value = dictionary[code]
                if isinstance(value, str):
                    encoded = (<str>value).encode("utf-8")
                else:
                    encoded = <bytes>value
                builder.append(encoded)
            else:
                builder.append_null()

        vec = builder.finish()
        dict_offsets_view = <int32_t[:dict_size + 1]>dict_offsets_buf
        dict_lengths_view = <int32_t[:dict_size]>dict_lengths_buf
        arena_view = <uint8_t[:arena_bytes_count]>arena_buf
        _attach_dictionary_storage_from_buffers(vec, codes, dict_offsets_view, dict_lengths_view, arena_view, False)
        return vec
    finally:
        if dict_offsets_buf != NULL:
            free(dict_offsets_buf)
        if dict_lengths_buf != NULL:
            free(dict_lengths_buf)
        if arena_buf != NULL:
            free(arena_buf)


cdef StringVector from_dict_buffers(
    const int32_t[::1] codes,
    const int32_t[::1] dict_offsets,
    const int32_t[::1] dict_lengths,
    const uint8_t[::1] arena_bytes,
    object row_validity=None,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dict_lengths.shape[0]
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t total_bytes = 0
    cdef uint8_t[::1] validity_view
    cdef const char* arena_ptr
    cdef StringVectorBuilder builder
    cdef StringVector vec

    if row_count == 0:
        vec = StringVector(0, 0)
        return vec
    if dict_size == 0:
        raise ValueError("StringVector.from_dict_buffers requires a non-empty dictionary")
    if dict_offsets.shape[0] != dict_size:
        raise ValueError("dict_offsets length must match dict_lengths length")

    for i in range(dict_size):
        if dict_offsets[i] < 0 or dict_lengths[i] < 0:
            raise ValueError("dictionary offsets and lengths must be non-negative")
        if dict_offsets[i] + dict_lengths[i] > arena_bytes.shape[0]:
            raise ValueError("dictionary offset/length out of arena bounds")

    if row_validity is not None:
        validity_view = row_validity
        if validity_view.shape[0] != row_count:
            raise ValueError("row_validity length must match codes length")
        for i in range(row_count):
            if validity_view[i] != 0:
                code = <Py_ssize_t>codes[i]
                if code < 0 or code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                total_bytes += dict_lengths[code]
    else:
        for i in range(row_count):
            code = <Py_ssize_t>codes[i]
            if code < 0 or code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            total_bytes += dict_lengths[code]

    builder = StringVectorBuilder.with_counts(row_count, total_bytes)
    arena_ptr = <const char*>&arena_bytes[0] if arena_bytes.shape[0] > 0 else NULL

    if row_validity is not None:
        for i in range(row_count):
            if validity_view[i] != 0:
                code = <Py_ssize_t>codes[i]
                builder.append_bytes(arena_ptr + dict_offsets[code], dict_lengths[code])
            else:
                builder.append_null()
    else:
        for i in range(row_count):
            code = <Py_ssize_t>codes[i]
            builder.append_bytes(arena_ptr + dict_offsets[code], dict_lengths[code])

    vec = builder.finish()
    _attach_dictionary_storage_from_buffers(vec, codes, dict_offsets, dict_lengths, arena_bytes, False)
    return vec


cdef StringVector from_dict_buffers_dict_only(
    const int32_t[::1] codes,
    const int32_t[::1] dict_offsets,
    const int32_t[::1] dict_lengths,
    const uint8_t[::1] arena_bytes,
    object row_validity=None,
):
    """Same inputs as from_dict_buffers, but produces a dictionary-encoded StringVector
    (data_length < length in the unified view). Used by the cpp-pipeline so
    downstream Morsel.slice / StringVector.take hit the dict-preserving fast path
    instead of materialising a fresh string buffer per slice."""
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dict_lengths.shape[0]
    cdef Py_ssize_t i
    cdef uint8_t[::1] validity_view
    cdef Py_ssize_t nb_bytes
    cdef StringVector vec

    if row_count == 0:
        vec = StringVector(0, 0)
        return vec
    if dict_size == 0:
        raise ValueError("StringVector.from_dict_buffers_dict_only requires a non-empty dictionary")
    if dict_offsets.shape[0] != dict_size:
        raise ValueError("dict_offsets length must match dict_lengths length")

    for i in range(dict_size):
        if dict_offsets[i] < 0 or dict_lengths[i] < 0:
            raise ValueError("dictionary offsets and lengths must be non-negative")
        if dict_offsets[i] + dict_lengths[i] > arena_bytes.shape[0]:
            raise ValueError("dictionary offset/length out of arena bounds")

    if row_validity is not None:
        validity_view = row_validity
        if validity_view.shape[0] != row_count:
            raise ValueError("row_validity length must match codes length")
        for i in range(row_count):
            if validity_view[i] != 0:
                if codes[i] < 0 or codes[i] >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {codes[i]}")
    else:
        for i in range(row_count):
            if codes[i] < 0 or codes[i] >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {codes[i]}")

    # Build the dict-only skeleton: ptr.data and ptr.offsets stay NULL.
    vec = StringVector(0, 0, True)
    vec.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False
    vec.ptr.data = NULL
    vec.ptr.offsets = NULL
    vec.ptr.null_bitmap = NULL
    vec.ptr.length = <size_t>row_count
    vec.ptr.type = DRAKEN_STRING

    # Convert byte-per-row validity into an Arrow-style bitmap on ptr.null_bitmap.
    if row_validity is not None and row_count > 0:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memset(vec.ptr.null_bitmap, 0, <size_t>nb_bytes)
        for i in range(row_count):
            if validity_view[i] != 0:
                vec.ptr.null_bitmap[i >> 3] |= (1 << (i & 7))

    # Codes + dict storage: reuse the existing helper.
    _attach_dictionary_storage_from_buffers(
        vec, codes, dict_offsets, dict_lengths, arena_bytes, False
    )
    # Track A: min/max over dict values (small N).
    _populate_dict_min_max(vec)
    return vec


cdef StringVector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef int32_t* expanded_codes = NULL
    cdef int32_t* lengths_buf = NULL
    cdef uint8_t* row_validity = NULL
    cdef int32_t[::1] codes_view
    cdef int32_t[::1] offsets_view
    cdef int32_t[::1] lengths_view
    cdef uint8_t[::1] arena_view
    cdef uint8_t[::1] validity_view
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t arena_size
    cdef StringVector vec
    cdef const uint8_t* codes_u8
    cdef const uint16_t* codes_u16
    cdef const uint32_t* codes_u32

    if row_count == 0:
        vec = StringVector(0, 0)
        return vec
    if dict_size == 0:
        raise ValueError("StringVector.from_packed_dict requires a non-empty dictionary")
    if code_width != 1 and code_width != 2 and code_width != 4:
        raise ValueError("unsupported packed dictionary code width")

    if row_count > 0:
        expanded_codes = <int32_t*>malloc(row_count * sizeof(int32_t))
        if expanded_codes == NULL:
            raise MemoryError()
        if row_null_bitmap != NULL:
            row_validity = <uint8_t*>malloc(row_count)
            if row_validity == NULL:
                free(expanded_codes)
                raise MemoryError()
    lengths_buf = <int32_t*>malloc(dict_size * sizeof(int32_t))
    if lengths_buf == NULL:
        if expanded_codes != NULL:
            free(expanded_codes)
        if row_validity != NULL:
            free(row_validity)
        raise MemoryError()

    try:
        for i in range(dict_size):
            lengths_buf[i] = dict_offsets[i + 1] - dict_offsets[i]
        if code_width == 1:
            codes_u8 = codes
            for i in range(row_count):
                if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    expanded_codes[i] = 0
                    row_validity[i] = 0
                    continue
                code = <uint32_t>codes_u8[i]
                if code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                expanded_codes[i] = <int32_t>code
                if row_validity != NULL:
                    row_validity[i] = 1
        elif code_width == 2:
            codes_u16 = <const uint16_t*>codes
            for i in range(row_count):
                if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    expanded_codes[i] = 0
                    row_validity[i] = 0
                    continue
                code = <uint32_t>codes_u16[i]
                if code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                expanded_codes[i] = <int32_t>code
                if row_validity != NULL:
                    row_validity[i] = 1
        else:
            codes_u32 = <const uint32_t*>codes
            for i in range(row_count):
                if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    expanded_codes[i] = 0
                    row_validity[i] = 0
                    continue
                code = codes_u32[i]
                if code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
                expanded_codes[i] = <int32_t>code
                if row_validity != NULL:
                    row_validity[i] = 1

        codes_view = <int32_t[:row_count]>expanded_codes
        offsets_view = <int32_t[:dict_size]><int32_t*>dict_offsets
        lengths_view = <int32_t[:dict_size]>lengths_buf
        arena_size = dict_offsets[dict_size]
        if arena_size > 0:
            arena_view = <uint8_t[:arena_size]><uint8_t*>dict_data
        else:
            # Cython memoryviews can't be 0-length from a raw pointer. Use an
            # empty bytearray so from_dict_buffers receives a valid view whose
            # shape[0] is genuinely 0; downstream callers correctly skip the
            # arena memcpy.
            arena_view = bytearray(0)
        if row_validity != NULL:
            validity_view = <uint8_t[:row_count]>row_validity
            return from_dict_buffers(codes_view, offsets_view, lengths_view, arena_view, validity_view)
        return from_dict_buffers(codes_view, offsets_view, lengths_view, arena_view)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)
        if lengths_buf != NULL:
            free(lengths_buf)
        if row_validity != NULL:
            free(row_validity)

cdef Py_ssize_t _dict_find_code(StringVector vec, const char* val_ptr, Py_ssize_t val_len) noexcept nogil:
    """Return the code index for val in vec's dictionary, or -1 if absent."""
    cdef DrakenStringArena* gdv = _string_arena(vec)
    cdef Py_ssize_t d = <Py_ssize_t>gdv.length
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t entry_len
    cdef const uint8_t* entry_data
    for i in range(d):
        slot = &gdv.slots[i]
        entry_len = str_length(slot)
        if <Py_ssize_t>entry_len != val_len:
            continue
        entry_data = str_data(slot, gdv.arena)
        if val_len == 0 or memcmp(<const char*>entry_data, val_ptr, <size_t>val_len) == 0:
            return i
    return -1


cdef BoolVector _codes_to_boolvector_eq(StringVector vec, Py_ssize_t target_code):
    """BoolVector: codes[i] == target_code, propagating nulls."""
    cdef const uint32_t* codes = vec._unified_view.selection
    cdef uint8_t* nb_ptr = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i
    cdef uint32_t code

    memset(dst, 0, nbytes)
    if nb_ptr != NULL and nbytes != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nb_ptr, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    for i in range(n):
        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = codes[i]
        if <Py_ssize_t>code == target_code:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef BoolVector _codes_to_boolvector_neq(StringVector vec, Py_ssize_t target_code):
    """BoolVector: codes[i] != target_code, propagating nulls."""
    cdef const uint32_t* codes = vec._unified_view.selection
    cdef uint8_t* nb_ptr = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i
    cdef uint32_t code

    memset(dst, 0, nbytes)
    if nb_ptr != NULL and nbytes != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nb_ptr, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    for i in range(n):
        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = codes[i]
        if <Py_ssize_t>code != target_code:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef BoolVector _dict_ordered_scalar(StringVector vec, bytes value, int op):
    """Dict-level ordered scalar compare without materializing. op: 0=lt,1=gt,2=lte,3=gte."""
    cdef DrakenStringArena* gdv = _string_arena(vec)
    cdef Py_ssize_t d = <Py_ssize_t>gdv.length
    cdef const char* val_ptr = PyBytes_AS_STRING(value)
    cdef Py_ssize_t val_len = len(value)
    cdef uint8_t* pass_array = <uint8_t*>malloc(d)
    cdef Py_ssize_t i
    cdef DrakenStringSlot* dos_slot
    cdef const uint8_t* dos_data
    cdef uint32_t dos_len
    cdef int cmp_res

    if pass_array == NULL:
        raise MemoryError()
    try:
        for i in range(d):
            dos_slot = &gdv.slots[i]
            dos_len = str_length(dos_slot)
            dos_data = str_data(dos_slot, gdv.arena)
            cmp_res = _compare_bytes_lex(
                dos_data, <Py_ssize_t>dos_len,
                <const uint8_t*>val_ptr, val_len,
            )
            if op == 0:
                pass_array[i] = 1 if cmp_res < 0 else 0
            elif op == 1:
                pass_array[i] = 1 if cmp_res > 0 else 0
            elif op == 2:
                pass_array[i] = 1 if cmp_res <= 0 else 0
            else:
                pass_array[i] = 1 if cmp_res >= 0 else 0
        return _dict_compare_pass_array(vec, pass_array)
    finally:
        free(pass_array)


cdef BoolVector _dict_compare_pass_array(StringVector vec, uint8_t* pass_array):
    """BoolVector built from a per-code pass_array[code] lookup, propagating nulls."""
    cdef const uint32_t* codes = vec._unified_view.selection
    cdef uint8_t* nb_ptr = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i
    cdef uint32_t code

    memset(dst, 0, nbytes)
    if nb_ptr != NULL and nbytes != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nb_ptr, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    for i in range(n):
        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = codes[i]
        if pass_array[code]:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef StringVector _materialize_dict_string(StringVector vec):
    """Expand a dict-only StringVector to a dense StringVector (no src ptr.data needed)."""
    if vec._unified_view.data_length >= vec._unified_view.length:
        raise ValueError("Dictionary vector missing required data structures")
    cdef DrakenStringArena* mat_gdict = _string_arena(vec)
    cdef const uint32_t* codes = vec._unified_view.selection
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef DrakenStringSlot* mat_slot
    cdef const uint8_t* mat_data
    cdef uint32_t mat_len
    cdef StringVectorBuilder builder
    cdef Py_ssize_t dict_size = <Py_ssize_t>mat_gdict.length

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            continue
        code = codes[i]
        if code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        mat_slot = &mat_gdict.slots[code]
        total_bytes += <Py_ssize_t>str_length(mat_slot)

    builder = StringVectorBuilder(<Py_ssize_t>n, total_bytes, resizable=False)
    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            code = codes[i]
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            mat_slot = &mat_gdict.slots[code]
            mat_len = str_length(mat_slot)
            mat_data = str_data(mat_slot, mat_gdict.arena)
            builder.append_bytes(<const char*>mat_data, <Py_ssize_t>mat_len)

    return builder.finish()


cdef StringVector make_string_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const uint32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    Py_ssize_t arena_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded StringVector with no dense materialization.

    Args:
        codes:        Packed code array (code_width bytes per row, row_count entries).
        code_width:   Bytes per code: 1, 2, or 4.
        row_count:    Total number of rows (including nulls).
        dict_offsets: Byte start offsets for each dict entry (dict_size entries; uint32_t).
        dict_data:    Raw string bytes for all dict entries (arena).
        dict_size:    Number of unique dictionary values.
        arena_size:   Total byte length of dict_data.
        valid_bits:   Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        StringVector with dict encoding; ptr.data is NULL (no dense storage).
    """
    cdef StringVector vec = StringVector(0, 0, True)  # wrap=True: ptr starts as NULL
    cdef Py_ssize_t nb_bytes
    cdef DrakenStringArena* gs_dict
    cdef DrakenStringSlot* gs_slot
    cdef Py_ssize_t i
    cdef uint32_t entry_len
    cdef uint64_t arena_offset
    cdef uint32_t* codes_u32 = NULL
    cdef const uint8_t* codes_u8
    cdef const uint16_t* codes_u16
    cdef const uint32_t* codes_u32_src

    # Allocate minimal ptr header (data=NULL, offsets=NULL — dict-only, no dense storage)
    vec.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False
    vec.ptr.data = NULL
    vec.ptr.offsets = NULL
    vec.ptr.null_bitmap = NULL
    vec.ptr.length = <size_t>row_count
    vec.ptr.type = DRAKEN_STRING

    # Null bitmap from Arrow-style valid_bits
    if valid_bits != NULL:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, valid_bits, <size_t>nb_bytes)

    # Expand packed codes to uint32 (unified format always uses uint32 selection)
    if row_count > 0:
        codes_u32 = <uint32_t*>malloc(<size_t>row_count * sizeof(uint32_t))
        if codes_u32 == NULL:
            raise MemoryError()
        if code_width == 1:
            codes_u8 = codes
            for i in range(row_count):
                codes_u32[i] = <uint32_t>codes_u8[i]
        elif code_width == 2:
            codes_u16 = <const uint16_t*>codes
            for i in range(row_count):
                codes_u32[i] = <uint32_t>codes_u16[i]
        else:  # code_width == 4
            codes_u32_src = <const uint32_t*>codes
            for i in range(row_count):
                codes_u32[i] = codes_u32_src[i]

    # Dictionary values: stored as DrakenStringArena (DrakenStringSlot slots + byte arena)
    gs_dict = alloc_string_arena(DRAKEN_STRING, <size_t>dict_size, <size_t>arena_size)
    if gs_dict == NULL:
        raise MemoryError()
    # Populate slots from dict_offsets / dict_data.
    # dict_offsets has dict_size entries (start offset per entry; no sentinel end).
    # Length of entry i = dict_offsets[i+1] - dict_offsets[i] for i < dict_size-1;
    # length of last entry = arena_size - dict_offsets[dict_size - 1].
    for i in range(dict_size):
        gs_slot = &gs_dict.slots[i]
        if i < dict_size - 1:
            entry_len = dict_offsets[i + 1] - dict_offsets[i]
        else:
            entry_len = <uint32_t>(<Py_ssize_t>arena_size - <Py_ssize_t>dict_offsets[i])
        if entry_len <= <uint32_t>STR_INLINE_MAX:
            str_init_inline(gs_slot, dict_data + dict_offsets[i], entry_len)
        else:
            # arena_offset is the byte offset into gs_dict.arena where this string lives
            str_init_extern(gs_slot, dict_data + dict_offsets[i], entry_len, <uint64_t>dict_offsets[i])
    if arena_size > 0:
        memcpy(gs_dict.arena, <const void*>dict_data, <size_t>arena_size)
    gs_dict.arena_used = <size_t>arena_size
    gs_dict.length = <size_t>dict_size
    vec._owns_codes = (codes_u32 != NULL)


    vec._unified_view = draken_vector_from_dict(
        <void*>gs_dict, <uint32_t>dict_size,
        codes_u32, <uint32_t>row_count,
        DRAKEN_STRING, vec.ptr.null_bitmap,
    )

    # When dict_size >= row_count the discriminant (data_length < length) cannot
    # distinguish dict from dense. Materialize a VarBuffer so dense-path readers
    # can fall back to ptr.data/offsets (which are always row-indexed).
    cdef Py_ssize_t mat_total_bytes
    cdef Py_ssize_t mat_i
    cdef uint32_t mat_code
    cdef uint32_t mat_entry_len
    cdef uint8_t* mat_data
    cdef int32_t* mat_offsets
    cdef int32_t mat_pos
    if dict_size >= row_count and row_count > 0 and codes_u32 != NULL:
        mat_total_bytes = 0
        for mat_i in range(row_count):
            mat_code = codes_u32[mat_i]
            if mat_code < <uint32_t>dict_size:
                if mat_code < <uint32_t>dict_size - 1:
                    mat_entry_len = dict_offsets[mat_code + 1] - dict_offsets[mat_code]
                else:
                    mat_entry_len = <uint32_t>(<Py_ssize_t>arena_size - <Py_ssize_t>dict_offsets[mat_code])
                mat_total_bytes += <Py_ssize_t>mat_entry_len

        mat_data = <uint8_t*>malloc(<size_t>(mat_total_bytes if mat_total_bytes > 0 else 1))
        mat_offsets = <int32_t*>malloc(<size_t>((row_count + 1) * sizeof(int32_t)))
        if mat_data != NULL and mat_offsets != NULL:
            mat_offsets[0] = 0
            mat_pos = 0
            for mat_i in range(row_count):
                mat_code = codes_u32[mat_i]
                if mat_code < <uint32_t>dict_size:
                    if mat_code < <uint32_t>dict_size - 1:
                        mat_entry_len = dict_offsets[mat_code + 1] - dict_offsets[mat_code]
                    else:
                        mat_entry_len = <uint32_t>(<Py_ssize_t>arena_size - <Py_ssize_t>dict_offsets[mat_code])
                    if mat_entry_len > 0:
                        memcpy(mat_data + mat_pos, dict_data + dict_offsets[mat_code], <size_t>mat_entry_len)
                        mat_pos += mat_entry_len
                mat_offsets[mat_i + 1] = mat_pos
            vec.ptr.data = mat_data
            vec.ptr.offsets = mat_offsets
        else:
            free(mat_data)
            free(mat_offsets)

    # Track A: min/max over dict values (small N).
    _populate_dict_min_max(vec)
    return vec


cdef void copy_bitmap_shifted(uint8_t* src, uint8_t* dst, Py_ssize_t offset, Py_ssize_t length) noexcept nogil:
    cdef Py_ssize_t i
    cdef int shift = offset & 7
    cdef Py_ssize_t byte_offset = offset >> 3
    cdef Py_ssize_t num_bytes = (length + 7) // 8

    if shift == 0:
        memcpy(dst, src + byte_offset, num_bytes)
        return

    # Process all bytes except the last one
    for i in range(num_bytes - 1):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))

    # Handle the last byte
    i = num_bytes - 1
    cdef Py_ssize_t last_bit_index = offset + length - 1
    cdef Py_ssize_t last_byte_index = last_bit_index >> 3

    if last_byte_index > (byte_offset + i):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))
    else:
        dst[i] = (src[byte_offset + i] >> shift)

cdef inline bint is_null(uint8_t* bitmap, Py_ssize_t i):
    """Check if row i is null, given Arrow-style bitmap (1=valid, 0=null)."""
    if bitmap == NULL:
        return False
    return not ((bitmap[i >> 3] >> (i & 7)) & 1)

cdef StringVector from_arrow_struct(object array):
    """
    Convert an Arrow StructArray into a StringVector of JSON strings.
    Each row becomes {"field": value, ...}
    """
    cdef Py_ssize_t n = len(array)
    cdef list field_names = [f.name for f in array.type]
    cdef int nfields = len(field_names)
    cdef Py_ssize_t nb_size

    # crude capacity guess: 64 bytes per row
    cdef StringVector vec = StringVector(n, n * 64, False)
    vec.owns_data = True
    cdef DrakenVarBuffer* ptr = vec.ptr

    cdef object bufs = array.buffers()
    cdef intptr_t nb_addr
    cdef uint8_t* parent_null_bitmap = NULL
    if bufs[0] is not None:
        nb_addr = bufs[0].address
        parent_null_bitmap = <uint8_t*> nb_addr

        # allocate and copy null bitmap into Draken
        nb_size = (n + 7) // 8
        ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(ptr.null_bitmap, parent_null_bitmap, nb_size)
    else:
        ptr.null_bitmap = NULL

    cdef Py_ssize_t offset = 0
    cdef Py_ssize_t i, j
    cdef bytes json_bytes
    cdef const char* jb_ptr

    ptr.offsets[0] = 0

    for i in range(n):
        if is_null(parent_null_bitmap, i):
            # just carry forward same offset (null row = empty string)
            ptr.offsets[i+1] = offset
            continue

        # build JSON row as Python string for now
        row_items = []
        for j in range(nfields):
            val = array.field(j)[i].as_py()
            if val is None:
                row_items.append(f'"{field_names[j]}": null')
            elif isinstance(val, str):
                # naive escaping
                row_items.append(f'"{field_names[j]}": "{val}"')
            else:
                row_items.append(f'"{field_names[j]}": {val}')
        json_str = "{" + ",".join(row_items) + "}"
        json_bytes = json_str.encode("utf8")

        jb_ptr = PyBytes_AS_STRING(json_bytes)
        memcpy(<char*>ptr.data + offset, jb_ptr, len(json_bytes))

        offset += len(json_bytes)
        ptr.offsets[i+1] = offset

    # Phase 6 dual-alive: build a StringArena alongside the VarBuffer.
    # Phase 7 will free VarBuffer data/offsets once morsel.pyx is migrated.
    cdef DrakenStringArena* json_arena = _varbuffer_to_string_arena(
        <const uint8_t*>vec.ptr.data,
        vec.ptr.offsets,
        vec.ptr.null_bitmap,
        <Py_ssize_t>vec.ptr.length,
    )

    vec._unified_view = draken_vector_from_dense(
        <void*>json_arena, <uint32_t>vec.ptr.length, DRAKEN_STRING,
        vec.ptr.null_bitmap,
    )
    return vec

cdef StringVector _materialize_const_string(StringVector const_vec):
    """Expand a CONSTANT StringVector to a dense StringVector."""
    cdef Py_ssize_t n = <Py_ssize_t>const_vec.ptr.length
    cdef StringVectorBuilder builder
    cdef Py_ssize_t val_len
    cdef Py_ssize_t i

    cdef _ConstView _cv_mat
    if const_vec._unified_view.validity != NULL or const_vec._unified_view.data == NULL:
        builder = StringVectorBuilder(n, 0)
        for i in range(n):
            builder.append_null()
    else:
        _cv_mat = _const_view(<DrakenStringArena*>const_vec._unified_view.data)
        val_len = <Py_ssize_t>_cv_mat.length
        builder = StringVectorBuilder(n, n * val_len)
        for i in range(n):
            builder.append_bytes(<char*>_cv_mat.data, val_len)
    return builder.finish()



#################################

cpdef StringVector uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    Operates on the StringArena encoding; applies SIMD transform per string.
    """
    cdef Py_ssize_t n = input.ptr.length
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>input._unified_view.data
    cdef DrakenStringArena* out_arena
    cdef DrakenStringSlot* in_slot
    cdef uint8_t* nb = input.ptr.null_bitmap
    cdef Py_ssize_t i
    cdef Py_ssize_t slen
    cdef const uint8_t* sdata
    cdef Py_ssize_t nb_size

    # constant: data_length == 1
    if input._unified_view.data_length == 1:
        builder = StringVectorBuilder.with_estimate(n, 16)
        for val in input.to_pylist():
            if val is None:
                builder.append_null()
            else:
                builder.append(val.upper())
        return builder.finish()

    # Allocate output arena with same capacity as input
    out_arena = alloc_string_arena(DRAKEN_STRING, n, in_arena.arena_used)

    # Copy arena bytes, then SIMD-transform the whole arena buffer
    if in_arena.arena_used > 0:
        memcpy(out_arena.arena, in_arena.arena, in_arena.arena_used)
        simd_to_upper(<char*>out_arena.arena, in_arena.arena_used)
    out_arena.arena_used = in_arena.arena_used

    # Copy slots; for inline strings, apply transform to inline bytes in the raw slot memory.
    # Long-form strings are handled by the SIMD pass over the arena buffer above.
    memcpy(out_arena.slots, in_arena.slots, n * sizeof(DrakenStringSlot))
    for i in range(n):
        if nb != NULL and ((nb[i >> 3] >> (i & 7)) & 1) == 0:
            continue  # null row: leave slot as-is
        in_slot = &out_arena.slots[i]
        slen = str_length(<const DrakenStringSlot*>in_slot)
        if slen > 0 and str_is_inline(<const DrakenStringSlot*>in_slot):
            # Inline bytes are embedded in the slot; transform in-place.
            # str_data returns a pointer into the slot's inline storage.
            simd_to_upper(<char*>str_data(<const DrakenStringSlot*>in_slot, NULL), slen)

    # Copy null bitmap
    cdef uint8_t* out_nb = NULL
    if nb != NULL:
        nb_size = (n + 7) // 8
        out_nb = <uint8_t*>malloc(nb_size)
        if out_nb == NULL:
            free_string_arena(out_arena)
            raise MemoryError()
        memcpy(out_nb, nb, nb_size)

    # Build result StringVector (wrap=True: no VarBuffer allocated by __cinit__)
    cdef StringVector result = StringVector(0, 0, wrap=True)
    result.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if result.ptr == NULL:
        free_string_arena(out_arena)
        if out_nb != NULL:
            free(out_nb)
        raise MemoryError()
    result.ptr.data = NULL
    result.ptr.offsets = NULL
    result.ptr.null_bitmap = out_nb
    result.ptr.length = n
    result.ptr.type = DRAKEN_STRING
    result.owns_data = True

    result._unified_view = draken_vector_from_dense(
        <void*>out_arena, <uint32_t>n, DRAKEN_STRING, out_nb,
    )
    return result


cpdef StringVector lowercase(StringVector input):
    """
    Return a new StringVector with all non-null values lowercased.
    Operates on the StringArena encoding; applies SIMD transform per string.
    """
    cdef Py_ssize_t n = input.ptr.length
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>input._unified_view.data
    cdef DrakenStringArena* out_arena
    cdef DrakenStringSlot* in_slot
    cdef uint8_t* nb = input.ptr.null_bitmap
    cdef Py_ssize_t i
    cdef Py_ssize_t slen
    cdef Py_ssize_t nb_size

    # constant: data_length == 1
    if input._unified_view.data_length == 1:
        builder = StringVectorBuilder.with_estimate(n, 16)
        for val in input.to_pylist():
            if val is None:
                builder.append_null()
            else:
                builder.append(val.lower())
        return builder.finish()

    # Allocate output arena with same capacity as input
    out_arena = alloc_string_arena(DRAKEN_STRING, n, in_arena.arena_used)

    # Copy arena bytes, then SIMD-transform the whole arena buffer
    if in_arena.arena_used > 0:
        memcpy(out_arena.arena, in_arena.arena, in_arena.arena_used)
        simd_to_lower(<char*>out_arena.arena, in_arena.arena_used)
    out_arena.arena_used = in_arena.arena_used

    # Copy slots; for inline strings, apply transform to inline bytes in the raw slot memory
    memcpy(out_arena.slots, in_arena.slots, n * sizeof(DrakenStringSlot))
    for i in range(n):
        if nb != NULL and ((nb[i >> 3] >> (i & 7)) & 1) == 0:
            continue  # null row: leave slot as-is
        in_slot = &out_arena.slots[i]
        slen = str_length(<const DrakenStringSlot*>in_slot)
        if slen > 0 and str_is_inline(<const DrakenStringSlot*>in_slot):
            # Inline bytes are embedded in the slot; transform in-place
            simd_to_lower(<char*>str_data(<const DrakenStringSlot*>in_slot, NULL), slen)

    # Copy null bitmap
    cdef uint8_t* out_nb = NULL
    if nb != NULL:
        nb_size = (n + 7) // 8
        out_nb = <uint8_t*>malloc(nb_size)
        if out_nb == NULL:
            free_string_arena(out_arena)
            raise MemoryError()
        memcpy(out_nb, nb, nb_size)

    # Build result StringVector (wrap=True: no VarBuffer allocated by __cinit__)
    cdef StringVector result = StringVector(0, 0, wrap=True)
    result.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if result.ptr == NULL:
        free_string_arena(out_arena)
        if out_nb != NULL:
            free(out_nb)
        raise MemoryError()
    result.ptr.data = NULL
    result.ptr.offsets = NULL
    result.ptr.null_bitmap = out_nb
    result.ptr.length = n
    result.ptr.type = DRAKEN_STRING
    result.owns_data = True

    result._unified_view = draken_vector_from_dense(
        <void*>out_arena, <uint32_t>n, DRAKEN_STRING, out_nb,
    )
    return result


cpdef object split_single_char(StringVector input, char delimiter):
    """
    Fast SIMD-based split for single-character delimiter.
    Returns an ArrayVector where each element is an array of strings.

    Uses SIMD to find all delimiter positions (AVX2 on x86, NEON on ARM),
    then builds output in a single pass with dynamic allocation.
    """
    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef size_t n = input.length
    cdef char* data = <char*>in_ptr.data
    cdef int32_t* in_offsets = in_ptr.offsets
    cdef size_t total_bytes = in_offsets[n]
    cdef size_t i, j
    cdef int32_t start, end
    cdef size_t delim_pos

    # Find all delimiter positions in the entire buffer using SIMD
    cdef vector[size_t] delim_positions = simd_find_all(data, total_bytes, delimiter)
    cdef size_t num_delims = delim_positions.size()

    # Pre-allocate with upper bounds (worst case: every delimiter creates a segment)
    # This eliminates the counting pass
    cdef size_t max_segments = total_bytes + n  # Worst case: every byte is delimiter + 1 per string
    cdef size_t output_capacity = total_bytes  # Worst case: no delimiters removed

    # Create child StringVector with max possible segments
    cdef StringVector child_vec = StringVector(max_segments)
    cdef DrakenVarBuffer* child_ptr = child_vec.ptr

    # Allocate cache-aligned output buffer (64-byte aligned for optimal SIMD performance)
    cdef size_t alignment = 64
    cdef size_t aligned_size = output_capacity + alignment
    cdef void* raw_buffer = PyMem_Malloc(aligned_size)
    if raw_buffer == NULL:
        raise MemoryError()

    # Align to 64-byte boundary (cache line)
    cdef uintptr_t addr = <uintptr_t>raw_buffer
    cdef uintptr_t aligned_addr = (addr + alignment - 1) & ~(alignment - 1)
    child_ptr.data = <uint8_t*>aligned_addr

    # Create ArrayVector
    cdef ArrayVector result = ArrayVector.__new__(ArrayVector)
    cdef DrakenArrayBuffer* arr_ptr = <DrakenArrayBuffer*>malloc(sizeof(DrakenArrayBuffer))
    if arr_ptr == NULL:
        raise MemoryError()

    arr_ptr.offsets = <int32_t*>PyMem_Malloc((n + 1) * sizeof(int32_t))
    if arr_ptr.offsets == NULL:
        raise MemoryError()
    arr_ptr.offsets[0] = 0

    arr_ptr.null_bitmap = NULL
    arr_ptr.length = n
    arr_ptr.values = NULL
    arr_ptr.value_type = DRAKEN_STRING

    result.ptr = arr_ptr
    result.owns_offsets = True
    result.owns_null_bitmap = False

    # Single pass: copy data and build offsets
    cdef char* child_data = <char*>child_ptr.data
    cdef size_t read_pos = 0
    cdef size_t write_pos = 0
    cdef size_t segment_idx = 0
    cdef size_t seg_len
    cdef size_t next_delim_pos
    cdef size_t delim_idx = 0

    # Process each input string
    for i in range(n):
        start = in_offsets[i]
        end = in_offsets[i + 1]

        # Skip delimiters before this string
        while delim_idx < num_delims and delim_positions[delim_idx] < start:
            delim_idx += 1

        # Process this string's segments
        read_pos = start

        # Handle each delimiter within this string
        while delim_idx < num_delims and delim_positions[delim_idx] < end:
            next_delim_pos = delim_positions[delim_idx]

            # Copy segment up to delimiter
            child_ptr.offsets[segment_idx] = write_pos
            seg_len = next_delim_pos - read_pos
            if seg_len > 0:
                # For small segments (< 16 bytes), avoid memcpy overhead
                if seg_len < 16:
                    for j in range(seg_len):
                        child_data[write_pos + j] = data[read_pos + j]
                else:
                    memcpy(child_data + write_pos, data + read_pos, seg_len)
                write_pos += seg_len

            segment_idx += 1
            read_pos = next_delim_pos + 1  # Skip delimiter
            delim_idx += 1

        # Copy final segment (after last delimiter or whole string if no delimiters)
        child_ptr.offsets[segment_idx] = write_pos
        seg_len = end - read_pos
        if seg_len > 0:
            # Same optimization for final segment
            if seg_len < 16:
                for j in range(seg_len):
                    child_data[write_pos + j] = data[read_pos + j]
            else:
                memcpy(child_data + write_pos, data + read_pos, seg_len)
            write_pos += seg_len

        segment_idx += 1

        # Set array offset for this row
        arr_ptr.offsets[i + 1] = segment_idx

    # Set actual segment count and final offsets
    child_ptr.length = segment_idx
    child_ptr.offsets[segment_idx] = write_pos

    # Attach child vector to ArrayVector
    result._child = child_vec

    return result
