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

from draken.core.buffers cimport ConstAccessor
from draken.core.buffers cimport DictAccessor
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from draken.core.buffers cimport DRAKEN_ENCODING_RLE
from draken.core.buffers cimport DrakenRLEBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenConstantStringPayload
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.var_vector cimport alloc_var_buffer, buf_dtype, free_var_buffer
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
    simd_mix_hash_from_dict_cw1,
    simd_mix_hash_from_dict_cw2,
    simd_mix_hash_from_dict_cw4,
    simd_mix_hash_from_dict_nullable_cw1,
    simd_mix_hash_from_dict_nullable_cw2,
    simd_mix_hash_from_dict_nullable_cw4,
    simd_popcount,
)
from draken.vectors.bool_vector cimport BoolVector

DEF STRING_HASH_CHUNK = 256


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


cdef inline uint8_t _dict_code_width_for_size(Py_ssize_t dict_size) noexcept:
    if dict_size <= 256:
        return 1
    if dict_size <= 65536:
        return 2
    return 4


cdef inline uint32_t _read_packed_code(const uint8_t* codes, uint8_t code_width, Py_ssize_t row_idx) noexcept nogil:
    if code_width == 1:
        return (<const uint8_t*>codes)[row_idx]
    if code_width == 2:
        return (<const uint16_t*>codes)[row_idx]
    return (<const uint32_t*>codes)[row_idx]


cdef void _release_dict_storage(StringVector vec) noexcept:
    if vec._dict_codes != NULL:
        free(vec._dict_codes)
        vec._dict_codes = NULL
    if vec._dict_values != NULL:
        free_var_buffer(vec._dict_values, True)
        vec._dict_values = NULL
    if vec._dict_code_counts != NULL:
        free(vec._dict_code_counts)
        vec._dict_code_counts = NULL
    vec._dict_code_counts_valid = False
    vec._dict_code_width = 0
    vec._dict_ordered = 0
    vec._dict_accessor.codes = NULL
    vec._dict_accessor.code_width = 0
    vec._dict_accessor.row_nulls = NULL
    vec._dict_accessor.length = 0
    vec._dict_accessor.dict_values = NULL
    vec._dict_accessor.value_type = DRAKEN_STRING
    if not vec._has_const:
        vec._encoding = DRAKEN_ENCODING_DENSE


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
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t bitmap_bytes
    cdef Py_ssize_t arena_size = arena_bytes.shape[0]
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef DrakenVarBuffer* dict_values

    _release_dict_storage(vec)

    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
    else:
        vec._dict_codes = NULL

    dict_values = alloc_var_buffer(DRAKEN_STRING, <size_t>dict_size, <size_t>arena_size)
    if dict_size > 0:
        for i in range(dict_size):
            dict_values.offsets[i] = dict_offsets[i]
        dict_values.offsets[dict_size] = <int32_t>arena_size
    if arena_size > 0:
        memcpy(dict_values.data, <const void*>&arena_bytes[0], <size_t>arena_size)

    if dict_entry_null_bitmap != NULL:
        bitmap_bytes = (dict_size + 7) >> 3
        dict_values.null_bitmap = <uint8_t*>malloc(<size_t>bitmap_bytes)
        if dict_values.null_bitmap == NULL:
            raise MemoryError()
        memcpy(dict_values.null_bitmap, dict_entry_null_bitmap, <size_t>bitmap_bytes)

    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code_width == 1:
            (<uint8_t*>vec._dict_codes)[i] = <uint8_t>code
        elif code_width == 2:
            (<uint16_t*>vec._dict_codes)[i] = <uint16_t>code
        else:
            (<uint32_t*>vec._dict_codes)[i] = <uint32_t>code

    vec._dict_values = dict_values
    vec._dict_code_width = code_width
    vec._dict_ordered = 1 if ordered else 0
    vec._encoding = DRAKEN_ENCODING_DICTIONARY


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
        cdef StringVector vec
        cdef DrakenConstantStringPayload* payload
        cdef bytes value_bytes
        cdef char* src
        cdef Py_ssize_t src_len

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
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._encoding = DRAKEN_ENCODING_CONSTANT

        payload = <DrakenConstantStringPayload*> malloc(sizeof(DrakenConstantStringPayload))
        if payload == NULL:
            raise MemoryError()
        payload.data = NULL
        payload.length = 0
        vec._const_value = payload

        if not is_null:
            value_bytes = _coerce_literal_bytes(value)
            if value_bytes is None:
                raise TypeError("StringVector.from_constant expects bytes-like or str value")
            src_len = len(value_bytes)
            payload.length = <int32_t>src_len
            if src_len > 0:
                src = PyBytes_AS_STRING(value_bytes)
                payload.data = <uint8_t*> malloc(<size_t>src_len)
                if payload.data == NULL:
                    raise MemoryError()
                memcpy(payload.data, src, <size_t>src_len)

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
        self._dict_values = NULL
        self._dict_codes = NULL
        self._dict_code_width = 0
        self._dict_ordered = 0
        self._dict_accessor.codes = NULL
        self._dict_accessor.code_width = 0
        self._dict_accessor.row_nulls = NULL
        self._dict_accessor.length = 0
        self._dict_accessor.dict_values = NULL
        self._dict_accessor.value_type = DRAKEN_STRING
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_STRING
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = NULL
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL
        self._dict_code_counts = NULL
        self._dict_code_counts_valid = False

    def __dealloc__(self):
        if self._rle_buffer != NULL:
            if self._rle_buffer.run_values != NULL:
                free(self._rle_buffer.run_values)
            if self._rle_buffer.run_lengths != NULL:
                free(self._rle_buffer.run_lengths)
            if self._rle_buffer.null_bitmap != NULL:
                free(self._rle_buffer.null_bitmap)
            if self._rle_buffer.run_str_lens != NULL:
                free(self._rle_buffer.run_str_lens)
            if self._rle_buffer.run_str_offsets != NULL:
                free(self._rle_buffer.run_str_offsets)
            free(self._rle_buffer)
            self._rle_buffer = NULL
        _release_dict_storage(self)
        if self._const_value != NULL:
            if self._const_value.data != NULL:
                free(self._const_value.data)
            free(self._const_value)
            self._const_value = NULL

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

    cdef DictAccessor* dict_accessor(self) noexcept:
        if self._dict_values == NULL or self._dict_codes == NULL or self.ptr == NULL:
            return NULL
        self._dict_accessor.codes = self._dict_codes
        self._dict_accessor.code_width = self._dict_code_width
        self._dict_accessor.row_nulls = self.ptr.null_bitmap
        self._dict_accessor.length = self.ptr.length
        self._dict_accessor.dict_values = self._dict_values
        self._dict_accessor.value_type = self._dict_values.type
        return &self._dict_accessor

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL or self._const_value == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_STRING
        self._const_accessor.value_ptr = <void*>self._const_value
        self._const_accessor.is_null = 1 if self._const_is_null else 0
        return &self._const_accessor

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL or self._has_const:
            return NULL
        return self.ptr.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL or self._has_const:
            return NULL
        return self.ptr.null_bitmap

    # ------------------------------------------------------------------
    # Encoded-form accessors (dict and RLE) for aggregation kernels.
    # The caller must check `self._encoding` before calling these; they
    # do not validate the encoding.
    # ------------------------------------------------------------------
    cdef Py_ssize_t c_length(self) noexcept nogil:
        if self.ptr == NULL:
            return 0
        return <Py_ssize_t>self.ptr.length

    cdef Py_ssize_t c_dict_size(self) noexcept nogil:
        if self._dict_values == NULL:
            return 0
        return <Py_ssize_t>self._dict_values.length

    cdef uint8_t c_dict_code_width(self) noexcept nogil:
        return self._dict_code_width

    cdef const uint8_t* c_dict_codes_ptr(self) noexcept nogil:
        return <const uint8_t*>self._dict_codes

    cdef const uint8_t* c_dict_value_ptr(
        self, Py_ssize_t i, Py_ssize_t* out_len
    ) noexcept nogil:
        cdef DrakenVarBuffer* dv = self._dict_values
        cdef int32_t start, end
        if dv == NULL or i < 0 or <size_t>i >= dv.length:
            out_len[0] = 0
            return NULL
        start = dv.offsets[i]
        end = dv.offsets[i + 1]
        out_len[0] = <Py_ssize_t>(end - start)
        return (<const uint8_t*>dv.data) + start

    cdef bint c_dict_value_is_null(self, Py_ssize_t i) noexcept nogil:
        cdef DrakenVarBuffer* dv = self._dict_values
        if dv == NULL or dv.null_bitmap == NULL:
            return False
        return not ((dv.null_bitmap[i >> 3] >> (i & 7)) & 1)

    cdef const uint8_t* c_row_null_bitmap(self) noexcept nogil:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

    cdef const int64_t* c_dict_code_counts_ptr(self) except NULL:
        """Return a pointer to a length-`dict_size` int64 array of per-code
        occurrence counts.  Computed once on first access and cached on the
        vector.  Counts only include rows that are *valid* (non-null in the
        row null bitmap)."""
        cdef DrakenVarBuffer* dv = self._dict_values
        cdef Py_ssize_t dict_size
        cdef Py_ssize_t n
        cdef Py_ssize_t i
        cdef uint32_t code
        cdef const uint8_t* codes
        cdef const uint8_t* row_nulls
        cdef int64_t* counts
        cdef uint8_t code_width

        if self._encoding != DRAKEN_ENCODING_DICTIONARY:
            raise ValueError("c_dict_code_counts_ptr: vector is not dict-encoded")
        if dv == NULL:
            raise ValueError("c_dict_code_counts_ptr: missing dictionary values")

        if self._dict_code_counts_valid and self._dict_code_counts != NULL:
            return self._dict_code_counts

        dict_size = <Py_ssize_t>dv.length
        # Allocate (calloc) and zero-fill; even for dict_size==0 we keep a
        # 1-byte allocation so the returned pointer is never NULL.
        if self._dict_code_counts != NULL:
            free(self._dict_code_counts)
            self._dict_code_counts = NULL

        counts = <int64_t*>malloc(<size_t>(dict_size if dict_size > 0 else 1) * sizeof(int64_t))
        if counts == NULL:
            raise MemoryError()
        memset(counts, 0, <size_t>(dict_size if dict_size > 0 else 1) * sizeof(int64_t))

        if dict_size > 0 and self._dict_codes != NULL and self.ptr != NULL:
            n = <Py_ssize_t>self.ptr.length
            codes = <const uint8_t*>self._dict_codes
            code_width = self._dict_code_width
            row_nulls = self.ptr.null_bitmap
            with nogil:
                for i in range(n):
                    if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
                        continue
                    code = _read_packed_code(codes, code_width, i)
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
        cdef DrakenVarBuffer* dv = self._dict_values
        cdef int32_t start, end
        cdef size_t str_len
        cdef uint64_t per_string_hash
        cdef uint64_t scratch
        if dv == NULL or i < 0 or <size_t>i >= dv.length:
            return NULL_HASH
        if dv.null_bitmap != NULL and not ((dv.null_bitmap[i >> 3] >> (i & 7)) & 1):
            return NULL_HASH
        start = dv.offsets[i]
        end = dv.offsets[i + 1]
        str_len = <size_t>(end - start)
        if str_len <= 32:
            per_string_hash = _short_string_hash(<const uint8_t*>dv.data + start, str_len)
        else:
            per_string_hash = XXH3_64bits(<const void*>(<const uint8_t*>dv.data + start), str_len)
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
        if self._encoding != DRAKEN_ENCODING_DICTIONARY:
            raise ValueError("dict_value_at: vector is not dict-encoded")
        if i < 0 or i >= self.c_dict_size():
            raise IndexError("dict index out of range")
        if self.c_dict_value_is_null(i):
            return None
        p = self.c_dict_value_ptr(i, &length)
        return PyBytes_FromStringAndSize(<const char*>p, length)

    def dict_code_at(self, Py_ssize_t i):
        if self._encoding != DRAKEN_ENCODING_DICTIONARY:
            raise ValueError("dict_code_at: vector is not dict-encoded")
        if self._dict_codes == NULL:
            raise ValueError("dict_code_at: missing codes")
        if i < 0 or i >= <Py_ssize_t>self.ptr.length:
            raise IndexError("row index out of range")
        return <Py_ssize_t>_read_packed_code(
            <const uint8_t*>self._dict_codes, self._dict_code_width, i
        )

    def dict_code_counts(self):
        cdef const int64_t* counts
        cdef Py_ssize_t dict_size
        cdef Py_ssize_t i
        if self._encoding != DRAKEN_ENCODING_DICTIONARY:
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

    @property
    def dictionary_value_type(self):
        if self._dict_values == NULL:
            return None
        return self._dict_values.type

    @property
    def dictionary_size(self):
        if self._dict_values == NULL:
            return 0
        return self._dict_values.length

    @property
    def code_width(self):
        return self._dict_code_width if self._dict_values != NULL else None

    @property
    def ordered(self):
        return bool(self._dict_ordered) if self._dict_values != NULL else False

    def to_arrow(self):
        """
        Zero-copy conversion to Arrow StringArray (bytes-based).
        Keeps a reference to this vector to prevent premature garbage collection.
        """
        import pyarrow as pa

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).to_arrow()

        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.binary())
            return pa.array(
                [PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)] * self.ptr.length,
                type=pa.binary(),
            )

        cdef DrakenVarBuffer* ptr = self.ptr
        if ptr == NULL:
            # Defensive: empty vector
            return pa.array([], type=pa.binary())
        cdef size_t n = ptr.length

        # Data buffer: all the concatenated string bytes
        # Pass self as base object to keep the vector alive
        # If there are no bytes or the data pointer is NULL, create an empty
        # pyarrow buffer instead of passing a NULL pointer to foreign_buffer
        total_bytes = ptr.offsets[n]
        if total_bytes <= 0 or ptr.data == NULL:
            data_buf = pa.py_buffer(b"")
        else:
            data_buf = pa.foreign_buffer(<intptr_t>ptr.data, total_bytes, base=self)

        # Offsets buffer: (n+1) * int32_t entries
        offs_buf = pa.foreign_buffer(<intptr_t>ptr.offsets, (n + 1) * sizeof(int32_t), base=self)

        # Null bitmap buffer (optional)
        if ptr.null_bitmap != NULL:
            null_buf = pa.foreign_buffer(<intptr_t>ptr.null_bitmap, (n + 7) // 8, base=self)
        else:
            null_buf = None

        return pa.Array.from_buffers(pa.binary(), n, [null_buf, offs_buf, data_buf])

    cdef object item_at(self, Py_ssize_t i):
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self)[i]
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint8_t byte, bit
        cdef int32_t start, end
        cdef Py_ssize_t nbytes
        cdef char* base

        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")

        if self._has_const:
            if self._const_is_null:
                return None
            return PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)

        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None

        start = ptr.offsets[i]
        end = ptr.offsets[i+1]
        nbytes = end - start
        base = <char*>ptr.data
        return PyBytes_FromStringAndSize(base + start, nbytes)

    def __getitem__(self, Py_ssize_t i):
        """Return entry i as raw bytes, or None if null."""
        return self.item_at(i)

    def __iter__(self):
        if self._has_const:
            return iter(self.to_pylist())
        return _StringVectorIterator(self)

    def c_iter(self):
        """Return a C-level iterator for high-performance kernel operations."""
        if self._has_const:
            raise NotImplementedError("StringVector.c_iter() is not available for constant encoding")
        return _StringVectorCIterator._from_ptr(self.ptr)

    cpdef Py_ssize_t byte_length(self, Py_ssize_t i):
        """Return the number of bytes for row ``i`` without materializing the value."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).byte_length(i)
        cdef DrakenVarBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")
        if self._has_const:
            if self._const_is_null:
                return 0
            return self._const_value.length
        return ptr.offsets[i + 1] - ptr.offsets[i]

    cpdef object buffers(self):
        """Expose data, offsets, and null bitmap buffers as zero-copy views."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).buffers()
        if self._has_const:
            raise NotImplementedError("StringVector.buffers() is not available for constant encoding")
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t total_bytes = ptr.offsets[n]
        cdef object data_view

        if total_bytes <= 0 or ptr.data == NULL:
            data_view = memoryview(b"")
        else:
            data_view = <uint8_t[:total_bytes]> ptr.data

        return (
            data_view,
            <int32_t[:n + 1]> ptr.offsets,
            self.null_bitmap(),
        )

    cpdef object null_bitmap(self):
        """Return the null bitmap as a Python ``memoryview``, or ``None`` if all values are valid."""
        if self._has_const:
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
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).lengths()
        if self._has_const:
            raise NotImplementedError("StringVector.lengths() is not available for constant encoding")
        return <int32_t[: self.ptr.length + 1]> self.ptr.offsets

    cpdef object view(self):
        """Return a lightweight pointer/length view for zero-copy consumers."""
        if self._has_const:
            raise NotImplementedError("StringVector.view() is not available for constant encoding")
        return _StringVectorView(self)

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVarBuffer* ptr
        cdef Py_ssize_t n
        cdef Py_ssize_t nb_size
        cdef Py_ssize_t bits_in_last
        cdef Py_ssize_t valid_count
        cdef uint8_t last_byte_mask
        cdef uint8_t byte_val

        ptr = self.ptr
        n = ptr.length
        if self._has_const:
            return n if self._const_is_null else 0
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

    cpdef Vector materialize(self):
        """Return a dense StringVector, expanding dict/const/RLE encodings if needed."""
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef StringVectorBuilder builder
        cdef Py_ssize_t i, val_len, data_bytes, off_start, off_end
        cdef const char* data_ptr
        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            if ptr.data == NULL:
                # dict-only path (make_string_dict_only): codes in _dict_codes, expand via dict
                return _materialize_dict_string(self)
            else:
                # from_dict_buffers path: dense data already in ptr.data + ptr.offsets
                data_bytes = <Py_ssize_t>ptr.offsets[n] if ptr.offsets != NULL else 0
                builder = StringVectorBuilder(n, data_bytes)
                data_ptr = <const char*>ptr.data
                for i in range(n):
                    if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
                        builder.append_null()
                    else:
                        off_start = <Py_ssize_t>ptr.offsets[i]
                        off_end = <Py_ssize_t>ptr.offsets[i + 1]
                        builder.append_bytes(data_ptr + off_start, off_end - off_start)
                return builder.finish()
        if self._has_const:
            if self._const_is_null or self._const_value == NULL:
                builder = StringVectorBuilder(n, 0)
                for i in range(n):
                    builder.append_null()
            else:
                val_len = <Py_ssize_t>self._const_value.length
                builder = StringVectorBuilder(n, n * val_len)
                for i in range(n):
                    builder.append_bytes(<char*>self._const_value.data, val_len)
            return builder.finish()
        return self

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, offset_bytes, null_bytes
        cdef uint64_t dict_data_bytes, dict_offset_bytes, code_bytes
        if self._has_const:
            return <uint64_t>self._const_value.length if self._const_value != NULL else 0
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            code_bytes = n * self._dict_code_width
            if self._dict_values != NULL and self._dict_values.offsets != NULL:
                dict_data_bytes = <uint64_t><uint32_t>self._dict_values.offsets[self._dict_values.length]
                dict_offset_bytes = (<uint64_t>self._dict_values.length + 1) * sizeof(int32_t)
            else:
                dict_data_bytes = 0
                dict_offset_bytes = 0
            null_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
            return code_bytes + dict_data_bytes + dict_offset_bytes + null_bytes
        # Dense
        data_bytes = <uint64_t><uint32_t>ptr.offsets[n] if ptr.offsets != NULL else 0
        offset_bytes = (n + 1) * sizeof(int32_t)
        null_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + offset_bytes + null_bytes

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if self._has_const:
            for i in range(n):
                buf[i] = 1 if self._const_is_null else 0
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
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            _eq_code = _dict_find_code(self, PyBytes_AS_STRING(value), len(value))
            if _eq_code < 0:
                return BoolVector(<size_t>self.ptr.length)
            return _codes_to_boolvector_eq(self, _eq_code)
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int cmp_res

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i

        # Process in chunks for better cache performance
        for i in range(n):
            # Check null first (most likely to fail)
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start

            # Length check before expensive memcmp
            if str_len != val_len:
                continue

            if memcmp(<char*>ptr.data + start, val_ptr, str_len) == 0:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector not_equals(self, bytes value):
        """Return mask: 1 if not equal to value, else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            _neq_code = _dict_find_code(self, PyBytes_AS_STRING(value), len(value))
            if _neq_code < 0:
                return _codes_to_boolvector_neq(self, <Py_ssize_t>self._dict_values.length)
            return _codes_to_boolvector_neq(self, _neq_code)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            if str_len != <int32_t>val_len:
                dst[i >> 3] |= (1 << (i & 7))
            elif memcmp(<char*>ptr.data + start, val_ptr, <size_t>str_len) != 0:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector less_than(self, bytes value):
        """Return mask: 1 if element < value (lexicographic bytes), else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _dict_ordered_scalar(self, value, 0)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(<char*>ptr.data + start, val_ptr, <size_t>min_len)
            if cmp_res < 0 or (cmp_res == 0 and str_len < <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector greater_than(self, bytes value):
        """Return mask: 1 if element > value (lexicographic bytes), else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _dict_ordered_scalar(self, value, 1)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(<char*>ptr.data + start, val_ptr, <size_t>min_len)
            if cmp_res > 0 or (cmp_res == 0 and str_len > <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector less_than_or_equals(self, bytes value):
        """Return mask: 1 if element <= value (lexicographic bytes), else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _dict_ordered_scalar(self, value, 2)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(<char*>ptr.data + start, val_ptr, <size_t>min_len)
            if cmp_res < 0 or (cmp_res == 0 and str_len <= <int32_t>val_len):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector greater_than_or_equals(self, bytes value):
        """Return mask: 1 if element >= value (lexicographic bytes), else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _dict_ordered_scalar(self, value, 3)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>self._const_value.data,
                self._const_value.length,
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            min_len = str_len if str_len < <int32_t>val_len else <int32_t>val_len
            cmp_res = 0
            if min_len > 0:
                cmp_res = memcmp(<char*>ptr.data + start, val_ptr, <size_t>min_len)
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
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).equals_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.equals_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).equals_vector(other)
        if other._has_const:
            return self.equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 0)

    cpdef BoolVector not_equals_vector(self, StringVector other):
        """Element-wise op 1 between two StringVectors with null propagation."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).not_equals_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.not_equals_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).not_equals_vector(other)
        if other._has_const:
            return self.not_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 1)

    cpdef BoolVector less_than_vector(self, StringVector other):
        """Element-wise op 2 between two StringVectors with null propagation."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).less_than_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.less_than_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).less_than_vector(other)
        if other._has_const:
            return self.less_than_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 2)

    cpdef BoolVector less_than_or_equals_vector(self, StringVector other):
        """Element-wise op 3 between two StringVectors with null propagation."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).less_than_or_equals_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.less_than_or_equals_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).less_than_or_equals_vector(other)
        if other._has_const:
            return self.less_than_or_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 3)

    cpdef BoolVector greater_than_vector(self, StringVector other):
        """Element-wise op 4 between two StringVectors with null propagation."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).greater_than_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.greater_than_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).greater_than_vector(other)
        if other._has_const:
            return self.greater_than_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 4)

    cpdef BoolVector greater_than_or_equals_vector(self, StringVector other):
        """Element-wise op 5 between two StringVectors with null propagation."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).greater_than_or_equals_vector(other)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self.greater_than_or_equals_vector(_materialize_dict_string(other))
        if self._has_const:
            return _materialize_const_string(self).greater_than_or_equals_vector(other)
        if other._has_const:
            return self.greater_than_or_equals_vector(_materialize_const_string(other))
        return self._compare_vector_op(other, 5)

    cpdef BoolVector in_list(self, object value_set):
        """
        Return mask: 1 if element is a member of value_set, else 0. Propagates NULLs.
        value_set must be a set or frozenset of bytes.
        """
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).in_list(value_set)
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

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cell_bytes = PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)
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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            cell_bytes = PyBytes_FromStringAndSize(<char*>ptr.data + start, <Py_ssize_t>str_len)
            if cell_bytes in value_set:
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector like(self, bytes pattern, bint ignore_case=False):
        """Return mask: 1 if element matches SQL LIKE pattern, else 0. Propagates NULLs.

        Optimized for dictionary-encoded vectors: tests each unique value once.
        """
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
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i, dict_idx, dict_size
        cdef uint32_t code
        cdef DrakenVarBuffer* dict_values_buf
        cdef const uint8_t* dict_data
        cdef uint8_t* dict_like_results = NULL
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            return _constant_bool_result(
                n,
                _sv_sql_like_match(
                    <const uint8_t*>self._const_value.data,
                    self._const_value.length,
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
            if self._encoding == DRAKEN_ENCODING_DICTIONARY:
                dict_values_buf = self._dict_values
                if dict_values_buf == NULL or dict_values_buf.data == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>dict_values_buf.length
                dict_codes = self._dict_codes
                if dict_codes == NULL or dict_size == 0:
                    return out  # Fallback to empty result

                dict_code_width = self._dict_code_width
                dict_row_nulls = self.ptr.null_bitmap
                dict_data = <const uint8_t*>dict_values_buf.data

                # Allocate results array for each dictionary entry
                dict_like_results = <uint8_t*>malloc(dict_size)
                if dict_like_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    start = dict_values_buf.offsets[dict_idx]
                    end = dict_values_buf.offsets[dict_idx + 1]
                    str_len = end - start

                    if _sv_sql_like_match(
                        dict_data + start, <Py_ssize_t>str_len,
                        <const uint8_t*>pat_ptr, pat_len, ignore_case,
                    ):
                        dict_like_results[dict_idx] = 1
                    else:
                        dict_like_results[dict_idx] = 0

                # Scatter results by code index
                for i in range(n):
                    if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    code = _read_packed_code(dict_codes, dict_code_width, i)
                    if dict_like_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant)
            else:
                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    start = ptr.offsets[i]
                    end = ptr.offsets[i + 1]
                    str_len = end - start
                    if _sv_sql_like_match(
                        <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
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
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i, dict_idx, dict_size
        cdef uint32_t code
        cdef bytes cell_bytes
        cdef DrakenVarBuffer* dict_values_buf
        cdef const uint8_t* dict_data
        cdef uint8_t* dict_rlike_results = NULL
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls

        compiled = re.compile(pattern)

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            cell_bytes = PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)
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
            if self._encoding == DRAKEN_ENCODING_DICTIONARY:
                dict_values_buf = self._dict_values
                if dict_values_buf == NULL or dict_values_buf.data == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>dict_values_buf.length
                dict_codes = self._dict_codes
                if dict_codes == NULL or dict_size == 0:
                    return out  # Fallback to empty result

                dict_code_width = self._dict_code_width
                dict_row_nulls = self.ptr.null_bitmap
                dict_data = <const uint8_t*>dict_values_buf.data

                # Allocate results array for each dictionary entry
                dict_rlike_results = <uint8_t*>malloc(dict_size)
                if dict_rlike_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    start = dict_values_buf.offsets[dict_idx]
                    end = dict_values_buf.offsets[dict_idx + 1]
                    str_len = end - start
                    cell_bytes = PyBytes_FromStringAndSize(<char*>dict_data + start, <Py_ssize_t>str_len)
                    if compiled.search(cell_bytes) is not None:
                        dict_rlike_results[dict_idx] = 1
                    else:
                        dict_rlike_results[dict_idx] = 0

                # Scatter results by code index
                for i in range(n):
                    if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    code = _read_packed_code(dict_codes, dict_code_width, i)
                    if dict_rlike_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant)
            else:
                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    start = ptr.offsets[i]
                    end = ptr.offsets[i + 1]
                    str_len = end - start
                    cell_bytes = PyBytes_FromStringAndSize(<char*>ptr.data + start, <Py_ssize_t>str_len)
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
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i, j, dict_idx, dict_size
        cdef uint32_t code
        cdef uint8_t byte
        cdef DrakenVarBuffer* dict_values_buf
        cdef const uint8_t* dict_data
        cdef uint8_t* dict_contains_results = NULL
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls
        cdef uint8_t* data_lower = NULL
        cdef Py_ssize_t data_len
        cdef VolnitskyTable* tbl = NULL

        # Constant vector case
        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
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
                            <const uint8_t*>self._const_value.data,
                            self._const_value.length,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ),
                        False,
                    )
                return _constant_bool_result(
                    n,
                    _sv_contains_cs(
                        <const uint8_t*>self._const_value.data,
                        self._const_value.length,
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
            if self._encoding == DRAKEN_ENCODING_DICTIONARY:
                dict_values_buf = self._dict_values
                if dict_values_buf == NULL or dict_values_buf.data == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>dict_values_buf.length
                dict_codes = self._dict_codes
                if dict_codes == NULL or dict_size == 0:
                    return out  # Fallback to empty result

                dict_code_width = self._dict_code_width
                dict_row_nulls = self.ptr.null_bitmap
                dict_data = <const uint8_t*>dict_values_buf.data

                # Allocate results array for each dictionary entry
                dict_contains_results = <uint8_t*>malloc(dict_size)
                if dict_contains_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    start = dict_values_buf.offsets[dict_idx]
                    end = dict_values_buf.offsets[dict_idx + 1]
                    str_len = end - start

                    if ignore_case:
                        if _sv_contains_ci(
                            dict_data + start, <Py_ssize_t>str_len,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ):
                            dict_contains_results[dict_idx] = 1
                        else:
                            dict_contains_results[dict_idx] = 0
                    else:
                        if _sv_contains_cs(
                            dict_data + start, <Py_ssize_t>str_len,
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
                    code = _read_packed_code(dict_codes, dict_code_width, i)
                    if dict_contains_results[code]:
                        dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant)
            else:
                # For case-insensitive: pre-lowercase entire buffer once
                if ignore_case and ptr.data != NULL:
                    data_len = ptr.offsets[n]
                    data_lower = <uint8_t*>malloc(data_len)
                    if data_lower == NULL:
                        raise MemoryError()
                    # Copy and lowercase entire buffer in one pass
                    for j in range(data_len):
                        data_lower[j] = _sv_ascii_lower((<const uint8_t*>ptr.data)[j])

                # Process each row
                for i in range(n):
                    if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    start = ptr.offsets[i]
                    end = ptr.offsets[i + 1]
                    str_len = end - start

                    if ignore_case:
                        # Use pre-lowercased buffer for case-sensitive search
                        if _sv_contains_cs(
                            data_lower + start, <Py_ssize_t>str_len,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
                            tbl,
                        ):
                            dst[i >> 3] |= (1 << (i & 7))
                    else:
                        if _sv_contains_cs(
                            <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
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
        cdef size_t rle_r, rle_n
        cdef uint8_t* rle_arena
        cdef uint32_t* rle_run_offs
        cdef int32_t* rle_run_lens
        cdef int32_t* rle_run_counts
        cdef int32_t rle_j
        cdef object rle_s
        cdef DrakenVarBuffer* ptr
        cdef Py_ssize_t n
        cdef list out
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef char* data
        cdef uint8_t byte, bit
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).to_pylist()
        ptr = self.ptr
        n = ptr.length
        out = []
        data = <char*> ptr.data

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append(PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length))
            return out

        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            out.append(PyBytes_FromStringAndSize(data + start, end - start))

        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t value
        cdef Py_ssize_t i, j, block
        cdef uint8_t byte
        cdef size_t str_len
        cdef int32_t start, end
        cdef Py_ssize_t idx
        cdef uint64_t[STRING_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint64_t* dst
        cdef DrakenVarBuffer* dict_values_buf
        cdef Py_ssize_t dict_size
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls
        cdef uint64_t* dict_hashes_ptr = NULL
        cdef const uint8_t* data
        cdef uint32_t code
        cdef const uint8_t* dense_data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.hash_into: output buffer too small")

        if self._has_const:
            if self._const_is_null:
                value = NULL_HASH
            else:
                if self._const_value.length <= 32:
                    value = _short_string_hash(<const uint8_t*>self._const_value.data, <size_t>self._const_value.length)
                else:
                    value = XXH3_64bits(<const void*>self._const_value.data, <size_t>self._const_value.length)
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

        # Dictionary-encoded path
        dict_values_buf = self._dict_values
        dict_size = <Py_ssize_t>dict_values_buf.length
        dict_codes = self._dict_codes
        dict_code_width = self._dict_code_width
        dict_row_nulls = self.ptr.null_bitmap

        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                raise MemoryError("StringVector.hash_into: cannot allocate dict hash buffer")
            try:
                # Hash each dictionary entry
                data = <const uint8_t*>dict_values_buf.data
                for dict_idx in range(dict_size):
                    # Get the dictionary value at dict_idx
                    start = dict_values_buf.offsets[dict_idx]
                    end = dict_values_buf.offsets[dict_idx + 1]
                    str_len = <size_t>(end - start)
                    if str_len <= 32:
                        dict_hashes_ptr[dict_idx] = _short_string_hash(data + start, str_len)
                    else:
                        dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>(data + start), str_len)

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
                                code = _read_packed_code(dict_codes, dict_code_width, idx)
                                scratch[j] = dict_hashes_ptr[code]
                    else:
                        for j in range(block):
                            code = _read_packed_code(dict_codes, dict_code_width, i + j)
                            scratch[j] = dict_hashes_ptr[code]

                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            finally:
                free(dict_hashes_ptr)
            return

        # Dense (non-dictionary, non-constant) path
        dense_data = <const uint8_t*> ptr.data
        offsets = ptr.offsets
        nb_ptr = ptr.null_bitmap
        dst = &out_buf[offset]

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
                        start = offsets[idx]
                        end = offsets[idx + 1]
                        str_len = <size_t>(end - start)
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(dense_data + start, str_len)
                        else:
                            scratch[j] = XXH3_64bits(dense_data + start, str_len)
                else:
                    for j in range(block):
                        start = offsets[i + j]
                        end = offsets[i + j + 1]
                        str_len = <size_t>(end - start)
                        if str_len <= 32:
                            scratch[j] = _short_string_hash(dense_data + start, str_len)
                        else:
                            scratch[j] = XXH3_64bits(dense_data + start, str_len)

                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
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
        cdef DictAccessor* da
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls
        cdef DrakenVarBuffer* dict_values_buf
        cdef const uint8_t* data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr

        if n == 0:
            return 0

        if self._has_const:
            if self._const_is_null:
                value = NULL_HASH
            else:
                if self._const_value.length <= 32:
                    value = _short_string_hash(<const uint8_t*>self._const_value.data, <size_t>self._const_value.length)
                else:
                    value = XXH3_64bits(<const void*>self._const_value.data, <size_t>self._const_value.length)
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
        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            # Dictionary-encoded path: access member variables directly (no GIL needed)
            dict_values_buf = self._dict_values
            # Validate dictionary structures before dereferencing
            if dict_values_buf == NULL or dict_values_buf.data == NULL:
                return 1  # Fall back to Python hash
            dict_size = <Py_ssize_t>dict_values_buf.length
            dict_codes = self._dict_codes
            if dict_codes == NULL:
                return 1  # Fall back to Python hash
            dict_code_width = self._dict_code_width
            dict_row_nulls = self.ptr.null_bitmap

            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                return 1  # OOM, fall back to Python hash

            # Hash each dictionary entry
            data = <const uint8_t*>dict_values_buf.data
            for dict_idx in range(dict_size):
                # Get the dictionary value at dict_idx
                start = dict_values_buf.offsets[dict_idx]
                end = dict_values_buf.offsets[dict_idx + 1]
                str_len = <size_t>(end - start)
                if str_len <= 32:
                    dict_hashes_ptr[dict_idx] = _short_string_hash(data + start, str_len)
                else:
                    dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>(data + start), str_len)

            # Fused gather-and-mix: index dict_hashes_ptr by code and fold
            # directly into out[] without the per-chunk scratch buffer.
            # Specialized per code width (1/2/4 bytes) and per null/non-null
            # to keep the inner loop branch-free.
            if dict_row_nulls != NULL:
                if dict_code_width == 1:
                    simd_mix_hash_from_dict_nullable_cw1(
                        out, dict_hashes_ptr, dict_codes, dict_row_nulls, 0, <size_t>n)
                elif dict_code_width == 2:
                    simd_mix_hash_from_dict_nullable_cw2(
                        out, dict_hashes_ptr, <uint16_t*>dict_codes, dict_row_nulls, 0, <size_t>n)
                else:
                    simd_mix_hash_from_dict_nullable_cw4(
                        out, dict_hashes_ptr, <uint32_t*>dict_codes, dict_row_nulls, 0, <size_t>n)
            else:
                if dict_code_width == 1:
                    simd_mix_hash_from_dict_cw1(
                        out, dict_hashes_ptr, dict_codes, <size_t>n)
                elif dict_code_width == 2:
                    simd_mix_hash_from_dict_cw2(
                        out, dict_hashes_ptr, <uint16_t*>dict_codes, <size_t>n)
                else:
                    simd_mix_hash_from_dict_cw4(
                        out, dict_hashes_ptr, <uint32_t*>dict_codes, <size_t>n)
            free(dict_hashes_ptr)
            return 0

        # Dense (non-dictionary, non-constant) path
        # Validate that we have valid data and offset structures
        if ptr.data == NULL or ptr.offsets == NULL:
            return 1  # Fall back to Python hash

        data = <const uint8_t*> ptr.data
        offsets = ptr.offsets
        nb_ptr = ptr.null_bitmap

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
                    start = offsets[idx]
                    end = offsets[idx + 1]
                    str_len = <size_t>(end - start)
                    if str_len <= 32:
                        scratch[j] = _short_string_hash(data + start, str_len)
                    else:
                        scratch[j] = XXH3_64bits(data + start, str_len)
            else:
                for j in range(block):
                    start = offsets[i + j]
                    end = offsets[i + j + 1]
                    str_len = <size_t>(end - start)
                    if str_len <= 32:
                        scratch[j] = _short_string_hash(data + start, str_len)
                    else:
                        scratch[j] = XXH3_64bits(data + start, str_len)

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
        cdef DrakenVarBuffer* dict_values_buf
        cdef const uint8_t* data
        cdef int32_t* offsets
        cdef uint8_t* nb_ptr
        cdef const uint8_t* dict_codes
        cdef uint8_t dict_code_width
        cdef uint8_t* dict_row_nulls
        cdef uint32_t code

        if n == 0:
            return 0

        if self._has_const:
            if self._const_is_null:
                value = null_sentinel
            elif self._const_value.length <= 32:
                value = _short_string_hash(<const uint8_t*>self._const_value.data, <size_t>self._const_value.length)
            else:
                value = XXH3_64bits(<const void*>self._const_value.data, <size_t>self._const_value.length)
            for i in range(n):
                out[i] = value
            return 0

        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            dict_values_buf = self._dict_values
            if dict_values_buf == NULL or dict_values_buf.data == NULL:
                return 1
            dict_size = <Py_ssize_t>dict_values_buf.length
            dict_codes = self._dict_codes
            if dict_codes == NULL:
                return 1
            dict_code_width = self._dict_code_width
            dict_row_nulls = self.ptr.null_bitmap

            dict_hashes_ptr = <uint64_t*>malloc(dict_size * sizeof(uint64_t))
            if dict_hashes_ptr == NULL:
                return 1

            data = <const uint8_t*>dict_values_buf.data
            for dict_idx in range(dict_size):
                start = dict_values_buf.offsets[dict_idx]
                end = dict_values_buf.offsets[dict_idx + 1]
                str_len = <size_t>(end - start)
                if str_len <= 32:
                    dict_hashes_ptr[dict_idx] = _short_string_hash(data + start, str_len)
                else:
                    dict_hashes_ptr[dict_idx] = XXH3_64bits(<const void*>(data + start), str_len)

            # Scatter directly — no mix step.
            if dict_row_nulls != NULL:
                if dict_code_width == 1:
                    for i in range(n):
                        out[i] = null_sentinel if not ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) else dict_hashes_ptr[(<const uint8_t*>dict_codes)[i]]
                elif dict_code_width == 2:
                    for i in range(n):
                        out[i] = null_sentinel if not ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) else dict_hashes_ptr[(<const uint16_t*>dict_codes)[i]]
                else:
                    for i in range(n):
                        out[i] = null_sentinel if not ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) else dict_hashes_ptr[(<const uint32_t*>dict_codes)[i]]
            else:
                if dict_code_width == 1:
                    for i in range(n):
                        out[i] = dict_hashes_ptr[(<const uint8_t*>dict_codes)[i]]
                elif dict_code_width == 2:
                    for i in range(n):
                        out[i] = dict_hashes_ptr[(<const uint16_t*>dict_codes)[i]]
                else:
                    for i in range(n):
                        out[i] = dict_hashes_ptr[(<const uint32_t*>dict_codes)[i]]

            free(dict_hashes_ptr)
            return 0

        if ptr.data == NULL or ptr.offsets == NULL:
            return 1

        data = <const uint8_t*>ptr.data
        offsets = ptr.offsets
        nb_ptr = ptr.null_bitmap

        if nb_ptr != NULL:
            for i in range(n):
                byte = nb_ptr[i >> 3]
                if ((byte >> (i & 7)) & 1) == 0:
                    out[i] = null_sentinel
                    continue
                start = offsets[i]
                end = offsets[i + 1]
                str_len = <size_t>(end - start)
                if str_len <= 32:
                    out[i] = _short_string_hash(data + start, str_len)
                else:
                    out[i] = XXH3_64bits(data + start, str_len)
        else:
            for i in range(n):
                start = offsets[i]
                end = offsets[i + 1]
                str_len = <size_t>(end - start)
                if str_len <= 32:
                    out[i] = _short_string_hash(data + start, str_len)
                else:
                    out[i] = XXH3_64bits(data + start, str_len)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for StringVector: pack first 7 bytes into big-endian int64."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            _materialize_dict_string(self).compress_into(out_buf, offset)
            return
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.compress: output buffer too small")

        cdef int32_t start, end
        cdef Py_ssize_t i, j, copy_len
        cdef char* base = <char*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t acc

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out_buf[offset + i] = <int64_t>(-(1 << 63))
            else:
                copy_len = self._const_value.length
                if copy_len > 7:
                    copy_len = 7
                acc = <uint64_t>0
                memcpy(&acc, <const void*>self._const_value.data, <size_t>copy_len)
                acc = BSWAP64(acc)
                for i in range(n):
                    out_buf[offset + i] = <int64_t>acc
            return

        for i in range(n):
            if has_nulls and ((null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                out_buf[offset + i] = <int64_t>(-(1 << 63))
                continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            copy_len = end - start
            if copy_len > 7:
                copy_len = 7
            acc = <uint64_t>0
            memcpy(&acc, <const void*>(base + start), <size_t>copy_len)
            acc = BSWAP64(acc)
            out_buf[offset + i] = <int64_t>acc

    cpdef StringVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t out_n
        cdef Py_ssize_t out_i
        cdef int32_t out_src_idx
        cdef uint32_t gathered_code
        cdef uint8_t code_width_local
        cdef Py_ssize_t code_bytes
        cdef uint8_t* src_codes
        cdef uint8_t* dst_codes
        cdef DrakenVarBuffer* src_dict_values
        cdef DrakenVarBuffer* dst_dict_values
        cdef Py_ssize_t take_dict_size
        cdef Py_ssize_t dict_arena_size
        cdef Py_ssize_t nb_bytes_dict
        cdef bint src_has_row_nulls
        cdef uint8_t* src_row_nulls
        cdef uint8_t* dst_row_nulls
        cdef uint8_t src_bit_local
        cdef Py_ssize_t src_len_check
        cdef StringVector dict_result

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            # Dict-in → dict-out: gather codes for the requested rows, share the
            # dictionary verbatim. Avoids materializing ~N decoded strings just
            # to throw most away. The dictionary is typically tiny relative to
            # the row count, so the per-row work drops to a packed-code copy.
            out_n = indices.shape[0]
            code_width_local = self._dict_code_width
            src_codes = self._dict_codes
            src_dict_values = self._dict_values
            src_row_nulls = self.ptr.null_bitmap
            src_has_row_nulls = src_row_nulls != NULL
            src_len_check = <Py_ssize_t>self.ptr.length

            if src_dict_values == NULL:
                raise ValueError("dict-encoded vector has no dictionary values")

            take_dict_size = <Py_ssize_t>src_dict_values.length
            dict_arena_size = <Py_ssize_t>src_dict_values.offsets[take_dict_size]

            # Bounds-check indices up front (matches the dense path's behavior).
            for out_i in range(out_n):
                out_src_idx = indices[out_i]
                if out_src_idx < 0 or out_src_idx >= src_len_check:
                    raise IndexError(
                        f"Index {out_src_idx} out of bounds for length {src_len_check}"
                    )

            dict_result = StringVector(0, 0, True)
            dict_result.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
            if dict_result.ptr == NULL:
                raise MemoryError()
            dict_result.owns_data = False
            dict_result.ptr.data = NULL
            dict_result.ptr.offsets = NULL
            dict_result.ptr.null_bitmap = NULL
            dict_result.ptr.length = <size_t>out_n
            dict_result.ptr.type = DRAKEN_STRING

            # Gather packed codes.
            code_bytes = out_n * <Py_ssize_t>code_width_local
            if code_bytes > 0:
                dst_codes = <uint8_t*>malloc(<size_t>code_bytes)
                if dst_codes == NULL:
                    raise MemoryError()
                if code_width_local == 1:
                    for out_i in range(out_n):
                        gathered_code = (<const uint8_t*>src_codes)[indices[out_i]]
                        (<uint8_t*>dst_codes)[out_i] = <uint8_t>gathered_code
                elif code_width_local == 2:
                    for out_i in range(out_n):
                        gathered_code = (<const uint16_t*>src_codes)[indices[out_i]]
                        (<uint16_t*>dst_codes)[out_i] = <uint16_t>gathered_code
                else:
                    for out_i in range(out_n):
                        gathered_code = (<const uint32_t*>src_codes)[indices[out_i]]
                        (<uint32_t*>dst_codes)[out_i] = <uint32_t>gathered_code
                dict_result._dict_codes = dst_codes

            # Gather row null bitmap if present.
            if src_has_row_nulls and out_n > 0:
                nb_bytes_dict = (out_n + 7) >> 3
                dst_row_nulls = <uint8_t*>malloc(<size_t>nb_bytes_dict)
                if dst_row_nulls == NULL:
                    raise MemoryError()
                memset(dst_row_nulls, 0, <size_t>nb_bytes_dict)
                for out_i in range(out_n):
                    out_src_idx = indices[out_i]
                    src_bit_local = (
                        (src_row_nulls[out_src_idx >> 3] >> (out_src_idx & 7)) & 1
                    )
                    if src_bit_local:
                        dst_row_nulls[out_i >> 3] |= (1 << (out_i & 7))
                dict_result.ptr.null_bitmap = dst_row_nulls

            # Copy the dictionary verbatim. The dictionary is typically small
            # relative to N, and copying keeps ownership simple (each vector
            # owns its dict storage; freeing one doesn't dangle the other).
            dst_dict_values = alloc_var_buffer(
                DRAKEN_STRING, <size_t>take_dict_size, <size_t>dict_arena_size
            )
            for out_i in range(take_dict_size + 1):
                dst_dict_values.offsets[out_i] = src_dict_values.offsets[out_i]
            if dict_arena_size > 0:
                memcpy(
                    dst_dict_values.data,
                    <const void*>src_dict_values.data,
                    <size_t>dict_arena_size,
                )
            # Copy dict-entry null bitmap if any.
            if src_dict_values.null_bitmap != NULL and take_dict_size > 0:
                nb_bytes_dict = (take_dict_size + 7) >> 3
                dst_dict_values.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes_dict)
                if dst_dict_values.null_bitmap == NULL:
                    raise MemoryError()
                memcpy(
                    dst_dict_values.null_bitmap,
                    <const void*>src_dict_values.null_bitmap,
                    <size_t>nb_bytes_dict,
                )
            dict_result._dict_values = dst_dict_values

            dict_result._dict_code_width = code_width_local
            dict_result._dict_ordered = self._dict_ordered
            dict_result._encoding = DRAKEN_ENCODING_DICTIONARY

            dict_result._dict_accessor.codes = dict_result._dict_codes
            dict_result._dict_accessor.code_width = code_width_local
            dict_result._dict_accessor.row_nulls = dict_result.ptr.null_bitmap
            dict_result._dict_accessor.length = <size_t>out_n
            dict_result._dict_accessor.dict_values = dict_result._dict_values

            return dict_result
        cdef DrakenVarBuffer* src_ptr = self.ptr
        cdef Py_ssize_t n = indices.shape[0]
        cdef size_t total_bytes = 0
        cdef Py_ssize_t i
        cdef int32_t src_idx
        cdef Py_ssize_t dict_size
        cdef int32_t* taken_codes = NULL
        cdef int32_t[::1] taken_codes_view
        cdef int32_t[::1] dictionary_offsets_view
        cdef int32_t[::1] dictionary_lengths_view
        cdef uint8_t[::1] dictionary_arena_view

        if self._has_const:
            if self._const_is_null or self._const_value == NULL or self._const_value.data == NULL:
                return StringVector.from_constant(None, n, is_null=True)
            else:
                return StringVector.from_constant(
                    PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length),
                    n,
                    is_null=False,
                )

        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= <Py_ssize_t> src_ptr.length:
                raise IndexError(
                    f"Index {src_idx} out of bounds for length {src_ptr.length}"
                )
            total_bytes += <size_t>(
                src_ptr.offsets[src_idx + 1] - src_ptr.offsets[src_idx]
            )

        cdef StringVector result = StringVector(<size_t> n, total_bytes)
        cdef DrakenVarBuffer* dst_ptr = result.ptr
        cdef char* src_data = <char*> src_ptr.data
        cdef char* dst_data = <char*> dst_ptr.data
        cdef int32_t* dst_offsets = dst_ptr.offsets
        cdef int32_t dst_offset = 0
        cdef bint has_nulls = src_ptr.null_bitmap != NULL
        cdef Py_ssize_t nb_size
        cdef int32_t start, end
        cdef int32_t byte_len
        cdef uint8_t src_bit

        dst_offsets[0] = 0

        if has_nulls and n > 0:
            nb_size = (n + 7) >> 3
            dst_ptr.null_bitmap = <uint8_t*> malloc(nb_size)
            if dst_ptr.null_bitmap == NULL:
                raise MemoryError()
            memset(dst_ptr.null_bitmap, 0, nb_size)
        else:
            dst_ptr.null_bitmap = NULL

        for i in range(n):
            src_idx = indices[i]
            start = src_ptr.offsets[src_idx]
            end = src_ptr.offsets[src_idx + 1]
            byte_len = end - start

            if byte_len > 0:
                memcpy(dst_data + dst_offset, src_data + start, byte_len)

            dst_offset += byte_len
            dst_offsets[i + 1] = dst_offset

            if has_nulls:
                src_bit = (
                    (src_ptr.null_bitmap[src_idx >> 3] >> (src_idx & 7)) & 1
                )
                if src_bit:
                    dst_ptr.null_bitmap[i >> 3] |= (1 << (i & 7))

        return result

    cpdef object min(self):
        """Return lexicographically smallest non-null string value, or None if all null or empty."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).min()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef int32_t best_start = -1, best_end = -1
        cdef char* data = <char*> ptr.data
        cdef int cmp
        cdef int32_t cur_len, best_len, common_len
        cdef char* best_ptr = NULL
        cdef char* cur_ptr
        cdef uint8_t byte, bit

        if self._has_const:
            if self._const_is_null:
                return None
            return PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)

        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]

            if best_start == -1:
                best_start = start
                best_end = end
                best_ptr = data + start
                continue

            cur_ptr = data + start
            cur_len = end - start
            best_len = best_end - best_start
            common_len = cur_len if cur_len < best_len else best_len
            cmp = memcmp(cur_ptr, best_ptr, common_len)
            # Tie on the shared prefix: shorter string is lex-smaller.
            if cmp == 0 and cur_len < best_len:
                cmp = -1
            if cmp < 0:
                best_start = start
                best_end = end
                best_ptr = cur_ptr

        if best_start == -1:
            return None
        return PyBytes_FromStringAndSize(best_ptr, best_end - best_start)

    cpdef object max(self):
        """Return lexicographically largest non-null string value, or None if all null or empty."""
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_string(self).max()
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef int32_t best_start = -1, best_end = -1
        cdef char* data = <char*> ptr.data
        cdef int cmp
        cdef int32_t cur_len, best_len, common_len
        cdef char* best_ptr = NULL
        cdef char* cur_ptr
        cdef uint8_t byte, bit

        if self._has_const:
            if self._const_is_null:
                return None
            return PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)

        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]

            if best_start == -1:
                best_start = start
                best_end = end
                best_ptr = data + start
                continue

            cur_ptr = data + start
            cur_len = end - start
            best_len = best_end - best_start
            common_len = cur_len if cur_len < best_len else best_len
            cmp = memcmp(cur_ptr, best_ptr, common_len)
            # Tie on the shared prefix: longer string is lex-larger.
            if cmp == 0 and cur_len > best_len:
                cmp = 1
            if cmp > 0:
                best_start = start
                best_end = end
                best_ptr = cur_ptr

        if best_start == -1:
            return None
        return PyBytes_FromStringAndSize(best_ptr, best_end - best_start)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two strings at given indices lexicographically. Returns -1, 0, 1. Assumes non-null."""
        cdef DrakenVarBuffer* ptr
        cdef char* data
        cdef int32_t left_start, left_end, right_start, right_end
        cdef Py_ssize_t left_len, right_len, common_len
        cdef int cmp_result
        cdef uint32_t lc, rc
        cdef const uint8_t* lp
        cdef const uint8_t* rp

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            # Dict-aware path: compare via packed codes without materializing the
            # dictionary. For an ordered dict, code order matches value order, so
            # a single integer compare suffices. Otherwise dereference into the
            # dict's varbuffer and memcmp the underlying bytes.
            lc = _read_packed_code(self._dict_codes, self._dict_code_width, left_idx)
            rc = _read_packed_code(self._dict_codes, self._dict_code_width, right_idx)
            if lc == rc:
                return 0
            if self._dict_ordered:
                return -1 if lc < rc else 1
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

        ptr = self.ptr
        data = <char*> ptr.data

        if self._has_const:
            return 0

        left_start = ptr.offsets[left_idx]
        left_end = ptr.offsets[left_idx + 1]
        right_start = ptr.offsets[right_idx]
        right_end = ptr.offsets[right_idx + 1]

        left_len = left_end - left_start
        right_len = right_end - right_start

        common_len = left_len if left_len < right_len else right_len
        cmp_result = memcmp(data + left_start, data + right_start, common_len)

        if cmp_result != 0:
            return -1 if cmp_result < 0 else 1

        if left_len < right_len:
            return -1
        elif left_len > right_len:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenVarBuffer* ptr = self.ptr

        if self._has_const:
            return self._const_is_null

        if ptr.null_bitmap == NULL:
            return False

        cdef uint8_t byte = ptr.null_bitmap[idx >> 3]
        return ((byte >> (idx & 7)) & 1) == 0

    cpdef sum(self):
        """Sum is not defined for string vectors."""
        raise NotImplementedError("sum() is not supported for StringVector")

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        k = min(<Py_ssize_t>self.ptr.length, 5)
        if self._has_const:
            vals = [None if self._const_is_null else PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)] * k
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

    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef char* _base
    cdef int32_t* _offsets
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    def __cinit__(self, StringVector vec):
        self._ptr = vec.ptr
        self._pos = 0
        self._length = self._ptr.length
        self._base = <char*>self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap
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

        cdef int32_t start = self._offsets[i]
        cdef int32_t end = self._offsets[i + 1]
        return PyBytes_FromStringAndSize(self._base + start, end - start)


cdef class _StringVectorCIterator:
    """
    Highly optimized C-level iterator with minimal overhead.
    """

    def __cinit__(self):
        # Initialize with NULL; must use _from_ptr factory method
        self._ptr = NULL
        self._pos = 0
        self._length = 0
        self._base = NULL
        self._offsets = NULL
        self._nulls = NULL
        self._has_nulls = False

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr):
        """Factory method to create iterator from a buffer pointer."""
        cdef _StringVectorCIterator it = _StringVectorCIterator.__new__(_StringVectorCIterator)
        it._ptr = ptr
        it._pos = 0
        it._length = ptr.length
        it._base = <char*>ptr.data
        it._offsets = ptr.offsets
        it._nulls = ptr.null_bitmap
        it._has_nulls = (it._nulls != NULL)
        return it

    cdef inline bint next(self, StringElement* elem) nogil:
        """
        Ultra-fast inline method for C-level iteration.
        """
        if self._pos >= self._length:
            return False

        cdef Py_ssize_t i = self._pos
        self._pos += 1

        # Check for null
        if self._has_nulls and ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            elem.ptr = self._base + self._offsets[i]
            elem.length = self._offsets[i + 1] - self._offsets[i]
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
        cdef int32_t start, end

        if self._nulls != NULL and ((self._nulls[index >> 3] >> (index & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            start = self._ptr.offsets[index]
            end = self._ptr.offsets[index + 1]
            elem.ptr = self._base + start
            elem.length = end - start
            elem.is_null = False

        return elem


cdef class _StringVectorView:
    """Zero-copy helper exposing raw pointer/length access."""

    def __cinit__(self, StringVector vec):
        self._ptr = vec.ptr
        self._data = <char*> self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap

    cpdef intptr_t value_ptr(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
            raise IndexError("Index out of range")
        return <intptr_t> (self._data + self._offsets[i])

    cpdef Py_ssize_t value_len(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
            raise IndexError("Index out of range")
        return <Py_ssize_t>(<uint32_t>self._offsets[i + 1] - <uint32_t>self._offsets[i])

    cpdef bint is_null(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
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
        """Finalize construction and hand off the built vector."""
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
        return StringVector(0, 0)
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
    """Same inputs as from_dict_buffers, but produces a dict-only StringVector
    (ptr.data == NULL). Used by the cpp-pipeline so downstream Morsel.slice /
    StringVector.take hit the dict-preserving fast path instead of materialising
    a fresh string buffer per slice."""
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dict_lengths.shape[0]
    cdef Py_ssize_t i
    cdef uint8_t[::1] validity_view
    cdef Py_ssize_t nb_bytes
    cdef StringVector vec

    if row_count == 0:
        return StringVector(0, 0)
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

    # Wire the dict accessor so downstream dict-aware kernels see the same view
    # that the take() fast path produces.
    vec._dict_accessor.codes = vec._dict_codes
    vec._dict_accessor.code_width = vec._dict_code_width
    vec._dict_accessor.row_nulls = vec.ptr.null_bitmap
    vec._dict_accessor.length = <size_t>row_count
    vec._dict_accessor.dict_values = vec._dict_values
    vec._dict_accessor.value_type = DRAKEN_STRING

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

    if row_count == 0:
        return StringVector(0, 0)
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
        for i in range(row_count):
            if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                expanded_codes[i] = 0
                row_validity[i] = 0
                continue
            code = _read_packed_code(codes, code_width, i)
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
    cdef DrakenVarBuffer* dv = vec._dict_values
    cdef Py_ssize_t d = <Py_ssize_t>dv.length
    cdef Py_ssize_t i, entry_len
    cdef int32_t start, end
    for i in range(d):
        start = dv.offsets[i]
        end = dv.offsets[i + 1]
        entry_len = end - start
        if entry_len != val_len:
            continue
        if val_len == 0 or memcmp(<const char*>dv.data + start, val_ptr, <size_t>val_len) == 0:
            return i
    return -1


cdef BoolVector _codes_to_boolvector_eq(StringVector vec, Py_ssize_t target_code):
    """BoolVector: codes[i] == target_code, propagating nulls."""
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
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
        code = _read_packed_code(codes, code_width, i)
        if <Py_ssize_t>code == target_code:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef BoolVector _codes_to_boolvector_neq(StringVector vec, Py_ssize_t target_code):
    """BoolVector: codes[i] != target_code, propagating nulls."""
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
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
        code = _read_packed_code(codes, code_width, i)
        if <Py_ssize_t>code != target_code:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef BoolVector _dict_ordered_scalar(StringVector vec, bytes value, int op):
    """Dict-level ordered scalar compare without materializing. op: 0=lt,1=gt,2=lte,3=gte."""
    cdef DrakenVarBuffer* dv = vec._dict_values
    cdef Py_ssize_t d = <Py_ssize_t>dv.length
    cdef const char* val_ptr = PyBytes_AS_STRING(value)
    cdef Py_ssize_t val_len = len(value)
    cdef uint8_t* pass_array = <uint8_t*>malloc(d)
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef int cmp_res

    if pass_array == NULL:
        raise MemoryError()
    try:
        for i in range(d):
            start = dv.offsets[i]
            end = dv.offsets[i + 1]
            cmp_res = _compare_bytes_lex(
                <const uint8_t*>dv.data + start, end - start,
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
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
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
        code = _read_packed_code(codes, code_width, i)
        if pass_array[code]:
            dst[i >> 3] |= (1 << (i & 7))
    return out


cdef StringVector _materialize_dict_string(StringVector vec):
    """Expand a dict-only StringVector to a dense StringVector (no src ptr.data needed)."""
    if vec._dict_values == NULL or vec._dict_codes == NULL:
        raise ValueError("Dictionary vector missing required data structures")
    cdef DrakenVarBuffer* dict_values = vec._dict_values
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef int32_t start, end
    cdef const char* arena = <const char*>dict_values.data
    cdef StringVectorBuilder builder
    cdef Py_ssize_t dict_size = <Py_ssize_t>dict_values.length

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            continue
        code = _read_packed_code(codes, code_width, i)
        if code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        start = dict_values.offsets[code]
        end = dict_values.offsets[code + 1]
        total_bytes += end - start

    builder = StringVectorBuilder(<Py_ssize_t>n, total_bytes, resizable=False)
    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            code = _read_packed_code(codes, code_width, i)
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            start = dict_values.offsets[code]
            end = dict_values.offsets[code + 1]
            builder.append_bytes(arena + start, end - start)

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
        StringVector with DRAKEN_ENCODING_DICTIONARY; ptr.data is NULL (no dense storage).
    """
    cdef StringVector vec = StringVector(0, 0, True)  # wrap=True: ptr starts as NULL
    cdef Py_ssize_t code_bytes = row_count * <Py_ssize_t>code_width
    cdef Py_ssize_t nb_bytes
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t i

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

    # Packed code array
    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(<size_t>code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
        memcpy(vec._dict_codes, codes, <size_t>code_bytes)

    # Dictionary values: arena + offsets stored as DrakenVarBuffer
    dict_values = alloc_var_buffer(DRAKEN_STRING, <size_t>dict_size, <size_t>arena_size)
    for i in range(dict_size):
        dict_values.offsets[i] = <int32_t>dict_offsets[i]
    dict_values.offsets[dict_size] = <int32_t>arena_size
    if arena_size > 0:
        memcpy(dict_values.data, <const void*>dict_data, <size_t>arena_size)
    vec._dict_values = dict_values

    vec._dict_code_width = code_width
    vec._dict_ordered = 0
    vec._encoding = DRAKEN_ENCODING_DICTIONARY

    vec._dict_accessor.codes = vec._dict_codes
    vec._dict_accessor.code_width = code_width
    vec._dict_accessor.row_nulls = vec.ptr.null_bitmap
    vec._dict_accessor.length = <size_t>row_count
    vec._dict_accessor.dict_values = vec._dict_values
    vec._dict_accessor.value_type = DRAKEN_STRING

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

    return vec

cdef StringVector _materialize_const_string(StringVector const_vec):
    """Expand a CONSTANT StringVector to a dense StringVector."""
    cdef Py_ssize_t n = <Py_ssize_t>const_vec.ptr.length
    cdef StringVectorBuilder builder
    cdef Py_ssize_t val_len
    cdef Py_ssize_t i

    if const_vec._const_is_null or const_vec._const_value == NULL:
        builder = StringVectorBuilder(n, 0)
        for i in range(n):
            builder.append_null()
    else:
        val_len = <Py_ssize_t>const_vec._const_value.length
        builder = StringVectorBuilder(n, n * val_len)
        for i in range(n):
            builder.append_bytes(<char*>const_vec._const_value.data, val_len)
    return builder.finish()


cdef StringVector from_rle_builder(
        uint8_t* arena,
        uint32_t* run_str_offsets,
        int32_t* run_str_lens,
        int32_t* run_lengths,
        size_t num_runs,
        size_t total_length):
    """
    Build an RLE StringVector from pre-built run arrays.  Called from the
    Parquet skip-dense path where C++ has already resolved dict codes to
    actual string bytes and packed them into a flat arena.

    Ownership: this function malloc-copies all inputs so the caller's C++
    vectors can be destroyed after the call returns.
    """
    import sys as _sys
    _draken = _sys.modules.get('draken')
    if _draken is not None and _draken._RLE_FORBIDDEN:
        raise RuntimeError("RLE vector construction is forbidden (draken._RLE_FORBIDDEN=True)")
    cdef StringVector vec = StringVector(0, 0, True)
    cdef DrakenRLEBuffer* buf
    cdef size_t arena_size = 0
    cdef size_t r

    if num_runs > 0:
        arena_size = <size_t>run_str_offsets[num_runs - 1] + <size_t>run_str_lens[num_runs - 1]

    buf = <DrakenRLEBuffer*>malloc(sizeof(DrakenRLEBuffer))
    if buf == NULL:
        raise MemoryError()

    # Copy byte arena
    if arena_size > 0:
        buf.run_values = malloc(arena_size)
        if buf.run_values == NULL:
            free(buf)
            raise MemoryError()
        memcpy(buf.run_values, arena, arena_size)
    else:
        buf.run_values = NULL

    # Copy run_lengths (repeat counts)
    buf.run_lengths = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if buf.run_lengths == NULL:
        if buf.run_values != NULL: free(buf.run_values)
        free(buf)
        raise MemoryError()
    memcpy(buf.run_lengths, run_lengths, num_runs * sizeof(int32_t))

    # Copy run_str_offsets
    buf.run_str_offsets = <uint32_t*>malloc(num_runs * sizeof(uint32_t))
    if buf.run_str_offsets == NULL:
        free(buf.run_lengths)
        if buf.run_values != NULL: free(buf.run_values)
        free(buf)
        raise MemoryError()
    memcpy(buf.run_str_offsets, run_str_offsets, num_runs * sizeof(uint32_t))

    # Copy run_str_lens
    buf.run_str_lens = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if buf.run_str_lens == NULL:
        free(buf.run_str_offsets)
        free(buf.run_lengths)
        if buf.run_values != NULL: free(buf.run_values)
        free(buf)
        raise MemoryError()
    memcpy(buf.run_str_lens, run_str_lens, num_runs * sizeof(int32_t))

    buf.null_bitmap = NULL
    buf.num_runs    = num_runs
    buf.length      = total_length
    buf.type        = DRAKEN_STRING

    # Allocate a minimal ptr so __len__ and length property work
    vec.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        free(buf.run_str_lens)
        free(buf.run_str_offsets)
        free(buf.run_lengths)
        if buf.run_values != NULL: free(buf.run_values)
        free(buf)
        raise MemoryError()
    vec.ptr.data        = NULL
    vec.ptr.offsets     = NULL
    vec.ptr.null_bitmap = NULL
    vec.ptr.length      = total_length
    vec.ptr.type        = DRAKEN_STRING
    vec.owns_data       = False

    vec._rle_buffer = buf
    vec._encoding   = DRAKEN_ENCODING_RLE

    return vec


cpdef StringVector _test_make_rle_string(list values, list run_lengths, object null_bitmap=None):
    """Construct an RLE-encoded StringVector for tests.

    Each entry in ``values`` is a ``bytes``/``str`` run value, paired with the
    matching ``run_lengths`` repetition count.  Optional ``null_bitmap`` is a
    bytes-like row-level validity bitmap (1 = valid).
    """
    if len(values) != len(run_lengths):
        raise ValueError("values and run_lengths must have the same length")

    cdef Py_ssize_t num_runs = len(values)
    cdef Py_ssize_t total = 0
    cdef Py_ssize_t arena_size = 0
    cdef Py_ssize_t i
    cdef Py_ssize_t bitmap_bytes
    cdef bytes b
    cdef list encoded = []

    for i in range(num_runs):
        v = values[i]
        if isinstance(v, str):
            b = v.encode("utf8")
        elif isinstance(v, (bytes, bytearray, memoryview)):
            b = bytes(v)
        else:
            raise TypeError("RLE run value must be str/bytes")
        encoded.append(b)
        arena_size += len(b)
        if run_lengths[i] < 0:
            raise ValueError("run length must be non-negative")
        total += <Py_ssize_t>run_lengths[i]

    cdef StringVector vec = StringVector(0, 0, True)
    cdef DrakenRLEBuffer* buf = <DrakenRLEBuffer*>malloc(sizeof(DrakenRLEBuffer))
    if buf == NULL:
        raise MemoryError()

    cdef uint8_t* arena = NULL
    cdef int32_t* run_lens_arr = NULL
    cdef uint32_t* run_offs_arr = NULL
    cdef int32_t* run_str_lens_arr = NULL
    cdef uint32_t off = 0
    cdef uint8_t* nb = NULL

    try:
        if arena_size > 0:
            arena = <uint8_t*>malloc(<size_t>arena_size)
            if arena == NULL:
                raise MemoryError()
        if num_runs > 0:
            run_lens_arr = <int32_t*>malloc(<size_t>num_runs * sizeof(int32_t))
            run_offs_arr = <uint32_t*>malloc(<size_t>num_runs * sizeof(uint32_t))
            run_str_lens_arr = <int32_t*>malloc(<size_t>num_runs * sizeof(int32_t))
            if run_lens_arr == NULL or run_offs_arr == NULL or run_str_lens_arr == NULL:
                raise MemoryError()
            for i in range(num_runs):
                b = encoded[i]
                run_offs_arr[i] = off
                run_str_lens_arr[i] = <int32_t>len(b)
                if len(b) > 0:
                    memcpy(arena + off, <const char*>PyBytes_AS_STRING(b), <size_t>len(b))
                off += <uint32_t>len(b)
                run_lens_arr[i] = <int32_t>run_lengths[i]

        if null_bitmap is not None:
            bm = bytes(null_bitmap)
            bitmap_bytes = (total + 7) >> 3
            if len(bm) < bitmap_bytes:
                raise ValueError("null_bitmap is shorter than required")
            nb = <uint8_t*>malloc(<size_t>bitmap_bytes)
            if nb == NULL:
                raise MemoryError()
            memcpy(nb, <const char*>PyBytes_AS_STRING(bm), <size_t>bitmap_bytes)

        buf.run_values       = <void*>arena
        buf.run_lengths      = run_lens_arr
        buf.run_str_offsets  = run_offs_arr
        buf.run_str_lens     = run_str_lens_arr
        buf.null_bitmap      = nb
        buf.num_runs         = <size_t>num_runs
        buf.length           = <size_t>total
        buf.type             = DRAKEN_STRING

        vec.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
        if vec.ptr == NULL:
            raise MemoryError()
        vec.ptr.data        = NULL
        vec.ptr.offsets     = NULL
        vec.ptr.null_bitmap = NULL
        vec.ptr.length      = <size_t>total
        vec.ptr.type        = DRAKEN_STRING
        vec.owns_data       = False
        vec._rle_buffer     = buf
        vec._encoding       = DRAKEN_ENCODING_RLE
    except:
        if arena != NULL: free(arena)
        if run_lens_arr != NULL: free(run_lens_arr)
        if run_offs_arr != NULL: free(run_offs_arr)
        if run_str_lens_arr != NULL: free(run_str_lens_arr)
        if nb != NULL: free(nb)
        free(buf)
        raise

    return vec


#################################

cpdef StringVector uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    """
    # Handle constant-encoded vectors: they have NULL offsets, so materialize first
    if input._has_const or input.ptr.offsets == NULL:
        builder = StringVectorBuilder.with_estimate(len(input), 16)
        for val in input.to_pylist():
            if val is None:
                builder.append_null()
            else:
                builder.append(val.upper())
        return builder.finish()

    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef Py_ssize_t n = in_ptr.length
    cdef int32_t total_bytes = in_ptr.offsets[n]
    cdef Py_ssize_t nb_size

    # Allocate new buffer with same size
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data

    # Copy entire data buffer
    if total_bytes > 0:
        memcpy(out_data, in_data, total_bytes)
        # Apply uppercase transformation to entire buffer using SIMD
        simd_to_upper(out_data, total_bytes)

    # Copy offsets
    memcpy(out_ptr.offsets, in_ptr.offsets, (n + 1) * sizeof(int32_t))

    # Copy null bitmap if present
    if in_ptr.null_bitmap != NULL:
        nb_size = (n + 7) // 8
        out_ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if out_ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(out_ptr.null_bitmap, in_ptr.null_bitmap, nb_size)

    return result


cpdef StringVector lowercase(StringVector input):
    """
    Return a new StringVector with all non-null values lowercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    """
    # Handle constant-encoded vectors: they have NULL offsets, so materialize first
    if input._has_const or input.ptr.offsets == NULL:
        builder = StringVectorBuilder.with_estimate(len(input), 16)
        for val in input.to_pylist():
            if val is None:
                builder.append_null()
            else:
                builder.append(val.lower())
        return builder.finish()

    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef Py_ssize_t n = in_ptr.length
    cdef int32_t total_bytes = in_ptr.offsets[n]
    cdef Py_ssize_t nb_size

    # Allocate new buffer with same size
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data

    # Copy entire data buffer
    if total_bytes > 0:
        memcpy(out_data, in_data, total_bytes)
        # Apply lowercase transformation to entire buffer using SIMD
        simd_to_lower(out_data, total_bytes)

    # Copy offsets
    memcpy(out_ptr.offsets, in_ptr.offsets, (n + 1) * sizeof(int32_t))

    # Copy null bitmap if present
    if in_ptr.null_bitmap != NULL:
        nb_size = (n + 7) // 8
        out_ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if out_ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(out_ptr.null_bitmap, in_ptr.null_bitmap, nb_size)

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
