# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
from libc.stdint cimport int32_t, intptr_t, uint8_t, uint64_t, int64_t, uint32_t, uint16_t, uintptr_t
from libc.string cimport memcpy, memset, memcmp
from libc.stdlib cimport malloc, realloc, free

from opteryx.draken.core.buffers cimport ConstAccessor
from opteryx.draken.core.buffers cimport DictAccessor
from opteryx.draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from opteryx.draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from opteryx.draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.var_vector cimport alloc_var_buffer, buf_dtype, free_var_buffer
from opteryx.draken.vectors.array_vector cimport ArrayVector, DrakenArrayBuffer

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

cdef extern from *:
    """
    #ifdef __GNUC__
    #define PREFETCH(addr) __builtin_prefetch(addr, 0, 3)
    #else
    #define PREFETCH(addr)
    #endif
    """
    void PREFETCH(const void* addr) nogil

from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash
from opteryx.draken.vectors.bool_vector cimport BoolVector

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
        dict_values.offsets[dict_size] = dict_offsets[dict_size]
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


cdef inline uint64_t _short_string_hash(const uint8_t* ptr, size_t n) nogil:
    cdef uint64_t first
    cdef uint64_t last
    cdef uint64_t h

    if n <= 8:
        first = _load_le_u64_partial(ptr, n)
        return mix_hash(<uint64_t>n, first)

    # 9..16 bytes: combine first 8 and last 8 with length-based seed.
    first = _load_le_u64_partial(ptr, 8)
    last = _load_le_u64_partial(ptr + (n - 8), 8)
    h = mix_hash(<uint64_t>n, first)
    return mix_hash(h, last)


# ---------------------------------------------------------------------------
# Scalar-comparison helpers (nogil, used by StringVector kernels)
# ---------------------------------------------------------------------------

cdef inline uint8_t _sv_ascii_lower(uint8_t b) noexcept nogil:
    if b >= 65 and b <= 90:
        return b + 32
    return b


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
) noexcept nogil:
    """Case-sensitive substring search."""
    cdef Py_ssize_t i, j
    if ndl_len == 0:
        return True
    if ndl_len > hay_len:
        return False
    for i in range(hay_len - ndl_len + 1):
        if haystack[i] == needle[0]:
            j = 1
            while j < ndl_len and haystack[i + j] == needle[j]:
                j += 1
            if j == ndl_len:
                return True
    return False


cdef bint _sv_contains_ci(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle_lower,
    Py_ssize_t ndl_len,
) noexcept nogil:
    """Case-insensitive substring search; needle_lower must already be lowercased."""
    cdef Py_ssize_t i, j
    if ndl_len == 0:
        return True
    if ndl_len > hay_len:
        return False
    for i in range(hay_len - ndl_len + 1):
        if _sv_ascii_lower(haystack[i]) == needle_lower[0]:
            j = 1
            while j < ndl_len and _sv_ascii_lower(haystack[i + j]) == needle_lower[j]:
                j += 1
            if j == ndl_len:
                return True
    return False


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

    def __dealloc__(self):
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

        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.binary())
            return pa.array(
                [PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length)] * self.ptr.length,
                type=pa.binary(),
            )
        
        cdef DrakenVarBuffer* ptr = self.ptr
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

    def __getitem__(self, Py_ssize_t i):
        """
        Return entry i as raw bytes, or None if null.
        """
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

        # Check for null value
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
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte, bit
        if self._has_const:
            return n if self._const_is_null else 0
        if ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                count += 1
        return count

    # Optimized equality check using SIMD-friendly operations
    cpdef BoolVector equals(self, bytes value):
        """
        Return mask: 1 if equal to value, else 0.
        Optimized version with reduced branching and better cache locality.
        """
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

    cpdef BoolVector in_list(self, object value_set):
        """
        Return mask: 1 if element is a member of value_set, else 0. Propagates NULLs.
        value_set must be a set or frozenset of bytes.
        """
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
        """Return mask: 1 if element matches SQL LIKE pattern, else 0. Propagates NULLs."""
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
        cdef Py_ssize_t i

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

        return out

    cpdef BoolVector rlike(self, bytes pattern):
        """Return mask: 1 if element matches regex pattern, else 0. Propagates NULLs."""
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
        cdef Py_ssize_t i
        cdef bytes cell_bytes

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

        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            cell_bytes = PyBytes_FromStringAndSize(<char*>ptr.data + start, <Py_ssize_t>str_len)
            if compiled.search(cell_bytes):
                dst[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector contains(self, bytes substr, bint ignore_case=False):
        """Return mask: 1 if element contains substr, else 0. Propagates NULLs."""
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
        cdef Py_ssize_t i, j

        if self._has_const:
            if self._const_is_null:
                return _constant_bool_result(n, False, True)
            if ignore_case and ndl_len > 0:
                ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
                if ndl_lower == NULL:
                    raise MemoryError()
                for j in range(ndl_len):
                    ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])
            try:
                if ignore_case:
                    return _constant_bool_result(
                        n,
                        _sv_contains_ci(
                            <const uint8_t*>self._const_value.data,
                            self._const_value.length,
                            ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                            ndl_len,
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
                    ),
                    False,
                )
            finally:
                if ndl_lower != NULL:
                    free(ndl_lower)

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

        if ignore_case and ndl_len > 0:
            ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
            if ndl_lower == NULL:
                raise MemoryError()
            for j in range(ndl_len):
                ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])

        try:
            for i in range(n):
                if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                str_len = end - start
                if ignore_case:
                    if _sv_contains_ci(
                        <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
                        ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                        ndl_len,
                    ):
                        dst[i >> 3] |= (1 << (i & 7))
                else:
                    if _sv_contains_cs(
                        <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
                        <const uint8_t*>ndl_ptr_char, ndl_len,
                    ):
                        dst[i >> 3] |= (1 << (i & 7))
        finally:
            if ndl_lower != NULL:
                free(ndl_lower)

        return out

    cpdef list to_pylist(self):
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef list out = []
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef char* data = <char*> ptr.data
        cdef uint8_t byte, bit

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
        cdef Py_ssize_t i

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.hash_into: output buffer too small")

        if self._has_const:
            if self._const_is_null:
                value = NULL_HASH
            else:
                if self._const_value.length <= 16:
                    value = _short_string_hash(<const uint8_t*>self._const_value.data, <size_t>self._const_value.length)
                else:
                    value = XXH3_64bits(<const void*>self._const_value.data, <size_t>self._const_value.length)
            for i in range(n):
                out_buf[offset + i] = mix_hash(out_buf[offset + i], value)
            return

        cdef const uint8_t* data = <const uint8_t*> ptr.data
        cdef int32_t* offsets = ptr.offsets
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef Py_ssize_t j
        cdef uint8_t byte
        cdef size_t str_len
        cdef int32_t start, end
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint64_t[STRING_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef Py_ssize_t idx

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
                        if str_len <= 16:
                            scratch[j] = _short_string_hash(data + start, str_len)
                        else:
                            scratch[j] = XXH3_64bits(data + start, str_len)
                else:
                    for j in range(block):
                        start = offsets[i + j]
                        end = offsets[i + j + 1]
                        str_len = <size_t>(end - start)
                        if str_len <= 16:
                            scratch[j] = _short_string_hash(data + start, str_len)
                        else:
                            scratch[j] = XXH3_64bits(data + start, str_len)
                
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for StringVector: pack first 7 bytes into big-endian int64."""
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
                for j in range(copy_len):
                    acc = (acc << 8) | (<uint64_t>(<uint8_t>((<char*>self._const_value.data)[j])))
                acc = acc << (<uint64_t>(8 * (7 - copy_len)))
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
            for j in range(copy_len):
                acc = (acc << 8) | (<uint64_t>(<uint8_t>base[start + j]))
            acc = acc << (<uint64_t>(8 * (7 - copy_len)))
            out_buf[offset + i] = <int64_t>acc

    cpdef StringVector take(self, int32_t[::1] indices):
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
            return StringVector.from_constant(
                None if self._const_is_null else PyBytes_FromStringAndSize(<char*>self._const_value.data, self._const_value.length),
                n,
                is_null=self._const_is_null,
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
            memset(dst_ptr.null_bitmap, 0xFF, nb_size)
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
                if not src_bit:
                    dst_ptr.null_bitmap[i >> 3] &= ~(1 << (i & 7))

        return result

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(self.ptr.length, 5)
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
        return self._offsets[i + 1] - self._offsets[i]

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
        arena_view = <uint8_t[:arena_size]><uint8_t*>dict_data
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

#################################

cpdef StringVector uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    Uses SIMD operations on the entire data buffer for maximum performance.
    """
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
