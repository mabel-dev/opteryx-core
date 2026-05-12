# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Int64Vector: Cython implementation of a fixed-width int64 column vector for Draken.

This module provides:
- The Int64Vector class for efficient int64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for int64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc

from draken.core.buffers cimport ConstAccessor
from draken.core.buffers cimport DictAccessor
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from draken.core.buffers cimport DRAKEN_ENCODING_RLE
from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenRLEBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport (
    MIX_HASH_CONSTANT,
    NULL_HASH,
    Vector,
    mix_hash,
    simd_mix_hash,
    simd_popcount,
)

from draken.vectors.bool_vector cimport BoolVector

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_int64_compare.hpp" namespace "draken::int64_cmp" nogil:
    void bit_fill_range(uint8_t* dst, size_t start, size_t count)
    bint dispatch_compare_once(int op, int64_t a, int64_t b)
    void dispatch_scalar_nonnull(int op, const int64_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const int64_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const int64_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const int64_t* a, const int64_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const int64_t* a, const int64_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const int64_t* a, const int64_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const int64_t* a, const int64_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const int64_t* a, const int64_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

cdef extern from "draken/vectors/_int64_reductions.hpp" namespace "draken::int64_red" nogil:
    int64_t sum_nonnull(const int64_t* data, size_t n)
    int64_t sum_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n)
    int64_t min_nonnull(const int64_t* data, size_t n)
    int64_t max_nonnull(const int64_t* data, size_t n)
    size_t  min_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_min)
    size_t  max_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_max)


cdef inline uint8_t _dict_code_width_for_size(Py_ssize_t dict_size) noexcept:
    if dict_size <= 256:
        return 1
    if dict_size <= 65536:
        return 2
    return 4

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000

cdef inline uint32_t _read_packed_code(const uint8_t* codes, uint8_t code_width, Py_ssize_t row_idx) noexcept nogil:
    if code_width == 1:
        return (<const uint8_t*>codes)[row_idx]
    if code_width == 2:
        return (<const uint16_t*>codes)[row_idx]
    return (<const uint32_t*>codes)[row_idx]


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1


cdef void _release_dict_storage(Int64Vector vec) noexcept:
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
    vec._dict_accessor.value_type = DRAKEN_INT64
    vec._encoding = DRAKEN_ENCODING_DENSE


cdef void _release_rle_storage_int64(Int64Vector vec) noexcept:
    """Free all RLE buffer resources owned by vec."""
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef void _attach_dictionary_storage(Int64Vector vec, const int32_t[::1] codes, const int64_t[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(int64_t)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t bitmap_bytes

    _release_dict_storage(vec)

    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
    else:
        vec._dict_codes = NULL

    dict_values = alloc_var_buffer(DRAKEN_INT64, <size_t>dict_size, <size_t>dict_bytes)
    dict_values.offsets[0] = 0
    for i in range(dict_size):
        dict_values.offsets[i + 1] = <int32_t>((i + 1) * sizeof(int64_t))
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>&dictionary[0], <size_t>dict_bytes)

    # Copy dictionary entry-level null bitmap if provided
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

cdef class Int64Vector(Vector):

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef int64_t[::1] dictionary_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("q", dictionary)

        codes_view = codes
        dictionary_view = dictionary

        if row_validity is None:
            return from_dict(codes_view, dictionary_view)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary_view, validity_view)

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef Int64Vector vec = Int64Vector(0)

        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else <int64_t>int(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
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
        self._dict_accessor.value_type = DRAKEN_INT64
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_INT64
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_dict_storage(self)
        _release_rle_storage_int64(self)
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
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
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_INT64
        self._const_accessor.value_ptr = <void*>&self._const_value
        self._const_accessor.is_null = 1 if self._const_is_null else 0
        return &self._const_accessor

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL or self._has_const or self._encoding == DRAKEN_ENCODING_RLE:
            return NULL
        return self.ptr.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL or self._has_const or self._encoding == DRAKEN_ENCODING_RLE:
            return NULL
        return self.ptr.null_bitmap

    # Python-friendly properties (backed by C getters for kernels)
    @property
    def length(self):
        return buf_length(self.ptr)

    def __len__(self):
        return buf_length(self.ptr)

    @property
    def itemsize(self):
        return buf_itemsize(self.ptr)

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

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t byte
        cdef uint8_t bit
        cdef size_t cumulative = 0
        cdef size_t run_idx
        cdef int64_t* rle_vals
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value
        if self._encoding == DRAKEN_ENCODING_RLE:
            # Linear scan through runs to find value at logical index i
            rle_vals = <int64_t*>self._rle_buffer.run_values
            for run_idx in range(self._rle_buffer.num_runs):
                cumulative += <size_t>self._rle_buffer.run_lengths[run_idx]
                if <size_t>i < cumulative:
                    if self._rle_buffer.null_bitmap != NULL:
                        byte = self._rle_buffer.null_bitmap[i >> 3]
                        if not ((byte >> (i & 7)) & 1):
                            return None
                    return rle_vals[run_idx]
            raise IndexError("Index out of bounds")
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
                return None
            return (<int64_t*>self._dict_values.data)[
                <Py_ssize_t>_read_packed_code(self._dict_codes, self._dict_code_width, i)
            ]
        cdef int64_t* data = <int64_t*> ptr.data
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa

        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            return _materialize_dict_int64(self).to_arrow()

        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.int64())
            return pa.array([self._const_value] * self.ptr.length, type=pa.int64())

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
                pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.int64(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Int64Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        if self._encoding == DRAKEN_ENCODING_RLE:
            # Materialize to dense then take
            return _materialize_rle_int64(self).take(indices)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_int64(self).take(indices)
        if self._has_const:
            return Int64Vector.from_constant(
                None if self._const_is_null else self._const_value,
                n,
                is_null=self._const_is_null,
            )
        cdef Int64Vector out = Int64Vector(<size_t>n)
        cdef int64_t* src = <int64_t*> self.ptr.data
        cdef int64_t* dst = <int64_t*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef int32_t* taken_codes = NULL
        cdef int32_t[::1] taken_codes_view
        cdef int64_t[::1] dictionary_view
        cdef Py_ssize_t dict_size = 0
        cdef Py_ssize_t out_nbytes
        cdef int32_t src_idx
        cdef uint8_t byte

        # If source has no null bitmap, copy directly
        if src_null == NULL:
            for i in range(n):
                src_idx = indices[i]
                dst[i] = src[src_idx]
            out.ptr.null_bitmap = NULL
        else:
            # Source has nulls - allocate a null bitmap for the output and preserve nulls
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*> malloc(out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            # zero-initialize
            for i in range(out_nbytes):
                out_null[i] = 0

            for i in range(n):
                src_idx = indices[i]
                byte = src_null[src_idx >> 3]
                if byte & (1 << (src_idx & 7)):
                    dst[i] = src[src_idx]
                    out_null[i >> 3] |= (1 << (i & 7))
                else:
                    dst[i] = 0

            out.ptr.null_bitmap = out_null

        if self._dict_values != NULL and self._dict_codes != NULL:
            dict_size = self._dict_values.length
            if n > 0:
                taken_codes = <int32_t*>malloc(n * sizeof(int32_t))
                if taken_codes == NULL:
                    if out_null != NULL:
                        free(out_null)
                        out.ptr.null_bitmap = NULL
                    raise MemoryError()
            try:
                for i in range(n):
                    src_idx = indices[i]
                    if src_null != NULL:
                        byte = src_null[src_idx >> 3]
                        if (byte & (1 << (src_idx & 7))) == 0:
                            taken_codes[i] = 0
                            continue
                    taken_codes[i] = <int32_t>_read_packed_code(self._dict_codes, self._dict_code_width, src_idx)

                if n > 0:
                    taken_codes_view = <int32_t[:n]>taken_codes
                else:
                    taken_codes_view = <int32_t[:0]>taken_codes
                if dict_size > 0:
                    dictionary_view = <int64_t[:dict_size]><int64_t*>self._dict_values.data
                else:
                    dictionary_view = <int64_t[:0]><int64_t*>self._dict_values.data
                _attach_dictionary_storage(out, taken_codes_view, dictionary_view, self._dict_ordered != 0)
            finally:
                if taken_codes != NULL:
                    free(taken_codes)
        return out

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n):
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* null_bm = NULL
        memset(dst, 0, nbytes)
        if nbytes != 0:
            null_bm = <uint8_t*>malloc(nbytes)
            if null_bm == NULL:
                raise MemoryError()
            memset(null_bm, 0, nbytes)
            out.ptr.null_bitmap = null_bm
        else:
            out.ptr.null_bitmap = NULL
        return out

    cdef BoolVector _compare_scalar_rle(self, int64_t value, int op):
        # Compare RLE vector against a scalar without materialising to dense int64.
        # Evaluates the predicate once per run, bit-fills the result, then applies
        # the row-level null bitmap (if any) via a single SIMD AND pass.
        cdef size_t n = self._rle_buffer.length
        cdef size_t num_runs = self._rle_buffer.num_runs
        cdef int64_t* rle_vals = <int64_t*>self._rle_buffer.run_values
        cdef int32_t* rle_lens = self._rle_buffer.run_lengths
        cdef uint8_t* rle_nulls = self._rle_buffer.null_bitmap
        cdef Py_ssize_t nbytes = (n + 7) >> 3

        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        memset(dst, 0, nbytes)

        cdef uint8_t* out_null = NULL
        if rle_nulls != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, rle_nulls, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        cdef size_t pos = 0
        cdef size_t r
        for r in range(num_runs):
            if dispatch_compare_once(op, rle_vals[r], value):
                bit_fill_range(dst, pos, <size_t>rle_lens[r])
            pos += <size_t>rle_lens[r]

        if out_null != NULL:
            simd_and_mask(dst, dst, out_null, <size_t>nbytes)

        return out

    cdef BoolVector _compare_scalar_dict(self, int64_t value, int op):
        # Compare dict-encoded vector against a scalar without materialising to dense int64.
        # Evaluates the predicate once per dictionary entry into a tiny match_table,
        # then walks the code array gathering bits. code_width dispatch is hoisted
        # outside the loop so the inner loop has no per-row branching on width.
        # Null bitmap applied via a single simd_and_mask pass at the end.
        cdef Py_ssize_t dict_size = <Py_ssize_t>self._dict_values.length
        cdef int64_t* dict_data = <int64_t*>self._dict_values.data
        cdef uint8_t* codes = self._dict_codes
        cdef uint8_t code_width = self._dict_code_width
        cdef uint8_t* row_nulls = self.ptr.null_bitmap
        cdef Py_ssize_t n = <Py_ssize_t>self.ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3

        cdef uint8_t* match_table = <uint8_t*>malloc(<size_t>dict_size)
        if match_table == NULL:
            raise MemoryError()

        cdef Py_ssize_t d
        for d in range(dict_size):
            match_table[d] = 1 if dispatch_compare_once(op, dict_data[d], value) else 0

        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        memset(dst, 0, nbytes)

        cdef uint8_t* out_null = NULL
        if row_nulls != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                free(match_table)
                raise MemoryError()
            memcpy(out_null, row_nulls, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        cdef Py_ssize_t i
        cdef const uint8_t* codes8
        cdef const uint16_t* codes16
        cdef const uint32_t* codes32
        if code_width == 1:
            codes8 = <const uint8_t*>codes
            for i in range(n):
                if match_table[codes8[i]]:
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        elif code_width == 2:
            codes16 = <const uint16_t*>codes
            for i in range(n):
                if match_table[codes16[i]]:
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            codes32 = <const uint32_t*>codes
            for i in range(n):
                if match_table[codes32[i]]:
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))

        free(match_table)

        if out_null != NULL:
            simd_and_mask(dst, dst, out_null, <size_t>nbytes)

        return out

    cdef BoolVector _compare_scalar(self, int64_t value, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_scalar_rle(value, op)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return self._compare_scalar_dict(value, op)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef bint matched

        if self._has_const:
            n = ptr.length
            nbytes = (n + 7) >> 3
            out = BoolVector(<size_t>n)
            dst = <uint8_t*>out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if self._const_is_null:
                return self._make_all_null_bool(n)

            matched = dispatch_compare_once(op, self._const_value, value)
            if matched and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        n = ptr.length
        nbytes = (n + 7) >> 3
        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data

        memset(dst, 0, nbytes)
        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        cdef size_t valid_count
        # Gate: above ~70% null density, the branching kernel wins by
        # short-circuiting writes for null rows. Below that, the branchless
        # kernel wins by avoiding mispredicted comparison-result branches.
        # op dispatch happens once here; the C++ kernel runs a tight loop
        # with no per-row branching on op.
        if src_null == NULL:
            dispatch_scalar_nonnull(op, data, value, dst, <size_t>n)
        else:
            valid_count = simd_popcount(src_null, <size_t>nbytes)
            if n > 0 and (valid_count * 10) < (<size_t>n * 3):
                dispatch_scalar_branching(op, data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless(op, data, value, src_null, dst, <size_t>n)
        return out

    cdef BoolVector _compare_vector(self, Int64Vector other, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_int64(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_vector(_materialize_rle_int64(other), op)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            return _materialize_dict_int64(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY:
            return self._compare_vector(_materialize_dict_int64(other), op)
        # Const fast paths: avoid O(n) materialisation.
        # self[i] OP other[i] where self is const V = V OP other[i]
        #   = other[i] reversed_op V, so flip directional ops.
        cdef Py_ssize_t const_n
        cdef Py_ssize_t const_nbytes
        cdef BoolVector const_out
        cdef uint8_t* const_dst
        cdef uint8_t* const_null
        cdef int reversed_op
        if self._has_const:
            const_n = self.ptr.length
            if const_n != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if self._const_is_null:
                return self._make_all_null_bool(const_n)
            # Reverse: gt(2)↔lt(4), ge(3)↔le(5); eq(0) and ne(1) unchanged.
            if op == 2:   reversed_op = 4
            elif op == 3: reversed_op = 5
            elif op == 4: reversed_op = 2
            elif op == 5: reversed_op = 3
            else:         reversed_op = op
            return other._compare_scalar(self._const_value, reversed_op)
        if other._has_const:
            if self.ptr.length != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if other._const_is_null:
                return self._make_all_null_bool(self.ptr.length)
            return self._compare_scalar(other._const_value, op)

        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data
        memset(dst, 0, nbytes)

        if (null1 != NULL or null2 != NULL) and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # op dispatch and null-pointer specialisation happen once here.
        # Gate: >~70% nulls → branching kernel (skips work for null rows).
        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False
        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        if null1 == NULL and null2 == NULL:
            dispatch_vector_nonnull(op, data1, data2, dst, <size_t>n)
        elif use_branching:
            if null1 != NULL and null2 != NULL:
                dispatch_vector_both_null_branching(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
            elif null1 != NULL:
                dispatch_vector_one_null_branching(op, data1, data2, null1, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branching(op, data1, data2, null2, dst, out_null, <size_t>n)
        elif null1 != NULL and null2 == NULL:
            dispatch_vector_one_null_branchless(op, data1, data2, null1, dst, out_null, <size_t>n)
        elif null1 == NULL and null2 != NULL:
            dispatch_vector_one_null_branchless(op, data1, data2, null2, dst, out_null, <size_t>n)
        else:
            dispatch_vector_both_null_branchless(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Int64Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Int64Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Int64Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Int64Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Int64Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Int64Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_int64(self).between(lower, upper, lower_inclusive, upper_inclusive)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_int64(self).between(lower, upper, lower_inclusive, upper_inclusive)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef int64_t* data = <int64_t*>ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t i
        cdef uint8_t mask
        cdef bint in_range

        memset(dst, 0, nbytes)

        if self._has_const:
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*>malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            if lower_inclusive:
                in_range = self._const_value >= lower
            else:
                in_range = self._const_value > lower
            if in_range:
                if upper_inclusive:
                    in_range = self._const_value <= upper
                else:
                    in_range = self._const_value < upper
            if in_range and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
            out.ptr.null_bitmap = NULL
            return out

        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        # 4 specialised loops: bound-inclusivity hoisted outside the hot path
        if src_null == NULL:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if lower <= data[i] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if lower <= data[i] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if lower < data[i] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if lower < data[i] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[i] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[i] < upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[i] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[i] < upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_int64(self).in_list(value_set)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_int64(self).in_list(value_set)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        if self._has_const:
            n = ptr.length
            nbytes = (n + 7) >> 3
            out = BoolVector(<size_t>n)
            dst = <uint8_t*> out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            if self._const_value in value_set and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        n = ptr.length
        nbytes = (n + 7) >> 3
        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data

        memset(dst, 0, nbytes)
        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if data[i] in value_set:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef int64_t sum(self):
        # Encoding-specific reductions handle their own structure; the dense
        # path delegates to the C++ kernel for SIMD-friendly throughput.
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef Py_ssize_t n

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer == NULL:
                return 0
            return _sum_rle_int64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _sum_dict_int64(self)
        if self._has_const:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)
        ptr = self.ptr
        data = <int64_t*> ptr.data
        n = ptr.length
        if n == 0:
            return 0
        if ptr.null_bitmap == NULL:
            return sum_nonnull(data, <size_t>n)
        return sum_nullable_branchless(data, ptr.null_bitmap, <size_t>n)

    cpdef int64_t min(self):
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef Py_ssize_t n
        cdef int64_t out
        cdef size_t valid_count

        if self._encoding == DRAKEN_ENCODING_RLE:
            return _min_rle_int64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _min_dict_int64(self)
        if self._has_const:
            if self.ptr.length == 0 or self._const_is_null:
                raise ValueError("Cannot compute min of empty column")
            return self._const_value
        ptr = self.ptr
        data = <int64_t*> ptr.data
        n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        if ptr.null_bitmap == NULL:
            return min_nonnull(data, <size_t>n)

        valid_count = min_nullable_branchless(data, ptr.null_bitmap, <size_t>n, &out)
        if valid_count == 0:
            raise ValueError("Cannot compute min of all-null column")
        return out

    cpdef int64_t max(self):
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef Py_ssize_t n
        cdef int64_t out
        cdef size_t valid_count

        if self._encoding == DRAKEN_ENCODING_RLE:
            return _max_rle_int64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _max_dict_int64(self)
        if self._has_const:
            if self.ptr.length == 0 or self._const_is_null:
                raise ValueError("Cannot compute max of empty column")
            return self._const_value
        ptr = self.ptr
        data = <int64_t*> ptr.data
        n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        if ptr.null_bitmap == NULL:
            return max_nonnull(data, <size_t>n)

        valid_count = max_nullable_branchless(data, ptr.null_bitmap, <size_t>n, &out)
        if valid_count == 0:
            raise ValueError("Cannot compute max of all-null column")
        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two values at given indices. Returns -1, 0, 1. Assumes non-null."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_int64(self).compare_at(left_idx, right_idx)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_int64(self).compare_at(left_idx, right_idx)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef int64_t left_val, right_val

        if self._has_const:
            return 0

        left_val = data[left_idx]
        right_val = data[right_idx]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t rle_byte
        cdef uint8_t byte

        if self._has_const:
            return self._const_is_null

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return False
            rle_byte = self._rle_buffer.null_bitmap[idx >> 3]
            return ((rle_byte >> (idx & 7)) & 1) == 0

        if ptr.null_bitmap == NULL:
            return False

        byte = ptr.null_bitmap[idx >> 3]
        return ((byte >> (idx & 7)) & 1) == 0

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                for i in range(n):
                    buf[i] = 0
            else:
                for i in range(n):
                    byte = self._rle_buffer.null_bitmap[i >> 3]
                    buf[i] = 0 if ((byte >> (i & 7)) & 1) else 1
            return <int8_t[:n]> buf

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

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return 0
            return <Py_ssize_t>self._rle_buffer.length - <Py_ssize_t>simd_popcount(
                self._rle_buffer.null_bitmap, (self._rle_buffer.length + 7) >> 3
            )
        if self._has_const:
            return n if self._const_is_null else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    cpdef Vector materialize(self):
        """Return a dense Int64Vector, expanding dict/const/RLE encodings if needed."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Int64Vector dense
        cdef int64_t* dst
        cdef Py_ssize_t i, nb_bytes
        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            if ptr.data == NULL:
                # dict-only path: codes in _dict_codes
                return _materialize_dict_int64(self)
            else:
                # from_dict path: dense data already in ptr.data, copy to new dense vector
                dense = Int64Vector(<size_t>n)
                dst = <int64_t*>dense.ptr.data
                memcpy(dst, ptr.data, <size_t>n * sizeof(int64_t))
                if ptr.null_bitmap != NULL:
                    nb_bytes = (n + 7) >> 3
                    dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                    if dense.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memcpy(dense.ptr.null_bitmap, ptr.null_bitmap, <size_t>nb_bytes)
                return dense
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_int64(self)
        if self._has_const:
            dense = Int64Vector(<size_t>n)
            dst = <int64_t*>dense.ptr.data
            if self._const_is_null:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(dense.ptr.null_bitmap, 0, <size_t>nb_bytes)
                memset(dst, 0, <size_t>n * sizeof(int64_t))
            else:
                for i in range(n):
                    dst[i] = self._const_value
                dense.ptr.null_bitmap = NULL
            return dense
        return self

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = ptr.length
        cdef uint64_t dict_bytes, code_bytes, null_bytes, data_bytes, bm_bytes
        if self._has_const:
            return 8  # sizeof(int64_t)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            dict_bytes = self._dict_values.length * 8 if self._dict_values != NULL else 0
            code_bytes = n * self._dict_code_width
            null_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
            return dict_bytes + code_bytes + null_bytes
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef int64_t* rle_vals
        cdef int32_t* rle_lens
        cdef size_t rle_runs
        cdef uint8_t* rle_nulls
        cdef Py_ssize_t pos
        cdef size_t r
        cdef int32_t run_len
        cdef int64_t run_val

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals = <int64_t*>self._rle_buffer.run_values
            rle_lens = self._rle_buffer.run_lengths
            rle_runs = self._rle_buffer.num_runs
            rle_nulls = self._rle_buffer.null_bitmap
            pos = 0
            for r in range(rle_runs):
                run_val = rle_vals[r]
                run_len = rle_lens[r]
                for i in range(run_len):
                    if rle_nulls != NULL and not ((rle_nulls[(pos + i) >> 3] >> ((pos + i) & 7)) & 1):
                        out.append(None)
                    else:
                        out.append(run_val)
                pos += run_len
            return out

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append(self._const_value)
            return out

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            return _materialize_dict_int64(self).to_pylist()

        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint8_t byte, bit

        if ptr.null_bitmap == NULL:
            for i in range(n):
                out.append(data[i])
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    out.append(data[i])
                else:
                    out.append(None)

        return out

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t* dst_base
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef uint8_t byte
        cdef uint64_t* dst
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef int64_t* rle_vals
        cdef int32_t* rle_lens
        cdef size_t rle_runs
        cdef uint8_t* rle_nulls
        cdef Py_ssize_t pos
        cdef size_t r
        cdef uint64_t run_hash
        cdef int32_t run_len
        cdef int64_t* data
        cdef uint64_t* as_uint64
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef int64_t* _dict_data
        cdef uint8_t* _dict_codes
        cdef uint8_t  _dict_cw
        cdef uint8_t* _dict_nb

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Int64Vector.hash_into: output buffer too small")
        dst_base = &out_buf[0]

        dst = dst_base + offset

        if self._encoding == DRAKEN_ENCODING_RLE:
            # RLE optimisation: hash each distinct value once, replicate run_length times
            rle_vals = <int64_t*>self._rle_buffer.run_values
            rle_lens = self._rle_buffer.run_lengths
            rle_runs = self._rle_buffer.num_runs
            rle_nulls = self._rle_buffer.null_bitmap
            pos = 0
            for r in range(rle_runs):
                run_len = rle_lens[r]
                run_hash = <uint64_t>rle_vals[r]
                i = 0
                while i < run_len:
                    block = run_len - i
                    if block > 1024:
                        block = 1024
                    for j in range(block):
                        scratch[j] = run_hash
                    if rle_nulls != NULL:
                        for j in range(block):
                            if not ((rle_nulls[(pos + i + j) >> 3] >> ((pos + i + j) & 7)) & 1):
                                scratch[j] = NULL_HASH
                    simd_mix_hash(dst + pos + i, scratch_ptr, <size_t>block)
                    i += block
                pos += run_len
            return

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            _dict_data  = <int64_t*>self._dict_values.data
            _dict_codes = self._dict_codes
            _dict_cw    = self._dict_code_width
            _dict_nb    = ptr.null_bitmap
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    if _dict_nb != NULL and not ((_dict_nb[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = <uint64_t>_dict_data[
                            <Py_ssize_t>_read_packed_code(_dict_codes, _dict_cw, i + j)
                        ]
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        data = <int64_t*> ptr.data
        as_uint64 = <uint64_t*> data
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(1024):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (as_uint64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        else:
            simd_mix_hash(dst, as_uint64, <size_t>n)

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef uint8_t byte
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef int64_t* _cd_dict_data
        cdef uint8_t* _cd_dict_codes
        cdef uint8_t  _cd_dict_cw
        cdef uint8_t* _cd_null_bitmap

        if n == 0:
            return 0

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(1024):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # DICTIONARY-only path: ptr.data is NULL, values looked up via codes
        if ptr.data == NULL and self._dict_codes != NULL:
            _cd_dict_data  = <int64_t*>self._dict_values.data
            _cd_dict_codes = self._dict_codes
            _cd_dict_cw    = self._dict_code_width
            _cd_null_bitmap = ptr.null_bitmap
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    if _cd_null_bitmap != NULL and not ((_cd_null_bitmap[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = <uint64_t>_cd_dict_data[
                            <Py_ssize_t>_read_packed_code(_cd_dict_codes, _cd_dict_cw, i + j)
                        ]
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint64_t* as_uint64 = <uint64_t*> data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (as_uint64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        else:
            simd_mix_hash(out, as_uint64, <size_t>n)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast per-element compress for Int64Vector (no Python conversions).

        Null values map to the NULL sentinel; non-null values are copied
        directly into the output buffer.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base
        cdef int64_t* dst
        cdef Py_ssize_t i
        cdef int64_t* rle_vals
        cdef int32_t* rle_lens
        cdef size_t rle_runs
        cdef uint8_t* rle_nulls
        cdef Py_ssize_t pos
        cdef size_t r
        cdef int32_t run_len
        cdef int64_t run_val
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef int64_t* src
        cdef int64_t* _ci_dict_data
        cdef uint8_t* _ci_dict_codes
        cdef uint8_t  _ci_dict_cw

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Int64Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        dst = dst_base + offset

        if self._encoding == DRAKEN_ENCODING_RLE:
            # Expand RLE into output buffer
            rle_vals = <int64_t*>self._rle_buffer.run_values
            rle_lens = self._rle_buffer.run_lengths
            rle_runs = self._rle_buffer.num_runs
            rle_nulls = self._rle_buffer.null_bitmap
            pos = 0
            for r in range(rle_runs):
                run_val = rle_vals[r]
                run_len = rle_lens[r]
                for i in range(run_len):
                    if rle_nulls != NULL and not ((rle_nulls[(pos + i) >> 3] >> ((pos + i) & 7)) & 1):
                        dst[pos + i] = INT64_MIN_VALUE
                    else:
                        dst[pos + i] = run_val
                pos += run_len
            return

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            _ci_dict_data  = <int64_t*>self._dict_values.data
            _ci_dict_codes = self._dict_codes
            _ci_dict_cw    = self._dict_code_width
            null_bitmap    = ptr.null_bitmap
            for i in range(n):
                if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                    dst[i] = INT64_MIN_VALUE
                else:
                    dst[i] = _ci_dict_data[
                        <Py_ssize_t>_read_packed_code(_ci_dict_codes, _ci_dict_cw, i)
                    ]
            return

        if self._has_const:
            for i in range(n):
                dst[i] = INT64_MIN_VALUE if self._const_is_null else self._const_value
            return

        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL
        src = <int64_t*> ptr.data

        if not has_nulls:
            # Fast path: bulk copy
            memcpy(<void*>dst, <const void*>src, <size_t>(n * sizeof(int64_t)))
            return

        for i in range(n):
            if (null_bitmap[i >> 3] >> (i & 7)) & 1:
                dst[i] = src[i]
            else:
                dst[i] = <int64_t> -9223372036854775808

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:k]
            return f"<Int64Vector(RLE) len={buf_length(self.ptr)} values={vals}>"
        if self._has_const:
            vals = [None if self._const_is_null else self._const_value] * k
            return f"<Int64Vector len={buf_length(self.ptr)} values={vals}>"
        cdef int64_t* data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Int64Vector len={buf_length(self.ptr)} values={vals}>"


cdef Int64Vector _materialize_rle_int64(Int64Vector rle_vec):
    """Expand an RLE Int64Vector to a dense Int64Vector.

    Allocates a dense array, fills it by expanding each run, then returns a new
    Int64Vector backed by that array. The RLE vector is not modified.
    """
    cdef size_t total = rle_vec._rle_buffer.length
    cdef Int64Vector dense = Int64Vector(<size_t>total)

    cdef int64_t* rle_vals = <int64_t*>rle_vec._rle_buffer.run_values
    cdef int32_t* rle_lens = rle_vec._rle_buffer.run_lengths
    cdef size_t num_runs = rle_vec._rle_buffer.num_runs
    cdef uint8_t* rle_nulls = rle_vec._rle_buffer.null_bitmap

    cdef int64_t* dst = <int64_t*>dense.ptr.data
    cdef size_t pos = 0
    cdef size_t r
    cdef int32_t run_len
    cdef int64_t run_val
    cdef Py_ssize_t j
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    for r in range(num_runs):
        run_val = rle_vals[r]
        run_len = rle_lens[r]
        for j in range(run_len):
            dst[pos + j] = run_val
        pos += <size_t>run_len

    if rle_nulls != NULL:
        null_bytes = (total + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            raise MemoryError()
        memcpy(null_copy, rle_nulls, null_bytes)
        dense.ptr.null_bitmap = null_copy
    else:
        dense.ptr.null_bitmap = NULL

    return dense


cdef Int64Vector _materialize_dict_int64(Int64Vector vec):
    """Expand a dict-only Int64Vector to a dense Int64Vector (no src ptr.data needed)."""
    if vec._dict_values == NULL or vec._dict_codes == NULL:
        raise ValueError("Dictionary encoding not properly initialized")

    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Int64Vector dense = Int64Vector(<size_t>n)
    cdef int64_t* dst = <int64_t*>dense.ptr.data
    cdef int64_t* dict_data = <int64_t*>vec._dict_values.data
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t i, dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint32_t code
    cdef Py_ssize_t nb_bytes

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            dst[i] = 0
        else:
            code = _read_packed_code(codes, code_width, i)
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: code {code} >= dict_size {dict_size}")
            dst[i] = dict_data[code]

    if null_bitmap != NULL:
        nb_bytes = (n + 7) >> 3
        dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if dense.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(dense.ptr.null_bitmap, null_bitmap, <size_t>nb_bytes)

    return dense


cdef Int64Vector make_int64_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded Int64Vector with no dense materialization.

    Args:
        codes:       Packed code array (code_width bytes per row, row_count entries).
        code_width:  Bytes per code: 1, 2, or 4.
        row_count:   Total number of rows.
        dictionary:  Array of unique int64 values (dict_size entries).
        dict_size:   Number of unique dictionary values.
        valid_bits:  Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        Int64Vector with DRAKEN_ENCODING_DICTIONARY; ptr.data is NULL (no dense storage).
    """
    cdef Int64Vector vec = Int64Vector(0)   # allocates ptr header; ptr.data = NULL
    cdef Py_ssize_t code_bytes = row_count * <Py_ssize_t>code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(int64_t)
    cdef Py_ssize_t nb_bytes
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t i

    vec.ptr.length = <size_t>row_count  # logical length; ptr.data stays NULL

    # Null bitmap from Arrow-style valid_bits
    if valid_bits != NULL:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, valid_bits, <size_t>nb_bytes)

    # Packed code array (direct copy from C++ output)
    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(<size_t>code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
        memcpy(vec._dict_codes, codes, <size_t>code_bytes)

    # Dictionary values
    dict_values = alloc_var_buffer(DRAKEN_INT64, <size_t>dict_size, <size_t>dict_bytes)
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>dictionary, <size_t>dict_bytes)
    for i in range(dict_size):
        dict_values.offsets[i] = <int32_t>(i * sizeof(int64_t))
    dict_values.offsets[dict_size] = <int32_t>dict_bytes
    vec._dict_values = dict_values

    vec._dict_code_width = code_width
    vec._dict_ordered = 0
    vec._encoding = DRAKEN_ENCODING_DICTIONARY

    vec._dict_accessor.codes = vec._dict_codes
    vec._dict_accessor.code_width = code_width
    vec._dict_accessor.row_nulls = vec.ptr.null_bitmap
    vec._dict_accessor.length = <size_t>row_count
    vec._dict_accessor.dict_values = vec._dict_values
    vec._dict_accessor.value_type = DRAKEN_INT64

    return vec


cdef Int64Vector from_rle_builder(
    int64_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded Int64Vector from raw C arrays.

    The caller passes pointers to builder-owned arrays; this function copies
    the run data into fresh malloc'd arrays owned by the vector.

    Args:
        run_values:  Pointer to int64_t values array (num_runs entries).
        run_lengths: Pointer to int32_t run lengths (num_runs entries).
        num_runs:    Number of runs.
        null_bitmap: Optional logical-row null bitmap (NULL = no nulls).

    Returns:
        Int64Vector with DRAKEN_ENCODING_RLE encoding.
    """
    cdef Int64Vector vec = Int64Vector(0, False)  # allocates ptr, data=NULL
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef int64_t* vals_copy
    cdef int32_t* lens_copy
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    # Compute total logical length
    for i in range(num_runs):
        total_length += <size_t>run_lengths[i]

    # Set ptr.length so the length property works
    vec.ptr.length = total_length

    if num_runs == 0:
        vec._encoding = DRAKEN_ENCODING_RLE
        return vec

    # Allocate and fill RLE buffer struct
    rle = <DrakenRLEBuffer*>malloc(sizeof(DrakenRLEBuffer))
    if rle == NULL:
        raise MemoryError()

    vals_copy = <int64_t*>malloc(num_runs * sizeof(int64_t))
    lens_copy = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if vals_copy == NULL or lens_copy == NULL:
        free(rle)
        if vals_copy != NULL:
            free(vals_copy)
        if lens_copy != NULL:
            free(lens_copy)
        raise MemoryError()

    memcpy(vals_copy, run_values, num_runs * sizeof(int64_t))
    memcpy(lens_copy, run_lengths, num_runs * sizeof(int32_t))

    rle.run_values = <void*>vals_copy
    rle.run_lengths = lens_copy
    rle.num_runs = num_runs
    rle.length = total_length
    rle.type = DRAKEN_INT64

    if null_bitmap != NULL:
        null_bytes = (total_length + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            free(vals_copy)
            free(lens_copy)
            free(rle)
            raise MemoryError()
        memcpy(null_copy, null_bitmap, null_bytes)
        rle.null_bitmap = null_copy
    else:
        rle.null_bitmap = NULL

    vec._rle_buffer = rle
    vec._encoding = DRAKEN_ENCODING_RLE
    return vec


cdef Int64Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
):
    """Wrap externally-malloc'd data + null_bitmap into an Int64Vector.

    Ownership of `data` and `null_bitmap` transfers to the Vector — both must
    have been allocated with the C standard library `malloc` (or be NULL),
    because `free_fixed_buffer` releases them with `free()` on dealloc.

    Used by the C++ IPC deserialiser (`src/cpp/ipc_deserialize.cpp`) to
    transfer ownership of nogil-allocated buffers without a second copy.
    """
    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        # We have not yet taken ownership of data/null_bitmap; the caller
        # frees them on a MemoryError raised from here.
        raise MemoryError()
    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = 8
    vec.ptr.length = length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    return vec


cdef Int64Vector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "Int64Vector.from_arrow expects a dense int64 Arrow array; "
            "use Int64Vector.from_dict for dictionary input"
        )

    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    # Null bitmap handling with offset support
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
            memset(dst_bitmap, 0, nb_size)

            src_bitmap = <uint8_t*> nb_addr

            # Copy bits shifting them
            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    return vec


cdef Int64Vector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Int64Vector vec = Int64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Int64Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)

    return vec


cdef Int64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Int64Vector vec = Int64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Int64Vector.from_dict requires a non-empty dictionary")
    if row_validity.shape[0] != row_count:
        raise ValueError("row_validity length must match codes length")

    nb_bytes = (row_count + 7) >> 3
    nb = <uint8_t*>malloc(nb_bytes)
    if nb == NULL:
        raise MemoryError()
    memset(nb, 0, nb_bytes)
    vec.ptr.null_bitmap = nb

    for i in range(row_count):
        if row_validity[i] != 0:
            code = <Py_ssize_t>codes[i]
            if code < 0 or code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            dst[i] = dictionary[code]
            nb[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            dst[i] = 0

    _attach_dictionary_storage(vec, codes, dictionary, False)

    return vec


cdef Int64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef Int64Vector vec = Int64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t bitmap_bytes
    cdef int32_t[::1] codes_view
    cdef int64_t[::1] dictionary_view
    cdef int32_t* expanded_codes = NULL

    if dict_size == 0:
        raise ValueError("Int64Vector.from_packed_dict requires a non-empty dictionary")
    if code_width != 1 and code_width != 2 and code_width != 4:
        raise ValueError("unsupported packed dictionary code width")

    if row_null_bitmap != NULL:
        bitmap_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(bitmap_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, row_null_bitmap, <size_t>bitmap_bytes)
    else:
        vec.ptr.null_bitmap = NULL

    if row_count > 0:
        expanded_codes = <int32_t*>malloc(row_count * sizeof(int32_t))
        if expanded_codes == NULL:
            raise MemoryError()

    try:
        for i in range(row_count):
            if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                dst[i] = 0
                expanded_codes[i] = 0
                continue
            code = _read_packed_code(codes, code_width, i)
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            dst[i] = dictionary[code]
            expanded_codes[i] = <int32_t>code

        if row_count > 0:
            codes_view = <int32_t[:row_count]>expanded_codes
        else:
            codes_view = <int32_t[:0]>expanded_codes
        if dict_size > 0:
            dictionary_view = <int64_t[:dict_size]><int64_t*>dictionary
        else:
            dictionary_view = <int64_t[:0]><int64_t*>dictionary
        _attach_dictionary_storage(vec, codes_view, dictionary_view, ordered, dict_entry_null_bitmap)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)

    return vec


cdef Int64Vector from_sequence(const int64_t[::1] data):
    """
    Create Int64Vector from a typed int64 memoryview (zero-copy).

    Args:
        data: const int64_t[::1] memoryview (C-contiguous)

    Returns:
        Int64Vector wrapping the memoryview data
    """
    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = 8
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.null_bitmap = NULL

    if data.shape[0] > 0:
        vec._arrow_data_buf = data.base if data.base is not None else data
        vec.ptr.data = <void*> &data[0]
    else:
        vec._arrow_data_buf = None
        vec.ptr.data = NULL

    return vec


# ---------------------------------------------------------------------------
# Encoding-aware reductions — sum/min/max without materialization.
#
# These avoid allocating a dense buffer and a second pass: dict-encoded
# vectors walk codes once and look up dict values directly; RLE walks runs
# (O(K) where K = run count) when no row-level null bitmap is present.
# ---------------------------------------------------------------------------

cdef int64_t _sum_dict_int64(Int64Vector vec) noexcept:
    cdef int64_t* dict_data = <int64_t*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef int64_t total = 0

    if nulls == NULL:
        with nogil:
            for i in range(n):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code < dict_size:
                    total += dict_data[code]
    else:
        with nogil:
            for i in range(n):
                if _bitmap_is_valid(nulls, i, 0):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        total += dict_data[code]
    return total


cdef int64_t _min_dict_int64(Int64Vector vec):
    cdef int64_t* dict_data = <int64_t*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i, start
    cdef uint32_t code
    cdef int64_t m
    cdef bint seen = False

    if n == 0:
        raise ValueError("Cannot compute min of empty column")

    # Find the first valid row to seed
    if nulls == NULL:
        code = _read_packed_code(codes, code_width, 0)
        if <Py_ssize_t>code >= dict_size:
            raise ValueError("dictionary index out of bounds at row 0")
        m = dict_data[code]
        seen = True
        start = 1
    else:
        start = -1
        for i in range(n):
            if _bitmap_is_valid(nulls, i, 0):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}")
                m = dict_data[code]
                seen = True
                start = i + 1
                break
        if not seen:
            raise ValueError("Cannot compute min of all-null column")

    if nulls == NULL:
        with nogil:
            for i in range(start, n):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code < dict_size:
                    if dict_data[code] < m:
                        m = dict_data[code]
    else:
        with nogil:
            for i in range(start, n):
                if _bitmap_is_valid(nulls, i, 0):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        if dict_data[code] < m:
                            m = dict_data[code]
    return m


cdef int64_t _max_dict_int64(Int64Vector vec):
    cdef int64_t* dict_data = <int64_t*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i, start
    cdef uint32_t code
    cdef int64_t m
    cdef bint seen = False

    if n == 0:
        raise ValueError("Cannot compute max of empty column")

    if nulls == NULL:
        code = _read_packed_code(codes, code_width, 0)
        if <Py_ssize_t>code >= dict_size:
            raise ValueError("dictionary index out of bounds at row 0")
        m = dict_data[code]
        seen = True
        start = 1
    else:
        start = -1
        for i in range(n):
            if _bitmap_is_valid(nulls, i, 0):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code >= dict_size:
                    raise ValueError(f"dictionary index out of bounds at row {i}")
                m = dict_data[code]
                seen = True
                start = i + 1
                break
        if not seen:
            raise ValueError("Cannot compute max of all-null column")

    if nulls == NULL:
        with nogil:
            for i in range(start, n):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code < dict_size:
                    if dict_data[code] > m:
                        m = dict_data[code]
    else:
        with nogil:
            for i in range(start, n):
                if _bitmap_is_valid(nulls, i, 0):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        if dict_data[code] > m:
                            m = dict_data[code]
    return m


cdef int64_t _sum_rle_int64(DrakenRLEBuffer* rle) noexcept:
    """Sum an RLE int64 column. Walks runs (O(K)) when no row-level null
    bitmap is present; otherwise falls back to per-row checks."""
    if rle == NULL or rle.num_runs == 0:
        return 0

    cdef int64_t* values = <int64_t*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef int64_t total = 0

    if nulls == NULL:
        # Pure run-level sum
        with nogil:
            for r in range(num_runs):
                total += values[r] * <int64_t>lengths[r]
        return total

    # Row-level null bitmap → walk rows by expanding runs
    row = 0
    for r in range(num_runs):
        n = <Py_ssize_t>lengths[r]
        for k in range(n):
            if _bitmap_is_valid(nulls, row + k, 0):
                total += values[r]
        row += n
    return total


cdef int64_t _min_rle_int64(DrakenRLEBuffer* rle):
    if rle == NULL or rle.num_runs == 0:
        raise ValueError("Cannot compute min of empty column")

    cdef int64_t* values = <int64_t*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef int64_t m
    cdef bint seen = False

    if nulls == NULL:
        # Find first non-empty run, then walk rest
        for r in range(num_runs):
            if lengths[r] > 0:
                if not seen:
                    m = values[r]
                    seen = True
                elif values[r] < m:
                    m = values[r]
        if not seen:
            raise ValueError("Cannot compute min of empty column")
        return m

    row = 0
    for r in range(num_runs):
        n = <Py_ssize_t>lengths[r]
        for k in range(n):
            if _bitmap_is_valid(nulls, row + k, 0):
                if not seen:
                    m = values[r]
                    seen = True
                elif values[r] < m:
                    m = values[r]
                # All other rows in this run share the same value — skip ahead
                break
        row += n
    if not seen:
        raise ValueError("Cannot compute min of all-null column")
    return m


cdef int64_t _max_rle_int64(DrakenRLEBuffer* rle):
    if rle == NULL or rle.num_runs == 0:
        raise ValueError("Cannot compute max of empty column")

    cdef int64_t* values = <int64_t*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef int64_t m
    cdef bint seen = False

    if nulls == NULL:
        for r in range(num_runs):
            if lengths[r] > 0:
                if not seen:
                    m = values[r]
                    seen = True
                elif values[r] > m:
                    m = values[r]
        if not seen:
            raise ValueError("Cannot compute max of empty column")
        return m

    row = 0
    for r in range(num_runs):
        n = <Py_ssize_t>lengths[r]
        for k in range(n):
            if _bitmap_is_valid(nulls, row + k, 0):
                if not seen:
                    m = values[r]
                    seen = True
                elif values[r] > m:
                    m = values[r]
                break
        row += n
    if not seen:
        raise ValueError("Cannot compute max of all-null column")
    return m
