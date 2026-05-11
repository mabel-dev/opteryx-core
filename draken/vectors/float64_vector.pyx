# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Float64Vector: Cython implementation of a fixed-width float64 column vector for Draken.

This module provides:
- The Float64Vector class for efficient float64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for float64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stdint cimport int32_t, int8_t, intptr_t, uint16_t, uint32_t, uint64_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.math cimport isinf, isnan, llround

from draken.core.buffers cimport ConstAccessor
from draken.core.buffers cimport DictAccessor
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from draken.core.buffers cimport DRAKEN_ENCODING_RLE
from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenRLEBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_FLOAT64
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector

cdef extern from "draken/vectors/_float64_reductions.hpp" namespace "draken::float64_red" nogil:
    double sum_nonnull(const double* data, size_t n)
    double sum_nullable_branchless(const double* data, const uint8_t* nulls, size_t n)
    double min_nonnull(const double* data, size_t n)
    double max_nonnull(const double* data, size_t n)
    size_t min_nullable_branchless(const double* data, const uint8_t* nulls, size_t n, double* out_min)
    size_t max_nullable_branchless(const double* data, const uint8_t* nulls, size_t n, double* out_max)

cdef extern from "draken/vectors/_float64_compare.hpp" namespace "draken::float64_cmp" nogil:
    bint dispatch_compare_once(int op, double a, double b)
    void dispatch_scalar_nonnull(int op, const double* data, double value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const double* data, double value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const double* data, double value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const double* a, const double* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const double* a, const double* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const double* a, const double* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const double* a, const double* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const double* a, const double* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

DEF FLOAT64_HASH_CHUNK = 1024

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000

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


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    cdef uint8_t byte = bitmap[idx >> 3]
    return (byte >> (idx & 7)) & 1


cdef void _release_rle_storage_float64(Float64Vector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef void _release_dict_storage(Float64Vector vec) noexcept:
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
    vec._dict_accessor.value_type = DRAKEN_FLOAT64
    vec._encoding = DRAKEN_ENCODING_DENSE


cdef void _attach_dictionary_storage(Float64Vector vec, const int32_t[::1] codes, const double[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(double)
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

    dict_values = alloc_var_buffer(DRAKEN_FLOAT64, <size_t>dict_size, <size_t>dict_bytes)
    dict_values.offsets[0] = 0
    for i in range(dict_size):
        dict_values.offsets[i + 1] = <int32_t>((i + 1) * sizeof(double))
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

cdef class Float64Vector(Vector):

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef double[::1] dictionary_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("d", dictionary)

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
        cdef Float64Vector vec = Float64Vector(0)

        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0.0 if is_null or value is None else <double>float(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT64, length, 8)
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
        self._dict_accessor.value_type = DRAKEN_FLOAT64
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_FLOAT64
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0.0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_dict_storage(self)
        _release_rle_storage_float64(self)
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
        self._const_accessor.value_type = DRAKEN_FLOAT64
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

    # Python-friendly properties
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
        cdef double* data
        cdef uint8_t byte
        cdef uint8_t bit
        cdef size_t cumulative = 0
        cdef size_t run_idx
        cdef double* rle_vals
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value
        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals = <double*>self._rle_buffer.run_values
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
            return (<double*>self._dict_values.data)[
                <Py_ssize_t>_read_packed_code(self._dict_codes, self._dict_code_width, i)
            ]
        data = <double*> ptr.data
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

        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.float64())
            return pa.array([self._const_value] * self.ptr.length, type=pa.float64())

        cdef size_t nbytes = buf_length(self.ptr) * sizeof(double)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.float64(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Float64Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self).take(indices)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self).take(indices)
        if self._has_const:
            return Float64Vector.from_constant(
                None if self._const_is_null else self._const_value,
                n,
                is_null=self._const_is_null,
            )
        cdef Float64Vector out = Float64Vector(<size_t>n)
        cdef double* src = <double*> self.ptr.data
        cdef double* dst = <double*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap
        cdef Py_ssize_t out_nbytes
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef uint8_t byte
        cdef int32_t* taken_codes = NULL
        cdef int32_t[::1] taken_codes_view
        cdef double[::1] dictionary_view
        cdef Py_ssize_t dict_size = 0

        # If source has no null bitmap, copy directly.
        if src_null == NULL:
            for i in range(n):
                src_idx = indices[i]
                dst[i] = src[src_idx]
            out.ptr.null_bitmap = NULL
        else:
            # Preserve nulls in the output bitmap.
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*> malloc(out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, out_nbytes)

            for i in range(n):
                src_idx = indices[i]
                byte = src_null[src_idx >> 3]
                if byte & (1 << (src_idx & 7)):
                    dst[i] = src[src_idx]
                    out_null[i >> 3] |= (1 << (i & 7))
                else:
                    dst[i] = 0.0

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
                    dictionary_view = <double[:dict_size]><double*>self._dict_values.data
                else:
                    dictionary_view = <double[:0]><double*>self._dict_values.data
                _attach_dictionary_storage(out, taken_codes_view, dictionary_view, self._dict_ordered != 0)
            finally:
                if taken_codes != NULL:
                    free(taken_codes)
        return out

    cdef inline bint _compare_float_values(self, double left, double right, int op) nogil:
        if op == 0:
            return left == right
        if op == 1:
            return left != right
        if op == 2:
            return left > right
        if op == 3:
            return left >= right
        if op == 4:
            return left < right
        return left <= right

    cdef BoolVector _compare_scalar(self, double value, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self)._compare_scalar(value, op)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self)._compare_scalar(value, op)

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

            matched = dispatch_compare_once(op, self._const_value, value)
            if matched and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        cdef double* data = <double*> ptr.data
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

        # Gate as in int64_vector: > ~70% null density → branching kernel
        # wins by short-circuiting null rows; otherwise the branchless kernel
        # avoids mispredicted comparison-result branches. The op is dispatched
        # once here and the inner C++ loop has no per-row branch on op.
        cdef size_t valid_count
        if src_null == NULL:
            dispatch_scalar_nonnull(op, data, value, dst, <size_t>n)
        else:
            valid_count = simd_popcount(src_null, <size_t>nbytes)
            if n > 0 and (valid_count * 10) < (<size_t>n * 3):
                dispatch_scalar_branching(op, data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless(op, data, value, src_null, dst, <size_t>n)
        return out

    cdef BoolVector _compare_vector(self, Float64Vector other, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_vector(_materialize_rle_float64(other), op)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_DICTIONARY and other.ptr.data == NULL:
            return self._compare_vector(_materialize_dict_float64(other), op)
        if self._has_const:
            return _materialize_const_float64(self)._compare_vector(other, op)
        if other._has_const:
            return self._compare_vector(_materialize_const_float64(other), op)

        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
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

        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False
        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        # Op dispatched once at the C++ boundary; the inner kernel is templated
        # so the compiler sees a single compile-time comparison per row.
        if null1 == NULL and null2 == NULL:
            dispatch_vector_nonnull(op, data1, data2, dst, <size_t>n)
        elif null1 != NULL and null2 == NULL:
            if use_branching:
                dispatch_vector_one_null_branching(op, data1, data2, null1, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branchless(op, data1, data2, null1, dst, out_null, <size_t>n)
        elif null1 == NULL and null2 != NULL:
            if use_branching:
                dispatch_vector_one_null_branching(op, data1, data2, null2, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branchless(op, data1, data2, null2, dst, out_null, <size_t>n)
        else:
            if use_branching:
                dispatch_vector_both_null_branching(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
            else:
                dispatch_vector_both_null_branchless(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
        return out

    cpdef BoolVector equals(self, double value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, double value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, double value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Float64Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, double value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, double value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Float64Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, double value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, double lower, double upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self).between(lower, upper, lower_inclusive, upper_inclusive)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self).between(lower, upper, lower_inclusive, upper_inclusive)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef double* data = <double*>ptr.data
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
            return _materialize_rle_float64(self).in_list(value_set)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self).in_list(value_set)

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

        cdef double* data = <double*> ptr.data
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

    cpdef double sum(self):
        cdef DrakenFixedBuffer* ptr
        cdef double* data
        cdef Py_ssize_t n

        if self._encoding == DRAKEN_ENCODING_RLE:
            return _sum_rle_float64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _sum_dict_float64(self)
        if self._has_const:
            if self._const_is_null:
                return 0.0
            return self.ptr.length * self._const_value
        ptr = self.ptr
        data = <double*> ptr.data
        n = ptr.length
        if n == 0:
            return 0.0
        if ptr.null_bitmap == NULL:
            return sum_nonnull(data, <size_t>n)
        return sum_nullable_branchless(data, ptr.null_bitmap, <size_t>n)

    cpdef double min(self):
        cdef DrakenFixedBuffer* ptr
        cdef double* data
        cdef Py_ssize_t n
        cdef double out
        cdef size_t valid_count

        if self._encoding == DRAKEN_ENCODING_RLE:
            return _min_rle_float64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _min_dict_float64(self)
        if self._has_const:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute min of empty column")
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value
        ptr = self.ptr
        data = <double*> ptr.data
        n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        if ptr.null_bitmap == NULL:
            return min_nonnull(data, <size_t>n)

        valid_count = min_nullable_branchless(data, ptr.null_bitmap, <size_t>n, &out)
        if valid_count == 0:
            raise ValueError("Cannot compute min of all-null column")
        return out

    cpdef double max(self):
        cdef DrakenFixedBuffer* ptr
        cdef double* data
        cdef Py_ssize_t n
        cdef double out
        cdef size_t valid_count

        if self._encoding == DRAKEN_ENCODING_RLE:
            return _max_rle_float64(self._rle_buffer)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _max_dict_float64(self)
        if self._has_const:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute max of empty column")
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value
        ptr = self.ptr
        data = <double*> ptr.data
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
            return _materialize_rle_float64(self).compare_at(left_idx, right_idx)
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self).compare_at(left_idx, right_idx)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef double left_val, right_val

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
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

        return <int8_t[:n]> buf

    cpdef int8_t[::1] is_null_with_nan(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null OR NaN, 0 otherwise.
        """
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self).is_null_with_nan()
        if self._encoding == DRAKEN_ENCODING_DICTIONARY and self.ptr.data == NULL:
            return _materialize_dict_float64(self).is_null_with_nan()

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        cdef double* data

        if buf == NULL:
            raise MemoryError()

        if self._has_const:
            for i in range(n):
                buf[i] = 1 if self._const_is_null else 0
            return <int8_t[:n]> buf

        data = <double*> ptr.data

        if ptr.null_bitmap == NULL:
            # No explicit nulls, but check for NaN
            for i in range(n):
                buf[i] = 1 if isnan(data[i]) else 0
        else:
            # Check both null bitmap and NaN
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit == 0:  # null bitmap says invalid
                    buf[i] = 1
                elif isnan(data[i]):  # value is NaN
                    buf[i] = 1
                else:
                    buf[i] = 0

        return <int8_t[:n]> buf

    @property
    def null_count(self):
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
        """Return a dense Float64Vector, expanding dict/const/RLE encodings if needed."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Float64Vector dense
        cdef double* dst
        cdef Py_ssize_t i, nb_bytes
        if self._encoding == DRAKEN_ENCODING_DICTIONARY:
            if ptr.data == NULL:
                # dict-only (make_float64_dict_only path): codes in _dict_codes
                return _materialize_dict_float64(self)
            else:
                # from_dict path: dense data already in ptr.data, copy to new dense vector
                dense = Float64Vector(<size_t>n)
                memcpy(dense.ptr.data, ptr.data, <size_t>n * sizeof(double))
                if ptr.null_bitmap != NULL:
                    nb_bytes = (n + 7) >> 3
                    dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                    if dense.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memcpy(dense.ptr.null_bitmap, ptr.null_bitmap, <size_t>nb_bytes)
                return dense
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float64(self)
        if self._has_const:
            dense = Float64Vector(<size_t>n)
            dst = <double*>dense.ptr.data
            if self._const_is_null:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(dense.ptr.null_bitmap, 0, <size_t>nb_bytes)
                memset(dst, 0, <size_t>n * sizeof(double))
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
            return 8  # sizeof(double)
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
        cdef uint8_t byte, bit
        cdef double* rle_vals
        cdef int32_t* rle_lens
        cdef size_t rle_runs
        cdef uint8_t* rle_nulls
        cdef Py_ssize_t pos
        cdef size_t r
        cdef int32_t run_len
        cdef double run_val

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals = <double*>self._rle_buffer.run_values
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

        cdef double* data = <double*> ptr.data

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

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t value
        cdef uint64_t* dst
        cdef uint64_t[FLOAT64_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef double* rle_vals_hash
        cdef uint64_t* rle_bits
        cdef int32_t* rle_lens_hash
        cdef size_t rle_runs_hash
        cdef uint8_t* rle_nulls_hash
        cdef Py_ssize_t hpos
        cdef size_t hr
        cdef uint64_t run_hash
        cdef int32_t run_len_h
        cdef double* data
        cdef uint64_t* bits
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef uint64_t is_valid
        cdef double* _dict_data
        cdef uint64_t* _dict_bits
        cdef uint8_t* _dict_codes_h
        cdef uint8_t  _dict_cw_h
        cdef uint8_t* _dict_nb_h
        cdef double   _dv

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float64Vector.hash_into: output buffer too small")

        dst = &out_buf[offset]

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals_hash = <double*>self._rle_buffer.run_values
            rle_bits = <uint64_t*>rle_vals_hash
            rle_lens_hash = self._rle_buffer.run_lengths
            rle_runs_hash = self._rle_buffer.num_runs
            rle_nulls_hash = self._rle_buffer.null_bitmap
            hpos = 0
            for hr in range(rle_runs_hash):
                run_len_h = rle_lens_hash[hr]
                run_hash = rle_bits[hr]
                i = 0
                while i < run_len_h:
                    block = run_len_h - i
                    if block > FLOAT64_HASH_CHUNK:
                        block = FLOAT64_HASH_CHUNK
                    for j in range(block):
                        scratch[j] = run_hash
                    if rle_nulls_hash != NULL:
                        for j in range(block):
                            if not ((rle_nulls_hash[(hpos + i + j) >> 3] >> ((hpos + i + j) & 7)) & 1):
                                scratch[j] = NULL_HASH
                    simd_mix_hash(dst + hpos + i, scratch_ptr, <size_t>block)
                    i += block
                hpos += run_len_h
            return

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            _dict_data    = <double*>self._dict_values.data
            _dict_bits    = <uint64_t*>_dict_data
            _dict_codes_h = self._dict_codes
            _dict_cw_h    = self._dict_code_width
            _dict_nb_h    = ptr.null_bitmap
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    if _dict_nb_h != NULL and not ((_dict_nb_h[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = _dict_bits[
                            <Py_ssize_t>_read_packed_code(_dict_codes_h, _dict_cw_h, i + j)
                        ]
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        data = <double*> ptr.data
        bits = <uint64_t*> data
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        if self._has_const:
            value = NULL_HASH if self._const_is_null else (<uint64_t*>&self._const_value)[0]
            for j in range(FLOAT64_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (bits[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
        else:
            simd_mix_hash(dst, bits, <size_t>n)
            return

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef uint64_t[FLOAT64_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint64_t* _cd_dict_bits
        cdef uint8_t* _cd_dict_codes
        cdef uint8_t  _cd_dict_cw
        cdef uint8_t* _cd_null_bitmap

        if n == 0:
            return 0

        if self._has_const:
            value = NULL_HASH if self._const_is_null else (<uint64_t*>&self._const_value)[0]
            for j in range(FLOAT64_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # DICTIONARY-only path: ptr.data is NULL, values looked up via codes
        if ptr.data == NULL and self._dict_codes != NULL:
            _cd_dict_bits  = <uint64_t*>self._dict_values.data
            _cd_dict_codes = self._dict_codes
            _cd_dict_cw    = self._dict_code_width
            _cd_null_bitmap = ptr.null_bitmap
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    if _cd_null_bitmap != NULL and not ((_cd_null_bitmap[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = _cd_dict_bits[
                            <Py_ssize_t>_read_packed_code(_cd_dict_codes, _cd_dict_cw, i + j)
                        ]
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        cdef double* data = <double*> ptr.data
        cdef uint64_t* bits = <uint64_t*> data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (bits[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t> block)
                i += block
        else:
            simd_mix_hash(out, bits, <size_t>n)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for Float64Vector with NaN/Inf handling and clamping."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base
        cdef int64_t* dst
        cdef Py_ssize_t i
        cdef double v
        cdef long long rv
        cdef int64_t MIN_SIGNED = <int64_t> -9223372036854775807
        cdef int64_t MAX_SIGNED = <int64_t> 9223372036854775807
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
        cdef double* rle_vals_c
        cdef int32_t* rle_lens_c
        cdef size_t rle_runs_c
        cdef uint8_t* rle_nulls_c
        cdef Py_ssize_t cpos
        cdef size_t cr
        cdef int32_t run_len_c
        cdef double run_val_c
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef double* _ci_dict_data
        cdef uint8_t* _ci_dict_codes
        cdef uint8_t  _ci_dict_cw

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float64Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        dst = dst_base + offset

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals_c = <double*>self._rle_buffer.run_values
            rle_lens_c = self._rle_buffer.run_lengths
            rle_runs_c = self._rle_buffer.num_runs
            rle_nulls_c = self._rle_buffer.null_bitmap
            cpos = 0
            for cr in range(rle_runs_c):
                run_val_c = rle_vals_c[cr]
                run_len_c = rle_lens_c[cr]
                for i in range(run_len_c):
                    if rle_nulls_c != NULL and not ((rle_nulls_c[(cpos + i) >> 3] >> ((cpos + i) & 7)) & 1):
                        dst[cpos + i] = NULL_FLAG
                        continue
                    v = run_val_c
                    if isnan(v):
                        dst[cpos + i] = NULL_FLAG
                    elif isinf(v):
                        dst[cpos + i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    else:
                        rv = llround(v)
                        if rv < MIN_SIGNED:
                            dst[cpos + i] = MIN_SIGNED
                        elif rv > MAX_SIGNED:
                            dst[cpos + i] = MAX_SIGNED
                        else:
                            dst[cpos + i] = <int64_t>rv
                cpos += run_len_c
            return

        if self._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
            _ci_dict_data  = <double*>self._dict_values.data
            _ci_dict_codes = self._dict_codes
            _ci_dict_cw    = self._dict_code_width
            null_bitmap    = ptr.null_bitmap
            for i in range(n):
                if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                    dst[i] = NULL_FLAG
                    continue
                v = _ci_dict_data[<Py_ssize_t>_read_packed_code(_ci_dict_codes, _ci_dict_cw, i)]
                if isnan(v):
                    dst[i] = NULL_FLAG
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                else:
                    rv = llround(v)
                    if rv < MIN_SIGNED:
                        dst[i] = MIN_SIGNED
                    elif rv > MAX_SIGNED:
                        dst[i] = MAX_SIGNED
                    else:
                        dst[i] = <int64_t>rv
            return

        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        if self._has_const:
            for i in range(n):
                if self._const_is_null:
                    dst[i] = NULL_FLAG
                    continue
                v = self._const_value
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv
            return

        cdef double* data = <double*> ptr.data
        if has_nulls:
            for i in range(n):
                if ((null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    dst[i] = NULL_FLAG
                    continue
                v = data[i]
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv
        else:
            for i in range(n):
                v = data[i]
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:k]
            return f"<Float64Vector(RLE) len={buf_length(self.ptr)} values={vals}>"
        if self._has_const:
            vals = [None if self._const_is_null else self._const_value] * k
            return f"<Float64Vector len={buf_length(self.ptr)} values={vals}>"
        cdef double* data = <double*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Float64Vector len={buf_length(self.ptr)} values={vals}>"


cdef Float64Vector _materialize_const_float64(Float64Vector const_vec):
    """Expand a CONSTANT Float64Vector to a dense Float64Vector."""
    cdef size_t n = const_vec.ptr.length
    cdef Float64Vector dense = Float64Vector(n)
    cdef double* dst = <double*>dense.ptr.data
    cdef double val = const_vec._const_value
    cdef bint is_null = const_vec._const_is_null
    cdef size_t i
    cdef size_t null_bytes
    cdef uint8_t* null_bm

    if is_null:
        null_bytes = (n + 7) >> 3
        null_bm = <uint8_t*>malloc(null_bytes)
        if null_bm == NULL:
            raise MemoryError()
        memset(null_bm, 0, null_bytes)
        dense.ptr.null_bitmap = null_bm
    else:
        for i in range(n):
            dst[i] = val
    return dense


cdef Float64Vector _materialize_rle_float64(Float64Vector rle_vec):
    """Expand an RLE Float64Vector to a dense Float64Vector."""
    cdef size_t total = rle_vec._rle_buffer.length
    cdef Float64Vector dense = Float64Vector(<size_t>total)
    cdef double* rle_vals = <double*>rle_vec._rle_buffer.run_values
    cdef int32_t* rle_lens = rle_vec._rle_buffer.run_lengths
    cdef size_t num_runs = rle_vec._rle_buffer.num_runs
    cdef uint8_t* rle_nulls = rle_vec._rle_buffer.null_bitmap
    cdef double* dst = <double*>dense.ptr.data
    cdef size_t pos = 0
    cdef size_t r
    cdef int32_t run_len
    cdef Py_ssize_t j
    cdef size_t null_bytes
    cdef uint8_t* null_copy
    for r in range(num_runs):
        run_len = rle_lens[r]
        for j in range(run_len):
            dst[pos + j] = rle_vals[r]
        pos += <size_t>run_len
    if rle_nulls != NULL:
        null_bytes = (total + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            raise MemoryError()
        memcpy(null_copy, rle_nulls, null_bytes)
        dense.ptr.null_bitmap = null_copy
    return dense


cdef Float64Vector _materialize_dict_float64(Float64Vector vec):
    """Expand a dict-only Float64Vector to a dense Float64Vector (no src ptr.data needed)."""
    if vec._dict_values == NULL or vec._dict_codes == NULL:
        raise ValueError("Dictionary encoding not properly initialized")

    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Float64Vector dense = Float64Vector(<size_t>n)
    cdef double* dst = <double*>dense.ptr.data
    cdef double* dict_data = <double*>vec._dict_values.data
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t i, dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint32_t code
    cdef Py_ssize_t nb_bytes

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            dst[i] = 0.0
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


cdef Float64Vector make_float64_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded Float64Vector with no dense materialization.

    Args:
        codes:       Packed code array (code_width bytes per row, row_count entries).
        code_width:  Bytes per code: 1, 2, or 4.
        row_count:   Total number of rows.
        dictionary:  Array of unique double values (dict_size entries).
        dict_size:   Number of unique dictionary values.
        valid_bits:  Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        Float64Vector with DRAKEN_ENCODING_DICTIONARY; ptr.data is NULL (no dense storage).
    """
    cdef Float64Vector vec = Float64Vector(0)   # allocates ptr header; ptr.data = NULL
    cdef Py_ssize_t code_bytes = row_count * <Py_ssize_t>code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(double)
    cdef Py_ssize_t nb_bytes
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t i

    vec.ptr.length = <size_t>row_count  # logical length; ptr.data stays NULL

    if valid_bits != NULL:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, valid_bits, <size_t>nb_bytes)

    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(<size_t>code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
        memcpy(vec._dict_codes, codes, <size_t>code_bytes)

    dict_values = alloc_var_buffer(DRAKEN_FLOAT64, <size_t>dict_size, <size_t>dict_bytes)
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>dictionary, <size_t>dict_bytes)
    for i in range(dict_size):
        dict_values.offsets[i] = <int32_t>(i * sizeof(double))
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
    vec._dict_accessor.value_type = DRAKEN_FLOAT64

    return vec


cdef Float64Vector from_rle_builder(
    double* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded Float64Vector from raw C arrays (copies data)."""
    cdef Float64Vector vec = Float64Vector(0, False)
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef double* vals_copy
    cdef int32_t* lens_copy
    cdef size_t null_bytes
    cdef uint8_t* null_copy
    for i in range(num_runs):
        total_length += <size_t>run_lengths[i]
    vec.ptr.length = total_length
    if num_runs == 0:
        vec._encoding = DRAKEN_ENCODING_RLE
        return vec
    rle = <DrakenRLEBuffer*>malloc(sizeof(DrakenRLEBuffer))
    if rle == NULL:
        raise MemoryError()
    vals_copy = <double*>malloc(num_runs * sizeof(double))
    lens_copy = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if vals_copy == NULL or lens_copy == NULL:
        free(rle)
        if vals_copy != NULL: free(vals_copy)
        if lens_copy != NULL: free(lens_copy)
        raise MemoryError()
    memcpy(vals_copy, run_values, num_runs * sizeof(double))
    memcpy(lens_copy, run_lengths, num_runs * sizeof(int32_t))
    rle.run_values = <void*>vals_copy
    rle.run_lengths = lens_copy
    rle.num_runs = num_runs
    rle.length = total_length
    rle.type = DRAKEN_FLOAT64
    if null_bitmap != NULL:
        null_bytes = (total_length + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            free(vals_copy); free(lens_copy); free(rle)
            raise MemoryError()
        memcpy(null_copy, null_bitmap, null_bytes)
        rle.null_bitmap = null_copy
    else:
        rle.null_bitmap = NULL
    vec._rle_buffer = rle
    vec._encoding = DRAKEN_ENCODING_RLE
    return vec


cdef Float64Vector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "Float64Vector.from_arrow expects a dense float64 Arrow array; "
            "use Float64Vector.from_dict for dictionary input"
        )

    cdef Float64Vector vec = Float64Vector(0, True)
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

    vec.ptr.type = DRAKEN_FLOAT64
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


cdef Float64Vector from_dict(const int32_t[::1] codes, const double[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Float64Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)

    return vec


cdef Float64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const double[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Float64Vector.from_dict requires a non-empty dictionary")
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
            dst[i] = 0.0

    _attach_dictionary_storage(vec, codes, dictionary, False)

    return vec


cdef Float64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t bitmap_bytes
    cdef int32_t[::1] codes_view
    cdef double[::1] dictionary_view
    cdef int32_t* expanded_codes = NULL

    if dict_size == 0:
        raise ValueError("Float64Vector.from_packed_dict requires a non-empty dictionary")
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
                dst[i] = 0.0
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
            dictionary_view = <double[:dict_size]><double*>dictionary
        else:
            dictionary_view = <double[:0]><double*>dictionary
        _attach_dictionary_storage(vec, codes_view, dictionary_view, ordered, dict_entry_null_bitmap)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)

    return vec


cdef Float64Vector from_sequence(double[::1] data):
    """
    Create Float64Vector from a typed double memoryview (zero-copy).

    Args:
        data: double[::1] memoryview (C-contiguous)

    Returns:
        Float64Vector wrapping the memoryview data
    """
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = 8
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    return vec


# ---------------------------------------------------------------------------
# Encoding-aware reductions — sum/min/max without materialization.
# ---------------------------------------------------------------------------

cdef double _sum_dict_float64(Float64Vector vec) noexcept:
    cdef double* dict_data = <double*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef double total = 0.0

    if nulls == NULL:
        with nogil:
            for i in range(n):
                code = _read_packed_code(codes, code_width, i)
                if <Py_ssize_t>code < dict_size:
                    total += dict_data[code]
    else:
        with nogil:
            for i in range(n):
                if _bitmap_is_valid(nulls, i):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        total += dict_data[code]
    return total


cdef double _min_dict_float64(Float64Vector vec):
    cdef double* dict_data = <double*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i, start
    cdef uint32_t code
    cdef double m = 0.0
    cdef bint seen = False

    if n == 0:
        raise ValueError("Cannot compute min of empty column")

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
            if _bitmap_is_valid(nulls, i):
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
                if _bitmap_is_valid(nulls, i):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        if dict_data[code] < m:
                            m = dict_data[code]
    return m


cdef double _max_dict_float64(Float64Vector vec):
    cdef double* dict_data = <double*>vec._dict_values.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Py_ssize_t i, start
    cdef uint32_t code
    cdef double m = 0.0
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
            if _bitmap_is_valid(nulls, i):
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
                if _bitmap_is_valid(nulls, i):
                    code = _read_packed_code(codes, code_width, i)
                    if <Py_ssize_t>code < dict_size:
                        if dict_data[code] > m:
                            m = dict_data[code]
    return m


cdef double _sum_rle_float64(DrakenRLEBuffer* rle) noexcept:
    if rle == NULL or rle.num_runs == 0:
        return 0.0

    cdef double* values = <double*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef double total = 0.0

    if nulls == NULL:
        with nogil:
            for r in range(num_runs):
                total += values[r] * <double>lengths[r]
        return total

    row = 0
    for r in range(num_runs):
        n = <Py_ssize_t>lengths[r]
        for k in range(n):
            if _bitmap_is_valid(nulls, row + k):
                total += values[r]
        row += n
    return total


cdef double _min_rle_float64(DrakenRLEBuffer* rle):
    if rle == NULL or rle.num_runs == 0:
        raise ValueError("Cannot compute min of empty column")

    cdef double* values = <double*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef double m = 0.0
    cdef bint seen = False

    if nulls == NULL:
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
            if _bitmap_is_valid(nulls, row + k):
                if not seen:
                    m = values[r]
                    seen = True
                elif values[r] < m:
                    m = values[r]
                break
        row += n
    if not seen:
        raise ValueError("Cannot compute min of all-null column")
    return m


cdef double _max_rle_float64(DrakenRLEBuffer* rle):
    if rle == NULL or rle.num_runs == 0:
        raise ValueError("Cannot compute max of empty column")

    cdef double* values = <double*>rle.run_values
    cdef int32_t* lengths = rle.run_lengths
    cdef size_t num_runs = rle.num_runs
    cdef uint8_t* nulls = rle.null_bitmap
    cdef Py_ssize_t r, n, row, k
    cdef double m = 0.0
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
            if _bitmap_is_valid(nulls, row + k):
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
