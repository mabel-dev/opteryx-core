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
Date32Vector: Cython implementation of a fixed-width date32 column vector for Draken.

This module provides:
- The Date32Vector class for efficient date32 column storage (days since Unix epoch)
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast comparison and null handling for date32 columns

Used for high-performance temporal analytics and columnar data processing in Draken.
"""

import datetime as _dt

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset, memcpy

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from draken.core.buffers cimport DRAKEN_DATE32
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_DICTIONARY
from draken.core.buffers cimport DrakenVector
from draken.vectors.int64_vector cimport _materialize_dict_int64
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.timestamp_vector cimport TimestampVector

cdef extern from "simd_hash.h":
    void simd_scale_date32(const int32_t* src, int64_t* dest, size_t count) nogil

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_date32_compare.hpp" namespace "draken::date32_cmp" nogil:
    void bit_fill_range(uint8_t* dst, size_t start, size_t count)
    bint dispatch_compare_once(int op, int32_t a, int32_t b)
    void dispatch_scalar_nonnull(int op, const int32_t* data, int32_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const int32_t* data, int32_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const int32_t* data, int32_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const int32_t* a, const int32_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

# Constants for temporal arithmetic (Phase 5b)
cdef int64_t MICROSECONDS_PER_DAY = 86_400_000_000
cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000
cdef uint8_t _CONST_NULL_BYTE = 0

DEF DATE32_HASH_CHUNK = 1024
_DATE_EPOCH_ORDINAL = _dt.date(1970, 1, 1).toordinal()


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1


cdef void _refresh_unified_date32(Date32Vector vec) noexcept:
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    vec._unified_view.length = <size_t>n
    vec._unified_view.itemsize = sizeof(int32_t)
    vec._unified_view.type = DRAKEN_DATE32
    if vec._has_const:
        vec._unified_view.data = <void*>&vec._const_value
        vec._unified_view.data_length = 1
        vec._unified_view.selection = NULL
        vec._unified_view.sel_width = 0
        vec._unified_view.validity = &_CONST_NULL_BYTE if vec._const_is_null else NULL
    else:
        vec._unified_view.data = vec.ptr.data
        vec._unified_view.data_length = <size_t>n
        vec._unified_view.selection = NULL
        vec._unified_view.sel_width = 0
        vec._unified_view.validity = vec.ptr.null_bitmap


cdef class Date32Vector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef Date32Vector vec = Date32Vector(0)
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else <int32_t>int(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        _refresh_unified_date32(vec)
        return vec

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef int32_t[::1] dictionary_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("i", dictionary)

        codes_view = codes
        dictionary_view = dictionary

        if row_validity is None:
            return from_dict(codes_view, dictionary_view)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary_view, validity_view)

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_DATE32, length, 4)
            self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_DATE32
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._unified_view.data = NULL
        self._unified_view.data_length = 0
        self._unified_view.selection = NULL
        self._unified_view.sel_width = 0
        self._unified_view.length = 0
        self._unified_view.validity = NULL
        self._unified_view.itemsize = sizeof(int32_t)
        self._unified_view.type = DRAKEN_DATE32
        if not wrap:
            _refresh_unified_date32(self)

    def __dealloc__(self):
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_DATE32
        self._const_accessor.value_ptr = <void*>&self._const_value
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

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

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

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value
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
                return pa.nulls(self.ptr.length, type=pa.date32())
            return pa.array([self._const_value] * self.ptr.length, type=pa.date32())

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.date32(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Date32Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            return Date32Vector.from_constant(
                None if self._const_is_null else self._const_value,
                indices.shape[0],
                is_null=self._const_is_null,
            )
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Date32Vector out = Date32Vector(<size_t>n)
        cdef int32_t* src = <int32_t*> self.ptr.data
        cdef int32_t* dst = <int32_t*> out.ptr.data
        for i in range(n):
            dst[i] = src[indices[i]]
        _refresh_unified_date32(out)
        return out

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n):
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* null_bm
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

    cpdef BoolVector _compare_scalar(self, int32_t value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef bint matched

        if uv.data_length == 1:
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

        cdef int32_t* data = <int32_t*> ptr.data
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
        if src_null == NULL:
            dispatch_scalar_nonnull(op, data, value, dst, <size_t>n)
        else:
            valid_count = simd_popcount(src_null, <size_t>nbytes)
            if n > 0 and (valid_count * 10) < (<size_t>n * 3):
                dispatch_scalar_branching(op, data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless(op, data, value, src_null, dst, <size_t>n)
        return out

    cpdef BoolVector _compare_vector_op(self, Date32Vector other, int op):
        # Consolidates the 6 *_vector comparison ops. Op dispatch and null-pointer
        # specialisation happen once here; the C++ kernel runs a tight loop with
        # no per-row branching. Const fast paths avoid O(n) materialisation.
        # Const fast paths: avoid O(n) materialisation.
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t const_n
        cdef int reversed_op
        if uv.data_length == 1:
            const_n = self.ptr.length
            if const_n != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if self._const_is_null:
                return self._make_all_null_bool(const_n)
            # Reverse directional ops: gt(2)<->lt(4), ge(3)<->le(5)
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
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
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

    cpdef BoolVector equals(self, int32_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector not_equals(self, int32_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector greater_than(self, int32_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_or_equals(self, int32_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector less_than(self, int32_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_or_equals(self, int32_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector between(self, int32_t lower, int32_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            return _materialize_const_date32(self).between(lower, upper, lower_inclusive, upper_inclusive)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*>ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef bint in_range

        memset(dst, 0, nbytes)

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

    cpdef BoolVector equals_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 0)

    cpdef BoolVector not_equals_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 1)

    cpdef BoolVector greater_than_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 2)

    cpdef BoolVector greater_than_or_equals_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 3)

    cpdef BoolVector less_than_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 4)

    cpdef BoolVector less_than_or_equals_vector(self, Date32Vector other):
        return self._compare_vector_op(other, 5)

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0. Propagates NULLs."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

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

    cpdef int32_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if uv.data_length == 1:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value

        cdef int32_t m
        cdef bint found = False

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        # Find minimum among remaining values
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):  # null
                    continue
            if data[i] < m:
                m = data[i]
        return m

    cpdef int32_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if uv.data_length == 1:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value

        cdef int32_t m
        cdef bint found = False

        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):  # null
                    continue
            if data[i] > m:
                m = data[i]
        return m

    cpdef int64_t sum(self):
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):  # null
                    continue
            total += data[i]
        return total

    cpdef Int64Vector subtract_date32_vector(self, Date32Vector other):
        """Subtract two Date32Vector values and return microseconds as Int64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Int64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t left_us, right_us

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Int64Vector(<size_t>n)
        dst = <int64_t*> out.ptr.data
        memset(dst, 0, n * sizeof(int64_t))

        if (null1 != NULL or null2 != NULL) and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            if valid1 and valid2:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                left_us = <int64_t>data1[i] * MICROSECONDS_PER_DAY
                right_us = <int64_t>data2[i] * MICROSECONDS_PER_DAY
                dst[i] = left_us - right_us
        return out

    cpdef Int64Vector subtract_timestamp_vector(self, TimestampVector other):
        """Subtract TimestampVector from Date32Vector and return microseconds as Int64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Int64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t left_us

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Int64Vector(<size_t>n)
        dst = <int64_t*> out.ptr.data
        memset(dst, 0, n * sizeof(int64_t))

        if (null1 != NULL or null2 != NULL) and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            if valid1 and valid2:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                left_us = <int64_t>data1[i] * MICROSECONDS_PER_DAY
                dst[i] = left_us - data2[i]
        return out

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
        cdef int8_t* buf
        cdef uint8_t byte, bit

        n = ptr.length
        buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        if uv.data_length == 1:
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
        if self._has_const:
            return n if self._const_is_null else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, bm_bytes
        if self._has_const:
            return buf_itemsize(ptr)
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef object date_fromordinal = _dt.date.fromordinal
        cdef int ordinal

        if uv.data_length == 1:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    ordinal = _DATE_EPOCH_ORDINAL + self._const_value
                    try:
                        out.append(date_fromordinal(ordinal))
                    except (OverflowError, ValueError):
                        out.append(self._const_value)
            return out
        if ptr.null_bitmap == NULL:
            for i in range(n):
                ordinal = _DATE_EPOCH_ORDINAL + data[i]
                try:
                    out.append(date_fromordinal(ordinal))
                except (OverflowError, ValueError):
                    out.append(data[i])
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    ordinal = _DATE_EPOCH_ORDINAL + data[i]
                    try:
                        out.append(date_fromordinal(ordinal))
                    except (OverflowError, ValueError):
                        out.append(data[i])
                else:
                    out.append(None)

        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare date32 values at two indices. Returns -1, 0, or 1."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t left_val, right_val
        cdef bint left_is_null, right_is_null

        # Check nulls
        left_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, left_idx, 0)
        right_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, right_idx, 0)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        left_val = (<int32_t*>ptr.data)[left_idx]
        right_val = (<int32_t*>ptr.data)[right_idx]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        else:
            return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        if ptr.null_bitmap == NULL:
            return False
        return not _bitmap_is_valid(ptr.null_bitmap, idx, 0)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint8_t byte, bit
        cdef uint64_t value
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t[DATE32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if uv.data_length == 1:
            value = NULL_HASH if self._const_is_null else <uint64_t><int64_t>self._const_value
            for j in range(DATE32_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > DATE32_HASH_CHUNK:
                    block = DATE32_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Date32Vector.hash_into: output buffer too small")

        cdef uint64_t is_valid
        i = 0
        while i < n:
            block = n - i
            if block > DATE32_HASH_CHUNK:
                block = DATE32_HASH_CHUNK
            if has_nulls:
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (<uint64_t>(<int64_t> data[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
            else:
                for j in range(block):
                    scratch[j] = <uint64_t>(<int64_t> data[i + j])
            simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t value
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t[DATE32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t><int64_t>self._const_value
            for j in range(DATE32_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > DATE32_HASH_CHUNK:
                    block = DATE32_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        if n == 0:
            return 0

        cdef uint64_t is_valid
        i = 0
        while i < n:
            block = n - i
            if block > DATE32_HASH_CHUNK:
                block = DATE32_HASH_CHUNK
            if has_nulls:
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (<uint64_t>(<int64_t> data[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
            else:
                for j in range(block):
                    scratch[j] = <uint64_t>(<int64_t> data[i + j])
            simd_mix_hash(out + i, scratch_ptr, <size_t> block)
            i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for Date32Vector: scale int32 days to int64 microseconds (to match datetimes)."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
        cdef int64_t MICROSECONDS_PER_DAY = <int64_t>86400000000
        cdef int64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i
        cdef uint8_t byte, bit

        if uv.data_length == 1:
            for i in range(n):
                dst[i] = <int64_t> (-(1 << 63)) if self._const_is_null else (<int64_t> self._const_value * MICROSECONDS_PER_DAY)
            return
        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Date32Vector.compress: output buffer too small")

        if has_nulls:
            for i in range(n):
                byte = null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    dst[i] = <int64_t> data[i] * MICROSECONDS_PER_DAY
                else:
                    dst[i] = NULL_FLAG
        else:
            simd_scale_date32(data, dst, <size_t>n)

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        cdef int32_t* data
        if self._has_const:
            return f"<Date32Vector len={buf_length(self.ptr)} values={[None if self._const_is_null else self._const_value] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        data = <int32_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Date32Vector len={buf_length(self.ptr)} values={vals}>"


cdef Date32Vector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "Date32Vector.from_arrow expects a dense date32 Arrow array; "
            "use Date32Vector.from_dict for dictionary input"
        )

    cdef Date32Vector vec = Date32Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 4
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_DATE32
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr
    vec._arrow_data_buf = bufs[1]  # Keep Arrow data buffer alive

    # Variables for null bitmap handling
    cdef Py_ssize_t n_bytes
    cdef bytes new_bitmap
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap
    cdef int bit_offset
    cdef Py_ssize_t byte_offset
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef Py_ssize_t i

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (nb_addr + (offset >> 3))
            vec._arrow_null_buf = bufs[0]  # Keep Arrow null bitmap alive
        else:
            # Unaligned offset: copy and shift
            n_bytes = (vec.ptr.length + 7) // 8
            new_bitmap = PyBytes_FromStringAndSize(NULL, n_bytes)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap)

            byte_offset = offset >> 3
            bit_offset = offset & 7
            src_bitmap = <uint8_t*> nb_addr + byte_offset

            shift_down = bit_offset
            shift_up = 8 - bit_offset

            # We can safely read one extra byte because Arrow buffers are padded
            for i in range(n_bytes):
                val = src_bitmap[i] >> shift_down
                # Always OR with the next byte shifted up.
                # Even for the last byte, Arrow padding ensures src_bitmap[i+1] is accessible (though might be garbage, but we only care about valid bits)
                # Actually, for the last byte, we might not need the next byte if the length fits.
                # But simpler to just do it.
                val |= (src_bitmap[i+1] << shift_up)
                dst_bitmap[i] = val

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap # Keep alive
    else:
        vec.ptr.null_bitmap = NULL

    _refresh_unified_date32(vec)
    return vec


cdef Date32Vector from_dict(const int32_t[::1] codes, const int32_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Date32Vector vec = Date32Vector(<size_t>row_count)
    cdef int32_t* dst = <int32_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Date32Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _refresh_unified_date32(vec)
    return vec


cdef Date32Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int32_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Date32Vector vec = Date32Vector(<size_t>row_count)
    cdef int32_t* dst = <int32_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Date32Vector.from_dict requires a non-empty dictionary")
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

    _refresh_unified_date32(vec)
    return vec


cpdef Date32Vector from_int64_vector(Int64Vector source):
    """
    Convert an Int64Vector containing epoch-day values to Date32Vector.

    This is a native Draken conversion path (no Arrow interop).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t n = <Py_ssize_t>source.ptr.length
    cdef Date32Vector out
    cdef int64_t* src_data
    cdef int32_t* dst_data
    cdef uint8_t* src_null
    cdef size_t nb_bytes
    cdef uint8_t* out_null
    cdef bint is_valid
    cdef int64_t value64
    cdef int64_t int32_min = -2147483648
    cdef int64_t int32_max = 2147483647

    if source._encoding == DRAKEN_ENCODING_DICTIONARY and source.ptr.data == NULL:
        source = _materialize_dict_int64(source)

    if source._has_const:
        if source._const_is_null:
            return Date32Vector.from_constant(None, n, is_null=True)
        if source._const_value < int32_min or source._const_value > int32_max:
            raise OverflowError(f"date32 value out of range: {source._const_value}")
        return Date32Vector.from_constant(<int32_t>source._const_value, n)

    out = Date32Vector(<size_t>n)
    src_data = <int64_t*>source.ptr.data
    dst_data = <int32_t*>out.ptr.data
    src_null = <uint8_t*>source.ptr.null_bitmap

    if src_null != NULL:
        nb_bytes = (<size_t>n + 7) >> 3
        out_null = <uint8_t*>malloc(nb_bytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, src_null, nb_bytes)
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    for i in range(n):
        if src_null != NULL:
            is_valid = ((src_null[i >> 3] >> (i & 7)) & 1) != 0
            if not is_valid:
                dst_data[i] = 0
                continue

        value64 = src_data[i]
        if value64 < int32_min or value64 > int32_max:
            raise OverflowError(f"date32 value out of range at row {i}: {value64}")
        dst_data[i] = <int32_t>value64

    _refresh_unified_date32(out)
    return out


cdef Date32Vector _materialize_const_date32(Date32Vector const_vec):
    """Expand a CONSTANT Date32Vector to a dense Date32Vector."""
    cdef size_t n = const_vec.ptr.length
    cdef Date32Vector dense = Date32Vector(n)
    cdef int32_t* dst = <int32_t*>dense.ptr.data
    cdef int32_t val = const_vec._const_value
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
    _refresh_unified_date32(dense)
    return dense


