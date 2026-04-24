# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
from libc.stdlib cimport malloc

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from opteryx.compiled.draken.core.fixed_vector cimport alloc_fixed_buffer
from opteryx.compiled.draken.core.fixed_vector cimport buf_dtype
from opteryx.compiled.draken.core.fixed_vector cimport buf_itemsize
from opteryx.compiled.draken.core.fixed_vector cimport buf_length
from opteryx.compiled.draken.core.fixed_vector cimport free_fixed_buffer
from opteryx.compiled.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector

cdef extern from "simd_hash.h":
    void simd_scale_date32(const int32_t* src, int64_t* dest, size_t count) nogil

# Constants for temporal arithmetic (Phase 5b)
cdef int64_t MICROSECONDS_PER_DAY = 86_400_000_000

DEF DATE32_HASH_CHUNK = 1024
_DATE_EPOCH_ORDINAL = _dt.date(1970, 1, 1).toordinal()


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1


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
        if i < 0 or i >= ptr.length:
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
        if self._has_const:
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
        return out

    cdef inline bint _compare_date_values(self, int32_t left, int32_t right, int op) nogil:
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

    cdef BoolVector _compare_scalar(self, int32_t value, int op):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

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
                if self._compare_date_values(data[i], value, op):
                    dst[i >> 3] |= (1 << (i & 7))
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

    cpdef BoolVector equals_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 0):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector not_equals_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 1):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector greater_than_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 2):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector greater_than_or_equals_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 3):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector less_than_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 4):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector less_than_or_equals_vector(self, Date32Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_date_values(data1[i], data2[i], 5):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value

        cdef int32_t m
        cdef bint found = False

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        # Find minimum among remaining values
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            if data[i] < m:
                m = data[i]
        return m

    cpdef int32_t max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value

        cdef int32_t m
        cdef bint found = False

        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            if data[i] > m:
                m = data[i]
        return m

    cpdef int64_t sum(self):
        if self._has_const:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
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
        cdef DrakenFixedBuffer* ptr = self.ptr
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

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef object date_fromordinal = _dt.date.fromordinal
        cdef int ordinal

        if self._has_const:
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

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
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

        if self._has_const:
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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef int64_t NULL_FLAG = <int64_t> -9223372036854775808
        cdef int64_t MICROSECONDS_PER_DAY = <int64_t>86400000000
        cdef int64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i
        cdef uint8_t byte, bit

        if self._has_const:
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
        if self._has_const:
            return f"<Date32Vector len={buf_length(self.ptr)} values={[None if self._const_is_null else self._const_value] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int32_t* data = <int32_t*> self.ptr.data
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

    return out
