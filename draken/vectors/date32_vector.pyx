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
Date32Vector: INT32 day-count from Unix epoch (1970-01-01 == 0), stored in a
DrakenFixedBuffer with itemsize=4. Physical layout is identical to Integer32Vector;
Date32Vector is a distinct class because the domain (calendar dates) and operations
(date arithmetic, truncation) differ from general integer arithmetic.
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
from libc.stdint cimport uint32_t, uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DRAKEN_DATE32
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.vectors.integer64_vector cimport _materialize_dict_int64
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector
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



cdef class Date32Vector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef Date32Vector vec = Date32Vector(1)
        cdef int32_t val = 0 if (is_null or value is None) else <int32_t>int(value)
        (<int32_t*>vec.ptr.data)[0] = val
        vec.ptr.length = <size_t>length
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_DATE32,
            &_CONST_NULL_BYTE if is_null else NULL)
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
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_DATE32, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_DATE32, length, 4)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_DATE32, NULL)

    def __dealloc__(self):
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    cdef void _set_null_bitmap(self, uint8_t* bm) noexcept:
        self.ptr.null_bitmap = bm
        self._unified_view.validity = bm

    # Python-friendly properties (backed by C getters for kernels)
    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def itemsize(self):
        return 4

    @property
    def dtype(self):
        return DRAKEN_DATE32

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenVector* uv = self.unified()
        if i < 0 or i >= <Py_ssize_t>uv.length:
            raise IndexError("Index out of bounds")
        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None
        return (<int32_t*>uv.data)[uv.selection[i]]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.date32())
            return pa.array([(<int32_t*>uv.data)[0]] * self.ptr.length, type=pa.date32())

        cdef size_t nbytes = self.ptr.length * 4
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.date32(), self.ptr.length, buffers)

    # -------- Example op --------
    cpdef Date32Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Date32Vector out = Date32Vector(<size_t>n)
        cdef int32_t* data = <int32_t*>uv.data
        cdef int32_t* dst = <int32_t*>out.ptr.data
        cdef int32_t src_idx
        cdef Py_ssize_t nb_bytes

        for i in range(n):
            src_idx = indices[i]
            dst[i] = data[uv.selection[src_idx]]

        if uv.validity != NULL:
            nb_bytes = (n + 7) >> 3
            out.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
            if out.ptr.null_bitmap == NULL:
                raise MemoryError()
            memset(out.ptr.null_bitmap, 0xFF, nb_bytes)
            for i in range(n):
                src_idx = indices[i]
                if not ((uv.validity[src_idx >> 3] >> (src_idx & 7)) & 1):
                    out.ptr.null_bitmap[i >> 3] &= ~(<uint8_t>1 << (i & 7))

        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_DATE32, out.ptr.null_bitmap)
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
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i

        if nbytes > 0:
            memset(dst, 0, nbytes)

        if uv.validity != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, uv.validity, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                if dispatch_compare_once(op, data[uv.selection[i]], value):
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector _compare_vector_op(self, Date32Vector other, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef int32_t* data1 = <int32_t*>uv.data
        cdef int32_t* data2 = <int32_t*>ouv.data
        cdef bint null1, null2
        cdef Py_ssize_t i

        if n != <Py_ssize_t>ouv.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        memset(dst, 0, nbytes)

        if (uv.validity != NULL or ouv.validity != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            null1 = uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1)
            null2 = ouv.validity != NULL and not ((ouv.validity[i >> 3] >> (i & 7)) & 1)
            if null1 or null2:
                continue
            if out_null != NULL:
                out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
            if dispatch_compare_once(op, data1[uv.selection[i]], data2[ouv.selection[i]]):
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))
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
        cdef int32_t* data = <int32_t*>uv.data
        cdef uint8_t* src_null = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t v

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

        if lower_inclusive and upper_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    v = data[uv.selection[i]]
                    if lower <= v <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        elif lower_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    v = data[uv.selection[i]]
                    if lower <= v < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        elif upper_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    v = data[uv.selection[i]]
                    if lower < v <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    v = data[uv.selection[i]]
                    if lower < v < upper:
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
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef uint8_t* src_null = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

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

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if data[uv.selection[i]] in value_set:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef int32_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        cdef int32_t m
        cdef bint found = False

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            m = data[uv.selection[i]]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        for i in range(i + 1, n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            if data[uv.selection[i]] < m:
                m = data[uv.selection[i]]
        return m

    cpdef int32_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        cdef int32_t m
        cdef bint found = False

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            m = data[uv.selection[i]]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            if data[uv.selection[i]] > m:
                m = data[uv.selection[i]]
        return m

    cpdef int64_t sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int64_t total = 0
        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            total += data[uv.selection[i]]
        return total

    cpdef Integer64Vector subtract_date32_vector(self, Date32Vector other):
        """Subtract two Date32Vector values and return microseconds as Integer64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Integer64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t left_us, right_us

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Integer64Vector(<size_t>n)
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

    cpdef Integer64Vector subtract_timestamp_vector(self, TimestampVector other):
        """Subtract TimestampVector from Date32Vector and return microseconds as Integer64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int32_t* data1 = <int32_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Integer64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t left_us

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Integer64Vector(<size_t>n)
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
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf
        cdef uint8_t* null_bitmap = uv.validity

        buf = <int8_t*>PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        if null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                buf[i] = 0 if ((null_bitmap[i >> 3] >> (i & 7)) & 1) else 1
        return <int8_t[:n]> buf

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = self.unified()
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, bm_bytes
        if uv.data_length == 1:
            return buf_itemsize(ptr)
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef list out = []
        cdef object date_fromordinal = _dt.date.fromordinal
        cdef int ordinal
        cdef int32_t v

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                out.append(None)
                continue
            v = data[uv.selection[i]]
            ordinal = _DATE_EPOCH_ORDINAL + v
            try:
                out.append(date_fromordinal(ordinal))
            except (OverflowError, ValueError):
                out.append(v)

        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare date32 values at two indices. Returns -1, 0, or 1."""
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef int32_t left_val, right_val
        cdef bint left_is_null, right_is_null

        left_is_null = uv.validity != NULL and not ((uv.validity[left_idx >> 3] >> (left_idx & 7)) & 1)
        right_is_null = uv.validity != NULL and not ((uv.validity[right_idx >> 3] >> (right_idx & 7)) & 1)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        left_val = data[uv.selection[left_idx]]
        right_val = data[uv.selection[right_idx]]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        else:
            return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenVector* uv = self.unified()
        if uv.validity == NULL:
            return False
        return not ((uv.validity[idx >> 3] >> (idx & 7)) & 1)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint64_t[DATE32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Date32Vector.hash_into: output buffer too small")

        i = 0
        while i < n:
            block = n - i
            if block > DATE32_HASH_CHUNK:
                block = DATE32_HASH_CHUNK
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    scratch[j] = <uint64_t>(<int64_t>data[uv.selection[i + j]])
            simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    dst[i + j] = NULL_HASH
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenVector* uv = &self._unified_view
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i, j, block
        cdef uint64_t[DATE32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return 0

        i = 0
        while i < n:
            block = n - i
            if block > DATE32_HASH_CHUNK:
                block = DATE32_HASH_CHUNK
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    scratch[j] = <uint64_t>(<int64_t>data[uv.selection[i + j]])
            simd_mix_hash(out + i, scratch_ptr, <size_t>block)
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    out[i + j] = NULL_HASH
            i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for Date32Vector: scale int32 days to int64 microseconds (to match datetimes)."""
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
        cdef int64_t _MICROSECONDS_PER_DAY = <int64_t>86400000000
        cdef int64_t* dst = &out_buf[offset]
        cdef Py_ssize_t i

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Date32Vector.compress: output buffer too small")

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                dst[i] = NULL_FLAG
            else:
                dst[i] = <int64_t>data[uv.selection[i]] * _MICROSECONDS_PER_DAY

    def __str__(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, k = min(<Py_ssize_t>uv.length, 10)
        cdef int32_t* data = <int32_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef list vals = []
        for i in range(k):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                vals.append(None)
            else:
                vals.append(data[uv.selection[i]])
        return f"<Date32Vector len={uv.length} values={vals}>"


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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>row_count, DRAKEN_DATE32, vec.ptr.null_bitmap)
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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>row_count, DRAKEN_DATE32, vec.ptr.null_bitmap)
    return vec


cpdef Date32Vector from_int64_vector(Integer64Vector source):
    """
    Convert an Integer64Vector containing epoch-day values to Date32Vector.

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
    cdef DrakenVector* src_uv = source.unified()
    cdef int64_t cv

    out = Date32Vector(<size_t>n)
    src_data = <int64_t*>src_uv.data
    dst_data = <int32_t*>out.ptr.data
    src_null = src_uv.validity

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
        if src_null != NULL and not ((src_null[i >> 3] >> (i & 7)) & 1):
            dst_data[i] = 0
            continue
        value64 = src_data[<Py_ssize_t>src_uv.selection[i]]
        if value64 < int32_min or value64 > int32_max:
            raise OverflowError(f"date32 value out of range at row {i}: {value64}")
        dst_data[i] = <int32_t>value64

    out._unified_view = draken_vector_from_dense(
        out.ptr.data, <uint32_t>n, DRAKEN_DATE32, out.ptr.null_bitmap)
    return out


cdef Date32Vector _materialize_const_date32(Date32Vector const_vec):
    """Expand a CONSTANT Date32Vector to a dense Date32Vector."""
    cdef DrakenVector* uv = const_vec.unified()
    cdef size_t n = const_vec.ptr.length
    cdef Date32Vector dense = Date32Vector(n)
    cdef int32_t* dst = <int32_t*>dense.ptr.data
    cdef int32_t val
    cdef size_t i
    cdef size_t null_bytes
    cdef uint8_t* null_bm

    if uv.validity != NULL:
        null_bytes = (n + 7) >> 3
        null_bm = <uint8_t*>malloc(null_bytes)
        if null_bm == NULL:
            raise MemoryError()
        memset(null_bm, 0, null_bytes)
        dense.ptr.null_bitmap = null_bm
    else:
        val = (<int32_t*>uv.data)[0]
        for i in range(n):
            dst[i] = val
    dense._unified_view = draken_vector_from_dense(
        dense.ptr.data, <uint32_t>n, DRAKEN_DATE32, dense.ptr.null_bitmap)
    return dense


