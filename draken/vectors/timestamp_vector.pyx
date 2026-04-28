# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
TimestampVector: Cython implementation of a fixed-width timestamp column vector for Draken.

This module provides:
- The TimestampVector class for efficient timestamp column storage (microseconds since Unix epoch)
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast comparison and null handling for timestamp columns

Used for high-performance temporal analytics and columnar data processing in Draken.
"""

import datetime as _dt

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memset, memcpy

from draken.core.buffers cimport ConstAccessor, DictAccessor, DrakenFixedBuffer, DrakenRLEBuffer, DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_TIMESTAMP64
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_RLE
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.int64_vector cimport Int64Vector, _materialize_dict_int64
from draken.vectors.date32_vector cimport Date32Vector

# Constants for microseconds conversions
cdef int64_t MICROSECONDS_PER_DAY = 86_400_000_000
cdef int64_t MICROSECONDS_PER_SECOND = 1_000_000
cdef int64_t MICROSECONDS_PER_MILLISECOND = 1_000
cdef int64_t NULL_FLAG = <int64_t>-9223372036854775808

# Integer unit codes — avoids Python str comparison in the compress hot loop
DEF TIMESTAMP_HASH_CHUNK = 1024
DEF UNIT_NS = 0
DEF UNIT_US = 1
DEF UNIT_MS = 2
DEF UNIT_S  = 3
_TIMESTAMP_EPOCH = _dt.datetime(1970, 1, 1)


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


cdef void _release_dict_storage(TimestampVector vec) noexcept:
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
    vec._dict_accessor.value_type = DRAKEN_TIMESTAMP64
    vec._encoding = DRAKEN_ENCODING_DENSE


cdef void _release_rle_storage_timestamp(TimestampVector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef void _attach_dictionary_storage(TimestampVector vec, const int32_t[::1] codes, const int64_t[::1] dictionary, bint ordered) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(int64_t)
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

    dict_values = alloc_var_buffer(DRAKEN_TIMESTAMP64, <size_t>dict_size, <size_t>dict_bytes)
    dict_values.offsets[0] = 0
    for i in range(dict_size):
        dict_values.offsets[i + 1] = <int32_t>((i + 1) * sizeof(int64_t))
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>&dictionary[0], <size_t>dict_bytes)

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

cdef inline int _unit_code_from_str(str unit):
    if unit == 'ns':
        return UNIT_NS
    elif unit == 'us':
        return UNIT_US
    elif unit == 'ms':
        return UNIT_MS
    else:
        return UNIT_S

cdef inline int64_t _apply_unit_scale(int64_t v, int unit_code):
    cdef int64_t factor
    if unit_code == UNIT_NS:
        return v // 1000
    elif unit_code == UNIT_US:
        return v
    elif unit_code == UNIT_MS:
        factor = 1000
    else:  # UNIT_S
        factor = 1000000
    # Overflow-safe multiply (clamp to int64 limits)
    if v > 0 and v > 9223372036854775807 // factor:
        return 9223372036854775807
    if v < 0 and v < (-9223372036854775807 - 1) // factor:
        return -9223372036854775808
    return v * factor


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1

cdef int64_t _safe_multiply_int64(int64_t value, int64_t factor):
    """Multiply with overflow protection (clamp to int64_t limits)."""
    if factor > 0:
        if value > 0 and value > 9223372036854775807 // factor:
            return 9223372036854775807
        if value < 0 and value < -9223372036854775808 // factor:
            return -9223372036854775808
    return value * factor

cdef int64_t scale_timestamp_to_micros(int64_t value, str unit):
    """
    Scale raw timestamp value from Arrow unit to microseconds.
    Handles overflow by clamping to int64_t limits.
    """
    if unit == 'ns':
        return value // 1_000  # Integer division: nanoseconds to microseconds
    elif unit == 'us':
        return value  # Already in microseconds
    elif unit == 'ms':
        return _safe_multiply_int64(value, MICROSECONDS_PER_MILLISECOND)
    elif unit == 's':
        return _safe_multiply_int64(value, MICROSECONDS_PER_SECOND)
    else:
        raise ValueError(f"Unknown timestamp unit: {unit}")

cdef class TimestampVector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False, timestamp_unit="us"):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef TimestampVector vec = TimestampVector(0)
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec.null_bit_offset = 0
        vec.timestamp_unit = str(timestamp_unit)
        vec._unit_code = _unit_code_from_str(timestamp_unit)
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else <int64_t>int(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None, timestamp_unit="us"):
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
        timestamp_unit = str(timestamp_unit)

        if row_validity is None:
            return from_dict(codes_view, dictionary_view, timestamp_unit)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary_view, validity_view, timestamp_unit)

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        self.null_bit_offset = 0
        self._arrow_null_buf = None
        self._arrow_data_buf = None
        self.timestamp_unit = 'us'  # Default to microseconds
        self._unit_code = UNIT_US

        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_TIMESTAMP64, length, 8)
            self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_TIMESTAMP64
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._dict_values = NULL
        self._dict_codes = NULL
        self._dict_code_width = 0
        self._dict_ordered = 0
        self._dict_accessor.codes = NULL
        self._dict_accessor.code_width = 0
        self._dict_accessor.row_nulls = NULL
        self._dict_accessor.length = 0
        self._dict_accessor.dict_values = NULL
        self._dict_accessor.value_type = DRAKEN_TIMESTAMP64
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_rle_storage_timestamp(self)
        _release_dict_storage(self)
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
        self._const_accessor.value_type = DRAKEN_TIMESTAMP64
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

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef size_t ts_cumulative = 0
        cdef size_t ts_run
        cdef int64_t* rle_ts_vals
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value
        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_ts_vals = <int64_t*>self._rle_buffer.run_values
            for ts_run in range(self._rle_buffer.num_runs):
                ts_cumulative += <size_t>self._rle_buffer.run_lengths[ts_run]
                if <size_t>i < ts_cumulative:
                    if self._rle_buffer.null_bitmap != NULL:
                        if not ((self._rle_buffer.null_bitmap[i >> 3] >> (i & 7)) & 1):
                            return None
                    return rle_ts_vals[ts_run]
            raise IndexError("Index out of bounds")
        if ptr.null_bitmap != NULL:
            if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):
                return None
        return data[i]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.timestamp(self.timestamp_unit))
            return pa.array([self._const_value] * self.ptr.length, type=pa.timestamp(self.timestamp_unit))

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        cdef Py_ssize_t null_bytes
        if self.ptr.null_bitmap != NULL:
            null_bytes = (self.ptr.length + self.null_bit_offset + 7) // 8
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, null_bytes, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.timestamp(self.timestamp_unit), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef TimestampVector take(self, int32_t[::1] indices):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_timestamp(self).take(indices)
        if self._has_const:
            return TimestampVector.from_constant(
                None if self._const_is_null else self._const_value,
                indices.shape[0],
                is_null=self._const_is_null,
                timestamp_unit=self.timestamp_unit,
            )
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef TimestampVector out = TimestampVector(<size_t>n)
        cdef int64_t* src = <int64_t*> self.ptr.data
        cdef int64_t* dst = <int64_t*> out.ptr.data
        for i in range(n):
            dst[i] = src[indices[i]]
        return out

    cdef inline bint _compare_timestamp_values(self, int64_t left, int64_t right, int op) nogil:
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

    cdef BoolVector _compare_scalar(self, int64_t value, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_timestamp(self)._compare_scalar(value, op)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL

        memset(dst, 0, nbytes)
        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if src_null == NULL or _bitmap_is_valid(src_null, i, self.null_bit_offset):
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data[i], value, op):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector equals_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 0):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector not_equals_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 1):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector greater_than_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 2):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector greater_than_or_equals_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 3):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector less_than_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 4):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector less_than_or_equals_vector(self, TimestampVector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
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
            valid1 = True if null1 == NULL else _bitmap_is_valid(null1, i, self.null_bit_offset)
            valid2 = True if null2 == NULL else _bitmap_is_valid(null2, i, other.null_bit_offset)
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_timestamp_values(data1[i], data2[i], 5):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0. Propagates NULLs."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_timestamp(self).in_list(value_set)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
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

    cpdef int64_t min(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value

        cdef int64_t m
        cdef bint found = False
        cdef uint8_t byte, bit

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

    cpdef int64_t max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value

        cdef int64_t m
        cdef bint found = False
        cdef uint8_t byte, bit

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        # Find maximum among remaining values
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
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):  # null
                    continue
            total += data[i]
        return total

    cpdef Int64Vector subtract_timestamp_vector(self, TimestampVector other):
        """Subtract two TimestampVector values and return microseconds as Int64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Int64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2

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
                dst[i] = data1[i] - data2[i]
        return out

    cpdef Int64Vector subtract_date32_vector(self, Date32Vector other):
        """Subtract Date32Vector from TimestampVector and return microseconds as Int64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Int64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t right_us

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
                right_us = <int64_t>data2[i] * MICROSECONDS_PER_DAY
                dst[i] = data1[i] - right_us
        return out

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
        cdef int8_t* buf
        cdef uint8_t byte, bit

        if self._encoding == DRAKEN_ENCODING_RLE:
            n = <Py_ssize_t>self._rle_buffer.length
            buf = <int8_t*> PyMem_Malloc(n)
            if buf == NULL:
                raise MemoryError()
            if self._rle_buffer.null_bitmap == NULL:
                for i in range(n):
                    buf[i] = 0
            else:
                for i in range(n):
                    byte = self._rle_buffer.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    buf[i] = 0 if bit else 1
            return <int8_t[:n]> buf

        n = ptr.length
        buf = <int8_t*> PyMem_Malloc(n)
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
                buf[i] = 0 if _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset) else 1

        return <int8_t[:n]> buf

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return 0
            return <Py_ssize_t>self._rle_buffer.length - <Py_ssize_t>simd_popcount(
                self._rle_buffer.null_bitmap, (self._rle_buffer.length + 7) >> 3
            )
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if self._has_const:
            return n if self._const_is_null else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef object timedelta = _dt.timedelta
        cdef int64_t value
        cdef int64_t seconds
        cdef int64_t remainder
        cdef int64_t micros
        cdef object ts
        cdef int64_t* rle_ts_tp
        cdef int32_t* rle_lens_ts
        cdef size_t rle_runs_ts
        cdef uint8_t* rle_nulls_ts
        cdef Py_ssize_t ts_pos
        cdef size_t tsr
        cdef int32_t ts_run_len
        cdef int64_t ts_run_val

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_ts_tp = <int64_t*>self._rle_buffer.run_values
            rle_lens_ts = self._rle_buffer.run_lengths
            rle_runs_ts = self._rle_buffer.num_runs
            rle_nulls_ts = self._rle_buffer.null_bitmap
            ts_pos = 0
            for tsr in range(rle_runs_ts):
                ts_run_val = rle_ts_tp[tsr]
                ts_run_len = rle_lens_ts[tsr]
                for i in range(ts_run_len):
                    if rle_nulls_ts != NULL and not ((rle_nulls_ts[(ts_pos + i) >> 3] >> ((ts_pos + i) & 7)) & 1):
                        out.append(None)
                    else:
                        value = ts_run_val
                        if self.timestamp_unit == "s":
                            seconds = value
                            micros = 0
                        elif self.timestamp_unit == "ms":
                            seconds, remainder = divmod(value, 1000)
                            micros = remainder * 1000
                        elif self.timestamp_unit == "ns":
                            seconds, remainder = divmod(value, 1000000000)
                            micros = remainder // 1000
                        else:
                            seconds, remainder = divmod(value, 1000000)
                            micros = remainder
                        try:
                            ts = _TIMESTAMP_EPOCH + timedelta(seconds=seconds, microseconds=micros)
                        except (OverflowError, ValueError):
                            ts = value
                        out.append(ts)
                ts_pos += ts_run_len
            return out

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    value = self._const_value
                    if self.timestamp_unit == "s":
                        seconds = value
                        micros = 0
                    elif self.timestamp_unit == "ms":
                        seconds, remainder = divmod(value, 1000)
                        micros = remainder * 1000
                    elif self.timestamp_unit == "ns":
                        seconds, remainder = divmod(value, 1000000000)
                        micros = remainder // 1000
                    else:
                        seconds, remainder = divmod(value, 1000000)
                        micros = remainder
                    try:
                        ts = _TIMESTAMP_EPOCH + timedelta(seconds=seconds, microseconds=micros)
                    except (OverflowError, ValueError):
                        ts = value
                    out.append(ts)
            return out

        if ptr.null_bitmap == NULL:
            for i in range(n):
                value = data[i]
                if self.timestamp_unit == "s":
                    seconds = value
                    micros = 0
                elif self.timestamp_unit == "ms":
                    seconds, remainder = divmod(value, 1000)
                    micros = remainder * 1000
                elif self.timestamp_unit == "ns":
                    seconds, remainder = divmod(value, 1000000000)
                    micros = remainder // 1000
                else:
                    seconds, remainder = divmod(value, 1000000)
                    micros = remainder
                try:
                    ts = _TIMESTAMP_EPOCH + timedelta(seconds=seconds, microseconds=micros)
                except (OverflowError, ValueError):
                    ts = value
                out.append(ts)
        else:
            for i in range(n):
                if _bitmap_is_valid(ptr.null_bitmap, i, self.null_bit_offset):
                    value = data[i]
                    if self.timestamp_unit == "s":
                        seconds = value
                        micros = 0
                    elif self.timestamp_unit == "ms":
                        seconds, remainder = divmod(value, 1000)
                        micros = remainder * 1000
                    elif self.timestamp_unit == "ns":
                        seconds, remainder = divmod(value, 1000000000)
                        micros = remainder // 1000
                    else:
                        seconds, remainder = divmod(value, 1000000)
                        micros = remainder
                    try:
                        ts = _TIMESTAMP_EPOCH + timedelta(seconds=seconds, microseconds=micros)
                    except (OverflowError, ValueError):
                        ts = value
                    out.append(ts)
                else:
                    out.append(None)

        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare timestamp values at two indices. Returns -1, 0, or 1."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_timestamp(self).compare_at(left_idx, right_idx)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t left_val, right_val
        cdef bint left_is_null, right_is_null

        # Check nulls
        left_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, left_idx, 0)
        right_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, right_idx, 0)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        left_val = (<int64_t*>ptr.data)[left_idx]
        right_val = (<int64_t*>ptr.data)[right_idx]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        else:
            return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return False
            return not ((self._rle_buffer.null_bitmap[idx >> 3] >> (idx & 7)) & 1)
        cdef DrakenFixedBuffer* ptr = self.ptr
        if ptr.null_bitmap == NULL:
            return False
        return not _bitmap_is_valid(ptr.null_bitmap, idx, 0)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t value
        cdef uint64_t* dst = &out_buf[offset]
        cdef bint has_nulls = ptr.null_bitmap != NULL

        cdef uint64_t* as_uint64 = <uint64_t*> data
        cdef uint64_t[TIMESTAMP_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_timestamp(self).hash_into(out_buf, offset)
            return

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(TIMESTAMP_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > TIMESTAMP_HASH_CHUNK:
                    block = TIMESTAMP_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimestampVector.hash_into: output buffer too small")

        # Use shared MIX_HASH_CONSTANT directly; no need to pass it in.
        if not has_nulls:
            simd_mix_hash(dst, as_uint64, <size_t> n)
            return

        cdef uint64_t is_valid
        i = 0
        while i < n:
            block = n - i
            if block > TIMESTAMP_HASH_CHUNK:
                block = TIMESTAMP_HASH_CHUNK
            for j in range(block):
                is_valid = <uint64_t>_bitmap_is_valid(ptr.null_bitmap, i + j, self.null_bit_offset)
                scratch[j] = (as_uint64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
            simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef uint64_t* as_uint64 = <uint64_t*> data
        cdef uint64_t[TIMESTAMP_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(TIMESTAMP_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > TIMESTAMP_HASH_CHUNK:
                    block = TIMESTAMP_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        if n == 0:
            return 0

        if not has_nulls:
            simd_mix_hash(out, as_uint64, <size_t> n)
            return 0

        i = 0
        while i < n:
            block = n - i
            if block > TIMESTAMP_HASH_CHUNK:
                block = TIMESTAMP_HASH_CHUNK
            for j in range(block):
                is_valid = <uint64_t>_bitmap_is_valid(ptr.null_bitmap, i + j, self.null_bit_offset)
                scratch[j] = (as_uint64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
            simd_mix_hash(out + i, scratch_ptr, <size_t> block)
            i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for TimestampVector: scale raw int64 values to microseconds."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_timestamp(self).compress_into(out_buf, offset)
            return
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* src = <int64_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i
        cdef int64_t value

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimestampVector.compress: output buffer too small")
        if self._has_const:
            value = 0 if self._const_is_null else _apply_unit_scale(self._const_value, self._unit_code)
            for i in range(n):
                dst[i] = NULL_FLAG if self._const_is_null else value
            return

        # Apply scale factor based on timestamp unit
        if has_nulls:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i, self.null_bit_offset):
                    value = src[i]
                    dst[i] = _apply_unit_scale(value, self._unit_code)
                else:
                    dst[i] = NULL_FLAG
        else:
            for i in range(n):
                value = src[i]
                dst[i] = _apply_unit_scale(value, self._unit_code)

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        cdef int64_t* data
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:10]
            return f"<TimestampVector(RLE) len={self._rle_buffer.length} values={vals}>"
        if self._has_const:
            return f"<TimestampVector len={buf_length(self.ptr)} values={[None if self._const_is_null else self._const_value] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<TimestampVector len={buf_length(self.ptr)} values={vals}>"


cdef TimestampVector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "TimestampVector.from_arrow expects a dense timestamp Arrow array; "
            "use TimestampVector.from_dict for dictionary input"
        )

    cdef TimestampVector vec = TimestampVector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Extract timestamp unit from Arrow's type metadata
    cdef str timestamp_unit = 'us'  # Default fallback
    try:
        arrow_type = array.type
        if hasattr(arrow_type, 'unit'):
            timestamp_unit = arrow_type.unit
    except:
        pass  # Use default if metadata unavailable

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr
    cdef Py_ssize_t byte_offset

    vec.ptr.type = DRAKEN_TIMESTAMP64
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    # Variables for null bitmap handling
    cdef Py_ssize_t n_bytes
    cdef bytes new_bitmap
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap
    cdef int bit_offset
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef Py_ssize_t i

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (nb_addr + (offset >> 3))
            vec.null_bit_offset = 0
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

            for i in range(n_bytes):
                val = src_bitmap[i] >> shift_down
                val |= (src_bitmap[i+1] << shift_up)
                dst_bitmap[i] = val

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap # Keep alive
            vec.null_bit_offset = 0
    else:
        vec.ptr.null_bitmap = NULL
        vec.null_bit_offset = 0

    return vec


cdef TimestampVector from_dict(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    str timestamp_unit,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimestampVector vec = TimestampVector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("TimestampVector.from_dict requires a non-empty dictionary")

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)
    vec.ptr.null_bitmap = NULL
    vec.null_bit_offset = 0
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)

    return vec


cdef TimestampVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
    str timestamp_unit,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimestampVector vec = TimestampVector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("TimestampVector.from_dict requires a non-empty dictionary")
    if row_validity.shape[0] != row_count:
        raise ValueError("row_validity length must match codes length")

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)
    nb_bytes = (row_count + 7) >> 3
    nb = <uint8_t*>malloc(nb_bytes)
    if nb == NULL:
        raise MemoryError()
    memset(nb, 0, nb_bytes)
    vec.ptr.null_bitmap = nb
    vec.null_bit_offset = 0

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

cpdef TimestampVector from_int64_vector(Int64Vector source, str timestamp_unit="us"):
    """
    Convert an Int64Vector containing epoch timestamp values to TimestampVector.

    This is a native Draken conversion path (no Arrow interop).
    """
    cdef Py_ssize_t n = <Py_ssize_t>source.ptr.length
    cdef TimestampVector out
    cdef int64_t* src_data
    cdef int64_t* dst_data
    cdef uint8_t* src_null
    cdef size_t nb_bytes
    cdef uint8_t* out_null

    if source._encoding == DRAKEN_ENCODING_DICTIONARY and source.ptr.data == NULL:
        source = _materialize_dict_int64(source)

    if source._has_const:
        if source._const_is_null:
            return TimestampVector.from_constant(None, n, is_null=True, timestamp_unit=timestamp_unit)
        return TimestampVector.from_constant(
            source._const_value, n, timestamp_unit=timestamp_unit
        )

    out = TimestampVector(<size_t>n)
    out.timestamp_unit = timestamp_unit
    out._unit_code = _unit_code_from_str(timestamp_unit)

    src_data = <int64_t*>source.ptr.data
    dst_data = <int64_t*>out.ptr.data
    if n > 0:
        memcpy(dst_data, src_data, <size_t>n * sizeof(int64_t))

    src_null = <uint8_t*>source.ptr.null_bitmap
    if src_null != NULL:
        nb_bytes = (<size_t>n + 7) >> 3
        out_null = <uint8_t*>malloc(nb_bytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, src_null, nb_bytes)
        out.ptr.null_bitmap = out_null
        out.null_bit_offset = 0
    else:
        out.ptr.null_bitmap = NULL
        out.null_bit_offset = 0

    return out


cdef TimestampVector _materialize_rle_timestamp(TimestampVector rle_vec):
    """Expand an RLE TimestampVector to a dense TimestampVector.

    Preserves the timestamp_unit from the source vector.
    """
    cdef size_t total = rle_vec._rle_buffer.length
    cdef TimestampVector dense = TimestampVector(<size_t>total)
    dense.timestamp_unit = rle_vec.timestamp_unit
    dense._unit_code = rle_vec._unit_code

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
    dense.null_bit_offset = 0

    return dense


cdef TimestampVector from_rle_builder(
    int64_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    str timestamp_unit,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded TimestampVector from raw C arrays.

    Args:
        run_values:     Pointer to int64_t values array (num_runs entries).
        run_lengths:    Pointer to int32_t run lengths (num_runs entries).
        num_runs:       Number of runs.
        timestamp_unit: 's', 'ms', 'us', or 'ns'.
        null_bitmap:    Optional logical-row null bitmap (NULL = no nulls).

    Returns:
        TimestampVector with DRAKEN_ENCODING_RLE encoding.
    """
    cdef TimestampVector vec = TimestampVector(0)
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef int64_t* vals_copy
    cdef int32_t* lens_copy
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)

    for i in range(num_runs):
        total_length += <size_t>run_lengths[i]

    vec.ptr.length = total_length

    if num_runs == 0:
        vec._encoding = DRAKEN_ENCODING_RLE
        return vec

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
    rle.type = DRAKEN_TIMESTAMP64

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
