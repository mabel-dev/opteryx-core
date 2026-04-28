# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
TimeVector: Cython implementation of a fixed-width time column vector for Draken.

This module provides:
- The TimeVector class for efficient time column storage (time32 or time64)
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast null handling for time columns

Used for high-performance temporal analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer, DrakenRLEBuffer
from draken.core.buffers cimport DRAKEN_TIME32
from draken.core.buffers cimport DRAKEN_TIME64
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_RLE
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount

DEF TIME32_HASH_CHUNK = 1024


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    cdef uint8_t byte = bitmap[idx >> 3]
    return (byte >> (idx & 7)) & 1


cdef void _release_rle_storage_time(TimeVector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef class TimeVector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False, is_time64=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef TimeVector vec = TimeVector(0, is_time64)
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else <int64_t>int(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None, is_time64=False):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef int32_t[::1] dictionary32_view
        cdef int64_t[::1] dictionary64_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        codes_view = codes

        if is_time64:
            if not isinstance(dictionary, memoryview):
                dictionary = pyarray("q", dictionary)
            dictionary64_view = dictionary
            if row_validity is None:
                return from_dict64(codes_view, dictionary64_view)
            if not isinstance(row_validity, memoryview):
                row_validity = bytearray(1 if valid else 0 for valid in row_validity)
            validity_view = row_validity
            return from_dict64_nullable(codes_view, dictionary64_view, validity_view)

        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("i", dictionary)
        dictionary32_view = dictionary
        if row_validity is None:
            return from_dict(codes_view, dictionary32_view)
        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary32_view, validity_view)

    def __cinit__(self, size_t length=0, bint is_time64=False, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        self.is_time64 = is_time64
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            if is_time64:
                self.ptr = alloc_fixed_buffer(DRAKEN_TIME64, length, 8)
            else:
                self.ptr = alloc_fixed_buffer(DRAKEN_TIME32, length, 4)
            self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_rle_storage_time(self)
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_TIME64 if self.is_time64 else DRAKEN_TIME32
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
        cdef size_t tv_cumulative = 0
        cdef size_t tv_run
        cdef int64_t* rle_tv_vals
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value if self.is_time64 else <int32_t>self._const_value
        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_tv_vals = <int64_t*>self._rle_buffer.run_values
            for tv_run in range(self._rle_buffer.num_runs):
                tv_cumulative += <size_t>self._rle_buffer.run_lengths[tv_run]
                if <size_t>i < tv_cumulative:
                    if self._rle_buffer.null_bitmap != NULL:
                        if not ((self._rle_buffer.null_bitmap[i >> 3] >> (i & 7)) & 1):
                            return None
                    return rle_tv_vals[tv_run] if self.is_time64 else <int32_t>rle_tv_vals[tv_run]
            raise IndexError("Index out of bounds")
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        if self.is_time64:
            return (<int64_t*>ptr.data)[i]
        else:
            return (<int32_t*>ptr.data)[i]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        import pyarrow as pa
        if self._has_const:
            if self.is_time64:
                if self._const_is_null:
                    return pa.nulls(self.ptr.length, type=pa.time64('us'))
                return pa.array([self._const_value] * self.ptr.length, type=pa.time64('us'))
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.time32('s'))
            return pa.array([<int32_t>self._const_value] * self.ptr.length, type=pa.time32('s'))

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        # Default to microsecond precision for time64, second for time32
        if self.is_time64:
            return pa.Array.from_buffers(pa.time64('us'), buf_length(self.ptr), buffers)
        else:
            return pa.Array.from_buffers(pa.time32('s'), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef TimeVector take(self, int32_t[::1] indices):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_time(self).take(indices)
        if self._has_const:
            return TimeVector.from_constant(
                None if self._const_is_null else self._const_value,
                indices.shape[0],
                is_null=self._const_is_null,
                is_time64=self.is_time64,
            )
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef TimeVector out = TimeVector(<size_t>n, self.is_time64)
        cdef int64_t* src64
        cdef int64_t* dst64
        cdef int32_t* src32
        cdef int32_t* dst32

        if self.is_time64:
            src64 = <int64_t*> self.ptr.data
            dst64 = <int64_t*> out.ptr.data
            for i in range(n):
                dst64[i] = src64[indices[i]]
        else:
            src32 = <int32_t*> self.ptr.data
            dst32 = <int32_t*> out.ptr.data
            for i in range(n):
                dst32[i] = src32[indices[i]]
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
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

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
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef int64_t* data64
        cdef int32_t* data32
        cdef int64_t* rle_tv_tp
        cdef int32_t* rle_lens_tv
        cdef size_t rle_runs_tv
        cdef uint8_t* rle_nulls_tv
        cdef Py_ssize_t tv_pos
        cdef size_t tvr
        cdef int32_t tv_run_len

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_tv_tp = <int64_t*>self._rle_buffer.run_values
            rle_lens_tv = self._rle_buffer.run_lengths
            rle_runs_tv = self._rle_buffer.num_runs
            rle_nulls_tv = self._rle_buffer.null_bitmap
            tv_pos = 0
            for tvr in range(rle_runs_tv):
                tv_run_len = rle_lens_tv[tvr]
                for i in range(tv_run_len):
                    if rle_nulls_tv != NULL and not ((rle_nulls_tv[(tv_pos + i) >> 3] >> ((tv_pos + i) & 7)) & 1):
                        out.append(None)
                    else:
                        out.append(rle_tv_tp[tvr] if self.is_time64 else <int32_t>rle_tv_tp[tvr])
                tv_pos += tv_run_len
            return out

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append(self._const_value if self.is_time64 else <int32_t>self._const_value)
            return out
        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(data64[i])
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if bit:
                        out.append(data64[i])
                    else:
                        out.append(None)
        else:
            data32 = <int32_t*> ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(data32[i])
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if bit:
                        out.append(data32[i])
                    else:
                        out.append(None)

        return out

    cpdef int64_t min(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_time(self).min()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value

        cdef int64_t m
        cdef int64_t* data64
        cdef int32_t* data32
        cdef bint found = False

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            # Find first non-null value
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                m = data64[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute min of all-null column")
            # Find minimum among remaining values
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                if data64[i] < m:
                    m = data64[i]
        else:
            data32 = <int32_t*> ptr.data
            # Find first non-null value
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                m = <int64_t>data32[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute min of all-null column")
            # Find minimum among remaining values
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                if <int64_t>data32[i] < m:
                    m = <int64_t>data32[i]
        return m

    cpdef int64_t max(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_time(self).max()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value

        cdef int64_t m
        cdef int64_t* data64
        cdef int32_t* data32
        cdef bint found = False

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            # Find first non-null value
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                m = data64[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute max of all-null column")
            # Find maximum among remaining values
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                if data64[i] > m:
                    m = data64[i]
        else:
            data32 = <int32_t*> ptr.data
            # Find first non-null value
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                m = <int64_t>data32[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute max of all-null column")
            # Find maximum among remaining values
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                if <int64_t>data32[i] > m:
                    m = <int64_t>data32[i]
        return m

    cpdef int64_t sum(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_time(self).sum()
        if self._has_const:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        cdef int64_t* data64
        cdef int32_t* data32

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                total += data64[i]
        else:
            data32 = <int32_t*> ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i):  # null
                        continue
                total += <int64_t>data32[i]
        return total

    cpdef uint64_t[::1] hash(self):
        cdef Py_ssize_t n = self.ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        cdef Py_ssize_t i

        if buf == NULL:
            raise MemoryError()

        for i in range(n):
            buf[i] = 0

        cdef uint64_t[::1] view = <uint64_t[:n]> buf
        self.hash_into(view, 0)
        return view

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare time values at two indices. Returns -1, 0, or 1."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_time(self).compare_at(left_idx, right_idx)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t left_val, right_val
        cdef bint left_is_null, right_is_null

        # Check nulls
        left_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, left_idx)
        right_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, right_idx)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        # Extract values based on time32/time64
        if self.is_time64:
            left_val = (<int64_t*>ptr.data)[left_idx]
            right_val = (<int64_t*>ptr.data)[right_idx]
        else:
            left_val = <int64_t>(<int32_t*>ptr.data)[left_idx]
            right_val = <int64_t>(<int32_t*>ptr.data)[right_idx]

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
        return not _bitmap_is_valid(ptr.null_bitmap, idx)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef uint64_t value
        cdef uint8_t byte, bit
        cdef int64_t* data64
        cdef int32_t* data32
        cdef uint64_t* dst = &out_buf[offset]
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t[TIME32_HASH_CHUNK] scratch32
        cdef uint64_t* scratch32_ptr = <uint64_t*> scratch32

        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_time(self).hash_into(out_buf, offset)
            return

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(TIME32_HASH_CHUNK):
                scratch32[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                simd_mix_hash(dst + i, scratch32_ptr, <size_t>block)
                i += block
            return

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimeVector.hash_into: output buffer too small")

        cdef uint64_t is_valid
        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if not has_nulls:
                simd_mix_hash(dst, <uint64_t*> data64, <size_t> n)
                return
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                for j in range(block):
                    is_valid = (ptr.null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch32[j] = (<uint64_t> data64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch32_ptr, <size_t> block)
                i += block
        else:
            data32 = <int32_t*> ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (ptr.null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch32[j] = (<uint64_t>(<int64_t> data32[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch32[j] = <uint64_t>(<int64_t> data32[i + j])
                simd_mix_hash(dst + i, scratch32_ptr, <size_t> block)
                i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i
        cdef uint64_t value
        cdef uint8_t byte, bit
        cdef int64_t* data64
        cdef int32_t* data32
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t[TIME32_HASH_CHUNK] scratch32
        cdef uint64_t* scratch32_ptr = <uint64_t*> scratch32

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(TIME32_HASH_CHUNK):
                scratch32[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                simd_mix_hash(out + i, scratch32_ptr, <size_t>block)
                i += block
            return 0

        if n == 0:
            return 0

        cdef uint64_t is_valid
        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if not has_nulls:
                simd_mix_hash(out, <uint64_t*> data64, <size_t> n)
                return 0
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                for j in range(block):
                    is_valid = (ptr.null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch32[j] = (<uint64_t> data64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch32_ptr, <size_t> block)
                i += block
        else:
            data32 = <int32_t*> ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > TIME32_HASH_CHUNK:
                    block = TIME32_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (ptr.null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch32[j] = (<uint64_t>(<int64_t> data32[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch32[j] = <uint64_t>(<int64_t> data32[i + j])
                simd_mix_hash(out + i, scratch32_ptr, <size_t> block)
                i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for TimeVector: handle both time32 and time64."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_time(self).compress_into(out_buf, offset)
            return
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t NULL_FLAG = <int64_t> -9223372036854775808
        cdef Py_ssize_t i
        cdef int64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint8_t byte, bit
        cdef int64_t* data64
        cdef int32_t* data32

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimeVector.compress: output buffer too small")

        if self._has_const:
            for i in range(n):
                dst[i] = NULL_FLAG if self._const_is_null else self._const_value
            return

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if not has_nulls:
                memcpy(<void*>dst, <const void*>data64, <size_t>(n * sizeof(int64_t)))
                return
            for i in range(n):
                byte = null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    dst[i] = data64[i]
                else:
                    dst[i] = NULL_FLAG
        else:
            data32 = <int32_t*> ptr.data
            if has_nulls:
                for i in range(n):
                    byte = null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if bit:
                        dst[i] = <int64_t> data32[i]
                    else:
                        dst[i] = NULL_FLAG
            else:
                for i in range(n):
                    dst[i] = <int64_t> data32[i]

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        cdef int64_t* data64
        cdef int32_t* data32
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:10]
            return f"<TimeVector(RLE) len={self._rle_buffer.length} values={vals}>"
        if self._has_const:
            return f"<TimeVector len={buf_length(self.ptr)} is_time64={self.is_time64} values={[None if self._const_is_null else self._const_value] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)

        if self.is_time64:
            data64 = <int64_t*> self.ptr.data
            for i in range(k):
                vals.append(data64[i])
        else:
            data32 = <int32_t*> self.ptr.data
            for i in range(k):
                vals.append(data32[i])
        return f"<TimeVector len={buf_length(self.ptr)} is_time64={self.is_time64} values={vals}>"


cdef TimeVector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "TimeVector.from_arrow expects a dense time32/time64 Arrow array; "
            "use TimeVector.from_dict for dictionary input"
        )

    cdef bint is_time64 = pa.types.is_time64(array.type)
    cdef TimeVector vec = TimeVector(0, is_time64, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8 if is_time64 else 4
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32
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
    cdef Py_ssize_t byte_offset
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef Py_ssize_t i

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (nb_addr + (offset >> 3))
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
    else:
        vec.ptr.null_bitmap = NULL

    return vec


cdef TimeVector from_dict(const int32_t[::1] codes, const int32_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimeVector vec = TimeVector(<size_t>row_count, False)
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("TimeVector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        (<int32_t*>vec.ptr.data)[i] = dictionary[code]

    return vec


cdef TimeVector from_dict_nullable(
    const int32_t[::1] codes,
    const int32_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimeVector vec = TimeVector(<size_t>row_count, False)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("TimeVector.from_dict requires a non-empty dictionary")
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
            (<int32_t*>vec.ptr.data)[i] = dictionary[code]
            nb[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            (<int32_t*>vec.ptr.data)[i] = 0

    return vec


cdef TimeVector from_dict64(const int32_t[::1] codes, const int64_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimeVector vec = TimeVector(<size_t>row_count, True)
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("TimeVector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        (<int64_t*>vec.ptr.data)[i] = dictionary[code]

    return vec


cdef TimeVector from_dict64_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimeVector vec = TimeVector(<size_t>row_count, True)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("TimeVector.from_dict requires a non-empty dictionary")
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
            (<int64_t*>vec.ptr.data)[i] = dictionary[code]
            nb[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            (<int64_t*>vec.ptr.data)[i] = 0

    return vec


cdef TimeVector _materialize_rle_time(TimeVector rle_vec):
    """Expand an RLE TimeVector to a dense TimeVector.

    Run values are stored as int64_t; narrowed to int32_t for time32.
    """
    cdef size_t total = rle_vec._rle_buffer.length
    cdef bint is64 = rle_vec.is_time64
    cdef TimeVector dense = TimeVector(<size_t>total, is64)

    cdef int64_t* rle_vals = <int64_t*>rle_vec._rle_buffer.run_values
    cdef int32_t* rle_lens = rle_vec._rle_buffer.run_lengths
    cdef size_t num_runs = rle_vec._rle_buffer.num_runs
    cdef uint8_t* rle_nulls = rle_vec._rle_buffer.null_bitmap

    cdef size_t pos = 0
    cdef size_t r
    cdef int32_t run_len
    cdef int64_t run_val
    cdef Py_ssize_t j
    cdef int64_t* dst64
    cdef int32_t* dst32
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    if is64:
        dst64 = <int64_t*>dense.ptr.data
        for r in range(num_runs):
            run_val = rle_vals[r]
            run_len = rle_lens[r]
            for j in range(run_len):
                dst64[pos + j] = run_val
            pos += <size_t>run_len
    else:
        dst32 = <int32_t*>dense.ptr.data
        for r in range(num_runs):
            run_val = rle_vals[r]
            run_len = rle_lens[r]
            for j in range(run_len):
                dst32[pos + j] = <int32_t>run_val
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


cdef TimeVector from_rle_builder(
    int64_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    bint is_time64,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded TimeVector from raw C arrays.

    Run values are int64_t (widened even for time32).

    Args:
        run_values:  Pointer to int64_t values array (num_runs entries).
        run_lengths: Pointer to int32_t run lengths (num_runs entries).
        num_runs:    Number of runs.
        is_time64:   True for TIME64, False for TIME32.
        null_bitmap: Optional logical-row null bitmap (NULL = no nulls).

    Returns:
        TimeVector with DRAKEN_ENCODING_RLE encoding.
    """
    cdef TimeVector vec = TimeVector(0, is_time64)
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef int64_t* vals_copy
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
    rle.type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32

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
