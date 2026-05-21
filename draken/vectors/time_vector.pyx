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
from libc.stdint cimport uint32_t, uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_TIME32
from draken.core.buffers cimport DRAKEN_TIME64
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
import datetime
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount

DEF TIME32_HASH_CHUNK = 1024

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000
cdef uint8_t _CONST_NULL_BYTE = 0

cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    cdef uint8_t byte = bitmap[idx >> 3]
    return (byte >> (idx & 7)) & 1



cdef inline object _us_to_time(int64_t us):
    """Convert microseconds-since-midnight to datetime.time."""
    cdef int64_t us_rem = us % 1000000
    cdef int64_t s_total = us // 1000000
    cdef int64_t s = s_total % 60
    cdef int64_t m = (s_total // 60) % 60
    cdef int64_t h = s_total // 3600
    return datetime.time(<int>h, <int>m, <int>s, <int>us_rem)


cdef inline object _s_to_time(int32_t s_val):
    """Convert seconds-since-midnight to datetime.time."""
    cdef int32_t s = s_val % 60
    cdef int32_t m = (s_val // 60) % 60
    cdef int32_t h = s_val // 3600
    return datetime.time(<int>h, <int>m, <int>s)


cdef class TimeVector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False, is_time64=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef TimeVector vec = TimeVector(1, is_time64)
        if is_time64:
            (<int64_t*>vec.ptr.data)[0] = 0 if (is_null or value is None) else <int64_t>int(value)
        else:
            (<int32_t*>vec.ptr.data)[0] = 0 if (is_null or value is None) else <int32_t>int(value)
        vec.ptr.length = <size_t>length
        cdef DrakenType _time_type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, _time_type,
            &_CONST_NULL_BYTE if is_null else NULL)
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
        cdef DrakenType _time_type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, _time_type, NULL)
        else:
            if is_time64:
                self.ptr = alloc_fixed_buffer(DRAKEN_TIME64, length, 8)
            else:
                self.ptr = alloc_fixed_buffer(DRAKEN_TIME32, length, 4)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, _time_type, NULL)

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
        cdef DrakenVector* uv = self.unified()
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if uv.data_length == 1:
            if uv.validity != NULL:
                return None
            if self.is_time64:
                return (<int64_t*>uv.data)[0]
            return <int32_t>(<int64_t*>uv.data)[0]
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
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if self.is_time64:
                if uv.validity != NULL:
                    return pa.nulls(self.ptr.length, type=pa.time64('us'))
                return pa.array([(<int64_t*>uv.data)[0]] * self.ptr.length, type=pa.time64('us'))
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.time32('s'))
            return pa.array([(<int32_t*>uv.data)[0]] * self.ptr.length, type=pa.time32('s'))

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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef TimeVector out = TimeVector(<size_t>n, self.is_time64)
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* dst_null = NULL
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef int32_t idx
        cdef DrakenType time_type = DRAKEN_TIME64 if self.is_time64 else DRAKEN_TIME32

        if src_null != NULL and nbytes != 0:
            dst_null = <uint8_t*>malloc(nbytes)
            if dst_null == NULL:
                raise MemoryError()
            memset(dst_null, 0, nbytes)

        if self.is_time64:
            for i in range(n):
                idx = indices[i]
                if src_null != NULL and not ((src_null[idx >> 3] >> (idx & 7)) & 1):
                    (<int64_t*>out.ptr.data)[i] = 0
                else:
                    (<int64_t*>out.ptr.data)[i] = (<int64_t*>uv.data)[uv.selection[idx]]
                    if dst_null != NULL:
                        dst_null[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            for i in range(n):
                idx = indices[i]
                if src_null != NULL and not ((src_null[idx >> 3] >> (idx & 7)) & 1):
                    (<int32_t*>out.ptr.data)[i] = 0
                else:
                    (<int32_t*>out.ptr.data)[i] = (<int32_t*>uv.data)[uv.selection[idx]]
                    if dst_null != NULL:
                        dst_null[i >> 3] |= <uint8_t>(1 << (i & 7))

        out.ptr.null_bitmap = dst_null
        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, time_type, out.ptr.null_bitmap)
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

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = ptr.length
        if uv.data_length == 1:
            return n if uv.validity != NULL else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

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

        if uv.data_length == 1:
            if uv.validity != NULL:
                for i in range(n):
                    out.append(None)
            else:
                if self.is_time64:
                    t = _us_to_time((<int64_t*>uv.data)[0])
                else:
                    t = _s_to_time(<int32_t>(<int64_t*>uv.data)[0])
                for i in range(n):
                    out.append(t)
            return out
        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(_us_to_time(data64[i]))
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if bit:
                        out.append(_us_to_time(data64[i]))
                    else:
                        out.append(None)
        else:
            data32 = <int32_t*> ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(_s_to_time(data32[i]))
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if bit:
                        out.append(_s_to_time(data32[i]))
                    else:
                        out.append(None)

        return out

    cpdef int64_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if uv.data_length == 1:
            if uv.validity != NULL:
                raise ValueError("Cannot compute min of all-null column")
            return (<int64_t*>uv.data)[0]

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
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if uv.data_length == 1:
            if uv.validity != NULL:
                raise ValueError("Cannot compute max of all-null column")
            return (<int64_t*>uv.data)[0]

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
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return 0
            return <int64_t>(self.ptr.length * (<int64_t*>uv.data)[0])
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
        cdef DrakenFixedBuffer* ptr = self.ptr
        if ptr.null_bitmap == NULL:
            return False
        return not _bitmap_is_valid(ptr.null_bitmap, idx)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenVector* uv = self.unified()
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

        if uv.data_length == 1:
            if uv.validity != NULL:
                value = NULL_HASH
            elif self.is_time64:
                value = <uint64_t>(<int64_t*>uv.data)[0]
            else:
                value = <uint64_t><int64_t>(<int32_t*>uv.data)[0]
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

        cdef DrakenVector* _cuv = &self._unified_view
        if _cuv.data_length == 1:
            if _cuv.validity != NULL:
                value = NULL_HASH
            elif self.is_time64:
                value = <uint64_t>(<int64_t*>_cuv.data)[0]
            else:
                value = <uint64_t><int64_t>(<int32_t*>_cuv.data)[0]
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
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
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

        if uv.data_length == 1:
            for i in range(n):
                if uv.validity != NULL:
                    dst[i] = NULL_FLAG
                elif self.is_time64:
                    dst[i] = (<int64_t*>uv.data)[0]
                else:
                    dst[i] = <int64_t>(<int32_t*>uv.data)[0]
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
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            return f"<TimeVector len={buf_length(self.ptr)} is_time64={self.is_time64} values={[None if uv.validity != NULL else (<int64_t*>uv.data)[0]] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
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
    vec._arrow_data_buf = bufs[1]

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
            vec._arrow_null_buf = bufs[0]
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

    cdef DrakenType _fa_time_type = DRAKEN_TIME64 if is_time64 else DRAKEN_TIME32
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, _fa_time_type, vec.ptr.null_bitmap)
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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, DRAKEN_TIME32, vec.ptr.null_bitmap)
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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, DRAKEN_TIME32, vec.ptr.null_bitmap)
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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, DRAKEN_TIME64, vec.ptr.null_bitmap)
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

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, DRAKEN_TIME64, vec.ptr.null_bitmap)
    return vec


