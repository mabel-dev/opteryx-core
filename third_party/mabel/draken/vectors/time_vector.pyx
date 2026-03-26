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
from libc.stdlib cimport malloc
from libc.string cimport memset, memcpy

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from opteryx.compiled.draken.core.fixed_vector cimport alloc_fixed_buffer
from opteryx.compiled.draken.core.fixed_vector cimport buf_dtype
from opteryx.compiled.draken.core.fixed_vector cimport buf_itemsize
from opteryx.compiled.draken.core.fixed_vector cimport buf_length
from opteryx.compiled.draken.core.fixed_vector cimport free_fixed_buffer
from opteryx.compiled.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash

DEF TIME32_HASH_CHUNK = 1024

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

    def __dealloc__(self):
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
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value if self.is_time64 else <int32_t>self._const_value
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

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef int64_t* data64
        cdef int32_t* data32

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

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for i in range(n):
                out_buf[offset + i] = mix_hash(out_buf[offset + i], value)
            return

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimeVector.hash_into: output buffer too small")

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if not has_nulls:
                simd_mix_hash(dst, <uint64_t*> data64, <size_t> n)
                return
            for i in range(n):
                if has_nulls:
                    byte = ptr.null_bitmap[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    if not bit:
                        value = NULL_HASH
                    else:
                        value = <uint64_t> data64[i]
                else:
                    value = <uint64_t> data64[i]

                dst[i] = mix_hash(dst[i], value)
        else:
            data32 = <int32_t*> ptr.data
            if not has_nulls:
                i = 0
                while i < n:
                    block = n - i
                    if block > TIME32_HASH_CHUNK:
                        block = TIME32_HASH_CHUNK
                    for j in range(block):
                        scratch32[j] = <uint64_t>(<int64_t> data32[i + j])
                    simd_mix_hash(dst + i, scratch32_ptr, <size_t> block)
                    i += block
                return

            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    value = NULL_HASH
                else:
                    value = <uint64_t>(<int64_t> data32[i])

                dst[i] = mix_hash(dst[i], value)

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
            for i in range(n):
                out[i] = mix_hash(out[i], value)
            return 0

        if n == 0:
            return 0

        if self.is_time64:
            data64 = <int64_t*> ptr.data
            if not has_nulls:
                simd_mix_hash(out, <uint64_t*> data64, <size_t> n)
                return 0
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                value = <uint64_t> data64[i] if bit else NULL_HASH
                out[i] = mix_hash(out[i], value)
        else:
            data32 = <int32_t*> ptr.data
            if not has_nulls:
                i = 0
                while i < n:
                    block = n - i
                    if block > TIME32_HASH_CHUNK:
                        block = TIME32_HASH_CHUNK
                    for j in range(block):
                        scratch32[j] = <uint64_t>(<int64_t> data32[i + j])
                    simd_mix_hash(out + i, scratch32_ptr, <size_t> block)
                    i += block
                return 0
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                value = <uint64_t>(<int64_t> data32[i]) if bit else NULL_HASH
                out[i] = mix_hash(out[i], value)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for TimeVector: handle both time32 and time64."""
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
        if self._has_const:
            return f"<TimeVector len={buf_length(self.ptr)} is_time64={self.is_time64} values={[None if self._const_is_null else self._const_value] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int64_t* data64
        cdef int32_t* data32

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
