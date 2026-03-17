# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
IntegerVector: Draken fixed-width integer column for int8/int16/int32 widths.

Stores data at native width using DrakenFixedBuffer.itemsize (1/2/4).
Provides the same hash_into / __getitem__ / to_arrow interface as Int64Vector so
the GroupStateStore fast paths can accept any integer-width key column via a
single isinstance(key_vector, IntegerVector) check, dispatching on itemsize
internally — no data copies, no widening outside the hash map key cast.

int64 columns continue to use Int64Vector for the SIMD simd_mix_hash path.
"""

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport (
    int8_t, int16_t, int32_t, int64_t,
    uint8_t, uint64_t, intptr_t,
)
from libc.stdlib cimport malloc
from libc.string cimport memset

from opteryx.draken.core.buffers cimport DrakenFixedBuffer, DrakenType
from opteryx.draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32
from opteryx.draken.core.fixed_vector cimport (
    alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer,
)
from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, NULL_HASH, Vector, mix_hash, simd_mix_hash

DEF INTEGER_HASH_CHUNK = 1024


cdef class IntegerVector(Vector):
    """Fixed-width signed integer vector supporting int8, int16, and int32 widths."""

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

    def __cinit__(self, DrakenType dtype=DRAKEN_INT32, size_t length=0, bint wrap=False):
        cdef size_t itemsize
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            if dtype == DRAKEN_INT8:
                itemsize = 1
            elif dtype == DRAKEN_INT16:
                itemsize = 2
            else:
                itemsize = 4
            self.ptr = alloc_fixed_buffer(dtype, length, itemsize)
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

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
        cdef DrakenFixedBuffer* ptr = self.ptr
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if ptr.null_bitmap != NULL:
            if not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
                return None
        if ptr.itemsize == 1:
            return (<int8_t*>ptr.data)[i]
        elif ptr.itemsize == 2:
            return (<int16_t*>ptr.data)[i]
        else:
            return (<int32_t*>ptr.data)[i]

    def to_arrow(self):
        import pyarrow as pa
        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        cdef intptr_t addr = <intptr_t>self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)
        buffers = [None, data_buf]
        if self.ptr.null_bitmap != NULL:
            buffers[0] = pa.foreign_buffer(
                <intptr_t>self.ptr.null_bitmap,
                (self.ptr.length + 7) // 8,
                base=self,
            )
        if self.ptr.type == DRAKEN_INT8:
            pa_type = pa.int8()
        elif self.ptr.type == DRAKEN_INT16:
            pa_type = pa.int16()
        else:
            pa_type = pa.int32()
        return pa.Array.from_buffers(pa_type, buf_length(self.ptr), buffers)

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef uint8_t byte
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32
        cdef list out = []
        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(d8[i])
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    out.append(d8[i] if (byte >> (i & 7)) & 1 else None)
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(d16[i])
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    out.append(d16[i] if (byte >> (i & 7)) & 1 else None)
        else:
            d32 = <int32_t*>ptr.data
            if ptr.null_bitmap == NULL:
                for i in range(n):
                    out.append(d32[i])
            else:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    out.append(d32[i] if (byte >> (i & 7)) & 1 else None)
        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i, block, j
        cdef uint8_t byte
        cdef uint64_t value
        cdef uint64_t* dst
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32
        cdef uint64_t[INTEGER_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("IntegerVector.hash_into: output buffer too small")

        dst = &out_buf[0] + offset

        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            if not has_nulls:
                i = 0
                while i < n:
                    block = n - i
                    if block > INTEGER_HASH_CHUNK:
                        block = INTEGER_HASH_CHUNK
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d8[i + j])
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            else:
                for i in range(n):
                    byte = null_bitmap[i >> 3]
                    value = <uint64_t>(<int64_t>d8[i]) if (byte >> (i & 7)) & 1 else NULL_HASH
                    dst[i] = mix_hash(dst[i], value)
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            if not has_nulls:
                i = 0
                while i < n:
                    block = n - i
                    if block > INTEGER_HASH_CHUNK:
                        block = INTEGER_HASH_CHUNK
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d16[i + j])
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            else:
                for i in range(n):
                    byte = null_bitmap[i >> 3]
                    value = <uint64_t>(<int64_t>d16[i]) if (byte >> (i & 7)) & 1 else NULL_HASH
                    dst[i] = mix_hash(dst[i], value)
        else:  # itemsize == 4
            d32 = <int32_t*>ptr.data
            if not has_nulls:
                i = 0
                while i < n:
                    block = n - i
                    if block > INTEGER_HASH_CHUNK:
                        block = INTEGER_HASH_CHUNK
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d32[i + j])
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            else:
                for i in range(n):
                    byte = null_bitmap[i >> 3]
                    value = <uint64_t>(<int64_t>d32[i]) if (byte >> (i & 7)) & 1 else NULL_HASH
                    dst[i] = mix_hash(dst[i], value)

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        for i in range(k):
            vals.append(self[i])
        return f"<IntegerVector itemsize={buf_itemsize(self.ptr)} len={buf_length(self.ptr)} values={vals}>"


cdef IntegerVector from_arrow(object array):
    """Zero-copy wrap of a PyArrow int8/int16/int32 array as an IntegerVector."""
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "IntegerVector.from_arrow expects a dense 8/16/32-bit Arrow integer array; "
            "use IntegerVector.from_dict for dictionary input"
        )

    cdef DrakenType dtype
    cdef size_t itemsize
    cdef IntegerVector vec
    cdef object bufs
    cdef intptr_t base_ptr, nb_addr
    cdef Py_ssize_t arr_offset, nb_size, j
    cdef object new_bitmap_bytes
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap

    pa_type = array.type
    if pa_type.equals(pa.int8()):
        dtype = DRAKEN_INT8
        itemsize = 1
    elif pa_type.equals(pa.int16()):
        dtype = DRAKEN_INT16
        itemsize = 2
    else:
        dtype = DRAKEN_INT32
        itemsize = 4

    vec = IntegerVector(dtype, 0, True)
    vec.ptr = <DrakenFixedBuffer*>malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    base_ptr = <intptr_t>bufs[1].address
    arr_offset = array.offset

    vec.ptr.type = dtype
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t>len(array)
    vec.ptr.data = <void*>(base_ptr + arr_offset * <Py_ssize_t>itemsize)

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if arr_offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*>nb_addr) + (arr_offset >> 3)
        else:
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*>PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nb_size)
            src_bitmap = <uint8_t*>nb_addr
            for j in range(len(array)):
                if (src_bitmap[(arr_offset + j) >> 3] >> ((arr_offset + j) & 7)) & 1:
                    dst_bitmap[j >> 3] |= (1 << (j & 7))
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    return vec


cdef IntegerVector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef int64_t value
    cdef int64_t min_value
    cdef int64_t max_value
    cdef DrakenType dtype = DRAKEN_INT32
    cdef IntegerVector vec

    if dict_size == 0:
        raise ValueError("IntegerVector.from_dict requires a non-empty dictionary")

    min_value = dictionary[0]
    max_value = min_value
    for i in range(1, dict_size):
        value = dictionary[i]
        if value < min_value:
            min_value = value
        if value > max_value:
            max_value = value

    if min_value >= -128 and max_value <= 127:
        dtype = DRAKEN_INT8
    elif min_value >= -32768 and max_value <= 32767:
        dtype = DRAKEN_INT16

    vec = IntegerVector(dtype, <size_t>row_count)

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        value = dictionary[code]
        if vec.ptr.itemsize == 1:
            (<int8_t*>vec.ptr.data)[i] = <int8_t>value
        elif vec.ptr.itemsize == 2:
            (<int16_t*>vec.ptr.data)[i] = <int16_t>value
        else:
            (<int32_t*>vec.ptr.data)[i] = <int32_t>value

    return vec


cdef IntegerVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef int64_t value
    cdef int64_t min_value
    cdef int64_t max_value
    cdef DrakenType dtype = DRAKEN_INT32
    cdef IntegerVector vec
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("IntegerVector.from_dict requires a non-empty dictionary")
    if row_validity.shape[0] != row_count:
        raise ValueError("row_validity length must match codes length")

    min_value = dictionary[0]
    max_value = min_value
    for i in range(1, dict_size):
        value = dictionary[i]
        if value < min_value:
            min_value = value
        if value > max_value:
            max_value = value

    if min_value >= -128 and max_value <= 127:
        dtype = DRAKEN_INT8
    elif min_value >= -32768 and max_value <= 32767:
        dtype = DRAKEN_INT16

    vec = IntegerVector(dtype, <size_t>row_count)
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
            value = dictionary[code]
            if vec.ptr.itemsize == 1:
                (<int8_t*>vec.ptr.data)[i] = <int8_t>value
            elif vec.ptr.itemsize == 2:
                (<int16_t*>vec.ptr.data)[i] = <int16_t>value
            else:
                (<int32_t*>vec.ptr.data)[i] = <int32_t>value
            nb[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            if vec.ptr.itemsize == 1:
                (<int8_t*>vec.ptr.data)[i] = 0
            elif vec.ptr.itemsize == 2:
                (<int16_t*>vec.ptr.data)[i] = 0
            else:
                (<int32_t*>vec.ptr.data)[i] = 0

    return vec
