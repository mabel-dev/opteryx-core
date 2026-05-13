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
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport DrakenFixedBuffer, DrakenRLEBuffer, DrakenType
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64
from draken.core.buffers cimport ConstAccessor, DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_RLE
from draken.core.fixed_vector cimport (
    alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer,
)
from draken.vectors.vector cimport MIX_HASH_CONSTANT, NULL_HASH, Vector, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_integer_compare.hpp" namespace "draken::integer_cmp" nogil:
    # Single-value compare (for const / RLE paths)
    bint dispatch_compare_once(int op, int64_t a, int64_t b)
    # bit_fill_range: fill count bits in dst starting at bit-offset start
    void bit_fill_range(uint8_t* dst, size_t start, size_t count)
    # Scalar dispatchers — per element width
    void dispatch_scalar_nonnull_i8(int op, const int8_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless_i8(int op, const int8_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching_i8(int op, const int8_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_nonnull_i16(int op, const int16_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless_i16(int op, const int16_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching_i16(int op, const int16_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_nonnull_i32(int op, const int32_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless_i32(int op, const int32_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching_i32(int op, const int32_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    # Vector-vector dispatchers — same-type pairs
    void dispatch_vector_nonnull_i8_i8(int op, const int8_t* a, const int8_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i8_i8(int op, const int8_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i8_i8(int op, const int8_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i8_i8(int op, const int8_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i8_i8(int op, const int8_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i16_i16(int op, const int16_t* a, const int16_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i16_i16(int op, const int16_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i16_i16(int op, const int16_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i16_i16(int op, const int16_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i16_i16(int op, const int16_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i32_i32(int op, const int32_t* a, const int32_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    # Mixed-width pairs (self narrower)
    void dispatch_vector_nonnull_i8_i16(int op, const int8_t* a, const int16_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i8_i16(int op, const int8_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i8_i16(int op, const int8_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i8_i16(int op, const int8_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i8_i16(int op, const int8_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i8_i32(int op, const int8_t* a, const int32_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i8_i32(int op, const int8_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i8_i32(int op, const int8_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i8_i32(int op, const int8_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i8_i32(int op, const int8_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i16_i32(int op, const int16_t* a, const int32_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i16_i32(int op, const int16_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i16_i32(int op, const int16_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i16_i32(int op, const int16_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i16_i32(int op, const int16_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    # Mixed-width pairs (self wider)
    void dispatch_vector_nonnull_i16_i8(int op, const int16_t* a, const int8_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i16_i8(int op, const int16_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i16_i8(int op, const int16_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i16_i8(int op, const int16_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i16_i8(int op, const int16_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i32_i8(int op, const int32_t* a, const int8_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i32_i8(int op, const int32_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i32_i8(int op, const int32_t* a, const int8_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i32_i8(int op, const int32_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i32_i8(int op, const int32_t* a, const int8_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_nonnull_i32_i16(int op, const int32_t* a, const int16_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i32_i16(int op, const int32_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i32_i16(int op, const int32_t* a, const int16_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i32_i16(int op, const int32_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i32_i16(int op, const int32_t* a, const int16_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

DEF INTEGER_HASH_CHUNK = 1024


cdef void _release_rle_storage_integer(IntegerVector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1


cdef class IntegerVector(Vector):
    """Fixed-width signed integer vector supporting int8, int16, and int32 widths."""

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        cdef DrakenType dtype = DRAKEN_INT32
        cdef int64_t ivalue = 0
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        if not is_null and value is not None:
            ivalue = <int64_t>int(value)
            if ivalue >= -128 and ivalue <= 127:
                dtype = DRAKEN_INT8
            elif ivalue >= -32768 and ivalue <= 32767:
                dtype = DRAKEN_INT16
        cdef IntegerVector vec = IntegerVector(dtype, 0)
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else ivalue
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

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
        self._const_accessor.length = 0
        self._const_accessor.value_type = dtype
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_rle_storage_integer(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = self.ptr.type
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

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef size_t cumulative = 0
        cdef size_t run_idx
        cdef int64_t* rle_vals_gi
        cdef uint8_t rle_gi_byte
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return <int8_t>self._const_value if ptr.itemsize == 1 else (<int16_t>self._const_value if ptr.itemsize == 2 else <int32_t>self._const_value)
        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals_gi = <int64_t*>self._rle_buffer.run_values
            for run_idx in range(self._rle_buffer.num_runs):
                cumulative += <size_t>self._rle_buffer.run_lengths[run_idx]
                if <size_t>i < cumulative:
                    if self._rle_buffer.null_bitmap != NULL:
                        rle_gi_byte = self._rle_buffer.null_bitmap[i >> 3]
                        if not ((rle_gi_byte >> (i & 7)) & 1):
                            return None
                    return rle_vals_gi[run_idx]
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
        if self._has_const:
            if self.ptr.type == DRAKEN_INT8:
                if self._const_is_null:
                    return pa.nulls(self.ptr.length, type=pa.int8())
                return pa.array([<int8_t>self._const_value] * self.ptr.length, type=pa.int8())
            elif self.ptr.type == DRAKEN_INT16:
                if self._const_is_null:
                    return pa.nulls(self.ptr.length, type=pa.int16())
                return pa.array([<int16_t>self._const_value] * self.ptr.length, type=pa.int16())
            else:
                if self._const_is_null:
                    return pa.nulls(self.ptr.length, type=pa.int32())
                return pa.array([<int32_t>self._const_value] * self.ptr.length, type=pa.int32())
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
        cdef int64_t* rle_vals_int
        cdef int32_t* rle_lens_int
        cdef size_t rle_runs_int
        cdef uint8_t* rle_nulls_int
        cdef Py_ssize_t int_pos
        cdef size_t ir
        cdef int32_t int_run_len
        cdef int64_t int_run_val

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals_int = <int64_t*>self._rle_buffer.run_values
            rle_lens_int = self._rle_buffer.run_lengths
            rle_runs_int = self._rle_buffer.num_runs
            rle_nulls_int = self._rle_buffer.null_bitmap
            int_pos = 0
            for ir in range(rle_runs_int):
                int_run_val = rle_vals_int[ir]
                int_run_len = rle_lens_int[ir]
                for i in range(int_run_len):
                    if rle_nulls_int != NULL and not ((rle_nulls_int[(int_pos + i) >> 3] >> ((int_pos + i) & 7)) & 1):
                        out.append(None)
                    else:
                        out.append(int_run_val)
                int_pos += int_run_len
            return out

        if self._has_const:
            for i in range(n):
                out.append(None if self._const_is_null else self[i])
            return out
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

    cpdef int64_t min(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self).min()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value

        cdef int64_t m
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32
        cdef bint found = False

        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d8[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute min of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d8[i] < m:
                    m = <int64_t>d8[i]
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d16[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute min of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d16[i] < m:
                    m = <int64_t>d16[i]
        else:
            d32 = <int32_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d32[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute min of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d32[i] < m:
                    m = <int64_t>d32[i]
        return m

    cpdef int64_t max(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self).max()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value

        cdef int64_t m
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32
        cdef bint found = False

        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d8[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute max of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d8[i] > m:
                    m = <int64_t>d8[i]
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d16[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute max of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d16[i] > m:
                    m = <int64_t>d16[i]
        else:
            d32 = <int32_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                m = <int64_t>d32[i]
                found = True
                break
            if not found:
                raise ValueError("Cannot compute max of all-null column")
            for i in range(i + 1, n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                if <int64_t>d32[i] > m:
                    m = <int64_t>d32[i]
        return m

    cpdef int64_t sum(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self).sum()
        if self._has_const:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32

        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                total += <int64_t>d8[i]
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                total += <int64_t>d16[i]
        else:
            d32 = <int32_t*>ptr.data
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                        continue
                total += <int64_t>d32[i]
        return total

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare int values at two indices. Returns -1, 0, or 1."""
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self).compare_at(left_idx, right_idx)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t left_val, right_val
        cdef bint left_is_null, right_is_null

        # Check nulls
        left_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, left_idx, 0)
        right_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, right_idx, 0)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        # Extract and compare based on itemsize
        if ptr.itemsize == 1:
            left_val = <int64_t>(<int8_t*>ptr.data)[left_idx]
            right_val = <int64_t>(<int8_t*>ptr.data)[right_idx]
        elif ptr.itemsize == 2:
            left_val = <int64_t>(<int16_t*>ptr.data)[left_idx]
            right_val = <int64_t>(<int16_t*>ptr.data)[right_idx]
        else:  # itemsize == 4
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
        cdef uint8_t rle_byte

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return False
            rle_byte = self._rle_buffer.null_bitmap[idx >> 3]
            return ((rle_byte >> (idx & 7)) & 1) == 0

        if ptr.null_bitmap == NULL:
            return False
        return not _bitmap_is_valid(ptr.null_bitmap, idx, 0)

    cpdef IntegerVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self).take(indices)
        if self._has_const:
            return IntegerVector.from_constant(
                None if self._const_is_null else self._const_value,
                n,
                is_null=self._const_is_null,
            )
        cdef IntegerVector out = IntegerVector(self.ptr.type, <size_t>n)
        cdef uint8_t* src_null = self.ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t out_nbytes
        cdef int32_t src_idx
        cdef uint8_t byte
        cdef int8_t*  src8
        cdef int16_t* src16
        cdef int32_t* src32
        cdef int8_t*  dst8
        cdef int16_t* dst16
        cdef int32_t* dst32

        if self.ptr.itemsize == 1:
            src8 = <int8_t*>self.ptr.data
            dst8 = <int8_t*>out.ptr.data
            if src_null == NULL:
                for i in range(n):
                    dst8[i] = src8[indices[i]]
                out.ptr.null_bitmap = NULL
            else:
                out_nbytes = (n + 7) >> 3
                out_null = <uint8_t*>malloc(out_nbytes)
                if out_null == NULL:
                    raise MemoryError()
                memset(out_null, 0, out_nbytes)
                for i in range(n):
                    src_idx = indices[i]
                    byte = src_null[src_idx >> 3]
                    if byte & (1 << (src_idx & 7)):
                        dst8[i] = src8[src_idx]
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    else:
                        dst8[i] = 0
                out.ptr.null_bitmap = out_null
        elif self.ptr.itemsize == 2:
            src16 = <int16_t*>self.ptr.data
            dst16 = <int16_t*>out.ptr.data
            if src_null == NULL:
                for i in range(n):
                    dst16[i] = src16[indices[i]]
                out.ptr.null_bitmap = NULL
            else:
                out_nbytes = (n + 7) >> 3
                out_null = <uint8_t*>malloc(out_nbytes)
                if out_null == NULL:
                    raise MemoryError()
                memset(out_null, 0, out_nbytes)
                for i in range(n):
                    src_idx = indices[i]
                    byte = src_null[src_idx >> 3]
                    if byte & (1 << (src_idx & 7)):
                        dst16[i] = src16[src_idx]
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    else:
                        dst16[i] = 0
                out.ptr.null_bitmap = out_null
        else:
            src32 = <int32_t*>self.ptr.data
            dst32 = <int32_t*>out.ptr.data
            if src_null == NULL:
                for i in range(n):
                    dst32[i] = src32[indices[i]]
                out.ptr.null_bitmap = NULL
            else:
                out_nbytes = (n + 7) >> 3
                out_null = <uint8_t*>malloc(out_nbytes)
                if out_null == NULL:
                    raise MemoryError()
                memset(out_null, 0, out_nbytes)
                for i in range(n):
                    src_idx = indices[i]
                    byte = src_null[src_idx >> 3]
                    if byte & (1 << (src_idx & 7)):
                        dst32[i] = src32[src_idx]
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    else:
                        dst32[i] = 0
                out.ptr.null_bitmap = out_null
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

    cdef BoolVector _compare_scalar_rle(self, int64_t value, int op):
        # Evaluate predicate once per run, fill the result bitmap using
        # bit_fill_range, then AND with the row-level null bitmap — no
        # materialisation of the full dense array.
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

    cdef BoolVector _compare_scalar(self, int64_t value, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_scalar_rle(value, op)

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

        # Gate: >~70% null density → branching kernel (skips work for null rows).
        # Below that: branchless kernel avoids mispredicted comparison branches.
        # Op dispatch happens once here; the C++ kernel runs a tight loop with
        # no per-row branching on op or itemsize.
        cdef size_t valid_count
        cdef bint use_branching = False
        if src_null != NULL and n > 0:
            valid_count = simd_popcount(src_null, <size_t>nbytes)
            use_branching = (valid_count * 10) < (<size_t>n * 3)

        if ptr.itemsize == 1:
            if src_null == NULL:
                dispatch_scalar_nonnull_i8(op, <const int8_t*>ptr.data, value, dst, <size_t>n)
            elif use_branching:
                dispatch_scalar_branching_i8(op, <const int8_t*>ptr.data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless_i8(op, <const int8_t*>ptr.data, value, src_null, dst, <size_t>n)
        elif ptr.itemsize == 2:
            if src_null == NULL:
                dispatch_scalar_nonnull_i16(op, <const int16_t*>ptr.data, value, dst, <size_t>n)
            elif use_branching:
                dispatch_scalar_branching_i16(op, <const int16_t*>ptr.data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless_i16(op, <const int16_t*>ptr.data, value, src_null, dst, <size_t>n)
        else:  # itemsize == 4
            if src_null == NULL:
                dispatch_scalar_nonnull_i32(op, <const int32_t*>ptr.data, value, dst, <size_t>n)
            elif use_branching:
                dispatch_scalar_branching_i32(op, <const int32_t*>ptr.data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless_i32(op, <const int32_t*>ptr.data, value, src_null, dst, <size_t>n)
        return out

    cdef BoolVector _compare_vector(self, IntegerVector other, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_integer(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_vector(_materialize_rle_integer(other), op)

        # Const fast paths: avoid O(n) materialisation.
        cdef Py_ssize_t const_n
        cdef int reversed_op
        if self._has_const:
            const_n = self.ptr.length
            if const_n != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if self._const_is_null:
                return self._make_all_null_bool(const_n)
            # Reverse directional ops: gt(2)↔lt(4), ge(3)↔le(5); eq/ne unchanged.
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

        # Gate: >~70% nulls → branching kernel; below → branchless.
        # Itemsize dispatch and op dispatch are both hoisted outside the hot loop
        # by the C++ template instantiation.
        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False
        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        # Itemsize pair dispatch — hoisted outside the loop.
        # For each pair of itemsizes call the matching typed C++ dispatcher.
        cdef size_t s1 = ptr1.itemsize
        cdef size_t s2 = ptr2.itemsize

        if null1 == NULL and null2 == NULL:
            if s1 == 1 and s2 == 1:
                dispatch_vector_nonnull_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 2 and s2 == 2:
                dispatch_vector_nonnull_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 4 and s2 == 4:
                dispatch_vector_nonnull_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 1 and s2 == 2:
                dispatch_vector_nonnull_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 1 and s2 == 4:
                dispatch_vector_nonnull_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 2 and s2 == 4:
                dispatch_vector_nonnull_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 2 and s2 == 1:
                dispatch_vector_nonnull_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, dst, <size_t>n)
            elif s1 == 4 and s2 == 1:
                dispatch_vector_nonnull_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, dst, <size_t>n)
            else:  # s1 == 4, s2 == 2
                dispatch_vector_nonnull_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, dst, <size_t>n)
        elif use_branching:
            if null1 != NULL and null2 != NULL:
                if s1 == 1 and s2 == 1:
                    dispatch_vector_both_null_branching_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_both_null_branching_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_both_null_branching_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_both_null_branching_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_both_null_branching_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_both_null_branching_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_both_null_branching_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_both_null_branching_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_both_null_branching_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
            elif null1 != NULL:
                if s1 == 1 and s2 == 1:
                    dispatch_vector_one_null_branching_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_one_null_branching_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_one_null_branching_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_one_null_branching_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_one_null_branching_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_one_null_branching_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_one_null_branching_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_one_null_branching_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_one_null_branching_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
            else:  # null2 != NULL
                if s1 == 1 and s2 == 1:
                    dispatch_vector_one_null_branching_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_one_null_branching_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_one_null_branching_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_one_null_branching_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_one_null_branching_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_one_null_branching_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_one_null_branching_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_one_null_branching_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_one_null_branching_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
        else:
            # Branchless path
            if null1 != NULL and null2 != NULL:
                if s1 == 1 and s2 == 1:
                    dispatch_vector_both_null_branchless_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_both_null_branchless_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_both_null_branchless_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_both_null_branchless_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_both_null_branchless_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_both_null_branchless_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_both_null_branchless_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_both_null_branchless_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_both_null_branchless_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null1, null2, dst, out_null, <size_t>n)
            elif null1 != NULL:
                if s1 == 1 and s2 == 1:
                    dispatch_vector_one_null_branchless_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_one_null_branchless_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_one_null_branchless_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_one_null_branchless_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_one_null_branchless_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_one_null_branchless_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_one_null_branchless_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_one_null_branchless_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null1, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_one_null_branchless_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null1, dst, out_null, <size_t>n)
            else:  # null2 != NULL
                if s1 == 1 and s2 == 1:
                    dispatch_vector_one_null_branchless_i8_i8(op, <const int8_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 2:
                    dispatch_vector_one_null_branchless_i16_i16(op, <const int16_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 4:
                    dispatch_vector_one_null_branchless_i32_i32(op, <const int32_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 2:
                    dispatch_vector_one_null_branchless_i8_i16(op, <const int8_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 1 and s2 == 4:
                    dispatch_vector_one_null_branchless_i8_i32(op, <const int8_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 4:
                    dispatch_vector_one_null_branchless_i16_i32(op, <const int16_t*>ptr1.data, <const int32_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 2 and s2 == 1:
                    dispatch_vector_one_null_branchless_i16_i8(op, <const int16_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                elif s1 == 4 and s2 == 1:
                    dispatch_vector_one_null_branchless_i32_i8(op, <const int32_t*>ptr1.data, <const int8_t*>ptr2.data, null2, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_one_null_branchless_i32_i16(op, <const int32_t*>ptr1.data, <const int16_t*>ptr2.data, null2, dst, out_null, <size_t>n)
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, IntegerVector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, IntegerVector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, IntegerVector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, IntegerVector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, IntegerVector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, IntegerVector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Range check delegating to _compare_scalar (handles all integer widths)."""
        cdef BoolVector lo, hi
        if lower_inclusive:
            lo = self._compare_scalar(lower, 3)  # GtEq
        else:
            lo = self._compare_scalar(lower, 2)  # Gt
        if upper_inclusive:
            hi = self._compare_scalar(upper, 5)  # LtEq
        else:
            hi = self._compare_scalar(upper, 4)  # Lt
        return lo.and_vector(hi)

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

        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_integer(self).hash_into(out_buf, offset)
            return

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(INTEGER_HASH_CHUNK):
                scratch[j] = value
            if n > 0:
                dst = &out_buf[0] + offset
                i = 0
                while i < n:
                    block = n - i
                    if block > INTEGER_HASH_CHUNK:
                        block = INTEGER_HASH_CHUNK
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            return

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("IntegerVector.hash_into: output buffer too small")

        dst = &out_buf[0] + offset

        cdef uint64_t is_valid
        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d8[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d8[i + j])
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d16[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d16[i + j])
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        else:  # itemsize == 4
            d32 = <int32_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d32[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d32[i + j])
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i, block, j
        cdef uint8_t byte
        cdef uint64_t value, is_valid
        cdef int8_t* d8
        cdef int16_t* d16
        cdef int32_t* d32
        cdef uint64_t[INTEGER_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if self._has_const:
            value = NULL_HASH if self._const_is_null else <uint64_t>self._const_value
            for j in range(INTEGER_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        if n == 0:
            return 0

        if ptr.itemsize == 1:
            d8 = <int8_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d8[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d8[i + j])
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        elif ptr.itemsize == 2:
            d16 = <int16_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d16[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d16[i + j])
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        else:  # itemsize == 4
            d32 = <int32_t*>ptr.data
            i = 0
            while i < n:
                block = n - i
                if block > INTEGER_HASH_CHUNK:
                    block = INTEGER_HASH_CHUNK
                if has_nulls:
                    for j in range(block):
                        is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                        scratch[j] = (<uint64_t>(<int64_t>d32[i + j]) * is_valid) | (NULL_HASH * (1 - is_valid))
                else:
                    for j in range(block):
                        scratch[j] = <uint64_t>(<int64_t>d32[i + j])
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        return 0

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:10]
            return f"<IntegerVector(RLE) len={self._rle_buffer.length} values={vals}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
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
    elif pa_type.equals(pa.uint8()):
        dtype = DRAKEN_INT8
        itemsize = 1
    elif pa_type.equals(pa.uint16()):
        dtype = DRAKEN_INT16
        itemsize = 2
    elif pa_type.equals(pa.uint32()):
        dtype = DRAKEN_INT32
        itemsize = 4
    elif pa_type.equals(pa.int64()):
        dtype = DRAKEN_INT64
        itemsize = 8
    elif pa_type.equals(pa.uint64()):
        dtype = DRAKEN_INT64
        itemsize = 8
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


cdef IntegerVector _materialize_rle_integer(IntegerVector rle_vec):
    """Expand an RLE IntegerVector to a dense IntegerVector.

    Run values are stored as int64_t (widened); they are narrowed to the
    native width (int8/int16/int32) when written to the dense buffer.
    """
    cdef DrakenType dtype = rle_vec.ptr.type
    cdef size_t total = rle_vec._rle_buffer.length
    cdef IntegerVector dense = IntegerVector(dtype, <size_t>total)

    cdef int64_t* rle_vals = <int64_t*>rle_vec._rle_buffer.run_values
    cdef int32_t* rle_lens = rle_vec._rle_buffer.run_lengths
    cdef size_t num_runs = rle_vec._rle_buffer.num_runs
    cdef uint8_t* rle_nulls = rle_vec._rle_buffer.null_bitmap

    cdef size_t pos = 0
    cdef size_t r
    cdef int32_t run_len
    cdef int64_t run_val
    cdef Py_ssize_t j

    cdef int8_t* dst8
    cdef int16_t* dst16
    cdef int32_t* dst32
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    if dtype == DRAKEN_INT8:
        dst8 = <int8_t*>dense.ptr.data
        for r in range(num_runs):
            run_val = rle_vals[r]
            run_len = rle_lens[r]
            for j in range(run_len):
                dst8[pos + j] = <int8_t>run_val
            pos += <size_t>run_len
    elif dtype == DRAKEN_INT16:
        dst16 = <int16_t*>dense.ptr.data
        for r in range(num_runs):
            run_val = rle_vals[r]
            run_len = rle_lens[r]
            for j in range(run_len):
                dst16[pos + j] = <int16_t>run_val
            pos += <size_t>run_len
    else:  # DRAKEN_INT32
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


cdef IntegerVector from_rle_builder(
    int64_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    DrakenType dtype,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded IntegerVector from raw C arrays.

    The caller passes pointers to builder-owned arrays; this function copies
    the run data into fresh malloc'd arrays owned by the vector.  Run values
    are stored as int64_t regardless of the native width.

    Args:
        run_values:  Pointer to int64_t values array (num_runs entries).
        run_lengths: Pointer to int32_t run lengths (num_runs entries).
        num_runs:    Number of runs.
        dtype:       DRAKEN_INT8, DRAKEN_INT16, or DRAKEN_INT32.
        null_bitmap: Optional logical-row null bitmap (NULL = no nulls).

    Returns:
        IntegerVector with DRAKEN_ENCODING_RLE encoding.
    """
    cdef IntegerVector vec = IntegerVector(dtype, 0)  # ptr.data = NULL, ptr.length = 0
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef int64_t* vals_copy
    cdef int32_t* lens_copy
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    # Compute total logical length from run lengths
    for i in range(num_runs):
        total_length += <size_t>run_lengths[i]

    # Set ptr.length so the .length property returns the correct value
    vec.ptr.length = total_length

    if num_runs == 0:
        vec._encoding = DRAKEN_ENCODING_RLE
        return vec

    # Allocate RLE buffer header
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
    rle.type = dtype

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

    return vec
