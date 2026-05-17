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
BoolVector: Cython implementation of a zero-copy, bit-packed boolean column vector for Draken.

This matches Arrow's representation:
- Values are stored bit-packed in data buffer (1 bit per value).
- Nulls are stored in the null_bitmap (same layout).
- Zero-copy interop with Arrow via from_arrow/to_arrow.

"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memcpy, memset

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t, int64_t
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n)

from draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer, DrakenRLEBuffer, DrakenVector
from draken.core.buffers cimport DRAKEN_BOOL
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT, DRAKEN_ENCODING_RLE
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_length, free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount

cdef const uint64_t TRUE_HASH = <uint64_t>0x4f112caa54efa882ULL
cdef const uint64_t FALSE_HASH = <uint64_t>0xc2fd8b2343f83ce7ULL
cdef uint8_t _CONST_NULL_BYTE = 0

DEF BOOL_HASH_CHUNK = 1024


cdef void _release_rle_storage_bool(BoolVector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef void _refresh_unified_bool(BoolVector vec) noexcept:
    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    vec._unified_view.length = <size_t>n
    vec._unified_view.itemsize = 0  # bit-packed
    vec._unified_view.type = DRAKEN_BOOL
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


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1


cdef class BoolVector(Vector):
    # Re-Cythonize this implementation when the pxd layout changes.

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef BoolVector vec = BoolVector(0)
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0 if is_null or value is None else <uint8_t>(1 if bool(value) else 0)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        _refresh_unified_bool(vec)
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        cdef size_t nbytes

        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            # bit-packed, so allocate ceil(length/8) bytes
            nbytes = (length + 7) >> 3
            self.ptr = alloc_fixed_buffer(DRAKEN_BOOL, length, 1)  # itemsize=1 is logical
            if self.ptr != NULL:
                # allocate raw bytes with libc malloc so free_fixed_buffer (which calls free())
                # can safely free the buffer later. Do not mix Python allocator and free().
                self.ptr.data = malloc(nbytes)
                if self.ptr.data == NULL:
                    raise MemoryError()
                if nbytes > 0:
                    memset(self.ptr.data, 0, nbytes)
            self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_BOOL
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL
        self._unified_view.data = NULL
        self._unified_view.data_length = 0
        self._unified_view.selection = NULL
        self._unified_view.sel_width = 0
        self._unified_view.length = 0
        self._unified_view.validity = NULL
        self._unified_view.itemsize = 0
        self._unified_view.type = DRAKEN_BOOL
        if not wrap:
            _refresh_unified_bool(self)

    def __dealloc__(self):
        _release_rle_storage_bool(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_BOOL
        self._const_accessor.value_ptr = <void*>&self._const_value
        self._const_accessor.is_null = 1 if self._const_is_null else 0
        return &self._const_accessor

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    # Properties
    @property
    def length(self):
        return buf_length(self.ptr)

    def __len__(self):
        return buf_length(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef size_t bgi_cumulative = 0
        cdef size_t bgi_run
        cdef uint8_t* bgi_rle_vals
        cdef uint8_t val_byte
        cdef uint8_t byte, bit
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return bool(self._const_value)
        # null check
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        # extract bit
        val_byte = (<uint8_t*>ptr.data)[i >> 3]
        return bool((val_byte >> (i & 7)) & 1)

    # -------- Interop --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        if self._has_const:
            if self._const_is_null:
                return pa.nulls(self.ptr.length, type=pa.bool_())
            return pa.array([bool(self._const_value)] * self.ptr.length, type=pa.bool_())

        cdef size_t nbytes = (buf_length(self.ptr) + 7) >> 3
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.bool_(), buf_length(self.ptr), buffers)

    cpdef BoolVector and_vector(self, BoolVector other):
        """Element-wise AND between two BoolVector instances with SQL null semantics."""
        cdef BoolVector _mat
        cdef size_t _mat_n
        cdef uint8_t* _mat_dst
        cdef Py_ssize_t _mat_nb
        if self._has_const:
            _mat_n = self.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if self._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif self._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return _mat.and_vector(other)
        if other._has_const:
            _mat_n = other.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if other._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif other._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return self.and_vector(_mat)
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out

        # Fast path: no nulls on either side -> SIMD bulk AND, no 3VL needed.
        if ptr1.null_bitmap == NULL and ptr2.null_bitmap == NULL:
            out = BoolVector(<size_t>n)
            if nbytes > 0:
                simd_and_mask(
                    <uint8_t*> out.ptr.data,
                    <uint8_t*> ptr1.data,
                    <uint8_t*> ptr2.data,
                    <size_t>nbytes,
                )
            out.ptr.null_bitmap = NULL
            return out

        out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef uint8_t* a_null = ptr1.null_bitmap
        cdef uint8_t* b_null = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef uint8_t a_valid, b_valid, a_val, b_val
        cdef bint valid
        cdef bint result_true
        cdef bint all_valid = True
        cdef Py_ssize_t i
        memset(d, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            a_val = (a[i >> 3] >> (i & 7)) & 1
            b_val = (b[i >> 3] >> (i & 7)) & 1

            valid = False
            result_true = False

            # SQL 3VL: FALSE dominates, TRUE requires both valid+true, else NULL.
            if (a_valid and not a_val) or (b_valid and not b_val):
                valid = True
                result_true = False
            elif a_valid and b_valid:
                valid = True
                result_true = a_val and b_val

            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if result_true:
                    d[i >> 3] |= (1 << (i & 7))
            else:
                all_valid = False

        if all_valid:
            if out_null != NULL:
                free(out_null)
            out.ptr.null_bitmap = NULL
        else:
            out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector or_vector(self, BoolVector other):
        """Element-wise OR between two BoolVector instances with SQL null semantics."""
        cdef BoolVector _mat
        cdef size_t _mat_n
        cdef uint8_t* _mat_dst
        cdef Py_ssize_t _mat_nb
        if self._has_const:
            _mat_n = self.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if self._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif self._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return _mat.or_vector(other)
        if other._has_const:
            _mat_n = other.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if other._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif other._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return self.or_vector(_mat)
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out

        # Fast path: no nulls on either side -> SIMD bulk OR, no 3VL needed.
        if ptr1.null_bitmap == NULL and ptr2.null_bitmap == NULL:
            out = BoolVector(<size_t>n)
            if nbytes > 0:
                simd_or_mask(
                    <uint8_t*> out.ptr.data,
                    <uint8_t*> ptr1.data,
                    <uint8_t*> ptr2.data,
                    <size_t>nbytes,
                )
            out.ptr.null_bitmap = NULL
            return out

        out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef uint8_t* a_null = ptr1.null_bitmap
        cdef uint8_t* b_null = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef uint8_t a_valid, b_valid, a_val, b_val
        cdef bint valid
        cdef bint result_true
        cdef bint all_valid = True
        cdef Py_ssize_t i
        memset(d, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            a_val = (a[i >> 3] >> (i & 7)) & 1
            b_val = (b[i >> 3] >> (i & 7)) & 1

            valid = False
            result_true = False

            # SQL 3VL: TRUE dominates, FALSE requires both valid+false, else NULL.
            if (a_valid and a_val) or (b_valid and b_val):
                valid = True
                result_true = True
            elif a_valid and b_valid:
                valid = True
                result_true = False

            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if result_true:
                    d[i >> 3] |= (1 << (i & 7))
            else:
                all_valid = False

        if all_valid:
            if out_null != NULL:
                free(out_null)
            out.ptr.null_bitmap = NULL
        else:
            out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector xor_vector(self, BoolVector other):
        """Element-wise XOR between two BoolVector instances with SQL null semantics."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out

        # Fast path: no nulls on either side -> SIMD bulk XOR, no 3VL needed.
        if ptr1.null_bitmap == NULL and ptr2.null_bitmap == NULL:
            out = BoolVector(<size_t>n)
            if nbytes > 0:
                simd_xor_mask(
                    <uint8_t*> out.ptr.data,
                    <uint8_t*> ptr1.data,
                    <uint8_t*> ptr2.data,
                    <size_t>nbytes,
                )
            out.ptr.null_bitmap = NULL
            return out

        out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef uint8_t* a_null = ptr1.null_bitmap
        cdef uint8_t* b_null = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef uint8_t a_valid, b_valid, a_val, b_val
        cdef bint all_valid = True
        cdef Py_ssize_t i
        memset(d, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            if a_valid and b_valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                a_val = (a[i >> 3] >> (i & 7)) & 1
                b_val = (b[i >> 3] >> (i & 7)) & 1
                if a_val != b_val:
                    d[i >> 3] |= (1 << (i & 7))
            else:
                all_valid = False

        if all_valid:
            if out_null != NULL:
                free(out_null)
            out.ptr.null_bitmap = NULL
        else:
            out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector not_vector(self):
        """Element-wise NOT with SQL null semantics."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out

        # Fast path: no nulls -> SIMD bulk NOT.
        if ptr.null_bitmap == NULL:
            out = BoolVector(<size_t>n)
            if nbytes > 0:
                simd_not_mask(<uint8_t*> out.ptr.data, <uint8_t*> ptr.data, <size_t>nbytes)
                # NOT inverts trailing bits in the last byte from 0->1; mask them back to 0
                # so consumers like simd_popcount/any() don't see phantom set bits.
                if (n & 7) != 0:
                    (<uint8_t*> out.ptr.data)[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
            out.ptr.null_bitmap = NULL
            return out

        out = BoolVector(<size_t>n)
        cdef uint8_t* src = <uint8_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef bint all_valid = True
        cdef Py_ssize_t i

        memset(dst, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if ((src[i >> 3] >> (i & 7)) & 1) == 0:
                    dst[i >> 3] |= (1 << (i & 7))
            else:
                all_valid = False

        if all_valid:
            if out_null != NULL:
                free(out_null)
            out.ptr.null_bitmap = NULL
        else:
            out.ptr.null_bitmap = out_null
        return out

    # -------- Ops --------
    cpdef BoolVector take(self, int32_t[::1] indices):
        if self._has_const:
            return BoolVector.from_constant(
                None if self._const_is_null else bool(self._const_value),
                indices.shape[0],
                is_null=self._const_is_null,
            )
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* src = <uint8_t*> self.ptr.data
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* src_null = self.ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t length = self.ptr.length
        cdef int32_t idx
        cdef Py_ssize_t out_nbytes = (n + 7) >> 3
        # zero init
        for i in range(out_nbytes):
            dst[i] = 0

        if src_null != NULL and out_nbytes > 0:
            out_null = <uint8_t*> malloc(out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, out_nbytes)

        for i in range(n):
            idx = indices[i]
            if idx < 0 or idx >= length:
                if out_null != NULL:
                    free(out_null)
                raise IndexError("Index out of bounds")
            if src_null != NULL and ((src_null[idx >> 3] >> (idx & 7)) & 1) == 0:
                continue
            if out_null != NULL:
                out_null[i >> 3] |= (1 << (i & 7))
            if ((src[idx >> 3] >> (idx & 7)) & 1) != 0:
                dst[i >> 3] |= (1 << (i & 7))

        out.ptr.null_bitmap = out_null
        _refresh_unified_bool(out)
        return out

    cpdef BoolVector _compare_scalar(self, bint value, int op):
        """Scalar compare using Draken standard op codes: 0=Eq 1=Ne."""
        if op == 0:
            return self.equals(value)
        if op == 1:
            return self.not_equals(value)
        raise ValueError(f"BoolVector._compare_scalar: unsupported op {op}")

    cpdef BoolVector equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* src = <uint8_t*> ptr.data
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int target = 1 if value else 0
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
                if ((src[i >> 3] >> (i & 7)) & 1) == target:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector not_equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* src = <uint8_t*> ptr.data
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int target = 1 if value else 0
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
                if ((src[i >> 3] >> (i & 7)) & 1) != target:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector equals_vector(self, BoolVector other):
        """Element-wise equality between two BoolVectors with null propagation."""
        cdef BoolVector _mat
        cdef size_t _mat_n
        cdef uint8_t* _mat_dst
        cdef Py_ssize_t _mat_nb
        if self._has_const:
            _mat_n = self.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if self._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif self._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return _mat.equals_vector(other)
        if other._has_const:
            _mat_n = other.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if other._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif other._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return self.equals_vector(_mat)
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>ptr1.data
        cdef uint8_t* b = <uint8_t*>ptr2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = ptr1.null_bitmap
        cdef uint8_t* b_null = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef bint a_valid, b_valid, all_valid
        cdef Py_ssize_t i
        memset(d, 0, nbytes)
        all_valid = (a_null == NULL and b_null == NULL)
        if not all_valid and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            if a_valid and b_valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if ((a[i >> 3] >> (i & 7)) & 1) == ((b[i >> 3] >> (i & 7)) & 1):
                    d[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector not_equals_vector(self, BoolVector other):
        """Element-wise inequality between two BoolVectors with null propagation."""
        cdef BoolVector _mat
        cdef size_t _mat_n
        cdef uint8_t* _mat_dst
        cdef Py_ssize_t _mat_nb
        if self._has_const:
            _mat_n = self.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if self._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif self._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return _mat.not_equals_vector(other)
        if other._has_const:
            _mat_n = other.ptr.length
            _mat = BoolVector(_mat_n)
            _mat_nb = (_mat_n + 7) >> 3
            _mat_dst = <uint8_t*>_mat.ptr.data
            memset(_mat_dst, 0, _mat_nb)
            if other._const_is_null:
                if _mat_nb != 0:
                    _mat.ptr.null_bitmap = <uint8_t*>malloc(_mat_nb)
                    if _mat.ptr.null_bitmap == NULL:
                        raise MemoryError()
                    memset(_mat.ptr.null_bitmap, 0, _mat_nb)
            elif other._const_value:
                memset(_mat_dst, 0xFF, _mat_nb)
                if _mat_n & 7:
                    _mat_dst[_mat_nb - 1] &= <uint8_t>((1 << (_mat_n & 7)) - 1)
            return self.not_equals_vector(_mat)
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>ptr1.data
        cdef uint8_t* b = <uint8_t*>ptr2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = ptr1.null_bitmap
        cdef uint8_t* b_null = ptr2.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef bint a_valid, b_valid, all_valid
        cdef Py_ssize_t i
        memset(d, 0, nbytes)
        all_valid = (a_null == NULL and b_null == NULL)
        if not all_valid and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            if a_valid and b_valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if ((a[i >> 3] >> (i & 7)) & 1) != ((b[i >> 3] >> (i & 7)) & 1):
                    d[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = out_null
        return out

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Compress bools to int64_t where True=1, False=0, null=NULL_FLAG"""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("BoolVector.compress: output buffer too small")
        if self._has_const:
            for i in range(n):
                out_buf[offset + i] = <int64_t>(-(1 << 63)) if self._const_is_null else (1 if self._const_value else 0)
            return
        cdef uint8_t* data = <uint8_t*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint8_t dbyte, nbyte
        cdef Py_ssize_t remaining, bit_i
        i = 0
        while i < n:
            remaining = n - i
            if remaining > 8:
                remaining = 8
            dbyte = data[i >> 3]
            nbyte = null_bitmap[i >> 3] if has_nulls else 0xFF
            for bit_i in range(remaining):
                if (nbyte >> bit_i) & 1:
                    out_buf[offset + i + bit_i] = 1 if (dbyte >> bit_i) & 1 else 0
                else:
                    out_buf[offset + i + bit_i] = <int64_t>(-(1 << 63))
            i += remaining

    cpdef int8_t any(self):
        if self._has_const:
            return 0 if self._const_is_null else <int8_t>(1 if self._const_value else 0)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t nbytes = (ptr.length + 7) >> 3
        cdef Py_ssize_t i
        for i in range(nbytes):
            if (<uint8_t*>ptr.data)[i] != 0:
                return 1
        return 0

    cpdef int8_t all(self):
        if self._has_const:
            # all-null: no non-null True values, treat as vacuously true (SQL semantics)
            return 1 if self._const_is_null else <int8_t>(1 if self._const_value else 0)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        for i in range(n):
            if (((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1) == 0:
                return 0
        return 1

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf
        cdef uint8_t byte, bit
        buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
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

    @property
    def null_count(self):
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
            return 1  # single bool value
        # Bit-packed: 1 bit per element
        data_bytes = (n + 7) >> 3
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append(bool(self._const_value))
            return out
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            out.append(bool(val))
        return out

    cpdef bytes to_byte_array(self):
        """Export mask as bytes without intermediate Python list.

        Returns bytearray where each element is 1 (True/valid) or 0 (False/invalid).
        Nulls are treated as 0.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef bytearray out = bytearray(n)
        cdef uint8_t* data = <uint8_t*>ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef uint8_t byte, bit, val, fill_val

        if self._has_const:
            fill_val = 0 if self._const_is_null else (1 if self._const_value else 0)
            for i in range(n):
                out[i] = fill_val
            return bytes(out)

        for i in range(n):
            if null_bitmap != NULL:
                byte = null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out[i] = 0
                    continue
            val = (data[i >> 3] >> (i & 7)) & 1
            out[i] = 1 if val else 0
        return bytes(out)

    cpdef int64_t min(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            # For bool: min(true) = true, min(false) = false
            return <int64_t>self._const_value

        cdef uint8_t byte, bit, val
        cdef bint found = False

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            found = True
            # If we find false (0), that's the minimum
            if not val:
                return <int64_t>0
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        # Check remaining values for false
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            if not val:
                return <int64_t>0

        # All non-null values are true
        return <int64_t>1

    cpdef int64_t max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return <int64_t>self._const_value

        cdef uint8_t byte, bit, val
        cdef bint found = False

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            found = True
            # If we find true (1), that's the maximum
            if val:
                return <int64_t>1
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        # Check remaining values for true
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            if val:
                return <int64_t>1

        # All non-null values are false
        return <int64_t>0

    cpdef int64_t sum(self):
        # sum(bool) = count of true values
        if self._has_const:
            if self._const_is_null:
                return 0
            return <int64_t>(self.ptr.length * self._const_value)

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        cdef uint8_t val

        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i, 0):
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            if val:
                total += 1
        return total

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare bool values at two indices. Returns -1 (left < right), 0 (equal), or 1 (left > right)."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t left_val, right_val
        cdef bint left_is_null, right_is_null

        # Check nulls
        left_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, left_idx, 0)
        right_is_null = ptr.null_bitmap != NULL and not _bitmap_is_valid(ptr.null_bitmap, right_idx, 0)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal for comparison purposes

        # Extract bit values
        left_val = ((<uint8_t*>ptr.data)[left_idx >> 3] >> (left_idx & 7)) & 1
        right_val = ((<uint8_t*>ptr.data)[right_idx >> 3] >> (right_idx & 7)) & 1

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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef uint64_t value
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef Py_ssize_t idx = 0
        cdef uint8_t byte, bit
        cdef uint64_t* dst
        cdef uint8_t* values
        cdef bint has_nulls
        cdef uint64_t[BOOL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if self._has_const:
            value = NULL_HASH if self._const_is_null else (TRUE_HASH if self._const_value else FALSE_HASH)
            for j in range(BOOL_HASH_CHUNK):
                scratch[j] = value
            if n > 0:
                dst = &out_buf[offset]
                i = 0
                while i < n:
                    block = n - i
                    if block > BOOL_HASH_CHUNK:
                        block = BOOL_HASH_CHUNK
                    simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                    i += block
            return
        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("BoolVector.hash_into: output buffer too small")

        dst = &out_buf[offset]
        values = <uint8_t*> ptr.data
        has_nulls = ptr.null_bitmap != NULL

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                for j in range(block):
                    idx = i + j
                    if (values[idx >> 3] >> (idx & 7)) & 1:
                        scratch[j] = TRUE_HASH
                    else:
                        scratch[j] = FALSE_HASH
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
            return

        i = 0
        while i < n:
            block = n - i
            if block > BOOL_HASH_CHUNK:
                block = BOOL_HASH_CHUNK
            for j in range(block):
                idx = i + j
                if (ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1:
                    scratch[j] = TRUE_HASH if (values[idx >> 3] >> (idx & 7)) & 1 else FALSE_HASH
                else:
                    scratch[j] = NULL_HASH
            simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, block, j, idx
        cdef uint64_t value
        cdef uint64_t[BOOL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint8_t* values
        cdef bint has_nulls

        if self._has_const:
            value = NULL_HASH if self._const_is_null else (TRUE_HASH if self._const_value else FALSE_HASH)
            for i in range(BOOL_HASH_CHUNK):
                scratch[i] = value
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        if n == 0:
            return 0

        values = <uint8_t*> ptr.data
        has_nulls = ptr.null_bitmap != NULL

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                for j in range(block):
                    idx = i + j
                    if (values[idx >> 3] >> (idx & 7)) & 1:
                        scratch[j] = TRUE_HASH
                    else:
                        scratch[j] = FALSE_HASH
                simd_mix_hash(out + i, scratch_ptr, <size_t> block)
                i += block
            return 0

        i = 0
        while i < n:
            block = n - i
            if block > BOOL_HASH_CHUNK:
                block = BOOL_HASH_CHUNK
            for j in range(block):
                idx = i + j
                if (ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1:
                    scratch[j] = TRUE_HASH if (values[idx >> 3] >> (idx & 7)) & 1 else FALSE_HASH
                else:
                    scratch[j] = NULL_HASH
            simd_mix_hash(out + i, scratch_ptr, <size_t> block)
            i += block
        return 0

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        if self._has_const:
            return f"<BoolVector len={buf_length(self.ptr)} values={[None if self._const_is_null else bool(self._const_value)] * min(<Py_ssize_t>buf_length(self.ptr), 10)}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        for i in range(k):
            vals.append(bool(((<uint8_t*>self.ptr.data)[i >> 3] >> (i & 7)) & 1))
        return f"<BoolVector len={buf_length(self.ptr)} values={vals}>"


# ---------------------------------------------------------------------------
# nogil bitmap accessors and raw kernel wrappers — Stage 2 of the nogil VM plan.
#
# These operate on pre-allocated caller-owned uint8_t* buffers. No Python
# objects are created or destroyed. The caller (Stage 3 pre-pass) allocates
# all buffers; these functions only write into them.
#
# Return convention: 0 = all results valid (dest_null contents irrelevant),
# 1 = at least one null result (caller must use dest_null).
# ---------------------------------------------------------------------------

cdef bint c_get_bitmap_ptrs(
    BoolVector vec,
    uint8_t** data_out,
    uint8_t** null_out,
) noexcept nogil:
    """Return raw data/null pointers for a dense BoolVector without the GIL.

    Returns 1 if the vector is not dense (const/RLE encoded) — caller must
    materialize before entering the nogil zone.  Returns 0 on success.
    """
    if vec._has_const or vec._rle_buffer != NULL:
        return 1
    data_out[0] = <uint8_t*>vec.ptr.data
    null_out[0] = vec.ptr.null_bitmap
    return 0


cdef bint c_and_bitmap(
    uint8_t* dest,
    const uint8_t* a,
    uint8_t* a_null,
    const uint8_t* b,
    uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes,
    Py_ssize_t n,
) noexcept nogil:
    """Element-wise AND on pre-allocated uint8_t bitmaps with SQL 3VL null semantics.

    dest and dest_null must be pre-allocated to nbytes bytes and zero-initialised.
    Returns 0 if all results are valid, 1 if dest_null contains meaningful bits.
    """
    cdef Py_ssize_t i
    cdef uint8_t a_valid, b_valid, a_val, b_val
    cdef bint result_true, valid
    cdef bint any_null = False

    if a_null == NULL and b_null == NULL:
        simd_and_mask(dest, a, b, <size_t>nbytes)
        return 0

    for i in range(n):
        a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
        b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
        a_val = (a[i >> 3] >> (i & 7)) & 1
        b_val = (b[i >> 3] >> (i & 7)) & 1

        valid = False
        result_true = False

        if (a_valid and not a_val) or (b_valid and not b_val):
            valid = True
            result_true = False
        elif a_valid and b_valid:
            valid = True
            result_true = a_val and b_val

        if valid:
            dest_null[i >> 3] |= (1 << (i & 7))
            if result_true:
                dest[i >> 3] |= (1 << (i & 7))
        else:
            any_null = True

    return 1 if any_null else 0


cdef bint c_or_bitmap(
    uint8_t* dest,
    const uint8_t* a,
    uint8_t* a_null,
    const uint8_t* b,
    uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes,
    Py_ssize_t n,
) noexcept nogil:
    """Element-wise OR on pre-allocated uint8_t bitmaps with SQL 3VL null semantics.

    dest and dest_null must be pre-allocated to nbytes bytes and zero-initialised.
    Returns 0 if all results are valid, 1 if dest_null contains meaningful bits.
    """
    cdef Py_ssize_t i
    cdef uint8_t a_valid, b_valid, a_val, b_val
    cdef bint result_true, valid
    cdef bint any_null = False

    if a_null == NULL and b_null == NULL:
        simd_or_mask(dest, a, b, <size_t>nbytes)
        return 0

    for i in range(n):
        a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
        b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
        a_val = (a[i >> 3] >> (i & 7)) & 1
        b_val = (b[i >> 3] >> (i & 7)) & 1

        valid = False
        result_true = False

        if (a_valid and a_val) or (b_valid and b_val):
            valid = True
            result_true = True
        elif a_valid and b_valid:
            valid = True
            result_true = False

        if valid:
            dest_null[i >> 3] |= (1 << (i & 7))
            if result_true:
                dest[i >> 3] |= (1 << (i & 7))
        else:
            any_null = True

    return 1 if any_null else 0


cdef bint c_xor_bitmap(
    uint8_t* dest,
    const uint8_t* a,
    uint8_t* a_null,
    const uint8_t* b,
    uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes,
    Py_ssize_t n,
) noexcept nogil:
    """Element-wise XOR on pre-allocated uint8_t bitmaps with SQL 3VL null semantics.

    dest and dest_null must be pre-allocated to nbytes bytes and zero-initialised.
    Returns 0 if all results are valid, 1 if dest_null contains meaningful bits.
    """
    cdef Py_ssize_t i
    cdef uint8_t a_valid, b_valid
    cdef bint any_null = False

    if a_null == NULL and b_null == NULL:
        simd_xor_mask(dest, a, b, <size_t>nbytes)
        return 0

    for i in range(n):
        a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
        b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
        if a_valid and b_valid:
            dest_null[i >> 3] |= (1 << (i & 7))
            if (((a[i >> 3] >> (i & 7)) & 1) != ((b[i >> 3] >> (i & 7)) & 1)):
                dest[i >> 3] |= (1 << (i & 7))
        else:
            any_null = True

    return 1 if any_null else 0


cdef bint c_not_bitmap(
    uint8_t* dest,
    const uint8_t* src,
    uint8_t* src_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes,
    Py_ssize_t n,
) noexcept nogil:
    """Element-wise NOT on a pre-allocated uint8_t bitmap with SQL 3VL null semantics.

    dest and dest_null must be pre-allocated to nbytes bytes and zero-initialised.
    Returns 0 if all results are valid, 1 if dest_null contains meaningful bits.
    """
    cdef Py_ssize_t i
    cdef bint any_null = False

    if src_null == NULL:
        simd_not_mask(dest, src, <size_t>nbytes)
        if (n & 7) != 0:
            dest[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
        return 0

    for i in range(n):
        if (src_null[i >> 3] >> (i & 7)) & 1:
            dest_null[i >> 3] |= (1 << (i & 7))
            if ((src[i >> 3] >> (i & 7)) & 1) == 0:
                dest[i >> 3] |= (1 << (i & 7))
        else:
            any_null = True

    return 1 if any_null else 0


cdef BoolVector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
):
    """Wrap externally-malloc'd bit-packed data + null_bitmap into a BoolVector.

    `data` is a bit-packed payload of ceil(length/8) bytes; `length` is the
    logical row count. Ownership transfers to the Vector — both pointers must
    come from `malloc` (or be NULL).
    """
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1   # logical itemsize; storage is bit-packed
    vec.ptr.length = length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    _refresh_unified_bool(vec)
    return vec


cdef BoolVector from_arrow(object array):
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1
    vec.ptr.length = <size_t> len(array)

    # Data buffer handling
    cdef Py_ssize_t nbytes
    cdef uint8_t* src_data
    cdef uint8_t* dst_data
    cdef object new_data_bytes
    cdef Py_ssize_t i

    if offset % 8 == 0:
        # Aligned offset: zero-copy
        vec.ptr.data = <void*> (base_ptr + (offset >> 3))
    else:
        # Unaligned offset: must copy and shift data
        nbytes = (len(array) + 7) // 8
        new_data_bytes = PyBytes_FromStringAndSize(NULL, nbytes)
        dst_data = <uint8_t*> PyBytes_AS_STRING(new_data_bytes)
        memset(dst_data, 0, nbytes)

        src_data = <uint8_t*> base_ptr

        # Copy bits shifting them
        for i in range(len(array)):
            if (src_data[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                dst_data[i >> 3] |= (1 << (i & 7))

        vec.ptr.data = <void*> dst_data
        vec._arrow_data_buf = new_data_bytes

    # Null bitmap handling
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef object new_bitmap_bytes

    if bufs[0] is not None:
        nb_addr = bufs[0].address

        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            # Unaligned offset: must copy and shift nulls
            nbytes = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nbytes)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nbytes)

            src_bitmap = <uint8_t*> nb_addr

            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    _refresh_unified_bool(vec)
    return vec


cdef BoolVector from_sequence(uint8_t[::1] data):
    """
    Create BoolVector from a typed uint8 memoryview (zero-copy, bit-packed).

    Args:
        data: uint8_t[::1] memoryview (C-contiguous, bit-packed: 8 bools per byte)

    Returns:
        BoolVector wrapping the memoryview data

    Note:
        Input data should be bit-packed (8 boolean values per byte).
        The length will be inferred as data.shape[0] * 8.
    """
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1
    # Bit-packed: 8 booleans per byte
    vec.ptr.length = <size_t> (data.shape[0] * 8)
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    _refresh_unified_bool(vec)
    return vec


cdef BoolVector bool_vector_from_bits(
        uint8_t* value_bits,
        uint8_t* valid_bits,
        Py_ssize_t n):
    """
    Construct a BoolVector directly from pre-packed bit buffers (Arrow layout).

    This is intended for use by native decoders (e.g. rugo) that have already
    produced Arrow-format bit buffers and need a zero-extra-copy Draken vector.

    Args:
        value_bits: 1-bit-per-row packed boolean values (Arrow PLAIN / LSB-first).
                    Must have at least ceil(n / 8) bytes.
        valid_bits:  Arrow validity bitmap (1 = valid, 0 = null), same layout.
                    Pass NULL to indicate all rows are valid (no null bitmap allocated).
        n:          Number of logical rows.

    Returns:
        BoolVector owning copies of both buffers.
    """
    cdef Py_ssize_t nb = (n + 7) >> 3
    cdef uint8_t* bm
    cdef BoolVector vec = BoolVector(<size_t>n)
    # BoolVector(n) already allocates ptr.data via malloc; copy values in.
    memcpy(<uint8_t*>vec.ptr.data, value_bits, nb)
    if valid_bits != NULL:
        bm = <uint8_t*> malloc(nb)
        if bm == NULL:
            raise MemoryError()
        memcpy(bm, valid_bits, nb)
        vec.ptr.null_bitmap = bm
    else:
        vec.ptr.null_bitmap = NULL
    _refresh_unified_bool(vec)
    return vec


cdef BoolVector from_rle_builder(
    uint8_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    uint8_t* null_bitmap=NULL,
):
    """Create an RLE-encoded BoolVector from raw C arrays.

    Run values are uint8_t (0 = False, 1 = True).  This function copies the
    run data into fresh malloc'd arrays owned by the vector.

    Args:
        run_values:  Pointer to uint8_t values array (num_runs entries).
        run_lengths: Pointer to int32_t run lengths (num_runs entries).
        num_runs:    Number of runs.
        null_bitmap: Optional logical-row null bitmap (NULL = no nulls).

    Returns:
        BoolVector with DRAKEN_ENCODING_RLE encoding.
    """
    import sys as _sys
    _draken = _sys.modules.get('draken')
    if _draken is not None and _draken._RLE_FORBIDDEN:
        raise RuntimeError("RLE vector construction is forbidden (draken._RLE_FORBIDDEN=True)")
    cdef BoolVector vec = BoolVector(0)  # allocates ptr with data=NULL (length 0)
    cdef size_t total_length = 0
    cdef size_t i
    cdef DrakenRLEBuffer* rle
    cdef uint8_t* vals_copy
    cdef int32_t* lens_copy
    cdef size_t null_bytes
    cdef uint8_t* null_copy

    # Compute total logical length
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

    vals_copy = <uint8_t*>malloc(num_runs * sizeof(uint8_t))
    lens_copy = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if vals_copy == NULL or lens_copy == NULL:
        free(rle)
        if vals_copy != NULL:
            free(vals_copy)
        if lens_copy != NULL:
            free(lens_copy)
        raise MemoryError()

    memcpy(vals_copy, run_values, num_runs * sizeof(uint8_t))
    memcpy(lens_copy, run_lengths, num_runs * sizeof(int32_t))

    rle.run_values = <void*>vals_copy
    rle.run_lengths = lens_copy
    rle.num_runs = num_runs
    rle.length = total_length
    rle.type = DRAKEN_BOOL

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
