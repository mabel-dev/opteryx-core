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

from libc.stdint cimport int32_t, int8_t, intptr_t, uint32_t, uint64_t, uint8_t, int64_t
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)
    void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n)

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.core.buffers cimport DRAKEN_BOOL
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_length, free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount

cdef const uint64_t TRUE_HASH = <uint64_t>0x4f112caa54efa882ULL
cdef const uint64_t FALSE_HASH = <uint64_t>0xc2fd8b2343f83ce7ULL
cdef uint8_t _CONST_NULL_BYTE = 0

DEF BOOL_HASH_CHUNK = 1024




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
        cdef BoolVector vec = BoolVector(1)
        cdef uint8_t val = 0 if (is_null or value is None) else <uint8_t>(1 if bool(value) else 0)
        (<uint8_t*>vec.ptr.data)[0] = val
        vec.ptr.length = <size_t>length
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_BOOL,
            &_CONST_NULL_BYTE if is_null else NULL)
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        cdef size_t nbytes

        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_BOOL, NULL)
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
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_BOOL, NULL)

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    cdef void _set_null_bitmap(self, uint8_t* bm) noexcept:
        self.ptr.null_bitmap = bm
        self._unified_view.validity = bm

    # Properties
    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def dtype(self):
        return DRAKEN_BOOL

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenVector* uv = self.unified()
        cdef uint32_t sel
        if i < 0 or i >= <Py_ssize_t>uv.length:
            raise IndexError("Index out of bounds")
        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None
        sel = uv.selection[i]
        return bool(((<uint8_t*>uv.data)[sel >> 3] >> (sel & 7)) & 1)

    # -------- Interop --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.bool_())
            return pa.array([bool((<uint8_t*>uv.data)[0])] * self.ptr.length, type=pa.bool_())

        cdef size_t nbytes = (self.ptr.length + 7) >> 3
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.bool_(), self.ptr.length, buffers)

    cpdef BoolVector and_vector(self, BoolVector other):
        """Element-wise AND between two BoolVector instances with SQL null semantics."""
        cdef DrakenVector* uv1 = self.unified()
        cdef DrakenVector* uv2 = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv1.length
        if n != <Py_ssize_t>uv2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>uv1.data
        cdef uint8_t* b = <uint8_t*>uv2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = uv1.validity
        cdef uint8_t* b_null = uv2.validity
        cdef uint8_t* out_null = NULL
        cdef uint8_t a_valid, b_valid, a_val, b_val
        cdef bint valid, result_true, all_valid = True
        cdef Py_ssize_t i
        cdef uint32_t a_sel, b_sel
        memset(d, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            a_sel = uv1.selection[i]
            b_sel = uv2.selection[i]
            a_val = (a[a_sel >> 3] >> (a_sel & 7)) & 1
            b_val = (b[b_sel >> 3] >> (b_sel & 7)) & 1

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
        cdef DrakenVector* uv1 = self.unified()
        cdef DrakenVector* uv2 = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv1.length
        if n != <Py_ssize_t>uv2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>uv1.data
        cdef uint8_t* b = <uint8_t*>uv2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = uv1.validity
        cdef uint8_t* b_null = uv2.validity
        cdef uint8_t* out_null = NULL
        cdef uint8_t a_valid, b_valid, a_val, b_val
        cdef bint valid, result_true, all_valid = True
        cdef Py_ssize_t i
        cdef uint32_t a_sel, b_sel
        memset(d, 0, nbytes)
        if nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
        for i in range(n):
            a_valid = 1 if a_null == NULL else ((a_null[i >> 3] >> (i & 7)) & 1)
            b_valid = 1 if b_null == NULL else ((b_null[i >> 3] >> (i & 7)) & 1)
            a_sel = uv1.selection[i]
            b_sel = uv2.selection[i]
            a_val = (a[a_sel >> 3] >> (a_sel & 7)) & 1
            b_val = (b[b_sel >> 3] >> (b_sel & 7)) & 1

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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* src = <uint8_t*>uv.data
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef uint32_t sel_idx
        cdef Py_ssize_t out_nbytes = (n + 7) >> 3

        memset(dst, 0, out_nbytes)

        if src_null != NULL and out_nbytes > 0:
            out_null = <uint8_t*>malloc(<size_t>out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, <size_t>out_nbytes)

        for i in range(n):
            src_idx = indices[i]
            if src_null != NULL and not ((src_null[src_idx >> 3] >> (src_idx & 7)) & 1):
                continue
            if out_null != NULL:
                out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
            sel_idx = uv.selection[<Py_ssize_t>src_idx]
            if (src[sel_idx >> 3] >> (sel_idx & 7)) & 1:
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))

        out.ptr.null_bitmap = out_null
        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_BOOL, out.ptr.null_bitmap)
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
        cdef DrakenVector* uv1 = self.unified()
        cdef DrakenVector* uv2 = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv1.length
        if n != <Py_ssize_t>uv2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>uv1.data
        cdef uint8_t* b = <uint8_t*>uv2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = uv1.validity
        cdef uint8_t* b_null = uv2.validity
        cdef uint8_t* out_null = NULL
        cdef bint a_valid, b_valid, all_valid
        cdef Py_ssize_t i
        cdef uint32_t a_sel, b_sel
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
                a_sel = uv1.selection[i]
                b_sel = uv2.selection[i]
                if ((a[a_sel >> 3] >> (a_sel & 7)) & 1) == ((b[b_sel >> 3] >> (b_sel & 7)) & 1):
                    d[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector not_equals_vector(self, BoolVector other):
        """Element-wise inequality between two BoolVectors with null propagation."""
        cdef DrakenVector* uv1 = self.unified()
        cdef DrakenVector* uv2 = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv1.length
        if n != <Py_ssize_t>uv2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*>uv1.data
        cdef uint8_t* b = <uint8_t*>uv2.data
        cdef uint8_t* d = <uint8_t*>out.ptr.data
        cdef uint8_t* a_null = uv1.validity
        cdef uint8_t* b_null = uv2.validity
        cdef uint8_t* out_null = NULL
        cdef bint a_valid, b_valid, all_valid
        cdef Py_ssize_t i
        cdef uint32_t a_sel, b_sel
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
                a_sel = uv1.selection[i]
                b_sel = uv2.selection[i]
                if ((a[a_sel >> 3] >> (a_sel & 7)) & 1) != ((b[b_sel >> 3] >> (b_sel & 7)) & 1):
                    d[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = out_null
        return out

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Compress bools to int64_t where True=1, False=0, null=NULL_FLAG"""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i
        cdef uint32_t sel_idx
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("BoolVector.compress: output buffer too small")

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                out_buf[offset + i] = <int64_t>(-(1 << 63))
            else:
                sel_idx = uv.selection[i]
                out_buf[offset + i] = 1 if (data[sel_idx >> 3] >> (sel_idx & 7)) & 1 else 0

    cpdef int8_t any(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            if (data[sel >> 3] >> (sel & 7)) & 1:
                return 1
        return 0

    cpdef int8_t all(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            if not ((data[sel >> 3] >> (sel & 7)) & 1):
                return 0
        return 1

    cpdef int8_t[::1] is_null(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf
        cdef uint8_t* null_bitmap = uv.validity
        buf = <int8_t*> PyMem_Malloc(n)
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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, bm_bytes
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            return 1  # single bool value
        # Bit-packed: 1 bit per element
        data_bytes = (n + 7) >> 3
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef list out = []
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                out.append(None)
                continue
            sel = uv.selection[i]
            out.append(bool((data[sel >> 3] >> (sel & 7)) & 1))
        return out

    cpdef bytes to_byte_array(self):
        """Export mask as bytes without intermediate Python list.

        Returns bytearray where each element is 1 (True/valid) or 0 (False/invalid).
        Nulls are treated as 0.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef bytearray out = bytearray(n)
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                out[i] = 0
                continue
            sel = uv.selection[i]
            out[i] = 1 if (data[sel >> 3] >> (sel & 7)) & 1 else 0
        return bytes(out)

    cpdef int64_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        cdef uint8_t val
        cdef bint found = False

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            val = (data[sel >> 3] >> (sel & 7)) & 1
            found = True
            # If we find false (0), that's the minimum possible
            if not val:
                return <int64_t>0
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        # First non-null was true; check the rest for false
        for i in range(i + 1, n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            val = (data[sel >> 3] >> (sel & 7)) & 1
            if not val:
                return <int64_t>0

        # All non-null values are true
        return <int64_t>1

    cpdef int64_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        cdef uint8_t val
        cdef bint found = False

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            val = (data[sel >> 3] >> (sel & 7)) & 1
            found = True
            # If we find true (1), that's the maximum possible
            if val:
                return <int64_t>1
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        # First non-null was false; check the rest for true
        for i in range(i + 1, n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            val = (data[sel >> 3] >> (sel & 7)) & 1
            if val:
                return <int64_t>1

        # All non-null values are false
        return <int64_t>0

    cpdef int64_t sum(self):
        # sum(bool) = count of true values
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef int64_t total = 0
        cdef uint32_t sel

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                continue
            sel = uv.selection[i]
            if (data[sel >> 3] >> (sel & 7)) & 1:
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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, block, j
        cdef uint32_t sel_idx
        cdef uint64_t* dst
        cdef uint8_t* values = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint64_t[BOOL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("BoolVector.hash_into: output buffer too small")

        dst = &out_buf[offset]

        i = 0
        while i < n:
            block = n - i
            if block > BOOL_HASH_CHUNK:
                block = BOOL_HASH_CHUNK
            for j in range(block):
                if null_bitmap != NULL and not ((null_bitmap[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    sel_idx = uv.selection[i + j]
                    scratch[j] = TRUE_HASH if (values[sel_idx >> 3] >> (sel_idx & 7)) & 1 else FALSE_HASH
            simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenVector* uv = &self._unified_view
        cdef Py_ssize_t i, block, j
        cdef uint32_t sel_idx
        cdef uint64_t[BOOL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch
        cdef uint8_t* values = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity

        if n == 0:
            return 0

        if null_bitmap != NULL:
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                for j in range(block):
                    if (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1:
                        sel_idx = uv.selection[i + j]
                        scratch[j] = TRUE_HASH if (values[sel_idx >> 3] >> (sel_idx & 7)) & 1 else FALSE_HASH
                    else:
                        scratch[j] = NULL_HASH
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        else:
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                for j in range(block):
                    sel_idx = uv.selection[i + j]
                    scratch[j] = TRUE_HASH if (values[sel_idx >> 3] >> (sel_idx & 7)) & 1 else FALSE_HASH
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        return 0

    def __str__(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, k = min(<Py_ssize_t>uv.length, 10)
        cdef list vals = []
        cdef uint8_t* data = <uint8_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint32_t sel
        for i in range(k):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                vals.append(None)
            else:
                sel = uv.selection[i]
                vals.append(bool((data[sel >> 3] >> (sel & 7)) & 1))
        return f"<BoolVector len={uv.length} values={vals}>"


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
    """Return raw data/null pointers for a BoolVector without the GIL.

    Callers must access bits via the DrakenVector selection array:
        sel = vec._unified_view.selection[i]
        bit = (data[sel >> 3] >> (sel & 7)) & 1
    Returns 0 always; kept for ABI compatibility.
    """
    data_out[0] = <uint8_t*>vec._unified_view.data
    null_out[0] = vec._unified_view.validity
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
    vec._unified_view = draken_vector_from_dense(
        data, <uint32_t>length, DRAKEN_BOOL, null_bitmap)
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
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>(data.shape[0] * 8), DRAKEN_BOOL, NULL)
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
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>n, DRAKEN_BOOL, vec.ptr.null_bitmap)
    return vec


