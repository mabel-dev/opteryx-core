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
Integer64Vector: Cython implementation of a fixed-width int64 column vector for Draken.

This module provides:
- The Integer64Vector class for efficient int64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for int64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant, draken_vector_from_dict
from draken.core.buffers cimport draken_zero_validity
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport (
    NULL_HASH,
    Vector,
    simd_mix_hash,
    simd_popcount,
)

from draken.vectors.bool_vector cimport BoolVector

cdef extern from "draken/vectors/_int64_compare.hpp" namespace "draken::int64_cmp" nogil:
    void bit_fill_range(uint8_t* dst, size_t start, size_t count)
    bint dispatch_compare_once(int op, int64_t a, int64_t b)
    void dispatch_scalar_nonnull(int op, const int64_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const int64_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const int64_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const int64_t* a, const int64_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const int64_t* a, const int64_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const int64_t* a, const int64_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const int64_t* a, const int64_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const int64_t* a, const int64_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

cdef extern from "draken/vectors/_int64_float64_compare.hpp" namespace "draken::int64_float64_cmp" nogil:
    void dispatch_i64_f64_vector_nonnull(int op, const int64_t* data_int, const double* data_float, uint8_t* dst, size_t n)
    void dispatch_i64_f64_vector_branchless(int op, const int64_t* data_int, const double* data_float, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_i64_f64_vector_branching(int op, const int64_t* data_int, const double* data_float, const uint8_t* src_null, uint8_t* dst, size_t n)

cdef extern from "draken/vectors/_int64_reductions.hpp" namespace "draken::int64_red" nogil:
    int64_t sum_nonnull(const int64_t* data, size_t n)
    int64_t sum_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n)
    int64_t sum_nullable_branching(const int64_t* data, const uint8_t* nulls, size_t n)
    int64_t min_nonnull(const int64_t* data, size_t n)
    int64_t max_nonnull(const int64_t* data, size_t n)
    size_t  min_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_min)
    size_t  max_nullable_branchless(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_max)
    size_t  min_nullable_branching(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_min)
    size_t  max_nullable_branching(const int64_t* data, const uint8_t* nulls, size_t n, int64_t* out_max)


cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000


cdef void _release_dict_storage(Integer64Vector vec) noexcept:
    """Free dict storage. Codes and dict-data live in separate owned buffers
    pointed at by _unified_view.selection and _unified_view.data; ptr.data
    remains the materialized dense buffer (freed by free_fixed_buffer)."""
    if vec._owns_selection:
        free(<void*>vec._unified_view.selection)
    vec._owns_selection = False
    if vec._owns_dict_data and vec._unified_view.data != NULL:
        free(vec._unified_view.data)
    vec._owns_dict_data = False


cdef void _attach_dictionary_storage(Integer64Vector vec, const int32_t[::1] codes, const int64_t[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    """Dict unique values stored in a SEPARATE owned buffer (_owns_dict_data);
    ptr.data is left as the materialized dense buffer (downstream iterators
    that read ptr.data per row_count continue to work). Codes go in
    _unified_view.selection (owned via _owns_selection)."""
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Py_ssize_t i
    cdef uint32_t* codes_ptr = NULL
    cdef int64_t* dict_data_ptr = NULL

    _release_dict_storage(vec)

    if row_count > 0:
        codes_ptr = <uint32_t*>malloc(row_count * sizeof(uint32_t))
        if codes_ptr == NULL:
            raise MemoryError()
        for i in range(row_count):
            codes_ptr[i] = <uint32_t>codes[i]

    if dict_size > 0:
        dict_data_ptr = <int64_t*>malloc(<size_t>dict_size * sizeof(int64_t))
        if dict_data_ptr == NULL:
            if codes_ptr != NULL: free(codes_ptr)
            raise MemoryError()
        memcpy(dict_data_ptr, <const void*>&dictionary[0], <size_t>dict_size * sizeof(int64_t))

    vec._owns_dict_data = (dict_data_ptr != NULL)
    vec._owns_selection = (codes_ptr != NULL)
    vec._unified_view = draken_vector_from_dict(
        <void*>dict_data_ptr, <uint32_t>dict_size,
        codes_ptr, <uint32_t>row_count,
        DRAKEN_INT64, vec.ptr.null_bitmap)

cdef class Integer64Vector(Vector):

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

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef Integer64Vector vec = Integer64Vector(1)  # allocate 1-element buffer for const value
        cdef int64_t val = 0 if (is_null or value is None) else <int64_t>int(value)
        (<int64_t*>vec.ptr.data)[0] = val
        vec.ptr.length = <size_t>length
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_INT64,
            <uint8_t*>draken_zero_validity(<uint32_t>length) if is_null else NULL)
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        self._owns_dict_data = False
        self._owns_selection = False
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_INT64, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_INT64, NULL)

    def __dealloc__(self):
        _release_dict_storage(self)
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
        return 8

    @property
    def dtype(self):
        return DRAKEN_INT64

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_value_type(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return None
        return self._unified_view.type

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_size(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return 0
        return self._unified_view.data_length

    # Producer-layer introspection only — not for dispatch.
    @property
    def code_width(self):
        return 4 if self._unified_view.data_length < self._unified_view.length else None

    @property
    def ordered(self):
        return False

    cdef object item_at(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        if i < 0 or i >= <Py_ssize_t>uv.length:
            raise IndexError("Index out of bounds")
        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None
        return (<int64_t*>uv.data)[uv.selection[i]]

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        return self.item_at(i)

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        cdef DrakenVector* _ta_uv = self.unified()
        import pyarrow as pa

        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize().to_arrow()

        if _ta_uv.data_length == 1:
            if _ta_uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.int64())
            return pa.array([(<int64_t*>_ta_uv.data)[0]] * self.ptr.length, type=pa.int64())

        cdef size_t nbytes = self.ptr.length * 8
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
                pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.int64(), self.ptr.length, buffers)

    cpdef Integer64Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef int32_t src_idx
        cdef Integer64Vector out
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t* dst
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t out_nbytes

        out = Integer64Vector(<size_t>n)
        dst = <int64_t*>out.ptr.data

        if src_null != NULL and n > 0:
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*>malloc(<size_t>out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, <size_t>out_nbytes)

        for i in range(n):
            src_idx = indices[i]
            if src_null != NULL and not ((src_null[src_idx >> 3] >> (src_idx & 7)) & 1):
                dst[i] = 0
            else:
                dst[i] = data[<Py_ssize_t>uv.selection[<Py_ssize_t>src_idx]]
                if out_null != NULL:
                    out_null[i >> 3] |= <uint8_t>(1 << (i & 7))

        out.ptr.null_bitmap = out_null
        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_INT64, out.ptr.null_bitmap)
        return out

    cpdef BoolVector _compare_scalar(self, int64_t value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* src_null = uv.validity
        cdef int64_t* gathered = NULL
        cdef int64_t* kdata
        cdef Py_ssize_t i
        cdef Py_ssize_t null_count

        if n == 0:
            out.ptr.null_bitmap = NULL
            return out

        memset(dst, 0, nbytes)

        # Constant shape: the single value's comparison is the same for every
        # row, so evaluate once. Result validity mirrors input validity.
        if uv.data_length == 1:
            if dispatch_compare_once(op, data[0], value):
                if src_null == NULL:
                    bit_fill_range(dst, 0, <size_t>n)
                else:
                    memcpy(dst, src_null, <size_t>nbytes)
            if src_null != NULL:
                out_null = <uint8_t*>malloc(<size_t>nbytes)
                if out_null == NULL:
                    raise MemoryError()
                memcpy(out_null, src_null, <size_t>nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    out_null[nbytes - 1] &= mask
                    dst[nbytes - 1] &= mask
                out.ptr.null_bitmap = out_null
            else:
                out.ptr.null_bitmap = NULL
            return out

        # Flat shape (selection is the identity): the kernel reads data in
        # logical order directly. Dict shape: gather data[selection[i]] into a
        # contiguous scratch buffer so the kernel sees logical order.
        if <Py_ssize_t>uv.data_length == n:
            kdata = data
        else:
            gathered = <int64_t*>malloc(<size_t>n * sizeof(int64_t))
            if gathered == NULL:
                raise MemoryError()
            for i in range(n):
                gathered[i] = data[<Py_ssize_t>uv.selection[i]]
            kdata = gathered

        if src_null == NULL:
            dispatch_scalar_nonnull(op, kdata, value, dst, <size_t>n)
            out.ptr.null_bitmap = NULL
        else:
            out_null = <uint8_t*>malloc(<size_t>nbytes)
            if out_null == NULL:
                if gathered != NULL:
                    free(gathered)
                raise MemoryError()
            memcpy(out_null, src_null, <size_t>nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
            null_count = n - <Py_ssize_t>simd_popcount(src_null, <size_t>nbytes)
            if null_count * 10 > n * 7:
                dispatch_scalar_branching(op, kdata, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless(op, kdata, value, src_null, dst, <size_t>n)

        if gathered != NULL:
            free(gathered)
        return out

    cpdef BoolVector _compare_vector(self, Integer64Vector other, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef int64_t* adata = <int64_t*>uv.data
        cdef int64_t* bdata = <int64_t*>ouv.data
        cdef uint8_t* anull = uv.validity
        cdef uint8_t* bnull = ouv.validity
        cdef uint8_t* nside
        cdef int64_t* a_gathered = NULL
        cdef int64_t* b_gathered = NULL
        cdef int64_t* a
        cdef int64_t* b
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t i, na, nb
        cdef bint branching

        if n != <Py_ssize_t>ouv.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if n == 0:
            out.ptr.null_bitmap = NULL
            return out
        memset(dst, 0, nbytes)

        # Each operand must be contiguous in logical order for the kernel.
        # Flat reads data directly; constant/dict gather through selection
        # (constant broadcasts data[0] since its selection is all zeros).
        if <Py_ssize_t>uv.data_length == n:
            a = adata
        else:
            a_gathered = <int64_t*>malloc(<size_t>n * sizeof(int64_t))
            if a_gathered == NULL:
                raise MemoryError()
            for i in range(n):
                a_gathered[i] = adata[<Py_ssize_t>uv.selection[i]]
            a = a_gathered
        if <Py_ssize_t>ouv.data_length == n:
            b = bdata
        else:
            b_gathered = <int64_t*>malloc(<size_t>n * sizeof(int64_t))
            if b_gathered == NULL:
                if a_gathered != NULL:
                    free(a_gathered)
                raise MemoryError()
            for i in range(n):
                b_gathered[i] = bdata[<Py_ssize_t>ouv.selection[i]]
            b = b_gathered

        if anull == NULL and bnull == NULL:
            dispatch_vector_nonnull(op, a, b, dst, <size_t>n)
            out.ptr.null_bitmap = NULL
        else:
            out_null = <uint8_t*>malloc(<size_t>nbytes)
            if out_null == NULL:
                if a_gathered != NULL:
                    free(a_gathered)
                if b_gathered != NULL:
                    free(b_gathered)
                raise MemoryError()
            memset(out_null, 0, <size_t>nbytes)
            if anull != NULL and bnull != NULL:
                na = n - <Py_ssize_t>simd_popcount(anull, <size_t>nbytes)
                nb = n - <Py_ssize_t>simd_popcount(bnull, <size_t>nbytes)
                branching = (na if na > nb else nb) * 10 > n * 7
                if branching:
                    dispatch_vector_both_null_branching(op, a, b, anull, bnull, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_both_null_branchless(op, a, b, anull, bnull, dst, out_null, <size_t>n)
            else:
                nside = anull if anull != NULL else bnull
                na = n - <Py_ssize_t>simd_popcount(nside, <size_t>nbytes)
                branching = na * 10 > n * 7
                if branching:
                    dispatch_vector_one_null_branching(op, a, b, nside, dst, out_null, <size_t>n)
                else:
                    dispatch_vector_one_null_branchless(op, a, b, nside, dst, out_null, <size_t>n)
            out.ptr.null_bitmap = out_null

        if a_gathered != NULL:
            free(a_gathered)
        if b_gathered != NULL:
            free(b_gathered)
        return out

    cpdef BoolVector _compare_float64_vector(self, Float64Vector other, int op):
        """Compare Integer64Vector with Float64Vector via the cross-type kernel.

        int64 values are widened to double inside the kernel. Result is NULL
        where either side is NULL.
        """
        if other is None:
            raise TypeError("Expected Float64Vector, got None")
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef int64_t* adata = <int64_t*>uv.data
        cdef double* bdata = <double*>ouv.data
        cdef uint8_t* anull = uv.validity
        cdef uint8_t* bnull = ouv.validity
        cdef int64_t* a_gathered = NULL
        cdef double* b_gathered = NULL
        cdef int64_t* a
        cdef double* b
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* combined = NULL
        cdef uint8_t abyte, bbyte
        cdef Py_ssize_t i, b_idx, null_count
        cdef uint8_t mask

        if n != <Py_ssize_t>ouv.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if n == 0:
            out.ptr.null_bitmap = NULL
            return out
        memset(dst, 0, nbytes)

        if <Py_ssize_t>uv.data_length == n:
            a = adata
        else:
            a_gathered = <int64_t*>malloc(<size_t>n * sizeof(int64_t))
            if a_gathered == NULL:
                raise MemoryError()
            for i in range(n):
                a_gathered[i] = adata[<Py_ssize_t>uv.selection[i]]
            a = a_gathered
        if <Py_ssize_t>ouv.data_length == n:
            b = bdata
        else:
            b_gathered = <double*>malloc(<size_t>n * sizeof(double))
            if b_gathered == NULL:
                if a_gathered != NULL:
                    free(a_gathered)
                raise MemoryError()
            for i in range(n):
                b_gathered[i] = bdata[<Py_ssize_t>ouv.selection[i]]
            b = b_gathered

        if anull == NULL and bnull == NULL:
            dispatch_i64_f64_vector_nonnull(op, a, b, dst, <size_t>n)
            out.ptr.null_bitmap = NULL
        else:
            combined = <uint8_t*>malloc(<size_t>nbytes)
            if combined == NULL:
                if a_gathered != NULL:
                    free(a_gathered)
                if b_gathered != NULL:
                    free(b_gathered)
                raise MemoryError()
            for b_idx in range(nbytes):
                abyte = anull[b_idx] if anull != NULL else <uint8_t>0xFF
                bbyte = bnull[b_idx] if bnull != NULL else <uint8_t>0xFF
                combined[b_idx] = <uint8_t>(abyte & bbyte)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                combined[nbytes - 1] &= mask
            null_count = n - <Py_ssize_t>simd_popcount(combined, <size_t>nbytes)
            if null_count * 10 > n * 7:
                dispatch_i64_f64_vector_branching(op, a, b, combined, dst, <size_t>n)
            else:
                dispatch_i64_f64_vector_branchless(op, a, b, combined, dst, <size_t>n)
            out.ptr.null_bitmap = combined

        if a_gathered != NULL:
            free(a_gathered)
        if b_gathered != NULL:
            free(b_gathered)
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Integer64Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Integer64Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Integer64Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Integer64Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Integer64Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Integer64Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector equals_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 0)

    cpdef BoolVector not_equals_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 1)

    cpdef BoolVector greater_than_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 2)

    cpdef BoolVector greater_than_or_equals_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 3)

    cpdef BoolVector less_than_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 4)

    cpdef BoolVector less_than_or_equals_float64_vector(self, Float64Vector other):
        """Compare Integer64Vector with Float64Vector using native cross-type comparison."""
        return self._compare_float64_vector(other, 5)

    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef int64_t* data
        cdef uint8_t* src_null
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t i
        cdef uint8_t mask

        memset(dst, 0, nbytes)

        data = <int64_t*>uv.data
        src_null = uv.validity
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

        # 4 specialised loops: bound-inclusivity hoisted outside the hot path
        if src_null == NULL:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if lower <= data[uv.selection[i]] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if lower <= data[uv.selection[i]] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if lower < data[uv.selection[i]] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if lower < data[uv.selection[i]] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[uv.selection[i]] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[uv.selection[i]] < upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[uv.selection[i]] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[uv.selection[i]] < upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int64_t* data
        cdef uint8_t* src_null
        cdef Py_ssize_t i

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if nbytes > 0:
            memset(dst, 0, nbytes)

        data = <int64_t*>uv.data
        src_null = uv.validity
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

    cdef int64_t* _gather_logical(self, int64_t** owned) except? NULL:
        """Return a contiguous int64 buffer in logical row order. Flat shapes
        reuse the backing buffer (owned set to NULL); other shapes gather
        through selection into a freshly malloc'd buffer (owned set to it)."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t* buf
        cdef Py_ssize_t i
        owned[0] = NULL
        if <Py_ssize_t>uv.data_length == n:
            return data
        buf = <int64_t*>malloc(<size_t>n * sizeof(int64_t))
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = data[<Py_ssize_t>uv.selection[i]]
        owned[0] = buf
        return buf

    cpdef int64_t sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nulls = uv.validity
        cdef int64_t* owned = NULL
        cdef int64_t* kdata
        cdef int64_t total
        cdef Py_ssize_t null_count

        if n == 0:
            return 0
        kdata = self._gather_logical(&owned)
        if nulls == NULL:
            with nogil:
                total = sum_nonnull(kdata, <size_t>n)
        else:
            null_count = n - <Py_ssize_t>simd_popcount(nulls, <size_t>nbytes)
            with nogil:
                if null_count * 10 > n * 7:
                    total = sum_nullable_branching(kdata, nulls, <size_t>n)
                else:
                    total = sum_nullable_branchless(kdata, nulls, <size_t>n)
        if owned != NULL:
            free(owned)
        return total

    cpdef int64_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nulls = uv.validity
        cdef int64_t* owned = NULL
        cdef int64_t* kdata
        cdef int64_t m = 0
        cdef size_t count = 0
        cdef Py_ssize_t null_count

        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        kdata = self._gather_logical(&owned)
        if nulls == NULL:
            with nogil:
                m = min_nonnull(kdata, <size_t>n)
            count = <size_t>n
        else:
            null_count = n - <Py_ssize_t>simd_popcount(nulls, <size_t>nbytes)
            with nogil:
                if null_count * 10 > n * 7:
                    count = min_nullable_branching(kdata, nulls, <size_t>n, &m)
                else:
                    count = min_nullable_branchless(kdata, nulls, <size_t>n, &m)
        if owned != NULL:
            free(owned)
        if count == 0:
            raise ValueError("Cannot compute min of all-null column")
        return m

    cpdef int64_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef uint8_t* nulls = uv.validity
        cdef int64_t* owned = NULL
        cdef int64_t* kdata
        cdef int64_t m = 0
        cdef size_t count = 0
        cdef Py_ssize_t null_count

        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        kdata = self._gather_logical(&owned)
        if nulls == NULL:
            with nogil:
                m = max_nonnull(kdata, <size_t>n)
            count = <size_t>n
        else:
            null_count = n - <Py_ssize_t>simd_popcount(nulls, <size_t>nbytes)
            with nogil:
                if null_count * 10 > n * 7:
                    count = max_nullable_branching(kdata, nulls, <size_t>n, &m)
                else:
                    count = max_nullable_branchless(kdata, nulls, <size_t>n, &m)
        if owned != NULL:
            free(owned)
        if count == 0:
            raise ValueError("Cannot compute max of all-null column")
        return m

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two values at given indices. Returns -1, 0, 1. Assumes non-null."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t left_val = data[uv.selection[left_idx]]
        cdef int64_t right_val = data[uv.selection[right_idx]]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenVector* uv = self.unified()
        if uv.validity == NULL:
            return False
        return ((uv.validity[idx >> 3] >> (idx & 7)) & 1) == 0

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        cdef int8_t null_val

        if buf == NULL:
            raise MemoryError()

        if uv.validity == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                buf[i] = 0 if ((uv.validity[i >> 3] >> (i & 7)) & 1) else 1

        return <int8_t[:n]> buf

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    cpdef Vector materialize(self):
        """Return a dense Integer64Vector, expanding dict/const encodings if needed."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Integer64Vector dense
        cdef int64_t* dst
        cdef int64_t* mat_src
        cdef uint8_t* mat_null
        cdef Py_ssize_t i, nb_bytes

        if self._unified_view.data_length < self._unified_view.length:
            dense = Integer64Vector(<size_t>n)
            dst = <int64_t*>dense.ptr.data
            mat_src = <int64_t*>uv.data
            mat_null = uv.validity
            for i in range(n):
                if mat_null != NULL and not ((mat_null[i >> 3] >> (i & 7)) & 1):
                    dst[i] = 0
                else:
                    dst[i] = mat_src[<Py_ssize_t>uv.selection[i]]
            if mat_null != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memcpy(dense.ptr.null_bitmap, mat_null, <size_t>nb_bytes)
            return dense

        if uv.data_length == 1:
            dense = Integer64Vector(<size_t>n)
            dst = <int64_t*>dense.ptr.data
            if uv.validity != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(dense.ptr.null_bitmap, 0, <size_t>nb_bytes)
                memset(dst, 0, <size_t>n * sizeof(int64_t))
            else:
                for i in range(n):
                    dst[i] = (<int64_t*>uv.data)[0]
                dense.ptr.null_bitmap = NULL
            return dense

        return self

    cpdef Float64Vector to_float64_vector(self):
        """Convert to a Float64Vector by casting int64 values to float64.

        Returns a Float64Vector with int64 values converted to double-precision
        floating point. Used for arithmetic and comparison operations where
        mixed-type (int/float) operations are needed without materialization.
        """
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* src = <int64_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef Float64Vector out
        cdef double* dst
        cdef uint8_t* out_null
        cdef size_t nb_bytes

        out = Float64Vector(<size_t>n)
        dst = <double*>(<void*>out.ptr.data)

        for i in range(n):
            dst[i] = <double>src[<Py_ssize_t>uv.selection[i]]

        if uv.validity != NULL and n > 0:
            nb_bytes = (<size_t>n + 7) >> 3
            out_null = <uint8_t*>malloc(nb_bytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, uv.validity, nb_bytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        return out

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVector* uv = self.unified()
        cdef uint64_t n = <uint64_t>uv.length
        cdef uint64_t dict_bytes, code_bytes, null_bytes, data_bytes, bm_bytes
        if uv.data_length == 1:
            return 8  # sizeof(int64_t)
        if self._unified_view.data_length < self._unified_view.length:
            dict_bytes = uv.data_length * 8
            code_bytes = n * 4  # always uint32_t
            null_bytes = (n + 7) >> 3 if uv.validity != NULL else 0
            return dict_bytes + code_bytes + null_bytes
        data_bytes = n * sizeof(int64_t)
        bm_bytes = (n + 7) >> 3 if uv.validity != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef list out = []
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                out.append(None)
            else:
                out.append(data[<Py_ssize_t>uv.selection[i]])
        return out

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef uint64_t* dst_base
        cdef Py_ssize_t i, j, block
        cdef uint64_t* dst
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Integer64Vector.hash_into: output buffer too small")
        dst_base = &out_buf[0]
        dst = dst_base + offset

        i = 0
        while i < n:
            block = n - i
            if block > 1024:
                block = 1024
            for j in range(block):
                if null_bitmap != NULL and not ((null_bitmap[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    scratch[j] = <uint64_t>data[<Py_ssize_t>uv.selection[i + j]]
            simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenVector* uv = &self._unified_view
        cdef Py_ssize_t i, j, block
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef uint64_t is_valid

        if n == 0:
            return 0

        if null_bitmap != NULL:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (<uint64_t>data[<Py_ssize_t>uv.selection[i + j]] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        else:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    scratch[j] = <uint64_t>data[<Py_ssize_t>uv.selection[i + j]]
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        return 0

    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Single-column hash for COUNT(DISTINCT): delegates to c_hash_into with zeroed dest."""
        if n == 0:
            return 0
        # c_hash_into uses uv.selection[i] uniform access; zero dest first so
        # simd_mix_hash can XOR into a clean buffer.
        memset(out, 0, <size_t>n * sizeof(uint64_t))
        return self.c_hash_into(out, n)

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast per-element compress for Integer64Vector (no Python conversions).

        Null values map to the NULL sentinel; non-null values are read via
        uniform data[selection[i]] access.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int64_t* dst_base
        cdef int64_t* dst
        cdef Py_ssize_t i
        cdef uint8_t* null_bitmap = uv.validity
        cdef int64_t* data = <int64_t*>uv.data

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Integer64Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        dst = dst_base + offset

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                dst[i] = INT64_MIN_VALUE
            else:
                dst[i] = data[<Py_ssize_t>uv.selection[i]]

    def __str__(self):
        cdef DrakenVector* uv = &self._unified_view
        cdef Py_ssize_t i, k = min(<Py_ssize_t>uv.length, 10)
        cdef list vals = []
        cdef int64_t* data = <int64_t*>uv.data
        for i in range(k):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                vals.append(None)
            else:
                vals.append(data[uv.selection[i]])
        return f"<Integer64Vector len={uv.length} values={vals}>"


cdef Integer64Vector _materialize_dict_int64(Integer64Vector vec):
    """Expand a dict-only Integer64Vector to a dense Integer64Vector (no src ptr.data needed)."""
    if vec._unified_view.data_length >= vec._unified_view.length:
        raise ValueError("Dictionary encoding not properly initialized")

    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Integer64Vector dense = Integer64Vector(<size_t>n)
    cdef int64_t* dst = <int64_t*>dense.ptr.data
    cdef int64_t* dict_data = <int64_t*>uv.data
    cdef uint8_t* null_bitmap = uv.validity
    cdef Py_ssize_t i, dict_size = <Py_ssize_t>uv.data_length
    cdef uint32_t code
    cdef Py_ssize_t nb_bytes

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            dst[i] = 0
        else:
            code = uv.selection[i]
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: code {code} >= dict_size {dict_size}")
            dst[i] = dict_data[code]

    if null_bitmap != NULL:
        nb_bytes = (n + 7) >> 3
        dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if dense.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(dense.ptr.null_bitmap, null_bitmap, <size_t>nb_bytes)

    dense._unified_view = draken_vector_from_dense(
        dense.ptr.data, <uint32_t>n, DRAKEN_INT64, dense.ptr.null_bitmap)
    return dense


cdef Integer64Vector make_int64_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded Integer64Vector with no dense materialization.

    Args:
        codes:       uint32_t code array (row_count entries).
        row_count:   Total number of rows.
        dictionary:  Array of unique int64 values (dict_size entries).
        dict_size:   Number of unique dictionary values.
        valid_bits:  Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        Dictionary-encoded Integer64Vector. Unique values in _unified_view.data
        (owned int64 buffer), codes in _unified_view.selection (owned uint32),
        validity in _unified_view.validity. No parallel dict storage.
    """
    cdef Integer64Vector vec = Integer64Vector(0)
    cdef Py_ssize_t code_bytes = row_count * sizeof(uint32_t)
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(int64_t)
    cdef Py_ssize_t nb_bytes
    cdef uint32_t* codes_ptr = NULL
    cdef int64_t* dict_data_ptr = NULL

    vec.ptr.length = <size_t>row_count

    if valid_bits != NULL:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, valid_bits, <size_t>nb_bytes)

    if code_bytes > 0:
        codes_ptr = <uint32_t*>malloc(<size_t>code_bytes)
        if codes_ptr == NULL:
            raise MemoryError()
        memcpy(codes_ptr, codes, <size_t>code_bytes)

    if dict_bytes > 0:
        dict_data_ptr = <int64_t*>malloc(<size_t>dict_bytes)
        if dict_data_ptr == NULL:
            if codes_ptr != NULL: free(codes_ptr)
            raise MemoryError()
        memcpy(dict_data_ptr, <const void*>dictionary, <size_t>dict_bytes)

    vec._owns_dict_data = (dict_data_ptr != NULL)
    vec._owns_selection = (codes_ptr != NULL)
    vec._unified_view = draken_vector_from_dict(
        <void*>dict_data_ptr, <uint32_t>dict_size,
        codes_ptr, <uint32_t>row_count,
        DRAKEN_INT64, vec.ptr.null_bitmap)
    return vec


cdef Integer64Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
):
    """Wrap externally-malloc'd data + null_bitmap into an Integer64Vector.

    Ownership of `data` and `null_bitmap` transfers to the Vector — both must
    have been allocated with the C standard library `malloc` (or be NULL),
    because `free_fixed_buffer` releases them with `free()` on dealloc.

    Used by the C++ IPC deserialiser (`src/cpp/ipc_deserialize.cpp`) to
    transfer ownership of nogil-allocated buffers without a second copy.
    """
    cdef Integer64Vector vec = Integer64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        # We have not yet taken ownership of data/null_bitmap; the caller
        # frees them on a MemoryError raised from here.
        raise MemoryError()
    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = 8
    vec.ptr.length = length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    vec._unified_view = draken_vector_from_dense(data, <uint32_t>length, DRAKEN_INT64, null_bitmap)
    return vec


cdef Integer64Vector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Integer64Vector vec = Integer64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Integer64Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)
    return vec


cdef Integer64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Integer64Vector vec = Integer64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Integer64Vector.from_dict requires a non-empty dictionary")
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

    _attach_dictionary_storage(vec, codes, dictionary, False)
    return vec


cdef Integer64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int64_t* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef Integer64Vector vec = Integer64Vector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t bitmap_bytes
    cdef int32_t[::1] codes_view
    cdef int64_t[::1] dictionary_view
    cdef int32_t* expanded_codes = NULL

    if dict_size == 0:
        raise ValueError("Integer64Vector.from_packed_dict requires a non-empty dictionary")
    if code_width != 1 and code_width != 2 and code_width != 4:
        raise ValueError("unsupported packed dictionary code width")

    if row_null_bitmap != NULL:
        bitmap_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(bitmap_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, row_null_bitmap, <size_t>bitmap_bytes)
    else:
        vec.ptr.null_bitmap = NULL

    if row_count > 0:
        expanded_codes = <int32_t*>malloc(row_count * sizeof(int32_t))
        if expanded_codes == NULL:
            raise MemoryError()

    try:
        for i in range(row_count):
            if row_null_bitmap != NULL and ((row_null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                dst[i] = 0
                expanded_codes[i] = 0
                continue
            if code_width == 1:
                code = (<const uint8_t*>codes)[i]
            elif code_width == 2:
                code = (<const uint16_t*>codes)[i]
            else:
                code = (<const uint32_t*>codes)[i]
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            dst[i] = dictionary[code]
            expanded_codes[i] = <int32_t>code

        if row_count > 0:
            codes_view = <int32_t[:row_count]>expanded_codes
        else:
            codes_view = <int32_t[:0]>expanded_codes
        if dict_size > 0:
            dictionary_view = <int64_t[:dict_size]><int64_t*>dictionary
        else:
            dictionary_view = <int64_t[:0]><int64_t*>dictionary
        _attach_dictionary_storage(vec, codes_view, dictionary_view, ordered, dict_entry_null_bitmap)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)

    return vec


cdef Integer64Vector from_sequence(const int64_t[::1] data):
    """
    Create an owning Integer64Vector from a typed int64 memoryview.

    The (transient) source memoryview is copied once into a malloc'd buffer at
    the Python->native boundary; the vector then owns and frees that buffer.

    Args:
        data: const int64_t[::1] memoryview (C-contiguous)

    Returns:
        Integer64Vector owning a copy of the data
    """
    cdef size_t n = <size_t> data.shape[0]
    cdef int64_t* buf
    if n == 0:
        return from_decoded(NULL, NULL, 0)
    buf = <int64_t*>malloc(n * 8)
    if buf == NULL:
        raise MemoryError()
    memcpy(buf, &data[0], n * 8)
    return from_decoded(<void*>buf, NULL, n)
