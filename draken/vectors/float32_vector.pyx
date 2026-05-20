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
Float32Vector: Cython implementation of a fixed-width float32 column vector for Draken.
"""

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stdint cimport int32_t, int8_t, intptr_t, uint16_t, uint32_t, uint64_t, uint8_t
from libc.stddef cimport size_t
from libc.stdlib cimport free, malloc
from libc.math cimport isinf, isnan, llround

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_FLOAT32
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant, draken_vector_from_dict
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.vector cimport simd_mix_hash_from_dict_cw4, simd_mix_hash_from_dict_nullable_cw4
from draken.vectors.bool_vector cimport BoolVector

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_float32_compare.hpp" namespace "draken::float32_cmp" nogil:
    void bit_fill_range(uint8_t* dst, size_t start, size_t count)
    bint dispatch_compare_once(int op, float a, float b)
    void dispatch_scalar_nonnull(int op, const float* data, float value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const float* data, float value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const float* data, float value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const float* a, const float* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const float* a, const float* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const float* a, const float* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const float* a, const float* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const float* a, const float* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

DEF FLOAT32_HASH_CHUNK = 1024

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000
cdef uint8_t _CONST_NULL_BYTE = 0


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    cdef uint8_t byte = bitmap[idx >> 3]
    return (byte >> (idx & 7)) & 1


cdef void _release_dict_storage(Float32Vector vec) noexcept:
    """Free dict storage. Codes and dict-data live in separate owned buffers
    pointed at by _unified_view.selection and _unified_view.data; ptr.data
    remains the materialized dense buffer (freed by free_fixed_buffer)."""
    if vec._owns_selection:
        free(<void*>vec._unified_view.selection)
    vec._owns_selection = False
    if vec._owns_dict_data and vec._unified_view.data != NULL:
        free(vec._unified_view.data)
    vec._owns_dict_data = False


cdef void _attach_dictionary_storage(Float32Vector vec, const int32_t[::1] codes, const float[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    """Populate the unified view as a dict-encoded vector.

    Dictionary unique values are stored directly in vec.ptr.data (the SAME data
    pointer used for dense vectors). _unified_view.data == ptr.data. Codes go in
    _unified_view.selection (separately malloc'd, tracked by _owns_selection).
    The vector has ONE owned data buffer regardless of encoding.
    """
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Py_ssize_t i
    cdef uint32_t* codes_ptr = NULL

    _release_dict_storage(vec)

    if row_count > 0:
        codes_ptr = <uint32_t*>malloc(row_count * sizeof(uint32_t))
        if codes_ptr == NULL:
            raise MemoryError()
        for i in range(row_count):
            codes_ptr[i] = <uint32_t>codes[i]

    # Re-shape ptr.data to hold the dict values (replaces the row-sized buffer
    # allocated by __cinit__). free_fixed_buffer in __dealloc__ frees it.
    cdef float* dict_data_ptr = NULL
    if dict_size > 0:
        dict_data_ptr = <float*>malloc(<size_t>dict_size * sizeof(float))
        if dict_data_ptr == NULL:
            if codes_ptr != NULL: free(codes_ptr)
            raise MemoryError()
        memcpy(dict_data_ptr, <const void*>&dictionary[0], <size_t>dict_size * sizeof(float))

    vec._owns_dict_data = (dict_data_ptr != NULL)
    vec._owns_selection = (codes_ptr != NULL)
    vec._unified_view = draken_vector_from_dict(
        <void*>dict_data_ptr, <uint32_t>dict_size,
        codes_ptr, <uint32_t>row_count,
        DRAKEN_FLOAT32, vec.ptr.null_bitmap)
    # Per-dict-entry nulls are no longer carried separately under the unified
    # format. Callers must materialize them into row-level validity.
    if dict_entry_null_bitmap != NULL:
        pass

cdef class Float32Vector(Vector):

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef float[::1] dictionary_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("f", dictionary)

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
        cdef Float32Vector vec = Float32Vector(1)
        cdef float val = 0.0 if (is_null or value is None) else <float>float(value)
        (<float*>vec.ptr.data)[0] = val
        vec.ptr.length = <uint32_t>length
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_FLOAT32,
            &_CONST_NULL_BYTE if is_null else NULL)
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        self._owns_dict_data = False
        self._owns_selection = False
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_FLOAT32, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT32, length, 4)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_FLOAT32, NULL)

    def __dealloc__(self):
        _release_dict_storage(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        cdef DrakenVector* uv = &self._unified_view
        if uv.data_length == 1 and uv.length != 1:
            return NULL
        return self.ptr.null_bitmap

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa

        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.float32())
            return pa.array([(<float*>uv.data)[0]] * self.ptr.length, type=pa.float32())

        cdef size_t nbytes = self.ptr.length * sizeof(float)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.float32(), self.ptr.length, buffers)

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
        return DRAKEN_FLOAT32

    @property
    def dictionary_value_type(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return None
        return self._unified_view.type

    @property
    def dictionary_size(self):
        if self._unified_view.data_length >= self._unified_view.length:
            return 0
        return self._unified_view.data_length

    @property
    def code_width(self):
        return 4 if self._unified_view.data_length < self._unified_view.length else None

    @property
    def ordered(self):
        # Ordered-dict metadata is no longer stored separately from the unified
        # view. Callers relying on this for predicate pushdown must derive it
        # by inspecting the data array (e.g. via a one-pass sortedness check).
        return False

    cdef object item_at(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data
        cdef uint8_t byte
        cdef uint8_t bit
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if uv.data_length == 1:
            if uv.validity != NULL:
                return None
            return (<float*>uv.data)[0]
        if self._unified_view.data_length < self._unified_view.length:
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                return None
            return (<float*>uv.data)[<Py_ssize_t>uv.selection[i]]
        data = <float*> ptr.data
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        return self.item_at(i)

    cpdef Float32Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Py_ssize_t out_n_d = n
        cdef Py_ssize_t dsz
        cdef uint32_t* gc = NULL
        cdef uint8_t* gn = NULL
        cdef Py_ssize_t cb_d
        cdef Py_ssize_t nb_d
        cdef Py_ssize_t si_d
        cdef Float32Vector dtake_result
        if self._unified_view.data_length < self._unified_view.length:
            # O(n) gather: copy selected codes and copy dict verbatim.
            dsz = <Py_ssize_t>self._unified_view.data_length
            cb_d = out_n_d * sizeof(uint32_t)
            if cb_d > 0:
                gc = <uint32_t*>malloc(<size_t>cb_d)
                if gc == NULL:
                    raise MemoryError()
                for i in range(out_n_d):
                    gc[i] = uv.selection[indices[i]]
            if self.ptr.null_bitmap != NULL and out_n_d > 0:
                nb_d = (out_n_d + 7) >> 3
                gn = <uint8_t*>malloc(<size_t>nb_d)
                if gn == NULL:
                    if gc != NULL: free(gc)
                    raise MemoryError()
                memset(gn, 0, <size_t>nb_d)
                for i in range(out_n_d):
                    si_d = indices[i]
                    if (self.ptr.null_bitmap[si_d >> 3] >> (si_d & 7)) & 1:
                        gn[i >> 3] |= (1 << (i & 7))
            try:
                dtake_result = make_float32_dict_only(
                    <const uint32_t*>gc,
                    out_n_d,
                    <const float*>self.ptr.data,
                    dsz,
                    gn,
                )
            finally:
                if gc != NULL: free(gc)
                if gn != NULL: free(gn)
            return dtake_result
        cdef DrakenVector* _tuv = self.unified()
        if _tuv.data_length == 1:
            return Float32Vector.from_constant(
                None if _tuv.validity != NULL else (<float*>_tuv.data)[0],
                n,
                is_null=(_tuv.validity != NULL),
            )
        cdef Float32Vector out = Float32Vector(<size_t>n)
        cdef float* src = <float*> self.ptr.data
        cdef float* dst = <float*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap
        cdef Py_ssize_t out_nbytes
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef uint8_t byte
        # If source has no null bitmap, copy directly.
        if src_null == NULL:
            for i in range(n):
                src_idx = indices[i]
                dst[i] = src[src_idx]
            out.ptr.null_bitmap = NULL
        else:
            # Preserve nulls in the output bitmap.
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*> malloc(out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, out_nbytes)

            for i in range(n):
                src_idx = indices[i]
                byte = src_null[src_idx >> 3]
                if byte & (1 << (src_idx & 7)):
                    dst[i] = src[src_idx]
                    out_null[i >> 3] |= (1 << (i & 7))
                else:
                    dst[i] = 0.0

            out.ptr.null_bitmap = out_null

        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_FLOAT32, out.ptr.null_bitmap)
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

    cdef BoolVector _compare_scalar(self, float value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef bint matched
        cdef float* data = <float*>uv.data
        cdef Py_ssize_t dict_size
        cdef uint8_t* match_table
        cdef Py_ssize_t d, i
        cdef uint8_t* src_null
        cdef size_t valid_count

        if nbytes > 0:
            memset(dst, 0, nbytes)

        if uv.data_length == 1:
            if uv.validity != NULL:
                return self._make_all_null_bool(n)
            matched = dispatch_compare_once(op, data[0], value)
            if matched and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
            out.ptr.null_bitmap = NULL
            return out

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

        if self._unified_view.data_length < self._unified_view.length:
            dict_size = <Py_ssize_t>uv.data_length
            match_table = <uint8_t*>malloc(<size_t>dict_size if dict_size > 0 else 1)
            if match_table == NULL:
                raise MemoryError()
            for d in range(dict_size):
                match_table[d] = 1 if dispatch_compare_once(op, data[d], value) else 0
            for i in range(n):
                if match_table[uv.selection[i]]:
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            free(match_table)
            if out_null != NULL:
                simd_and_mask(dst, dst, out_null, <size_t>nbytes)
            return out

        src_null = uv.validity
        if src_null == NULL:
            dispatch_scalar_nonnull(op, data, value, dst, <size_t>n)
        else:
            valid_count = simd_popcount(src_null, <size_t>nbytes)
            if n > 0 and (valid_count * 10) < (<size_t>n * 3):
                dispatch_scalar_branching(op, data, value, src_null, dst, <size_t>n)
            else:
                dispatch_scalar_branchless(op, data, value, src_null, dst, <size_t>n)
        return out

    cdef BoolVector _compare_vector(self, Float32Vector other, int op):
        # Const fast paths: avoid O(n) materialisation.
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef int reversed_op
        if uv.data_length == 1:
            if self.ptr.length != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if uv.validity != NULL:
                return self._make_all_null_bool(<Py_ssize_t>self.ptr.length)
            # Reverse: gt(2)<->lt(4), ge(3)<->le(5); eq(0) and ne(1) unchanged.
            if op == 2:   reversed_op = 4
            elif op == 3: reversed_op = 5
            elif op == 4: reversed_op = 2
            elif op == 5: reversed_op = 3
            else:         reversed_op = op
            return other._compare_scalar((<float*>uv.data)[0], reversed_op)
        if ouv.data_length == 1:
            if self.ptr.length != other.ptr.length:
                raise ValueError("Vectors must have the same length")
            if ouv.validity != NULL:
                return self._make_all_null_bool(<Py_ssize_t>self.ptr.length)
            return self._compare_scalar((<float*>ouv.data)[0], op)

        # For dict-encoded on either side: materialize then compare
        if self._unified_view.data_length < self._unified_view.length:
            return self.materialize()._compare_vector(other, op)
        if other._unified_view.data_length < other._unified_view.length:
            return self._compare_vector(other.materialize(), op)

        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef float* data1 = <float*> ptr1.data
        cdef float* data2 = <float*> ptr2.data
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

        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False
        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        if null1 == NULL and null2 == NULL:
            dispatch_vector_nonnull(op, data1, data2, dst, <size_t>n)
        elif use_branching:
            if null1 != NULL and null2 != NULL:
                dispatch_vector_both_null_branching(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
            elif null1 != NULL:
                dispatch_vector_one_null_branching(op, data1, data2, null1, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branching(op, data1, data2, null2, dst, out_null, <size_t>n)
        elif null1 != NULL and null2 == NULL:
            dispatch_vector_one_null_branchless(op, data1, data2, null1, dst, out_null, <size_t>n)
        elif null1 == NULL and null2 != NULL:
            dispatch_vector_one_null_branchless(op, data1, data2, null2, dst, out_null, <size_t>n)
        else:
            dispatch_vector_both_null_branchless(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
        return out

    cpdef BoolVector equals(self, float value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Float32Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, float value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Float32Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, float value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Float32Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, float value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Float32Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, float value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Float32Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, float value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Float32Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, float lower, float upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef float* data
        cdef uint8_t* src_null
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t i
        cdef uint8_t mask
        cdef bint in_range
        cdef Py_ssize_t dict_size, d, match_idx
        cdef uint8_t* match_table
        cdef float v

        if self._unified_view.data_length < self._unified_view.length:
            data = <float*>uv.data
            dict_size = <Py_ssize_t>uv.data_length
            src_null = uv.validity
            match_table = <uint8_t*>malloc(<size_t>dict_size if dict_size > 0 else 1)
            if match_table == NULL:
                raise MemoryError()
            for d in range(dict_size):
                v = data[d]
                if lower_inclusive and upper_inclusive:
                    match_table[d] = 1 if (lower <= v <= upper) else 0
                elif lower_inclusive:
                    match_table[d] = 1 if (lower <= v < upper) else 0
                elif upper_inclusive:
                    match_table[d] = 1 if (lower < v <= upper) else 0
                else:
                    match_table[d] = 1 if (lower < v < upper) else 0
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if src_null != NULL and nbytes != 0:
                out_null = <uint8_t*>malloc(nbytes)
                if out_null == NULL:
                    free(match_table)
                    raise MemoryError()
                memcpy(out_null, src_null, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    out_null[nbytes - 1] &= mask
                out.ptr.null_bitmap = out_null
            else:
                out.ptr.null_bitmap = NULL
            for i in range(n):
                if src_null != NULL and not ((src_null[i >> 3] >> (i & 7)) & 1):
                    pass
                else:
                    match_idx = <Py_ssize_t>uv.selection[i]
                    if match_table[match_idx]:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            free(match_table)
            return out

        memset(dst, 0, nbytes)

        if uv.data_length == 1:
            if uv.validity != NULL:
                if nbytes != 0:
                    out_null = <uint8_t*>malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            data = <float*>uv.data
            if lower_inclusive:
                in_range = data[0] >= lower
            else:
                in_range = data[0] > lower
            if in_range:
                if upper_inclusive:
                    in_range = data[0] <= upper
                else:
                    in_range = data[0] < upper
            if in_range and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
            out.ptr.null_bitmap = NULL
            return out

        data = <float*>uv.data
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

        if src_null == NULL:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if lower <= data[i] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if lower <= data[i] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if lower < data[i] <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if lower < data[i] < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            if lower_inclusive and upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[i] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif lower_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower <= data[i] < upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif upper_inclusive:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[i] <= upper:
                            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                for i in range(n):
                    if (src_null[i >> 3] >> (i & 7)) & 1:
                        if lower < data[i] < upper:
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
        cdef float* data
        cdef uint8_t* src_null
        cdef Py_ssize_t i
        cdef Py_ssize_t dict_size, d, match_idx
        cdef uint8_t* match_table_il

        if self._unified_view.data_length < self._unified_view.length:
            if not isinstance(value_set, (set, frozenset)):
                value_set = set(value_set)
            data = <float*>uv.data
            dict_size = <Py_ssize_t>uv.data_length
            src_null = uv.validity
            match_table_il = <uint8_t*>malloc(<size_t>dict_size if dict_size > 0 else 1)
            if match_table_il == NULL:
                raise MemoryError()
            for d in range(dict_size):
                match_table_il[d] = 1 if data[d] in value_set else 0
            out = BoolVector(<size_t>n)
            dst = <uint8_t*>out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if src_null != NULL and nbytes != 0:
                out_null = <uint8_t*>malloc(nbytes)
                if out_null == NULL:
                    free(match_table_il)
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
                    match_idx = <Py_ssize_t>uv.selection[i]
                    if match_table_il[match_idx]:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            free(match_table_il)
            return out

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if nbytes > 0:
            memset(dst, 0, nbytes)

        if uv.data_length == 1:
            if uv.validity != NULL:
                if nbytes != 0:
                    out_null = <uint8_t*>malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            data = <float*>uv.data
            if data[0] in value_set and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        data = <float*>uv.data
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
                if data[i] in value_set:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef float sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef float* data = <float*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i
        cdef uint32_t code
        cdef double total = 0.0  # accumulate in double to reduce rounding error

        if uv.data_length == 1:
            if uv.validity != NULL:
                return 0.0
            return n * data[0]

        if self._unified_view.data_length < self._unified_view.length:
            if uv.validity == NULL:
                with nogil:
                    for i in range(n):
                        total += data[uv.selection[i]]
            else:
                with nogil:
                    for i in range(n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            total += data[uv.selection[i]]
            return <float>total

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* dense_data = <float*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    total += dense_data[i]
        else:
            for i in range(n):
                total += dense_data[i]
        return <float>total

    cpdef float min(self):
        cdef DrakenVector* uv = self.unified()
        cdef float* data = <float*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, start
        cdef uint32_t code
        cdef float m
        cdef bint seen

        if uv.data_length == 1:
            if n == 0 or uv.validity != NULL:
                raise ValueError("Cannot compute min of empty or all-null column")
            return data[0]

        if self._unified_view.data_length < self._unified_view.length:
            if n == 0:
                raise ValueError("Cannot compute min of empty column")
            seen = False
            if uv.validity == NULL:
                m = data[uv.selection[0]]
                seen = True
                start = 1
                with nogil:
                    for i in range(start, n):
                        if data[uv.selection[i]] < m:
                            m = data[uv.selection[i]]
            else:
                start = 0
                for i in range(n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        m = data[uv.selection[i]]
                        seen = True
                        start = i + 1
                        break
                if not seen:
                    raise ValueError("Cannot compute min of all-null column")
                with nogil:
                    for i in range(start, n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            if data[uv.selection[i]] < m:
                                m = data[uv.selection[i]]
            return m

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* dense_data = <float*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint found = False

        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    m = dense_data[i]
                    found = True
                    start = i + 1
                    break
        else:
            m = dense_data[0]
            found = True
            start = 1

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        for i in range(start, n):
            if null_bitmap != NULL:
                if not _bitmap_is_valid(null_bitmap, i):
                    continue
            if dense_data[i] < m:
                m = dense_data[i]
        return m

    cpdef float max(self):
        cdef DrakenVector* uv = self.unified()
        cdef float* data = <float*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, start
        cdef uint32_t code
        cdef float m
        cdef bint seen

        if uv.data_length == 1:
            if n == 0 or uv.validity != NULL:
                raise ValueError("Cannot compute max of empty or all-null column")
            return data[0]

        if self._unified_view.data_length < self._unified_view.length:
            if n == 0:
                raise ValueError("Cannot compute max of empty column")
            seen = False
            if uv.validity == NULL:
                m = data[uv.selection[0]]
                seen = True
                start = 1
                with nogil:
                    for i in range(start, n):
                        if data[uv.selection[i]] > m:
                            m = data[uv.selection[i]]
            else:
                start = 0
                for i in range(n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        m = data[uv.selection[i]]
                        seen = True
                        start = i + 1
                        break
                if not seen:
                    raise ValueError("Cannot compute max of all-null column")
                with nogil:
                    for i in range(start, n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            if data[uv.selection[i]] > m:
                                m = data[uv.selection[i]]
            return m

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* dense_data = <float*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint found = False

        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    m = dense_data[i]
                    found = True
                    start = i + 1
                    break
        else:
            m = dense_data[0]
            found = True
            start = 1

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(start, n):
            if null_bitmap != NULL:
                if not _bitmap_is_valid(null_bitmap, i):
                    continue
            if dense_data[i] > m:
                m = dense_data[i]
        return m

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two values at given indices. Returns -1, 0, 1. Assumes non-null."""
        cdef DrakenVector* uv = self.unified()
        cdef float left_val, right_val
        cdef float* data

        if uv.data_length == 1:
            return 0

        if self._unified_view.data_length < self._unified_view.length:
            data = <float*>uv.data
            left_val = data[<Py_ssize_t>uv.selection[left_idx]]
            right_val = data[<Py_ssize_t>uv.selection[right_idx]]
            if left_val < right_val:
                return -1
            elif left_val > right_val:
                return 1
            return 0

        data = <float*>uv.data

        left_val = data[left_idx]
        right_val = data[right_idx]

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
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if uv.data_length == 1:
            for i in range(n):
                buf[i] = 1 if uv.validity != NULL else 0
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

    cpdef int8_t[::1] is_null_with_nan(self):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        cdef float* data

        if buf == NULL:
            raise MemoryError()

        if uv.data_length == 1:
            data = <float*>uv.data
            for i in range(n):
                buf[i] = 1 if (uv.validity != NULL or isnan(data[0])) else 0
            return <int8_t[:n]> buf

        if self._unified_view.data_length < self._unified_view.length:
            data = <float*>uv.data
            for i in range(n):
                if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                    buf[i] = 1
                else:
                    buf[i] = 1 if isnan(data[<Py_ssize_t>uv.selection[i]]) else 0
            return <int8_t[:n]> buf

        data = <float*> ptr.data

        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 1 if isnan(data[i]) else 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit == 0:
                    buf[i] = 1
                elif isnan(data[i]):
                    buf[i] = 1
                else:
                    buf[i] = 0

        return <int8_t[:n]> buf

    @property
    def null_count(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        if uv.data_length == 1:
            # const-null: all rows are null; const-valid handled above (validity==NULL)
            return n
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    cpdef Vector materialize(self):
        """Return a dense Float32Vector, expanding dict/const encodings if needed."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Float32Vector dense
        cdef float* dst
        cdef float* mat_src
        cdef uint8_t* mat_null
        cdef Py_ssize_t i, nb_bytes

        if self._unified_view.data_length < self._unified_view.length:
            dense = Float32Vector(<size_t>n)
            dst = <float*>dense.ptr.data
            mat_src = <float*>uv.data
            mat_null = uv.validity
            for i in range(n):
                if mat_null != NULL and not ((mat_null[i >> 3] >> (i & 7)) & 1):
                    dst[i] = 0.0
                else:
                    dst[i] = mat_src[<Py_ssize_t>uv.selection[i]]
            if mat_null != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memcpy(dense.ptr.null_bitmap, mat_null, <size_t>nb_bytes)
            dense._unified_view = draken_vector_from_dense(
                dense.ptr.data, <uint32_t>n, DRAKEN_FLOAT32, dense.ptr.null_bitmap)
            return dense

        if uv.data_length == 1:
            dense = Float32Vector(<size_t>n)
            dst = <float*>dense.ptr.data
            if uv.validity != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(dense.ptr.null_bitmap, 0, <size_t>nb_bytes)
                memset(dst, 0, <size_t>n * sizeof(float))
            else:
                for i in range(n):
                    dst[i] = (<float*>uv.data)[0]
                dense.ptr.null_bitmap = NULL
            dense._unified_view = draken_vector_from_dense(
                dense.ptr.data, <uint32_t>n, DRAKEN_FLOAT32, dense.ptr.null_bitmap)
            return dense

        return self

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = <uint64_t>uv.length
        cdef uint64_t dict_bytes, code_bytes, null_bytes, data_bytes, bm_bytes
        if uv.data_length == 1:
            return 4  # sizeof(float)
        if self._unified_view.data_length < self._unified_view.length:
            dict_bytes = uv.data_length * 4
            code_bytes = n * sizeof(uint32_t)
            null_bytes = (n + 7) >> 3 if uv.validity != NULL else 0
            return dict_bytes + code_bytes + null_bytes
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef float* data = <float*>uv.data
        cdef list out = []
        cdef Py_ssize_t idx
        cdef uint8_t byte, bit

        if uv.data_length == 1:
            if uv.validity != NULL:
                return [None] * n
            return [data[0]] * n

        if self._unified_view.data_length < self._unified_view.length:
            for i in range(n):
                if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                    out.append(None)
                else:
                    idx = <Py_ssize_t>uv.selection[i]
                    out.append(data[idx])
            return out

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* dense_data = <float*> ptr.data

        if ptr.null_bitmap == NULL:
            for i in range(n):
                out.append(dense_data[i])
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    out.append(dense_data[i])
                else:
                    out.append(None)
        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t value
        cdef uint64_t* dst
        cdef uint64_t[FLOAT32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef float* data
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef uint64_t is_valid
        cdef uint32_t fbits

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.hash_into: output buffer too small")

        dst = &out_buf[offset]

        if self._unified_view.data_length < self._unified_view.length:
            null_bitmap = ptr.null_bitmap
            if null_bitmap == NULL:
                simd_mix_hash_from_dict_cw4(dst, <const uint64_t*>uv.data,
                                             uv.selection, <size_t>n)
            else:
                simd_mix_hash_from_dict_nullable_cw4(dst, <const uint64_t*>uv.data,
                                                      uv.selection, null_bitmap,
                                                      0, <size_t>n)
            return

        if uv.data_length == 1:
            if uv.validity != NULL:
                value = NULL_HASH
            else:
                fbits = (<uint32_t*>uv.data)[0]
                value = <uint64_t>fbits
            for j in range(FLOAT32_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        data = <float*> ptr.data
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        cdef uint64_t is_valid2
        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                for j in range(block):
                    is_valid2 = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = (<uint64_t>fbits * is_valid2) | (NULL_HASH * (1 - is_valid2))
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        else:
            # Widen float32 bits to uint64 before hashing
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                for j in range(block):
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = <uint64_t>fbits
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = &self._unified_view
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef uint64_t[FLOAT32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint8_t* _cd_null_bitmap
        cdef uint32_t fbits

        if n == 0:
            return 0

        if uv.data_length == 1:
            if uv.validity != NULL:
                value = NULL_HASH
            else:
                fbits = (<uint32_t*>uv.data)[0]
                value = <uint64_t>fbits
            for j in range(FLOAT32_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # DICTIONARY-only path
        if self._unified_view.data_length < self._unified_view.length:
            _cd_null_bitmap = ptr.null_bitmap
            if _cd_null_bitmap == NULL:
                simd_mix_hash_from_dict_cw4(out, <const uint64_t*>uv.data,
                                             uv.selection, <size_t>n)
            else:
                simd_mix_hash_from_dict_nullable_cw4(out, <const uint64_t*>uv.data,
                                                      uv.selection, _cd_null_bitmap,
                                                      0, <size_t>n)
            return 0

        cdef float* data = <float*> ptr.data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = (<uint64_t>fbits * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t> block)
                i += block
        else:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT32_HASH_CHUNK:
                    block = FLOAT32_HASH_CHUNK
                for j in range(block):
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = <uint64_t>fbits
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        return 0

    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Single-column hash for COUNT(DISTINCT): no prior dest state, no memset."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = &self._unified_view
        cdef float* data
        cdef uint8_t* null_bitmap
        cdef Py_ssize_t i, j, block
        cdef uint64_t is_valid, v
        cdef uint32_t fbits
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return 0

        if uv.data_length == 1:
            if uv.validity != NULL:
                v = NULL_HASH * MIX_HASH_CONSTANT + 1
            else:
                fbits = (<uint32_t*>uv.data)[0]
                v = <uint64_t>fbits * MIX_HASH_CONSTANT + 1
            v ^= v >> 32
            for i in range(n):
                out[i] = v
            return 0

        if self._unified_view.data_length < self._unified_view.length:
            memset(out, 0, <size_t>n * sizeof(uint64_t))
            return self.c_hash_into(out, n)

        data = <float*>ptr.data
        null_bitmap = ptr.null_bitmap

        if null_bitmap == NULL:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = <uint64_t>fbits
                # SIMD hash the widened block
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        else:
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    fbits = (<uint32_t*>data)[i + j]
                    scratch[j] = (<uint64_t>fbits * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for Float32Vector with NaN/Inf handling and clamping."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base
        cdef int64_t* dst
        cdef Py_ssize_t i
        cdef float v
        cdef long long rv
        cdef int64_t MIN_SIGNED = <int64_t> -9223372036854775807
        cdef int64_t MAX_SIGNED = <int64_t> 9223372036854775807
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef float* _ci_dict_data

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        dst = dst_base + offset

        if self._unified_view.data_length < self._unified_view.length:
            _ci_dict_data  = <float*>uv.data
            null_bitmap    = ptr.null_bitmap
            for i in range(n):
                if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                    dst[i] = NULL_FLAG
                    continue
                v = _ci_dict_data[<Py_ssize_t>uv.selection[i]]
                if isnan(v):
                    dst[i] = NULL_FLAG
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                else:
                    rv = llround(v)
                    if rv < MIN_SIGNED:
                        dst[i] = MIN_SIGNED
                    elif rv > MAX_SIGNED:
                        dst[i] = MAX_SIGNED
                    else:
                        dst[i] = <int64_t>rv
            return

        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        if uv.data_length == 1:
            for i in range(n):
                if uv.validity != NULL:
                    dst[i] = NULL_FLAG
                    continue
                v = (<float*>uv.data)[0]
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv
            return

        cdef float* data = <float*> ptr.data
        if has_nulls:
            for i in range(n):
                if ((null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
                    dst[i] = NULL_FLAG
                    continue
                v = data[i]
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv
        else:
            for i in range(n):
                v = data[i]
                if isnan(v):
                    dst[i] = NULL_FLAG
                    continue
                elif isinf(v):
                    dst[i] = MAX_SIGNED if v > 0.0 else MIN_SIGNED
                    continue
                rv = llround(v)
                if rv < MIN_SIGNED:
                    dst[i] = MIN_SIGNED
                elif rv > MAX_SIGNED:
                    dst[i] = MAX_SIGNED
                else:
                    dst[i] = <int64_t> rv

    def __str__(self):
        cdef DrakenVector* uv = self.unified()
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>self.ptr.length, 10)
        if uv.data_length == 1:
            vals = [None if uv.validity != NULL else (<float*>uv.data)[0]] * k
            return f"<Float32Vector len={self.ptr.length} values={vals}>"
        cdef float* data = <float*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Float32Vector len={self.ptr.length} values={vals}>"


cdef Float32Vector _materialize_dict_float32(Float32Vector vec):
    """Expand a dict-only Float32Vector to a dense Float32Vector (no src ptr.data needed)."""
    cdef DrakenVector* src_uv = vec.unified()
    if vec._unified_view.data_length >= vec._unified_view.length:
        raise ValueError("Dictionary encoding not properly initialized")

    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Float32Vector dense = Float32Vector(<size_t>n)
    cdef float* dst = <float*>dense.ptr.data
    cdef float* dict_data = <float*>src_uv.data
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t i, dict_size = <Py_ssize_t>vec._unified_view.data_length
    cdef uint32_t code
    cdef Py_ssize_t nb_bytes

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            dst[i] = 0.0
        else:
            code = src_uv.selection[i]
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
        dense.ptr.data, <uint32_t>n, DRAKEN_FLOAT32, dense.ptr.null_bitmap)
    return dense


cdef Float32Vector make_float32_dict_only(
    const uint32_t* codes,
    Py_ssize_t row_count,
    const float* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded Float32Vector with no dense materialization.

    Args:
        codes:       uint32_t code array (row_count entries).
        row_count:   Total number of rows.
        dictionary:  Array of unique float values (dict_size entries).
        dict_size:   Number of unique dictionary values.
        valid_bits:  Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        Dictionary-encoded Float32Vector. Unique values live in _unified_view.data
        (owned float buffer), codes live in _unified_view.selection (owned uint32),
        validity in _unified_view.validity. No parallel dict storage.
    """
    cdef Float32Vector vec = Float32Vector(0)   # allocates ptr header; no data yet
    cdef Py_ssize_t code_bytes = row_count * sizeof(uint32_t)
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(float)
    cdef Py_ssize_t nb_bytes
    cdef uint32_t* codes_ptr = NULL

    vec.ptr.length = <uint32_t>row_count  # logical length (rows)

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

    # Dictionary unique values go in ptr.data (the SAME slot dense uses).
    # free_fixed_buffer in __dealloc__ frees it via owns_data.
    cdef float* dict_data_ptr = NULL
    if dict_bytes > 0:
        dict_data_ptr = <float*>malloc(<size_t>dict_bytes)
        if dict_data_ptr == NULL:
            if codes_ptr != NULL: free(codes_ptr)
            raise MemoryError()
        memcpy(dict_data_ptr, <const void*>dictionary, <size_t>dict_bytes)

    vec._owns_dict_data = (dict_data_ptr != NULL)
    vec._owns_selection = (codes_ptr != NULL)
    vec._unified_view = draken_vector_from_dict(
        <void*>dict_data_ptr, <uint32_t>dict_size,
        codes_ptr, <uint32_t>row_count,
        DRAKEN_FLOAT32, vec.ptr.null_bitmap)
    return vec


cdef Float32Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
):
    """Wrap externally-malloc'd data + null_bitmap into a Float32Vector.

    Ownership transfers to the Vector — both pointers must come from `malloc`
    (or be NULL). See `Int64Vector.from_decoded` for the design rationale.
    """
    cdef Float32Vector vec = Float32Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.ptr.type = DRAKEN_FLOAT32
    vec.ptr.itemsize = 4
    vec.ptr.length = <uint32_t>length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    vec._unified_view = draken_vector_from_dense(data, <uint32_t>length, DRAKEN_FLOAT32, null_bitmap)
    return vec


cdef Float32Vector from_arrow(object array):
    """Zero-copy wrap of a PyArrow float32 array as Float32Vector."""
    cdef Float32Vector vec = Float32Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr
    cdef Py_ssize_t nb_size
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef object new_bitmap_bytes
    cdef Py_ssize_t i

    vec.ptr.type = DRAKEN_FLOAT32
    vec.ptr.itemsize = 4
    vec.ptr.length = <uint32_t> len(array)
    vec.ptr.data = <void*> (base_ptr + offset * 4)

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nb_size)
            src_bitmap = <uint8_t*> nb_addr
            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    cdef uint32_t _arr_len = <uint32_t>len(array)
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, _arr_len, DRAKEN_FLOAT32, vec.ptr.null_bitmap)
    return vec


cdef Float32Vector from_dict(const int32_t[::1] codes, const float[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float32Vector vec = Float32Vector(<size_t>row_count)
    cdef float* dst = <float*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Float32Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)
    return vec


cdef Float32Vector from_dict_nullable(
    const int32_t[::1] codes,
    const float[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float32Vector vec = Float32Vector(<size_t>row_count)
    cdef float* dst = <float*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Float32Vector.from_dict requires a non-empty dictionary")
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
            dst[i] = 0.0

    _attach_dictionary_storage(vec, codes, dictionary, False)
    return vec


cdef Float32Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const float* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef Float32Vector vec = Float32Vector(<size_t>row_count)
    cdef float* dst = <float*>vec.ptr.data
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t bitmap_bytes
    cdef int32_t[::1] codes_view
    cdef float[::1] dictionary_view
    cdef int32_t* expanded_codes = NULL

    if dict_size == 0:
        raise ValueError("Float32Vector.from_packed_dict requires a non-empty dictionary")
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
                dst[i] = 0.0
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
            dictionary_view = <float[:dict_size]><float*>dictionary
        else:
            dictionary_view = <float[:0]><float*>dictionary
        _attach_dictionary_storage(vec, codes_view, dictionary_view, ordered, dict_entry_null_bitmap)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)

    return vec


cdef Float32Vector from_sequence(float[::1] data):
    """
    Create Float32Vector from a typed float memoryview (zero-copy).

    Args:
        data: float[::1] memoryview (C-contiguous)

    Returns:
        Float32Vector wrapping the memoryview data
    """
    cdef Float32Vector vec = Float32Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_FLOAT32
    vec.ptr.itemsize = 4
    vec.ptr.length = <uint32_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL
    cdef uint32_t _seq_len = <uint32_t>data.shape[0]
    vec._unified_view = draken_vector_from_dense(vec.ptr.data, _seq_len, DRAKEN_FLOAT32, NULL)
    return vec
