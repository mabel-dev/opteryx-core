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
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
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


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    cdef uint8_t byte = bitmap[idx >> 3]
    return (byte >> (idx & 7)) & 1


cdef void _release_dict_storage(Float32Vector vec) noexcept:
    if vec._unified_view.selection != NULL:
        free(vec._unified_view.selection)
        vec._unified_view.selection = NULL
        vec._unified_view.sel_width = 0
    if vec._dict_values != NULL:
        free_var_buffer(vec._dict_values, True)
        vec._dict_values = NULL
    vec._dict_ordered = 0


cdef void _attach_dictionary_storage(Float32Vector vec, const int32_t[::1] codes, const float[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(float)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t bitmap_bytes
    cdef uint8_t* codes_ptr

    _release_dict_storage(vec)

    if code_bytes > 0:
        codes_ptr = <uint8_t*>malloc(code_bytes)
        if codes_ptr == NULL:
            raise MemoryError()
    else:
        codes_ptr = NULL

    dict_values = alloc_var_buffer(DRAKEN_FLOAT32, <size_t>dict_size, <size_t>dict_bytes)
    dict_values.offsets[0] = 0
    for i in range(dict_size):
        dict_values.offsets[i + 1] = <int32_t>((i + 1) * sizeof(float))
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>&dictionary[0], <size_t>dict_bytes)

    if dict_entry_null_bitmap != NULL:
        bitmap_bytes = (dict_size + 7) >> 3
        dict_values.null_bitmap = <uint8_t*>malloc(<size_t>bitmap_bytes)
        if dict_values.null_bitmap == NULL:
            raise MemoryError()
        memcpy(dict_values.null_bitmap, dict_entry_null_bitmap, <size_t>bitmap_bytes)

    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code_width == 1:
            (<uint8_t*>codes_ptr)[i] = <uint8_t>code
        elif code_width == 2:
            (<uint16_t*>codes_ptr)[i] = <uint16_t>code
        else:
            (<uint32_t*>codes_ptr)[i] = <uint32_t>code

    vec._dict_values = dict_values
    vec._dict_ordered = 1 if ordered else 0
    vec._unified_view.data = dict_values.data
    vec._unified_view.data_length = <size_t>dict_size
    vec._unified_view.selection = codes_ptr
    vec._unified_view.sel_width = code_width
    vec._unified_view.validity = vec.ptr.null_bitmap


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
        vec.ptr.length = <size_t>length
        vec._unified_view.length = <size_t>length
        vec._unified_view.data = vec.ptr.data
        vec._unified_view.data_length = 1
        vec._unified_view.selection = NULL
        vec._unified_view.sel_width = 0
        vec._unified_view.validity = &_CONST_NULL_BYTE if is_null else NULL
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT32, length, 4)
            self.owns_data = True
        self._dict_values = NULL
        self._dict_ordered = 0
        self._unified_view.data = NULL if wrap else self.ptr.data
        self._unified_view.data_length = 0 if wrap else <size_t>length
        self._unified_view.selection = NULL
        self._unified_view.sel_width = 0
        self._unified_view.length = 0 if wrap else <size_t>length
        self._unified_view.validity = NULL
        self._unified_view.itemsize = sizeof(float)
        self._unified_view.type = DRAKEN_FLOAT32

    def __dealloc__(self):
        _release_dict_storage(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL or self._unified_view.data_length == 1:
            return NULL
        return self.ptr.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL or self._unified_view.data_length == 1:
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

        cdef size_t nbytes = buf_length(self.ptr) * sizeof(float)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.float32(), buf_length(self.ptr), buffers)

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
    def dictionary_value_type(self):
        if self._dict_values == NULL:
            return None
        return self._dict_values.type

    @property
    def dictionary_size(self):
        if self._dict_values == NULL:
            return 0
        return self._dict_values.length

    @property
    def code_width(self):
        return self._unified_view.sel_width if self._dict_values != NULL else None

    @property
    def ordered(self):
        return bool(self._dict_ordered) if self._dict_values != NULL else False

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data
        cdef uint8_t byte
        cdef uint8_t bit
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        if uv.data_length == 1:
            if uv.validity != NULL:
                return None
            return (<float*>uv.data)[0]
        data = <float*> ptr.data
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    cpdef Float32Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef bint _is_null
        if uv.data_length == 1:
            _is_null = uv.validity != NULL
            return Float32Vector.from_constant(
                None if _is_null else (<float*>uv.data)[0],
                n,
                is_null=_is_null,
            )
        cdef Float32Vector out = Float32Vector(<size_t>n)
        cdef float* src = <float*> self.ptr.data
        cdef float* dst = <float*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap
        cdef Py_ssize_t out_nbytes
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef uint8_t byte
        cdef int32_t* taken_codes = NULL
        cdef int32_t[::1] taken_codes_view
        cdef float[::1] dictionary_view
        cdef Py_ssize_t dict_size = 0

        if src_null == NULL:
            for i in range(n):
                src_idx = indices[i]
                dst[i] = src[src_idx]
            out.ptr.null_bitmap = NULL
        else:
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

        if self._dict_values != NULL and self._unified_view.selection != NULL:
            dict_size = self._dict_values.length
            if n > 0:
                taken_codes = <int32_t*>malloc(n * sizeof(int32_t))
                if taken_codes == NULL:
                    if out_null != NULL:
                        free(out_null)
                        out.ptr.null_bitmap = NULL
                    raise MemoryError()
            try:
                for i in range(n):
                    src_idx = indices[i]
                    if src_null != NULL:
                        byte = src_null[src_idx >> 3]
                        if (byte & (1 << (src_idx & 7))) == 0:
                            taken_codes[i] = 0
                            continue
                    taken_codes[i] = <int32_t>_read_packed_code(<uint8_t*>self._unified_view.selection, self._unified_view.sel_width, src_idx)

                if n > 0:
                    taken_codes_view = <int32_t[:n]>taken_codes
                else:
                    taken_codes_view = <int32_t[:0]>taken_codes
                if dict_size > 0:
                    dictionary_view = <float[:dict_size]><float*>self._dict_values.data
                else:
                    dictionary_view = <float[:0]><float*>self._dict_values.data
                _attach_dictionary_storage(out, taken_codes_view, dictionary_view, self._dict_ordered != 0)
            finally:
                if taken_codes != NULL:
                    free(taken_codes)
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
        cdef uint8_t* codes
        cdef uint8_t code_width
        cdef uint8_t* match_table
        cdef Py_ssize_t d, i
        cdef const uint8_t* codes8
        cdef const uint16_t* codes16
        cdef const uint32_t* codes32
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

        if uv.selection != NULL:
            dict_size = <Py_ssize_t>uv.data_length
            codes = <uint8_t*>uv.selection
            code_width = uv.sel_width
            match_table = <uint8_t*>malloc(<size_t>dict_size if dict_size > 0 else 1)
            if match_table == NULL:
                raise MemoryError()
            for d in range(dict_size):
                match_table[d] = 1 if dispatch_compare_once(op, data[d], value) else 0
            if code_width == 1:
                codes8 = <const uint8_t*>codes
                for i in range(n):
                    if match_table[codes8[i]]:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif code_width == 2:
                codes16 = <const uint16_t*>codes
                for i in range(n):
                    if match_table[codes16[i]]:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            else:
                codes32 = <const uint32_t*>codes
                for i in range(n):
                    if match_table[codes32[i]]:
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
        # self[i] OP other[i] where self is const V = V OP other[i]
        #   = other[i] reversed_op V, so flip directional ops.
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

        # op dispatch and null-pointer specialisation happen once here.
        # Gate: >~70% nulls → branching kernel (skips work for null rows).
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

    cpdef BoolVector in_list(self, object value_set):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        if uv.data_length == 1:
            n = ptr.length
            nbytes = (n + 7) >> 3
            out = BoolVector(<size_t>n)
            dst = <uint8_t*> out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if uv.validity != NULL:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            if (<float*>uv.data)[0] in value_set and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        cdef float* data = <float*> ptr.data
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

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if data[i] in value_set:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef float sum(self):
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return 0.0
            return self.ptr.length * (<float*>uv.data)[0]
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef double total = 0.0  # accumulate in double to reduce rounding error
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    total += data[i]
        else:
            for i in range(n):
                total += data[i]
        return <float>total

    cpdef float min(self):
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute min of empty column")
            if uv.validity != NULL:
                raise ValueError("Cannot compute min of all-null column")
            return (<float*>uv.data)[0]
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef bint found = False
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        cdef float m = 0.0
        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    m = data[i]
                    found = True
                    break
        else:
            m = data[0]
            found = True
            i = 0

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        for i in range(i + 1, n):
            if null_bitmap != NULL:
                if not _bitmap_is_valid(null_bitmap, i):
                    continue
            if data[i] < m:
                m = data[i]
        return m

    cpdef float max(self):
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute max of empty column")
            if uv.validity != NULL:
                raise ValueError("Cannot compute max of all-null column")
            return (<float*>uv.data)[0]
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef bint found = False
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        cdef float m = 0.0
        if null_bitmap != NULL:
            for i in range(n):
                if _bitmap_is_valid(null_bitmap, i):
                    m = data[i]
                    found = True
                    break
        else:
            m = data[0]
            found = True
            i = 0

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if null_bitmap != NULL:
                if not _bitmap_is_valid(null_bitmap, i):
                    continue
            if data[i] > m:
                m = data[i]
        return m

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef float left_val, right_val

        if uv.data_length == 1:
            return 0

        left_val = data[left_idx]
        right_val = data[right_idx]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t byte

        if uv.data_length == 1:
            return uv.validity != NULL

        if ptr.null_bitmap == NULL:
            return False

        byte = ptr.null_bitmap[idx >> 3]
        return ((byte >> (idx & 7)) & 1) == 0

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
            for i in range(n):
                buf[i] = 1 if uv.validity != NULL else 0
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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if uv.data_length == 1:
            return n if uv.validity != NULL else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
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

        if uv.data_length == 1:
            if uv.validity != NULL:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append((<float*>uv.data)[0])
            return out

        cdef float* data = <float*> ptr.data

        if ptr.null_bitmap == NULL:
            for i in range(n):
                out.append(data[i])
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    out.append(data[i])
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

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.hash_into: output buffer too small")

        cdef float* data = <float*> ptr.data
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t[FLOAT32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint32_t fbits

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

        cdef uint64_t is_valid
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

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        cdef int64_t* dst = dst_base + offset
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i
        cdef float v
        cdef long long rv
        cdef int64_t MIN_SIGNED = <int64_t> -9223372036854775807
        cdef int64_t MAX_SIGNED = <int64_t> 9223372036854775807
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE

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
        cdef Py_ssize_t i, k
        cdef float* data
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        if uv.data_length == 1:
            vals = [None if uv.validity != NULL else (<float*>uv.data)[0]] * k
            return f"<Float32Vector len={buf_length(self.ptr)} values={vals}>"
        data = <float*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Float32Vector len={buf_length(self.ptr)} values={vals}>"


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
            code = _read_packed_code(codes, code_width, i)
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
    """
    cdef Float32Vector vec = Float32Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_FLOAT32
    vec.ptr.itemsize = 4
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    vec._unified_view.data = vec.ptr.data
    vec._unified_view.data_length = <size_t>data.shape[0]
    vec._unified_view.length = <size_t>data.shape[0]
    vec._unified_view.selection = NULL
    vec._unified_view.sel_width = 0
    vec._unified_view.validity = NULL
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
    vec.ptr.length = length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    vec._unified_view.data = data
    vec._unified_view.data_length = <size_t>length
    vec._unified_view.length = <size_t>length
    vec._unified_view.selection = NULL
    vec._unified_view.sel_width = 0
    vec._unified_view.validity = null_bitmap
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
    vec.ptr.length = <size_t> len(array)
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

    vec._unified_view.data = vec.ptr.data
    vec._unified_view.data_length = <size_t>vec.ptr.length
    vec._unified_view.length = <size_t>vec.ptr.length
    vec._unified_view.selection = NULL
    vec._unified_view.sel_width = 0
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec
