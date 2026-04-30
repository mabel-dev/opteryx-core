# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Float32Vector: Cython implementation of a fixed-width float32 column vector for Draken.
"""

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stdint cimport int32_t, int8_t, intptr_t, uint16_t, uint32_t, uint64_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.math cimport isinf, isnan, llround

from draken.core.buffers cimport ConstAccessor
from draken.core.buffers cimport DictAccessor
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from draken.core.buffers cimport DRAKEN_ENCODING_RLE
from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenRLEBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_FLOAT32
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector

DEF FLOAT32_HASH_CHUNK = 1024

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000

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


cdef void _release_rle_storage_float32(Float32Vector vec) noexcept:
    if vec._rle_buffer != NULL:
        if vec._rle_buffer.run_values != NULL:
            free(vec._rle_buffer.run_values)
        if vec._rle_buffer.run_lengths != NULL:
            free(vec._rle_buffer.run_lengths)
        if vec._rle_buffer.null_bitmap != NULL:
            free(vec._rle_buffer.null_bitmap)
        free(vec._rle_buffer)
        vec._rle_buffer = NULL


cdef void _release_dict_storage(Float32Vector vec) noexcept:
    if vec._dict_codes != NULL:
        free(vec._dict_codes)
        vec._dict_codes = NULL
    if vec._dict_values != NULL:
        free_var_buffer(vec._dict_values, True)
        vec._dict_values = NULL
    vec._dict_code_width = 0
    vec._dict_ordered = 0
    vec._dict_accessor.codes = NULL
    vec._dict_accessor.code_width = 0
    vec._dict_accessor.row_nulls = NULL
    vec._dict_accessor.length = 0
    vec._dict_accessor.dict_values = NULL
    vec._dict_accessor.value_type = DRAKEN_FLOAT32
    vec._encoding = DRAKEN_ENCODING_DENSE


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

    _release_dict_storage(vec)

    if code_bytes > 0:
        vec._dict_codes = <uint8_t*>malloc(code_bytes)
        if vec._dict_codes == NULL:
            raise MemoryError()
    else:
        vec._dict_codes = NULL

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
            (<uint8_t*>vec._dict_codes)[i] = <uint8_t>code
        elif code_width == 2:
            (<uint16_t*>vec._dict_codes)[i] = <uint16_t>code
        else:
            (<uint32_t*>vec._dict_codes)[i] = <uint32_t>code

    vec._dict_values = dict_values
    vec._dict_code_width = code_width
    vec._dict_ordered = 1 if ordered else 0
    vec._encoding = DRAKEN_ENCODING_DICTIONARY


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
        cdef Float32Vector vec = Float32Vector(0)

        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null)
        vec._const_value = 0.0 if is_null or value is None else <float>float(value)
        vec._encoding = DRAKEN_ENCODING_CONSTANT
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT32, length, 4)
            self.owns_data = True
        self._dict_values = NULL
        self._dict_codes = NULL
        self._dict_code_width = 0
        self._dict_ordered = 0
        self._dict_accessor.codes = NULL
        self._dict_accessor.code_width = 0
        self._dict_accessor.row_nulls = NULL
        self._dict_accessor.length = 0
        self._dict_accessor.dict_values = NULL
        self._dict_accessor.value_type = DRAKEN_FLOAT32
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_FLOAT32
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._const_value = 0.0
        self._has_const = False
        self._const_is_null = False
        self._rle_buffer = NULL

    def __dealloc__(self):
        _release_dict_storage(self)
        _release_rle_storage_float32(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef DictAccessor* dict_accessor(self) noexcept:
        if self._dict_values == NULL or self._dict_codes == NULL or self.ptr == NULL:
            return NULL
        self._dict_accessor.codes = self._dict_codes
        self._dict_accessor.code_width = self._dict_code_width
        self._dict_accessor.row_nulls = self.ptr.null_bitmap
        self._dict_accessor.length = self.ptr.length
        self._dict_accessor.dict_values = self._dict_values
        self._dict_accessor.value_type = self._dict_values.type
        return &self._dict_accessor

    cdef ConstAccessor* const_accessor(self) noexcept:
        if not self._has_const or self.ptr == NULL:
            return NULL
        self._const_accessor.length = self.ptr.length
        self._const_accessor.value_type = DRAKEN_FLOAT32
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
        return self._dict_code_width if self._dict_values != NULL else None

    @property
    def ordered(self):
        return bool(self._dict_ordered) if self._dict_values != NULL else False

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data
        cdef uint8_t byte
        cdef uint8_t bit
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        if self._has_const:
            if self._const_is_null:
                return None
            return self._const_value
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).__getitem__(i)
        data = <float*> ptr.data
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    cpdef Float32Vector take(self, int32_t[::1] indices):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).take(indices)
        cdef Py_ssize_t i, n = indices.shape[0]
        if self._has_const:
            return Float32Vector.from_constant(
                None if self._const_is_null else self._const_value,
                n,
                is_null=self._const_is_null,
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

        if self._dict_values != NULL and self._dict_codes != NULL:
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
                    taken_codes[i] = <int32_t>_read_packed_code(self._dict_codes, self._dict_code_width, src_idx)

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

    cdef inline bint _compare_float_values(self, float left, float right, int op) nogil:
        if op == 0:
            return left == right
        if op == 1:
            return left != right
        if op == 2:
            return left > right
        if op == 3:
            return left >= right
        if op == 4:
            return left < right
        return left <= right

    cdef BoolVector _compare_scalar(self, float value, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self)._compare_scalar(value, op)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
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
            dst = <uint8_t*> out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out

            matched = self._compare_float_values(self._const_value, value, op)
            if matched and nbytes > 0:
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
                if self._compare_float_values(data[i], value, op):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cdef BoolVector _compare_vector(self, Float32Vector other, int op):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self)._compare_vector(other, op)
        if other._encoding == DRAKEN_ENCODING_RLE:
            return self._compare_vector(_materialize_rle_float32(other), op)
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef float* data1 = <float*> ptr1.data
        cdef float* data2 = <float*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2, valid

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

        for i in range(n):
            valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
            valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
            valid = valid1 and valid2
            if valid:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_float_values(data1[i], data2[i], op):
                    dst[i >> 3] |= (1 << (i & 7))
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
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).in_list(value_set)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        if self._has_const:
            n = ptr.length
            nbytes = (n + 7) >> 3
            out = BoolVector(<size_t>n)
            dst = <uint8_t*> out.ptr.data
            if nbytes > 0:
                memset(dst, 0, nbytes)
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            if self._const_value in value_set and nbytes > 0:
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
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).sum()
        if self._has_const:
            if self._const_is_null:
                return 0.0
            return self.ptr.length * self._const_value
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef double total = 0.0  # accumulate in double to reduce rounding error
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i):
                    continue
            total += data[i]
        return <float>total

    cpdef float min(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).min()
        if self._has_const:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute min of empty column")
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null column")
            return self._const_value
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef bint found = False
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        cdef float m = 0.0
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i):
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i):
                    continue
            if data[i] < m:
                m = data[i]
        return m

    cpdef float max(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).max()
        if self._has_const:
            if self.ptr.length == 0:
                raise ValueError("Cannot compute max of empty column")
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null column")
            return self._const_value
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef bint found = False
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        cdef float m = 0.0
        for i in range(n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i):
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                if not _bitmap_is_valid(ptr.null_bitmap, i):
                    continue
            if data[i] > m:
                m = data[i]
        return m

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).compare_at(left_idx, right_idx)
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef float* data = <float*> ptr.data
        cdef float left_val, right_val

        if self._has_const:
            return 0

        left_val = data[left_idx]
        right_val = data[right_idx]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint8_t rle_byte
        cdef uint8_t byte

        if self._has_const:
            return self._const_is_null

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                return False
            rle_byte = self._rle_buffer.null_bitmap[idx >> 3]
            return ((rle_byte >> (idx & 7)) & 1) == 0

        if ptr.null_bitmap == NULL:
            return False

        byte = ptr.null_bitmap[idx >> 3]
        return ((byte >> (idx & 7)) & 1) == 0

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if self._encoding == DRAKEN_ENCODING_RLE:
            if self._rle_buffer.null_bitmap == NULL:
                for i in range(n):
                    buf[i] = 0
            else:
                for i in range(n):
                    byte = self._rle_buffer.null_bitmap[i >> 3]
                    buf[i] = 0 if ((byte >> (i & 7)) & 1) else 1
            return <int8_t[:n]> buf

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

    cpdef int8_t[::1] is_null_with_nan(self):
        if self._encoding == DRAKEN_ENCODING_RLE:
            return _materialize_rle_float32(self).is_null_with_nan()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        cdef float* data

        if buf == NULL:
            raise MemoryError()

        if self._has_const:
            for i in range(n):
                buf[i] = 1 if self._const_is_null else 0
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

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t data_bytes, bm_bytes
        if self._has_const:
            return buf_itemsize(ptr)
        data_bytes = <Py_ssize_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef float* rle_vals_f
        cdef int32_t* rle_lens_f
        cdef size_t rle_runs_f
        cdef uint8_t* rle_nulls_f
        cdef Py_ssize_t fpos
        cdef size_t fr
        cdef int32_t frun_len
        cdef float frun_val

        if self._encoding == DRAKEN_ENCODING_RLE:
            rle_vals_f = <float*>self._rle_buffer.run_values
            rle_lens_f = self._rle_buffer.run_lengths
            rle_runs_f = self._rle_buffer.num_runs
            rle_nulls_f = self._rle_buffer.null_bitmap
            fpos = 0
            for fr in range(rle_runs_f):
                frun_val = rle_vals_f[fr]
                frun_len = rle_lens_f[fr]
                for i in range(frun_len):
                    if rle_nulls_f != NULL and not ((rle_nulls_f[(fpos + i) >> 3] >> ((fpos + i) & 7)) & 1):
                        out.append(None)
                    else:
                        out.append(frun_val)
                fpos += frun_len
            return out

        if self._has_const:
            if self._const_is_null:
                for i in range(n):
                    out.append(None)
            else:
                for i in range(n):
                    out.append(self._const_value)
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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t value

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.hash_into: output buffer too small")

        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_float32(self).hash_into(out_buf, offset)
            return

        cdef float* data = <float*> ptr.data
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t[FLOAT32_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint32_t fbits

        if self._has_const:
            if self._const_is_null:
                value = NULL_HASH
            else:
                fbits = (<uint32_t*>&self._const_value)[0]
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
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float32Vector.compress: output buffer too small")

        if self._encoding == DRAKEN_ENCODING_RLE:
            _materialize_rle_float32(self).compress_into(out_buf, offset)
            return

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

        if self._has_const:
            for i in range(n):
                if self._const_is_null:
                    dst[i] = NULL_FLAG
                    continue
                v = self._const_value
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
        cdef list vals = []
        cdef Py_ssize_t i, k
        cdef float* data
        if self._encoding == DRAKEN_ENCODING_RLE:
            vals = self.to_pylist()[:10]
            return f"<Float32Vector(RLE) len={self._rle_buffer.length} values={vals}>"
        k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        if self._has_const:
            vals = [None if self._const_is_null else self._const_value] * k
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


cdef Float32Vector _materialize_rle_float32(Float32Vector rle_vec):
    """Expand RLE Float32Vector to dense."""
    cdef size_t total = rle_vec._rle_buffer.length
    cdef Float32Vector dense = Float32Vector(<size_t>total)
    cdef float* rle_vals = <float*>rle_vec._rle_buffer.run_values
    cdef int32_t* rle_lens = rle_vec._rle_buffer.run_lengths
    cdef size_t num_runs = rle_vec._rle_buffer.num_runs
    cdef uint8_t* rle_nulls = rle_vec._rle_buffer.null_bitmap
    cdef float* dst = <float*>dense.ptr.data
    cdef size_t pos = 0, r
    cdef int32_t run_len
    cdef Py_ssize_t j
    cdef size_t null_bytes
    cdef uint8_t* null_copy
    for r in range(num_runs):
        run_len = rle_lens[r]
        for j in range(run_len):
            dst[pos + j] = rle_vals[r]
        pos += <size_t>run_len
    if rle_nulls != NULL:
        null_bytes = (total + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            raise MemoryError()
        memcpy(null_copy, rle_nulls, null_bytes)
        dense.ptr.null_bitmap = null_copy
    return dense


cdef Float32Vector from_rle_builder(
    float* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    uint8_t* null_bitmap=NULL,
):
    """Create RLE-encoded Float32Vector from raw C arrays (copies data)."""
    cdef Float32Vector vec = Float32Vector(0, False)
    cdef size_t total_length = 0, i
    cdef DrakenRLEBuffer* rle
    cdef float* vals_copy
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
    vals_copy = <float*>malloc(num_runs * sizeof(float))
    lens_copy = <int32_t*>malloc(num_runs * sizeof(int32_t))
    if vals_copy == NULL or lens_copy == NULL:
        free(rle)
        if vals_copy != NULL: free(vals_copy)
        if lens_copy != NULL: free(lens_copy)
        raise MemoryError()
    memcpy(vals_copy, run_values, num_runs * sizeof(float))
    memcpy(lens_copy, run_lengths, num_runs * sizeof(int32_t))
    rle.run_values = <void*>vals_copy
    rle.run_lengths = lens_copy
    rle.num_runs = num_runs
    rle.length = total_length
    rle.type = DRAKEN_FLOAT32
    if null_bitmap != NULL:
        null_bytes = (total_length + 7) >> 3
        null_copy = <uint8_t*>malloc(null_bytes)
        if null_copy == NULL:
            free(vals_copy); free(lens_copy); free(rle)
            raise MemoryError()
        memcpy(null_copy, null_bitmap, null_bytes)
        rle.null_bitmap = null_copy
    else:
        rle.null_bitmap = NULL
    vec._rle_buffer = rle
    vec._encoding = DRAKEN_ENCODING_RLE
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

    return vec
