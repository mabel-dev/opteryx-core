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
Float64Vector: Cython implementation of a fixed-width float64 column vector for Draken.

This module provides:
- The Float64Vector class for efficient float64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for float64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset, memcpy

from libc.stdint cimport int32_t, int8_t, intptr_t, uint16_t, uint32_t, uint64_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.math cimport isinf, isnan, llround

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_FLOAT64
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount

cdef extern from "simd_hash.h" nogil:
    void simd_hash_i64(const uint64_t* src, uint64_t* dst, size_t count)

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

from draken.vectors.bool_vector cimport BoolVector

cdef extern from "draken/vectors/_float64_reductions.hpp" namespace "draken::float64_red" nogil:
    double sum_nonnull(const double* data, size_t n)
    double sum_nullable_branchless(const double* data, const uint8_t* nulls, size_t n)
    double min_nonnull(const double* data, size_t n)
    double max_nonnull(const double* data, size_t n)
    size_t min_nullable_branchless(const double* data, const uint8_t* nulls, size_t n, double* out_min)
    size_t max_nullable_branchless(const double* data, const uint8_t* nulls, size_t n, double* out_max)

cdef extern from "draken/vectors/_float64_compare.hpp" namespace "draken::float64_cmp" nogil:
    bint dispatch_compare_once(int op, double a, double b)
    void dispatch_scalar_nonnull(int op, const double* data, double value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless(int op, const double* data, double value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching(int op, const double* data, double value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull(int op, const double* a, const double* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless(int op, const double* a, const double* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching(int op, const double* a, const double* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless(int op, const double* a, const double* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching(int op, const double* a, const double* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

DEF FLOAT64_HASH_CHUNK = 1024

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


cdef void _release_dict_storage(Float64Vector vec) noexcept:
    if vec._unified_view.selection != NULL:
        free(vec._unified_view.selection)
        vec._unified_view.selection = NULL
        vec._unified_view.sel_width = 0
    if vec._dict_values != NULL:
        free_var_buffer(vec._dict_values, True)
        vec._dict_values = NULL
    vec._dict_ordered = 0


cdef void _attach_dictionary_storage(Float64Vector vec, const int32_t[::1] codes, const double[::1] dictionary, bint ordered, const uint8_t* dict_entry_null_bitmap=NULL) except *:
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef uint8_t code_width = _dict_code_width_for_size(dict_size)
    cdef Py_ssize_t code_bytes = row_count * code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(double)
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t bitmap_bytes

    _release_dict_storage(vec)

    cdef uint8_t* codes_ptr = NULL
    if code_bytes > 0:
        codes_ptr = <uint8_t*>malloc(code_bytes)
        if codes_ptr == NULL:
            raise MemoryError()

    dict_values = alloc_var_buffer(DRAKEN_FLOAT64, <size_t>dict_size, <size_t>dict_bytes)
    dict_values.offsets[0] = 0
    for i in range(dict_size):
        dict_values.offsets[i + 1] = <int32_t>((i + 1) * sizeof(double))
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>&dictionary[0], <size_t>dict_bytes)

    # Copy dictionary entry-level null bitmap if provided
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
    vec._unified_view.selection = codes_ptr
    vec._unified_view.sel_width = code_width
    vec._unified_view.data = dict_values.data
    vec._unified_view.data_length = <size_t>dict_size

cdef class Float64Vector(Vector):

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None):
        from array import array as pyarray

        cdef int32_t[::1] codes_view
        cdef double[::1] dictionary_view
        cdef uint8_t[::1] validity_view

        if not isinstance(codes, memoryview):
            codes = pyarray("i", codes)
        if not isinstance(dictionary, memoryview):
            dictionary = pyarray("d", dictionary)

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
        cdef Float64Vector vec = Float64Vector(1)
        cdef double val = 0.0 if (is_null or value is None) else <double>float(value)
        (<double*>vec.ptr.data)[0] = val
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
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT64, length, 8)
            self.owns_data = True
        self._dict_values = NULL
        self._dict_ordered = 0
        self._unified_view.data = NULL
        self._unified_view.data_length = 0
        self._unified_view.selection = NULL
        self._unified_view.sel_width = 0
        self._unified_view.length = 0
        self._unified_view.validity = NULL
        self._unified_view.itemsize = sizeof(double)
        self._unified_view.type = DRAKEN_FLOAT64
        if not wrap:
            self._unified_view.data = self.ptr.data
            self._unified_view.data_length = <size_t>length
            self._unified_view.length = <size_t>length
            self._unified_view.validity = NULL

    def __dealloc__(self):
        _release_dict_storage(self)
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        cdef DrakenVector* uv = &self._unified_view
        if uv.selection != NULL:
            return NULL
        if uv.data_length == 1 and uv.length != 1:
            return NULL
        return uv.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        cdef DrakenVector* uv = &self._unified_view
        if uv.data_length == 1 and uv.selection == NULL and uv.length != 1:
            return NULL
        return self.ptr.null_bitmap

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n):
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* null_bm = NULL
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

    # Python-friendly properties
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

    cdef object item_at(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data
        cdef uint8_t byte
        cdef uint8_t bit
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if uv.data_length == 1:
            if uv.validity != NULL:
                return None
            return (<double*>uv.data)[0]
        if uv.selection != NULL:
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                return None
            return (<double*>uv.data)[<Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, i)]
        data = <double*> ptr.data
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        return self.item_at(i)

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa

        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return pa.nulls(uv.length, type=pa.float64())
            return pa.array([(<double*>uv.data)[0]] * uv.length, type=pa.float64())

        cdef size_t nbytes = buf_length(self.ptr) * sizeof(double)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.float64(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Float64Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Py_ssize_t out_n_d = n
        cdef uint8_t cw_d
        cdef DrakenVarBuffer* src_dv
        cdef Py_ssize_t dsz
        cdef uint8_t* gc = NULL
        cdef uint8_t* gn = NULL
        cdef Py_ssize_t cb_d
        cdef Py_ssize_t nb_d
        cdef Py_ssize_t si_d
        cdef Float64Vector dtake_result
        if uv.selection != NULL:
            # O(n) gather: copy selected codes and copy dict verbatim.
            # Replaces the O(total_rows) _materialize_dict_float64().take() path.
            cw_d = uv.sel_width
            src_dv = self._dict_values
            dsz = <Py_ssize_t>src_dv.length if src_dv != NULL else 0
            cb_d = out_n_d * <Py_ssize_t>cw_d
            if cb_d > 0:
                gc = <uint8_t*>malloc(<size_t>cb_d)
                if gc == NULL:
                    raise MemoryError()
                if cw_d == 1:
                    for i in range(out_n_d):
                        (<uint8_t*>gc)[i] = (<const uint8_t*>uv.selection)[indices[i]]
                elif cw_d == 2:
                    for i in range(out_n_d):
                        (<uint16_t*>gc)[i] = (<const uint16_t*>uv.selection)[indices[i]]
                else:
                    for i in range(out_n_d):
                        (<uint32_t*>gc)[i] = (<const uint32_t*>uv.selection)[indices[i]]
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
                dtake_result = make_float64_dict_only(
                    gc,
                    cw_d, out_n_d,
                    <const double*>src_dv.data if src_dv != NULL else NULL,
                    dsz,
                    gn,
                )
            finally:
                if gc != NULL: free(gc)
                if gn != NULL: free(gn)
            return dtake_result
        cdef DrakenVector* _tuv = self.unified()
        if _tuv.data_length == 1:
            return Float64Vector.from_constant(
                None if _tuv.validity != NULL else (<double*>_tuv.data)[0],
                n,
                is_null=(_tuv.validity != NULL),
            )
        cdef Float64Vector out = Float64Vector(<size_t>n)
        cdef double* src = <double*> self.ptr.data
        cdef double* dst = <double*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap
        cdef Py_ssize_t out_nbytes
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef uint8_t byte
        cdef int32_t* taken_codes = NULL
        cdef int32_t[::1] taken_codes_view
        cdef double[::1] dictionary_view
        cdef Py_ssize_t dict_size = 0

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

        if self._dict_values != NULL and uv.selection != NULL:
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
                    taken_codes[i] = <int32_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, src_idx)

                if n > 0:
                    taken_codes_view = <int32_t[:n]>taken_codes
                else:
                    taken_codes_view = <int32_t[:0]>taken_codes
                if dict_size > 0:
                    dictionary_view = <double[:dict_size]><double*>self._dict_values.data
                else:
                    dictionary_view = <double[:0]><double*>self._dict_values.data
                _attach_dictionary_storage(out, taken_codes_view, dictionary_view, self._dict_ordered != 0)
            finally:
                if taken_codes != NULL:
                    free(taken_codes)
        out._unified_view.length = <size_t>n
        out._unified_view.validity = out.ptr.null_bitmap
        return out

    cdef inline bint _compare_float_values(self, double left, double right, int op) nogil:
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

    cpdef BoolVector _compare_scalar(self, double value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef bint matched
        cdef double* data = <double*>uv.data
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

    cpdef BoolVector _compare_vector(self, Float64Vector other, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int reversed_op
        cdef double* data1
        cdef double* data2
        cdef uint8_t* null1
        cdef uint8_t* null2
        cdef Py_ssize_t nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef size_t valid1_cnt, valid2_cnt, min_valid
        cdef bint use_branching = False

        # Const fast paths: avoid O(n) materialisation.
        if uv.data_length == 1:
            if n != <Py_ssize_t>ouv.length:
                raise ValueError("Vectors must have the same length")
            if uv.validity != NULL:
                return self._make_all_null_bool(n)
            # self[i] OP other[i] where self is const V = V OP other[i]
            #   = other[i] reversed_op V, so flip directional ops.
            if op == 2:   reversed_op = 4
            elif op == 3: reversed_op = 5
            elif op == 4: reversed_op = 2
            elif op == 5: reversed_op = 3
            else:         reversed_op = op
            return other._compare_scalar((<double*>uv.data)[0], reversed_op)

        if ouv.data_length == 1:
            if n != <Py_ssize_t>ouv.length:
                raise ValueError("Vectors must have the same length")
            if ouv.validity != NULL:
                return self._make_all_null_bool(n)
            return self._compare_scalar((<double*>ouv.data)[0], op)

        # For dict-encoded on either side: materialize then compare
        if uv.selection != NULL:
            return self.materialize()._compare_vector(other, op)
        if ouv.selection != NULL:
            return self._compare_vector(other.materialize(), op)

        data1 = <double*>self.ptr.data
        data2 = <double*>other.ptr.data
        null1 = self.ptr.null_bitmap
        null2 = other.ptr.null_bitmap
        nbytes = (n + 7) >> 3
        if n != <Py_ssize_t>other.ptr.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        memset(dst, 0, nbytes)

        if (null1 != NULL or null2 != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        if n > 0 and (null1 != NULL or null2 != NULL):
            valid1_cnt = simd_popcount(null1, <size_t>nbytes) if null1 != NULL else <size_t>n
            valid2_cnt = simd_popcount(null2, <size_t>nbytes) if null2 != NULL else <size_t>n
            min_valid = valid1_cnt if valid1_cnt < valid2_cnt else valid2_cnt
            use_branching = (min_valid * 10) < (<size_t>n * 3)

        # Op dispatched once at the C++ boundary; the inner kernel is templated
        # so the compiler sees a single compile-time comparison per row.
        if null1 == NULL and null2 == NULL:
            dispatch_vector_nonnull(op, data1, data2, dst, <size_t>n)
        elif null1 != NULL and null2 == NULL:
            if use_branching:
                dispatch_vector_one_null_branching(op, data1, data2, null1, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branchless(op, data1, data2, null1, dst, out_null, <size_t>n)
        elif null1 == NULL and null2 != NULL:
            if use_branching:
                dispatch_vector_one_null_branching(op, data1, data2, null2, dst, out_null, <size_t>n)
            else:
                dispatch_vector_one_null_branchless(op, data1, data2, null2, dst, out_null, <size_t>n)
        else:
            if use_branching:
                dispatch_vector_both_null_branching(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
            else:
                dispatch_vector_both_null_branchless(op, data1, data2, null1, null2, dst, out_null, <size_t>n)
        return out

    cpdef BoolVector equals(self, double value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, double value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, double value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Float64Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, double value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, double value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Float64Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, double value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Float64Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, double lower, double upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef double* data
        cdef uint8_t* src_null
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t i
        cdef uint8_t mask
        cdef bint in_range
        cdef Py_ssize_t dict_size, d, match_idx
        cdef uint8_t* match_table
        cdef uint8_t* codes
        cdef uint8_t sel_width
        cdef double v

        if uv.selection != NULL:
            data = <double*>uv.data
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
            codes = <uint8_t*>uv.selection
            sel_width = uv.sel_width
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
                    match_idx = <Py_ssize_t>_read_packed_code(codes, sel_width, i)
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
            data = <double*>uv.data
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

        data = <double*>uv.data
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
        cdef double* data
        cdef uint8_t* src_null
        cdef Py_ssize_t i
        cdef Py_ssize_t dict_size, d, match_idx
        cdef uint8_t* match_table_il
        cdef uint8_t* codes_il
        cdef uint8_t sel_width_il

        if uv.selection != NULL:
            if not isinstance(value_set, (set, frozenset)):
                value_set = set(value_set)
            data = <double*>uv.data
            dict_size = <Py_ssize_t>uv.data_length
            src_null = uv.validity
            match_table_il = <uint8_t*>malloc(<size_t>dict_size if dict_size > 0 else 1)
            if match_table_il == NULL:
                raise MemoryError()
            for d in range(dict_size):
                match_table_il[d] = 1 if data[d] in value_set else 0
            codes_il = <uint8_t*>uv.selection
            sel_width_il = uv.sel_width
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
                    match_idx = <Py_ssize_t>_read_packed_code(codes_il, sel_width_il, i)
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
            data = <double*>uv.data
            if data[0] in value_set and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        data = <double*>uv.data
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

    cpdef double sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef double* data = <double*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i
        cdef uint32_t code
        cdef double total = 0.0
        cdef uint8_t* codes
        cdef uint8_t code_width

        if uv.data_length == 1:
            if uv.validity != NULL:
                return 0.0
            return n * data[0]

        if uv.selection != NULL:
            codes = <uint8_t*>uv.selection
            code_width = uv.sel_width
            if uv.validity == NULL:
                with nogil:
                    for i in range(n):
                        code = _read_packed_code(codes, code_width, i)
                        total += data[code]
            else:
                with nogil:
                    for i in range(n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            code = _read_packed_code(codes, code_width, i)
                            total += data[code]
            return total

        if n == 0:
            return 0.0
        if uv.validity == NULL:
            return sum_nonnull(data, <size_t>n)
        return sum_nullable_branchless(data, uv.validity, <size_t>n)

    cpdef double min(self):
        cdef DrakenVector* uv = self.unified()
        cdef double* data = <double*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef size_t valid_count
        cdef double out
        cdef Py_ssize_t i, start
        cdef uint32_t code
        cdef double m
        cdef bint seen
        cdef uint8_t* codes
        cdef uint8_t code_width

        if uv.data_length == 1:
            if n == 0 or uv.validity != NULL:
                raise ValueError("Cannot compute min of empty or all-null column")
            return data[0]

        if uv.selection != NULL:
            if n == 0:
                raise ValueError("Cannot compute min of empty column")
            codes = <uint8_t*>uv.selection
            code_width = uv.sel_width
            seen = False
            if uv.validity == NULL:
                code = _read_packed_code(codes, code_width, 0)
                m = data[code]
                seen = True
                start = 1
                with nogil:
                    for i in range(start, n):
                        code = _read_packed_code(codes, code_width, i)
                        if data[code] < m:
                            m = data[code]
            else:
                start = 0
                for i in range(n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        code = _read_packed_code(codes, code_width, i)
                        m = data[code]
                        seen = True
                        start = i + 1
                        break
                if not seen:
                    raise ValueError("Cannot compute min of all-null column")
                with nogil:
                    for i in range(start, n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            code = _read_packed_code(codes, code_width, i)
                            if data[code] < m:
                                m = data[code]
            return m

        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        if uv.validity == NULL:
            return min_nonnull(data, <size_t>n)
        valid_count = min_nullable_branchless(data, uv.validity, <size_t>n, &out)
        if valid_count == 0:
            raise ValueError("Cannot compute min of all-null column")
        return out

    cpdef double max(self):
        cdef DrakenVector* uv = self.unified()
        cdef double* data = <double*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef size_t valid_count
        cdef double out
        cdef Py_ssize_t i, start
        cdef uint32_t code
        cdef double m
        cdef bint seen
        cdef uint8_t* codes
        cdef uint8_t code_width

        if uv.data_length == 1:
            if n == 0 or uv.validity != NULL:
                raise ValueError("Cannot compute max of empty or all-null column")
            return data[0]

        if uv.selection != NULL:
            if n == 0:
                raise ValueError("Cannot compute max of empty column")
            codes = <uint8_t*>uv.selection
            code_width = uv.sel_width
            seen = False
            if uv.validity == NULL:
                code = _read_packed_code(codes, code_width, 0)
                m = data[code]
                seen = True
                start = 1
                with nogil:
                    for i in range(start, n):
                        code = _read_packed_code(codes, code_width, i)
                        if data[code] > m:
                            m = data[code]
            else:
                start = 0
                for i in range(n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        code = _read_packed_code(codes, code_width, i)
                        m = data[code]
                        seen = True
                        start = i + 1
                        break
                if not seen:
                    raise ValueError("Cannot compute max of all-null column")
                with nogil:
                    for i in range(start, n):
                        if (uv.validity[i >> 3] >> (i & 7)) & 1:
                            code = _read_packed_code(codes, code_width, i)
                            if data[code] > m:
                                m = data[code]
            return m

        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        if uv.validity == NULL:
            return max_nonnull(data, <size_t>n)
        valid_count = max_nullable_branchless(data, uv.validity, <size_t>n, &out)
        if valid_count == 0:
            raise ValueError("Cannot compute max of all-null column")
        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare two values at given indices. Returns -1, 0, 1. Assumes non-null."""
        cdef DrakenVector* uv = self.unified()
        cdef double left_val, right_val
        cdef double* data

        if uv.data_length == 1:
            return 0

        if uv.selection != NULL:
            data = <double*>uv.data
            left_val = data[<Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, left_idx)]
            right_val = data[<Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, right_idx)]
            if left_val < right_val:
                return -1
            elif left_val > right_val:
                return 1
            return 0

        data = <double*>uv.data

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
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if uv.data_length == 1:
            # const: null if validity bitmap is set
            for i in range(n):
                buf[i] = 1 if uv.validity != NULL else 0
            return <int8_t[:n]> buf

        if uv.validity == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = uv.validity[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

        return <int8_t[:n]> buf

    cpdef int8_t[::1] is_null_with_nan(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null OR NaN, 0 otherwise.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        cdef double* data
        cdef Py_ssize_t idx

        if buf == NULL:
            raise MemoryError()

        if uv.data_length == 1:
            # const: null if validity bitmap is set; otherwise check single value for NaN
            data = <double*>uv.data
            for i in range(n):
                buf[i] = 1 if (uv.validity != NULL or isnan(data[0])) else 0
            return <int8_t[:n]> buf

        if uv.selection != NULL:
            data = <double*>uv.data
            for i in range(n):
                if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                    buf[i] = 1
                else:
                    idx = <Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, i)
                    buf[i] = 1 if isnan(data[idx]) else 0
            return <int8_t[:n]> buf

        data = <double*>uv.data

        if uv.validity == NULL:
            # No explicit nulls, but check for NaN
            for i in range(n):
                buf[i] = 1 if isnan(data[i]) else 0
        else:
            # Check both null bitmap and NaN
            for i in range(n):
                byte = uv.validity[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit == 0:  # null bitmap says invalid
                    buf[i] = 1
                elif isnan(data[i]):  # value is NaN
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
        """Return a dense Float64Vector, expanding dict/const encodings if needed."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Float64Vector dense
        cdef double* dst
        cdef double* mat_src
        cdef uint8_t* mat_null
        cdef Py_ssize_t i, nb_bytes

        if uv.selection != NULL:
            dense = Float64Vector(<size_t>n)
            dst = <double*>dense.ptr.data
            mat_src = <double*>uv.data
            mat_null = uv.validity
            for i in range(n):
                if mat_null != NULL and not ((mat_null[i >> 3] >> (i & 7)) & 1):
                    dst[i] = 0.0
                else:
                    dst[i] = mat_src[<Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, i)]
            if mat_null != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memcpy(dense.ptr.null_bitmap, mat_null, <size_t>nb_bytes)
            dense._unified_view.length = <size_t>n
            dense._unified_view.validity = dense.ptr.null_bitmap
            return dense

        if uv.data_length == 1:
            dense = Float64Vector(<size_t>n)
            dst = <double*>dense.ptr.data
            if uv.validity != NULL:
                nb_bytes = (n + 7) >> 3
                dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
                if dense.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(dense.ptr.null_bitmap, 0, <size_t>nb_bytes)
                memset(dst, 0, <size_t>n * sizeof(double))
            else:
                for i in range(n):
                    dst[i] = (<double*>uv.data)[0]
                dense.ptr.null_bitmap = NULL
            dense._unified_view.length = <size_t>n
            dense._unified_view.validity = dense.ptr.null_bitmap
            return dense

        return self

        return self

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = <uint64_t>uv.length
        cdef uint64_t dict_bytes, code_bytes, null_bytes, data_bytes, bm_bytes
        if uv.data_length == 1:
            return 8  # sizeof(double)
        if uv.selection != NULL:
            dict_bytes = uv.data_length * 8
            code_bytes = n * uv.sel_width
            null_bytes = (n + 7) >> 3 if uv.validity != NULL else 0
            return dict_bytes + code_bytes + null_bytes
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef double* data = <double*>uv.data
        cdef list out = []
        cdef Py_ssize_t idx
        cdef uint8_t byte, bit

        if uv.data_length == 1:
            if uv.validity != NULL:
                return [None] * n
            return [data[0]] * n

        if uv.selection != NULL:
            for i in range(n):
                if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                    out.append(None)
                else:
                    idx = <Py_ssize_t>_read_packed_code(<uint8_t*>uv.selection, uv.sel_width, i)
                    out.append(data[idx])
            return out

        if uv.validity == NULL:
            for i in range(n):
                out.append(data[i])
        else:
            for i in range(n):
                byte = uv.validity[i >> 3]
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
        cdef uint64_t* dst
        cdef uint64_t[FLOAT64_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef double* data
        cdef uint64_t* bits
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef uint64_t is_valid
        cdef double* _dict_data
        cdef uint64_t* _dict_bits
        cdef uint8_t* _dict_codes_h
        cdef uint8_t  _dict_cw_h
        cdef uint8_t* _dict_nb_h
        cdef double   _dv

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float64Vector.hash_into: output buffer too small")

        dst = &out_buf[offset]

        if uv.selection != NULL:
            _dict_data    = <double*>uv.data
            _dict_bits    = <uint64_t*>_dict_data
            _dict_codes_h = <uint8_t*>uv.selection
            _dict_cw_h    = uv.sel_width
            _dict_nb_h    = uv.validity
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    if _dict_nb_h != NULL and not ((_dict_nb_h[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = _dict_bits[
                            <Py_ssize_t>_read_packed_code(_dict_codes_h, _dict_cw_h, i + j)
                        ]
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        if uv.data_length == 1:
            value = NULL_HASH if uv.validity != NULL else (<uint64_t*>uv.data)[0]
            for j in range(FLOAT64_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
            return

        data = <double*> ptr.data
        bits = <uint64_t*> data
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (bits[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
        else:
            simd_mix_hash(dst, bits, <size_t>n)
            return

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = &self._unified_view
        cdef Py_ssize_t i, j, block
        cdef uint64_t value, is_valid
        cdef uint64_t[FLOAT64_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef uint64_t* _cd_dict_bits
        cdef uint8_t* _cd_dict_codes
        cdef uint8_t  _cd_dict_cw
        cdef uint8_t* _cd_null_bitmap

        if n == 0:
            return 0

        if uv.data_length == 1:
            value = NULL_HASH if uv.validity != NULL else (<uint64_t*>uv.data)[0]
            for j in range(FLOAT64_HASH_CHUNK):
                scratch[j] = value
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        # DICTIONARY-only path
        if uv.selection != NULL:
            _cd_dict_bits  = <uint64_t*>uv.data
            _cd_dict_codes = <uint8_t*>uv.selection
            _cd_dict_cw    = uv.sel_width
            _cd_null_bitmap = ptr.null_bitmap
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    if _cd_null_bitmap != NULL and not ((_cd_null_bitmap[(i+j) >> 3] >> ((i+j) & 7)) & 1):
                        scratch[j] = NULL_HASH
                    else:
                        scratch[j] = _cd_dict_bits[
                            <Py_ssize_t>_read_packed_code(_cd_dict_codes, _cd_dict_cw, i + j)
                        ]
                simd_mix_hash(out + i, scratch_ptr, <size_t>block)
                i += block
            return 0

        cdef double* data = <double*> ptr.data
        cdef uint64_t* bits = <uint64_t*> data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > FLOAT64_HASH_CHUNK:
                    block = FLOAT64_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (bits[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(out + i, scratch_ptr, <size_t> block)
                i += block
        else:
            simd_mix_hash(out, bits, <size_t>n)
        return 0

    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Single-column hash for COUNT(DISTINCT): no prior dest state, no memset."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = &self._unified_view
        cdef double* data
        cdef uint64_t* bits
        cdef uint8_t* null_bitmap
        cdef Py_ssize_t i, j, block
        cdef uint64_t is_valid, v
        cdef uint64_t[1024] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return 0

        if uv.data_length == 1:
            if uv.validity != NULL:
                v = NULL_HASH * MIX_HASH_CONSTANT + 1
            else:
                v = (<uint64_t*>uv.data)[0] * MIX_HASH_CONSTANT + 1
            v ^= v >> 32
            for i in range(n):
                out[i] = v
            return 0

        if uv.selection != NULL:
            memset(out, 0, <size_t>n * sizeof(uint64_t))
            return self.c_hash_into(out, n)

        data = <double*>ptr.data
        bits = <uint64_t*>data
        null_bitmap = ptr.null_bitmap

        if null_bitmap == NULL:
            simd_hash_i64(bits, out, <size_t>n)
        else:
            # Blocked approach: fill scratch with null-masked values, then
            # SIMD-hash the block — matches c_hash_into performance on nullable cols.
            i = 0
            while i < n:
                block = n - i
                if block > 1024:
                    block = 1024
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (bits[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_hash_i64(scratch_ptr, out + i, <size_t>block)
                i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for Float64Vector with NaN/Inf handling and clamping."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t* dst_base
        cdef int64_t* dst
        cdef Py_ssize_t i
        cdef double v
        cdef long long rv
        cdef int64_t MIN_SIGNED = <int64_t> -9223372036854775807
        cdef int64_t MAX_SIGNED = <int64_t> 9223372036854775807
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE
        cdef uint8_t* null_bitmap
        cdef bint has_nulls
        cdef double* _ci_dict_data
        cdef uint8_t* _ci_dict_codes
        cdef uint8_t  _ci_dict_cw

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float64Vector.compress: output buffer too small")

        dst_base = &out_buf[0]
        dst = dst_base + offset

        if uv.selection != NULL:
            _ci_dict_data  = <double*>uv.data
            _ci_dict_codes = <uint8_t*>uv.selection
            _ci_dict_cw    = uv.sel_width
            null_bitmap    = ptr.null_bitmap
            for i in range(n):
                if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                    dst[i] = NULL_FLAG
                    continue
                v = _ci_dict_data[<Py_ssize_t>_read_packed_code(_ci_dict_codes, _ci_dict_cw, i)]
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
                v = (<double*>uv.data)[0]
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

        cdef double* data = <double*> ptr.data
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
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        if uv.data_length == 1:
            vals = [None if uv.validity != NULL else (<double*>uv.data)[0]] * k
            return f"<Float64Vector len={buf_length(self.ptr)} values={vals}>"
        cdef double* data = <double*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Float64Vector len={buf_length(self.ptr)} values={vals}>"


cdef Float64Vector _materialize_dict_float64(Float64Vector vec):
    """Expand a dict-only Float64Vector to a dense Float64Vector (no src ptr.data needed)."""
    cdef DrakenVector* src_uv = vec.unified()
    if vec._dict_values == NULL or src_uv.selection == NULL:
        raise ValueError("Dictionary encoding not properly initialized")

    cdef Py_ssize_t n = <Py_ssize_t>vec.ptr.length
    cdef Float64Vector dense = Float64Vector(<size_t>n)
    cdef double* dst = <double*>dense.ptr.data
    cdef double* dict_data = <double*>src_uv.data
    cdef uint8_t* codes = <uint8_t*>src_uv.selection
    cdef uint8_t code_width = src_uv.sel_width
    cdef uint8_t* null_bitmap = vec.ptr.null_bitmap
    cdef Py_ssize_t i, dict_size = <Py_ssize_t>vec._dict_values.length
    cdef uint32_t code
    cdef Py_ssize_t nb_bytes

    for i in range(n):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            dst[i] = 0.0
        else:
            code = _read_packed_code(codes, code_width, i)
            if code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: code {code} >= dict_size {dict_size}")
            dst[i] = dict_data[code]

    if null_bitmap != NULL:
        nb_bytes = (n + 7) >> 3
        dense.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if dense.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(dense.ptr.null_bitmap, null_bitmap, <size_t>nb_bytes)

    dense._unified_view.length = <size_t>n
    dense._unified_view.validity = dense.ptr.null_bitmap
    return dense


cdef Float64Vector make_float64_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* valid_bits,
):
    """Create a dictionary-encoded Float64Vector with no dense materialization.

    Args:
        codes:       Packed code array (code_width bytes per row, row_count entries).
        code_width:  Bytes per code: 1, 2, or 4.
        row_count:   Total number of rows.
        dictionary:  Array of unique double values (dict_size entries).
        dict_size:   Number of unique dictionary values.
        valid_bits:  Arrow-style validity bitmap (1=valid, 0=null); NULL if non-nullable.

    Returns:
        Dictionary-encoded Float64Vector; selection != NULL, data holds dict values.
    """
    cdef Float64Vector vec = Float64Vector(0)   # allocates ptr header; no dense data buffer
    cdef Py_ssize_t code_bytes = row_count * <Py_ssize_t>code_width
    cdef Py_ssize_t dict_bytes = dict_size * sizeof(double)
    cdef Py_ssize_t nb_bytes
    cdef DrakenVarBuffer* dict_values
    cdef Py_ssize_t i

    vec.ptr.length = <size_t>row_count  # logical length; ptr.data stays NULL

    if valid_bits != NULL:
        nb_bytes = (row_count + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, valid_bits, <size_t>nb_bytes)

    cdef uint8_t* codes_ptr = NULL
    if code_bytes > 0:
        codes_ptr = <uint8_t*>malloc(<size_t>code_bytes)
        if codes_ptr == NULL:
            raise MemoryError()
        memcpy(codes_ptr, codes, <size_t>code_bytes)

    dict_values = alloc_var_buffer(DRAKEN_FLOAT64, <size_t>dict_size, <size_t>dict_bytes)
    if dict_bytes > 0:
        memcpy(dict_values.data, <const void*>dictionary, <size_t>dict_bytes)
    for i in range(dict_size):
        dict_values.offsets[i] = <int32_t>(i * sizeof(double))
    dict_values.offsets[dict_size] = <int32_t>dict_bytes
    vec._dict_values = dict_values
    vec._dict_ordered = 0
    vec._unified_view.selection = codes_ptr
    vec._unified_view.sel_width = code_width
    vec._unified_view.data = dict_values.data
    vec._unified_view.data_length = <size_t>dict_size
    vec._unified_view.length = <size_t>row_count
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec


cdef Float64Vector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
):
    """Wrap externally-malloc'd data + null_bitmap into a Float64Vector.

    Ownership transfers to the Vector — both pointers must come from `malloc`
    (or be NULL). See `Int64Vector.from_decoded` for the design rationale.
    """
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = 8
    vec.ptr.length = length
    vec.ptr.data = data
    vec.ptr.null_bitmap = null_bitmap
    vec.owns_data = True
    vec._unified_view.data = data
    vec._unified_view.data_length = length
    vec._unified_view.length = length
    vec._unified_view.validity = null_bitmap
    return vec


cdef Float64Vector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "Float64Vector.from_arrow expects a dense float64 Arrow array; "
            "use Float64Vector.from_dict for dictionary input"
        )

    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    # Null bitmap handling with offset support
    cdef Py_ssize_t nb_size
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef Py_ssize_t i
    cdef object new_bitmap_bytes

    if bufs[0] is not None:
        nb_addr = bufs[0].address

        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            # Unaligned offset: must copy and shift
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nb_size)

            src_bitmap = <uint8_t*> nb_addr

            # Copy bits shifting them
            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    cdef size_t _arr_len = <size_t>len(array)
    vec._unified_view.data = vec.ptr.data
    vec._unified_view.data_length = _arr_len
    vec._unified_view.length = _arr_len
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec


cdef Float64Vector from_dict(const int32_t[::1] codes, const double[::1] dictionary):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("Float64Vector.from_dict requires a non-empty dictionary")

    vec.ptr.null_bitmap = NULL
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)
    vec._unified_view.length = <size_t>row_count
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec


cdef Float64Vector from_dict_nullable(
    const int32_t[::1] codes,
    const double[::1] dictionary,
    const uint8_t[::1] row_validity,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("Float64Vector.from_dict requires a non-empty dictionary")
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
    vec._unified_view.length = <size_t>row_count
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec


cdef Float64Vector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const double* dictionary,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    cdef Float64Vector vec = Float64Vector(<size_t>row_count)
    cdef double* dst = <double*>vec.ptr.data
    cdef Py_ssize_t i
    cdef uint32_t code
    cdef Py_ssize_t bitmap_bytes
    cdef int32_t[::1] codes_view
    cdef double[::1] dictionary_view
    cdef int32_t* expanded_codes = NULL

    if dict_size == 0:
        raise ValueError("Float64Vector.from_packed_dict requires a non-empty dictionary")
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
            dictionary_view = <double[:dict_size]><double*>dictionary
        else:
            dictionary_view = <double[:0]><double*>dictionary
        _attach_dictionary_storage(vec, codes_view, dictionary_view, ordered, dict_entry_null_bitmap)
    finally:
        if expanded_codes != NULL:
            free(expanded_codes)

    vec._unified_view.length = <size_t>row_count
    vec._unified_view.validity = vec.ptr.null_bitmap
    return vec


cdef Float64Vector from_sequence(double[::1] data):
    """
    Create Float64Vector from a typed double memoryview (zero-copy).

    Args:
        data: double[::1] memoryview (C-contiguous)

    Returns:
        Float64Vector wrapping the memoryview data
    """
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = 8
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL
    cdef size_t _seq_len = <size_t>data.shape[0]
    vec._unified_view.data = vec.ptr.data
    vec._unified_view.data_length = _seq_len
    vec._unified_view.length = _seq_len
    vec._unified_view.validity = NULL
    return vec


