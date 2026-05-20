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
TimestampVector: INT64 microseconds from Unix epoch, stored in a DrakenFixedBuffer
with itemsize=8. Physical layout is identical to Integer64Vector; TimestampVector is
a distinct class because the domain (wall-clock instants) and operations (date_trunc,
extract, arithmetic with intervals) differ from general integer arithmetic.
"""

import datetime as _dt

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memset, memcpy

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_TIMESTAMP64
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant, draken_vector_from_dict
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector, _materialize_dict_int64
from draken.vectors.date32_vector cimport Date32Vector

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_timestamp_compare.hpp" namespace "draken::timestamp_cmp" nogil:
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

# Constants for microseconds conversions
cdef int64_t MICROSECONDS_PER_DAY = 86_400_000_000
cdef int64_t MICROSECONDS_PER_SECOND = 1_000_000
cdef int64_t MICROSECONDS_PER_MILLISECOND = 1_000
cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000
cdef int64_t NULL_FLAG = INT64_MIN_VALUE
cdef uint8_t _CONST_NULL_BYTE = 0

# Integer unit codes — avoids Python str comparison in the compress hot loop
DEF TIMESTAMP_HASH_CHUNK = 1024
DEF UNIT_NS = 0
DEF UNIT_US = 1
DEF UNIT_MS = 2
DEF UNIT_S  = 3
_TIMESTAMP_EPOCH = _dt.datetime(1970, 1, 1)



cdef void _release_dict_storage(TimestampVector vec) noexcept:
    """Free dict storage. Codes and dict-data live in separate owned buffers
    pointed at by _unified_view.selection and _unified_view.data; ptr.data
    remains the materialized dense buffer (freed by free_fixed_buffer)."""
    if vec._owns_selection:
        free(<void*>vec._unified_view.selection)
    vec._owns_selection = False
    if vec._owns_dict_data and vec._unified_view.data != NULL:
        free(vec._unified_view.data)
    vec._owns_dict_data = False


cdef void _attach_dictionary_storage(TimestampVector vec, const int32_t[::1] codes, const int64_t[::1] dictionary, bint ordered) except *:
    """Dict unique values stored in vec.ptr.data (same slot dense uses).
    _unified_view.data == ptr.data. Codes go in _unified_view.selection (owned)."""
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

    cdef int64_t* dict_data_ptr = NULL
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
        DRAKEN_TIMESTAMP64, vec.ptr.null_bitmap)

cdef inline int _unit_code_from_str(str unit):
    if unit == 'ns':
        return UNIT_NS
    elif unit == 'us':
        return UNIT_US
    elif unit == 'ms':
        return UNIT_MS
    else:
        return UNIT_S

cdef inline int64_t _apply_unit_scale(int64_t v, int unit_code):
    cdef int64_t factor
    if unit_code == UNIT_NS:
        return v // 1000
    elif unit_code == UNIT_US:
        return v
    elif unit_code == UNIT_MS:
        factor = 1000
    else:  # UNIT_S
        factor = 1000000
    # Overflow-safe multiply (clamp to int64 limits)
    if v > 0 and v > 9223372036854775807 // factor:
        return 9223372036854775807
    if v < 0 and v < (-9223372036854775807 - 1) // factor:
        return INT64_MIN_VALUE
    return v * factor


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    cdef uint8_t byte = bitmap[bit_index >> 3]
    return (byte >> (bit_index & 7)) & 1

cdef int64_t _safe_multiply_int64(int64_t value, int64_t factor):
    """Multiply with overflow protection (clamp to int64_t limits)."""
    if factor > 0:
        if value > 0 and value > 9223372036854775807 // factor:
            return 9223372036854775807
        if value < 0 and value < INT64_MIN_VALUE // factor:
            return INT64_MIN_VALUE
    return value * factor

cdef int64_t scale_timestamp_to_micros(int64_t value, str unit):
    """
    Scale raw timestamp value from Arrow unit to microseconds.
    Handles overflow by clamping to int64_t limits.
    """
    if unit == 'ns':
        return value // 1_000  # Integer division: nanoseconds to microseconds
    elif unit == 'us':
        return value  # Already in microseconds
    elif unit == 'ms':
        return _safe_multiply_int64(value, MICROSECONDS_PER_MILLISECOND)
    elif unit == 's':
        return _safe_multiply_int64(value, MICROSECONDS_PER_SECOND)
    else:
        raise ValueError(f"Unknown timestamp unit: {unit}")

cdef class TimestampVector(Vector):

    @classmethod
    def from_constant(cls, value, length, is_null=False, timestamp_unit="us"):
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        cdef TimestampVector vec = TimestampVector(1)
        cdef int64_t val = 0 if (is_null or value is None) else <int64_t>int(value)
        (<int64_t*>vec.ptr.data)[0] = val
        vec.ptr.length = <uint32_t>length
        vec.null_bit_offset = 0
        vec.timestamp_unit = str(timestamp_unit)
        vec._unit_code = _unit_code_from_str(timestamp_unit)
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_TIMESTAMP64,
            &_CONST_NULL_BYTE if is_null else NULL)
        return vec

    @classmethod
    def from_dict(cls, codes, dictionary, row_validity=None, timestamp_unit="us"):
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
        timestamp_unit = str(timestamp_unit)

        if row_validity is None:
            return from_dict(codes_view, dictionary_view, timestamp_unit)

        if not isinstance(row_validity, memoryview):
            row_validity = bytearray(1 if valid else 0 for valid in row_validity)
        validity_view = row_validity
        return from_dict_nullable(codes_view, dictionary_view, validity_view, timestamp_unit)

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        self.null_bit_offset = 0
        self._arrow_null_buf = None
        self._arrow_data_buf = None
        self.timestamp_unit = 'us'  # Default to microseconds
        self._unit_code = UNIT_US

        self._owns_selection = False
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_TIMESTAMP64, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_TIMESTAMP64, length, 8)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_TIMESTAMP64, NULL)

    def __dealloc__(self):
        _release_dict_storage(self)
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

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
        return DRAKEN_TIMESTAMP64

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenVector* uv = self.unified()
        if i < 0 or i >= <Py_ssize_t>uv.length:
            raise IndexError("Index out of bounds")
        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None
        return (<int64_t*>uv.data)[uv.selection[i]]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.timestamp(self.timestamp_unit))
            return pa.array([(<int64_t*>uv.data)[0]] * self.ptr.length, type=pa.timestamp(self.timestamp_unit))

        cdef size_t nbytes = self.ptr.length * 8
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        cdef Py_ssize_t null_bytes
        if self.ptr.null_bitmap != NULL:
            null_bytes = (self.ptr.length + self.null_bit_offset + 7) // 8
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, null_bytes, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.timestamp(self.timestamp_unit), self.ptr.length, buffers)

    # -------- Example op --------
    cpdef TimestampVector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef TimestampVector out = TimestampVector(<size_t>n)
        cdef int64_t* dst = <int64_t*> out.ptr.data
        cdef int64_t* data = <int64_t*>uv.data
        cdef int32_t src_idx
        cdef Py_ssize_t nb_bytes

        for i in range(n):
            src_idx = indices[i]
            dst[i] = data[uv.selection[src_idx]]

        if uv.validity != NULL:
            nb_bytes = (n + 7) >> 3
            out.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
            if out.ptr.null_bitmap == NULL:
                raise MemoryError()
            memset(out.ptr.null_bitmap, 0xFF, nb_bytes)
            for i in range(n):
                src_idx = indices[i]
                if not ((uv.validity[src_idx >> 3] >> (src_idx & 7)) & 1):
                    out.ptr.null_bitmap[i >> 3] &= ~(<uint8_t>1 << (i & 7))

        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_TIMESTAMP64, out.ptr.null_bitmap)
        return out

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

    cpdef BoolVector _compare_scalar(self, int64_t value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i

        if nbytes > 0:
            memset(dst, 0, nbytes)

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

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                if dispatch_compare_once(op, data[uv.selection[i]], value):
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef Vector materialize(self):
        """Return a dense TimestampVector, expanding dict/const encodings if needed."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef TimestampVector dense
        cdef int64_t* dst
        cdef int64_t* mat_src
        cdef uint8_t* mat_null
        cdef Py_ssize_t i, nb_bytes

        if self._unified_view.data_length < self._unified_view.length:
            dense = TimestampVector(<size_t>n)
            dense.timestamp_unit = self.timestamp_unit
            dense._unit_code = self._unit_code
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
            dense._unified_view = draken_vector_from_dense(
                dense.ptr.data, <uint32_t>n, DRAKEN_TIMESTAMP64, dense.ptr.null_bitmap)
            return dense

        if uv.data_length == 1:
            dense = TimestampVector(<size_t>n)
            dense.timestamp_unit = self.timestamp_unit
            dense._unit_code = self._unit_code
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
            dense._unified_view = draken_vector_from_dense(
                dense.ptr.data, <uint32_t>n, DRAKEN_TIMESTAMP64, dense.ptr.null_bitmap)
            return dense

        return self

    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        """Single-pass range check: lower OP value OP upper. NULL in → NULL out."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* src_null = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef int64_t v

        memset(dst, 0, nbytes)
        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        if lower_inclusive and upper_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    if out_null != NULL:
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    v = data[uv.selection[i]]
                    if lower <= v <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        elif lower_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    if out_null != NULL:
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    v = data[uv.selection[i]]
                    if lower <= v < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        elif upper_inclusive:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    if out_null != NULL:
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    v = data[uv.selection[i]]
                    if lower < v <= upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        else:
            for i in range(n):
                if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                    if out_null != NULL:
                        out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
                    v = data[uv.selection[i]]
                    if lower < v < upper:
                        dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector _compare_vector(self, TimestampVector other, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef int64_t* data1 = <int64_t*>uv.data
        cdef int64_t* data2 = <int64_t*>ouv.data
        cdef bint null1, null2
        cdef Py_ssize_t i

        if n != <Py_ssize_t>ouv.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        memset(dst, 0, nbytes)

        if (uv.validity != NULL or ouv.validity != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            null1 = uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1)
            null2 = ouv.validity != NULL and not ((ouv.validity[i >> 3] >> (i & 7)) & 1)
            if null1 or null2:
                continue
            if out_null != NULL:
                out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
            if dispatch_compare_once(op, data1[uv.selection[i]], data2[ouv.selection[i]]):
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector equals_vector(self, TimestampVector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals_vector(self, TimestampVector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than_vector(self, TimestampVector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals_vector(self, TimestampVector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than_vector(self, TimestampVector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals_vector(self, TimestampVector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0. Propagates NULLs."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* src_null = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        memset(dst, 0, nbytes)
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

    cpdef int64_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        cdef int64_t m
        cdef bint found = False

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            m = data[uv.selection[i]]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        for i in range(i + 1, n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            if data[uv.selection[i]] < m:
                m = data[uv.selection[i]]
        return m

    cpdef int64_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        cdef int64_t m
        cdef bint found = False

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            m = data[uv.selection[i]]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        for i in range(i + 1, n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            if data[uv.selection[i]] > m:
                m = data[uv.selection[i]]
        return m

    cpdef int64_t sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int64_t total = 0
        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                continue
            total += data[uv.selection[i]]
        return total

    cpdef Integer64Vector subtract_timestamp_vector(self, TimestampVector other):
        """Subtract two TimestampVector values and return microseconds as Integer64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Integer64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Integer64Vector(<size_t>n)
        dst = <int64_t*> out.ptr.data
        memset(dst, 0, n * sizeof(int64_t))

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
            if valid1 and valid2:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                dst[i] = data1[i] - data2[i]
        return out

    cpdef Integer64Vector subtract_date32_vector(self, Date32Vector other):
        """Subtract Date32Vector from TimestampVector and return microseconds as Integer64Vector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int32_t* data2 = <int32_t*> ptr2.data
        cdef uint8_t* null1 = ptr1.null_bitmap
        cdef uint8_t* null2 = ptr2.null_bitmap
        cdef Py_ssize_t i, n = ptr1.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Integer64Vector out
        cdef int64_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint valid1, valid2
        cdef int64_t right_us

        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        out = Integer64Vector(<size_t>n)
        dst = <int64_t*> out.ptr.data
        memset(dst, 0, n * sizeof(int64_t))

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
            if valid1 and valid2:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                right_us = <int64_t>data2[i] * MICROSECONDS_PER_DAY
                dst[i] = data1[i] - right_us
        return out

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int8_t* buf
        cdef uint8_t* null_bitmap = uv.validity

        buf = <int8_t*>PyMem_Malloc(n)
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
        """Return the number of nulls in the vector."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef DrakenVector* uv = self.unified()
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, bm_bytes
        if uv.data_length == 1:
            return buf_itemsize(ptr)
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef list out = []
        cdef object timedelta = _dt.timedelta
        cdef int64_t value
        cdef int64_t seconds
        cdef int64_t remainder
        cdef int64_t micros
        cdef object ts

        for i in range(n):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                out.append(None)
                continue
            value = data[uv.selection[i]]
            if self.timestamp_unit == "s":
                seconds = value
                micros = 0
            elif self.timestamp_unit == "ms":
                seconds, remainder = divmod(value, 1000)
                micros = remainder * 1000
            elif self.timestamp_unit == "ns":
                seconds, remainder = divmod(value, 1000000000)
                micros = remainder // 1000
            else:
                seconds, remainder = divmod(value, 1000000)
                micros = remainder
            try:
                ts = _TIMESTAMP_EPOCH + timedelta(seconds=seconds, microseconds=micros)
            except (OverflowError, ValueError):
                ts = value
            out.append(ts)

        return out

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare timestamp values at two indices. Returns -1, 0, or 1."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t left_val, right_val
        cdef bint left_is_null, right_is_null

        left_is_null = uv.validity != NULL and not ((uv.validity[left_idx >> 3] >> (left_idx & 7)) & 1)
        right_is_null = uv.validity != NULL and not ((uv.validity[right_idx >> 3] >> (right_idx & 7)) & 1)

        if left_is_null or right_is_null:
            return 0  # Nulls are considered equal

        left_val = data[uv.selection[left_idx]]
        right_val = data[uv.selection[right_idx]]

        if left_val < right_val:
            return -1
        elif left_val > right_val:
            return 1
        else:
            return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null."""
        cdef DrakenVector* uv = self.unified()
        if uv.validity == NULL:
            return False
        return not ((uv.validity[idx >> 3] >> (idx & 7)) & 1)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef uint64_t* as_uint64 = <uint64_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, j, block
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint64_t[TIMESTAMP_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch
        cdef uint64_t is_valid

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimestampVector.hash_into: output buffer too small")

        i = 0
        while i < n:
            block = n - i
            if block > TIMESTAMP_HASH_CHUNK:
                block = TIMESTAMP_HASH_CHUNK
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    scratch[j] = as_uint64[uv.selection[i + j]]
            simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    dst[i + j] = NULL_HASH
            i += block

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenVector* uv = &self._unified_view
        cdef uint64_t* as_uint64 = <uint64_t*>uv.data
        cdef Py_ssize_t i, j, block
        cdef uint64_t[TIMESTAMP_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return 0

        i = 0
        while i < n:
            block = n - i
            if block > TIMESTAMP_HASH_CHUNK:
                block = TIMESTAMP_HASH_CHUNK
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    scratch[j] = NULL_HASH
                else:
                    scratch[j] = as_uint64[uv.selection[i + j]]
            simd_mix_hash(out + i, scratch_ptr, <size_t>block)
            for j in range(block):
                if uv.validity != NULL and not ((uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1):
                    out[i + j] = NULL_HASH
            i += block
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for TimestampVector: scale raw int64 values to microseconds."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* src = <int64_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int64_t* dst = &out_buf[offset]
        cdef Py_ssize_t i

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("TimestampVector.compress: output buffer too small")

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                dst[i] = NULL_FLAG
            else:
                dst[i] = _apply_unit_scale(src[uv.selection[i]], self._unit_code)

    def __str__(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, k = min(<Py_ssize_t>uv.length, 10)
        cdef int64_t* data = <int64_t*>uv.data
        cdef uint8_t* null_bitmap = uv.validity
        cdef list vals = []
        for i in range(k):
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                vals.append(None)
            else:
                vals.append(data[uv.selection[i]])
        return f"<TimestampVector len={uv.length} values={vals}>"


cdef TimestampVector from_arrow(object array):
    import pyarrow as pa

    if pa.types.is_dictionary(array.type):
        raise TypeError(
            "TimestampVector.from_arrow expects a dense timestamp Arrow array; "
            "use TimestampVector.from_dict for dictionary input"
        )

    cdef TimestampVector vec = TimestampVector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Extract timestamp unit from Arrow's type metadata
    cdef str timestamp_unit = 'us'  # Default fallback
    try:
        arrow_type = array.type
        if hasattr(arrow_type, 'unit'):
            timestamp_unit = arrow_type.unit
    except:
        pass  # Use default if metadata unavailable

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr
    cdef Py_ssize_t byte_offset

    vec.ptr.type = DRAKEN_TIMESTAMP64
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
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef Py_ssize_t i

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (nb_addr + (offset >> 3))
            vec.null_bit_offset = 0
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
            vec.null_bit_offset = 0
    else:
        vec.ptr.null_bitmap = NULL
        vec.null_bit_offset = 0

    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>vec.ptr.length, DRAKEN_TIMESTAMP64, vec.ptr.null_bitmap)
    return vec


cdef TimestampVector from_dict(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    str timestamp_unit,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimestampVector vec = TimestampVector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code

    if dict_size == 0:
        raise ValueError("TimestampVector.from_dict requires a non-empty dictionary")

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)
    vec.ptr.null_bitmap = NULL
    vec.null_bit_offset = 0
    for i in range(row_count):
        code = <Py_ssize_t>codes[i]
        if code < 0 or code >= dict_size:
            raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
        dst[i] = dictionary[code]

    _attach_dictionary_storage(vec, codes, dictionary, False)
    return vec


cdef TimestampVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
    str timestamp_unit,
):
    cdef Py_ssize_t row_count = codes.shape[0]
    cdef Py_ssize_t dict_size = dictionary.shape[0]
    cdef TimestampVector vec = TimestampVector(<size_t>row_count)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef Py_ssize_t i
    cdef Py_ssize_t code
    cdef Py_ssize_t nb_bytes
    cdef uint8_t* nb

    if dict_size == 0:
        raise ValueError("TimestampVector.from_dict requires a non-empty dictionary")
    if row_validity.shape[0] != row_count:
        raise ValueError("row_validity length must match codes length")

    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)
    nb_bytes = (row_count + 7) >> 3
    nb = <uint8_t*>malloc(nb_bytes)
    if nb == NULL:
        raise MemoryError()
    memset(nb, 0, nb_bytes)
    vec.ptr.null_bitmap = nb
    vec.null_bit_offset = 0

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


cdef TimestampVector timestamp_dict_from_raw(
    int64_t num_rows,
    uint32_t* codes,            # takes ownership (uint32_t*)
    DrakenVarBuffer* dict_values,  # caller allocates; we copy data out and free
    uint8_t ordered,
    uint8_t* row_nulls,         # validity bitmap (1=valid); NULL if all valid; NOT owned
    str timestamp_unit,
):
    """Build a dict-encoded TimestampVector from pre-built raw buffers.

    Copies dict_values.data into vec.ptr.data (the SAME slot dense uses).
    Codes pointer ownership transfers to the vector via _owns_selection.
    """
    cdef TimestampVector vec = TimestampVector(0)
    cdef Py_ssize_t nb_bytes
    cdef Py_ssize_t dict_size = <Py_ssize_t>dict_values.length

    vec.ptr.length = <uint32_t>num_rows
    vec.timestamp_unit = timestamp_unit
    vec._unit_code = _unit_code_from_str(timestamp_unit)

    if row_nulls != NULL:
        nb_bytes = (num_rows + 7) >> 3
        vec.ptr.null_bitmap = <uint8_t*>malloc(<size_t>nb_bytes)
        if vec.ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(vec.ptr.null_bitmap, row_nulls, <size_t>nb_bytes)

    cdef int64_t* dict_data_ptr = NULL
    if dict_size > 0:
        dict_data_ptr = <int64_t*>malloc(<size_t>dict_size * sizeof(int64_t))
        if dict_data_ptr == NULL:
            raise MemoryError()
        memcpy(dict_data_ptr, dict_values.data, <size_t>dict_size * sizeof(int64_t))

    # Free the caller's DrakenVarBuffer wrapper now that we've copied its data.
    free_var_buffer(dict_values, True)

    vec._owns_dict_data = (dict_data_ptr != NULL)
    vec._owns_selection = (codes != NULL)
    vec._unified_view = draken_vector_from_dict(
        <void*>dict_data_ptr, <uint32_t>dict_size,
        codes, <uint32_t>num_rows,
        DRAKEN_TIMESTAMP64, vec.ptr.null_bitmap)
    return vec


cpdef TimestampVector from_int64_vector(Integer64Vector source, str timestamp_unit="us"):
    """
    Convert an Integer64Vector containing epoch timestamp values to TimestampVector.

    This is a native Draken conversion path (no Arrow interop).
    """
    cdef DrakenVector* src_uv = source.unified()
    cdef Py_ssize_t n = <Py_ssize_t>src_uv.length
    cdef TimestampVector out
    cdef int64_t* src_data
    cdef int64_t* dst_data
    cdef uint8_t* src_null
    cdef size_t nb_bytes
    cdef uint8_t* out_null
    cdef Py_ssize_t i

    out = TimestampVector(<size_t>n)
    out.timestamp_unit = timestamp_unit
    out._unit_code = _unit_code_from_str(timestamp_unit)

    src_data = <int64_t*>src_uv.data
    dst_data = <int64_t*>out.ptr.data
    src_null = src_uv.validity

    if src_null != NULL:
        nb_bytes = (<size_t>n + 7) >> 3
        out_null = <uint8_t*>malloc(nb_bytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, src_null, nb_bytes)
        out.ptr.null_bitmap = out_null
        for i in range(n):
            dst_data[i] = src_data[<Py_ssize_t>src_uv.selection[i]] if ((src_null[i >> 3] >> (i & 7)) & 1) else 0
    else:
        out.ptr.null_bitmap = NULL
        for i in range(n):
            dst_data[i] = src_data[<Py_ssize_t>src_uv.selection[i]]
    out.null_bit_offset = 0

    out._unified_view = draken_vector_from_dense(
        out.ptr.data, <uint32_t>n, DRAKEN_TIMESTAMP64, out.ptr.null_bitmap)
    return out


cdef TimestampVector _materialize_const_timestamp(TimestampVector const_vec):
    """Expand a CONSTANT TimestampVector to a dense TimestampVector."""
    cdef DrakenVector* src_uv = const_vec.unified()
    cdef size_t n = const_vec.ptr.length
    cdef TimestampVector dense = TimestampVector(n)
    dense.timestamp_unit = const_vec.timestamp_unit
    dense._unit_code = const_vec._unit_code
    cdef int64_t* dst = <int64_t*>dense.ptr.data
    cdef int64_t val
    cdef size_t i
    cdef size_t null_bytes
    cdef uint8_t* null_bm

    if src_uv.validity != NULL:
        null_bytes = (n + 7) >> 3
        null_bm = <uint8_t*>malloc(null_bytes)
        if null_bm == NULL:
            raise MemoryError()
        memset(null_bm, 0, null_bytes)
        dense.ptr.null_bitmap = null_bm
    else:
        val = (<int64_t*>src_uv.data)[0]
        for i in range(n):
            dst[i] = val
    dense._unified_view = draken_vector_from_dense(
        dense.ptr.data, <uint32_t>n, DRAKEN_TIMESTAMP64, dense.ptr.null_bitmap)
    return dense


