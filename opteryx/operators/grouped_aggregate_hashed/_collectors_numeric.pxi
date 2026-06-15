# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

# Numeric collectors — COUNT, SUM, MIN, MAX, AVG.
# Numeric state lives in Draken-owned fixed buffers so finalize can hand off
# buffers without copying. No Python in accumulate().
#

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t, INT64_MAX, INT64_MIN
from libc.stddef cimport size_t
from libc.math cimport HUGE_VAL
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, realloc, free

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector, DrakenType
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32
from draken.core.buffers cimport DRAKEN_FLOAT64, DRAKEN_FLOAT32
from draken.core.buffers cimport DRAKEN_DECIMAL
from draken.core.buffers cimport DRAKEN_DECIMAL128
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY
from draken.core.buffers cimport DRAKEN_TIMESTAMP64, DRAKEN_DATE32, DRAKEN_TIME32, DRAKEN_TIME64
from draken.core.buffers cimport draken_vector_from_dense
from draken.core.buffers cimport (
    DrakenStringArena, DrakenStringSlot,
    str_length, str_is_inline, str_prefix4, str_data,
    str_clone_with_offset, str_init_null, str_compare,
    STR_INLINE_MAX,
)
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded

# Hoist the shim Vector import to module level to avoid hot-path inline imports (E.30a)
from draken.vectors.vector import Vector as _V

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

from cpython.object cimport PyObject

# C-level Py_DECREF to balance the NEW reference the own_* builders return.
cdef extern from *:
    """static inline void _cn_decref(PyObject* op) { Py_XDECREF(op); }"""
    void _cn_decref(PyObject* op)

# Python-free typed-vector builders (own ownership of the buffers passed in).
# Used by the type-preserving finalize paths so the compiled collectors never
# touch a Python builder. own_timestamp/time carry the TIME/TIMESTAMP unit.
cdef extern from "core/draken_bridge.h":
    PyObject* draken_vector_own_timestamp(
        void* data, uint8_t* validity, uint32_t length, const char* unit_str)
    PyObject* draken_vector_own_time32(
        void* data, uint8_t* validity, uint32_t length, const char* unit_str)
    PyObject* draken_vector_own_time64(
        void* data, uint8_t* validity, uint32_t length, const char* unit_str)
    # Hand accumulated German-string slots + arena straight to a string-family
    # Vector (no per-row boxing, no rehash — slot hash32 is trusted). Ownership of
    # slots + arena + validity transfers on call.
    PyObject* draken_vector_own_string(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType vec_type)

from draken.core.buffers cimport DRAKEN_INTERVAL, DRAKEN_BOOL

cdef extern from "core/interval_slot.h":
    cdef struct DrakenIntervalSlot:
        int64_t months
        int64_t ms

# Same approximate single-scalar order INTERVAL uses everywhere (sort, compare,
# ungrouped min/max): total_ms = months*INTERVAL_MONTH_MS + ms, 1 month = 30 days
# = 86_400_000 ms/day. Imperfect (months/days vary) but consistent engine-wide.
# Value mirrors INTERVAL_MONTH_MS in ops/interval_ops.h (that header can't be
# cimported standalone — it has unmet deps). Folded inline below, not cimported.
cdef int64_t INTERVAL_MONTH_MS = 2592000000

# int128 type for the DECIMAL128 grouped collectors. Cython has no native 128-bit
# integer; this ctypedef emits C `__int128` (clang and gcc both support it). Cython's
# own type model treats it as an 8-byte integer, so we never rely on sizeof(int128_t)
# — the int128 buffers are sized with the literal 16. Pointer indexing and +/</> on
# int128_t* emit correct 16-byte C __int128 ops.
cdef extern from *:
    ctypedef signed long long int128_t "__int128"


cdef inline bint _num_bitmap_valid(uint8_t* bm, Py_ssize_t i) noexcept nogil:
    if bm == NULL:
        return True
    return ((bm[i >> 3] >> (i & 7)) & 1) != 0


# Fused integer source type for width-aware accumulation. Narrow-integer inputs
# (INT8/INT16/INT32) reach the grouped aggregators from sources that emit native
# widths (e.g. virtual datasets); the int64 collectors must read them at their
# true width and sign-extend, mirroring the scalar path's _exact_int_sum_as_double
# and the KeyStore's _ks_store_fixed_bulk_dict. Cython compiles one branch-free
# loop per specialization, so the width decision stays hoisted out of the hot loop.
ctypedef fused _int_src:
    int8_t
    int16_t
    int32_t
    int64_t


cdef inline void _sum_accumulate_int(
    const _int_src* data,
    const uint32_t* sel,
    uint8_t* nulls,
    const uint32_t* state_indices,
    int64_t* sums,
    uint8_t* seen,
    Py_ssize_t n_rows,
) noexcept nogil:
    cdef Py_ssize_t i
    cdef int64_t si
    for i in range(n_rows):
        if _num_bitmap_valid(nulls, i):
            si = state_indices[i]
            sums[si] += <int64_t>data[sel[i]]
            _bitmap_set(seen, si)


cdef inline void _minmax_accumulate_int(
    const _int_src* data,
    const uint32_t* sel,
    uint8_t* nulls,
    const uint32_t* state_indices,
    int64_t* values,
    uint8_t* seen,
    Py_ssize_t n_rows,
    int8_t direction,
) noexcept nogil:
    cdef Py_ssize_t i
    cdef int64_t si, v
    if direction == 0:   # ANY_VALUE — keep the first non-null per group
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                if not _num_bitmap_valid(seen, si):
                    values[si] = <int64_t>data[sel[i]]
                    _bitmap_set(seen, si)
    elif direction == 1:   # MIN
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                v = <int64_t>data[sel[i]]
                if not _num_bitmap_valid(seen, si) or v < values[si]:
                    values[si] = v
                _bitmap_set(seen, si)
    else:                # MAX
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                v = <int64_t>data[sel[i]]
                if not _num_bitmap_valid(seen, si) or v > values[si]:
                    values[si] = v
                _bitmap_set(seen, si)


cdef inline void _avg_accumulate_int(
    const _int_src* data,
    const uint32_t* sel,
    uint8_t* nulls,
    const uint32_t* state_indices,
    double* sums,
    int64_t* counts,
    Py_ssize_t n_rows,
) noexcept nogil:
    cdef Py_ssize_t i
    cdef int64_t si
    for i in range(n_rows):
        if _num_bitmap_valid(nulls, i):
            si = state_indices[i]
            sums[si] += <double>data[sel[i]]
            counts[si] += 1


cdef inline Py_ssize_t _bitmap_nbytes(int64_t length) noexcept nogil:
    return <Py_ssize_t>((length + 7) >> 3)


cdef inline uint8_t* _alloc_all_valid_bitmap(int64_t length) except NULL:
    cdef Py_ssize_t nbytes = _bitmap_nbytes(length)
    cdef uint8_t* bitmap
    if nbytes == 0:
        return NULL
    bitmap = <uint8_t*>malloc(nbytes)
    if bitmap == NULL:
        raise MemoryError()
    memset(bitmap, 0xFF, nbytes)
    return bitmap


cdef inline void _bitmap_clear(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    bitmap[index >> 3] &= ~(1 << (index & 7))


cdef inline void _bitmap_set(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    bitmap[index >> 3] |= (1 << (index & 7))


cdef inline void _ensure_validity_bitmap(DrakenFixedBuffer* buf) except *:
    if buf.null_bitmap == NULL:
        buf.null_bitmap = _alloc_all_valid_bitmap(<int64_t>buf.length)


cdef inline void _grow_fixed_buffer(DrakenFixedBuffer* buf, int64_t old_count, int64_t new_count) except *:
    # realloc rather than malloc+memcpy: at large sizes the allocator extends
    # the mapping in place, skipping the copy of the live prefix. The tail
    # memset is semantic, not hygiene — counts/sums must start at zero.
    cdef void* new_data
    cdef Py_ssize_t old_bytes
    cdef Py_ssize_t new_bytes

    if new_count <= old_count:
        buf.length = <size_t>new_count
        return

    old_bytes = <Py_ssize_t>(old_count * <int64_t>buf.itemsize)
    new_bytes = <Py_ssize_t>(new_count * <int64_t>buf.itemsize)

    if new_bytes == 0:
        if buf.data != NULL:
            free(buf.data)
        buf.data = NULL
        buf.length = <size_t>new_count
        return

    new_data = realloc(buf.data, new_bytes)
    if new_data == NULL:
        raise MemoryError()

    if new_bytes > old_bytes:
        memset(<uint8_t*>new_data + old_bytes, 0, new_bytes - old_bytes)

    buf.data = new_data
    buf.length = <size_t>new_count


cdef inline int64_t _grow_target(int64_t capacity, int64_t new_count) noexcept nogil:
    cdef int64_t target = capacity * 2 if capacity > 0 else new_count
    if target < new_count:
        target = new_count
    return target


cdef inline void _grow_bitmap(uint8_t** bitmap_ref, int64_t old_count, int64_t new_count, bint fill_valid) except *:
    cdef Py_ssize_t old_bytes = _bitmap_nbytes(old_count)
    cdef Py_ssize_t new_bytes = _bitmap_nbytes(new_count)
    cdef uint8_t fill_byte = 0xFF if fill_valid else 0x00
    cdef uint8_t* new_bitmap

    if new_bytes == 0:
        if bitmap_ref[0] != NULL:
            free(bitmap_ref[0])
            bitmap_ref[0] = NULL
        return

    # realloc preserves the live prefix; only the new tail needs the fill
    # byte. A fresh bitmap (NULL in) has no prefix, so fill everything.
    if bitmap_ref[0] == NULL:
        old_bytes = 0

    new_bitmap = <uint8_t*>realloc(bitmap_ref[0], new_bytes)
    if new_bitmap == NULL:
        raise MemoryError()

    if new_bytes > old_bytes:
        memset(new_bitmap + old_bytes, fill_byte, new_bytes - old_bytes)

    bitmap_ref[0] = new_bitmap


cdef inline Vector _materialize_fixed_buffer(
    DrakenFixedBuffer* src,
    int64_t start,
    int64_t stop,
    DrakenType dtype,
    size_t itemsize,
) except *:
    """Copy a range of a libc-malloc'd DrakenFixedBuffer into a fresh
    draken_malloc'd Vector. The source buffer is NOT consumed — the
    caller retains it.

    This is the producer-side primitive for collector finalize paths.
    The collector's internal state lives in libc malloc (alloc_fixed_buffer);
    Vectors require buffers in draken_malloc (mimalloc). Cross-allocator
    ownership transfer is impossible, so we copy.
    """
    cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
    cdef void* out_data
    cdef uint8_t* validity = NULL
    cdef Py_ssize_t i
    cdef Py_ssize_t bitmap_bytes
    cdef size_t nbytes

    if length <= 0:
        return _vector_from_decoded(NULL, NULL, 0, dtype)

    nbytes = <size_t>length * itemsize
    out_data = draken_malloc(nbytes)
    if out_data == NULL:
        raise MemoryError()
    memcpy(out_data, <uint8_t*>src.data + <size_t>start * itemsize, nbytes)

    if src.null_bitmap != NULL:
        bitmap_bytes = (length + 7) >> 3
        validity = <uint8_t*>draken_malloc(<size_t>bitmap_bytes)
        if validity == NULL:
            draken_free(out_data)
            raise MemoryError()
        memset(validity, 0xFF, bitmap_bytes)
        for i in range(length):
            if not _num_bitmap_valid(src.null_bitmap, start + i):
                validity[i >> 3] &= ~(1 << (i & 7))

    return _vector_from_decoded(out_data, validity, <uint32_t>length, dtype)


cdef inline Vector _consume_int64_buffer(DrakenFixedBuffer* buf) except *:
    """Copy the full buffer into a Vector and free the source. Used by
    finalize() where the collector has already swapped in a fresh state
    buffer and is handing the old one off.
    """
    cdef Vector out = _materialize_fixed_buffer(
        buf, 0, <int64_t>buf.length, DRAKEN_INT64, sizeof(int64_t)
    )
    free_fixed_buffer(buf, True)
    return out


cdef inline Vector _consume_float64_buffer(DrakenFixedBuffer* buf) except *:
    """Copy the full buffer into a Vector and free the source. Used by
    finalize() where the collector has already swapped in a fresh state.
    """
    cdef Vector out = _materialize_fixed_buffer(
        buf, 0, <int64_t>buf.length, DRAKEN_FLOAT64, sizeof(double)
    )
    free_fixed_buffer(buf, True)
    return out


cdef inline uint8_t* _validity_from_slice(uint8_t* src_nbm, int64_t start,
                                          int64_t length) except? NULL:
    """Build a fresh draken_malloc'd validity bitmap (1=valid) for rows
    [start, start+length) of src_nbm, or NULL if src_nbm is NULL (all valid).
    """
    cdef size_t bm_bytes
    cdef uint8_t* validity
    cdef int64_t i
    if src_nbm == NULL:
        return NULL
    bm_bytes = <size_t>((length + 7) >> 3)
    validity = <uint8_t*>draken_malloc(bm_bytes if bm_bytes > 0 else 1)
    if validity == NULL:
        raise MemoryError()
    memset(validity, 0xFF, bm_bytes)
    for i in range(length):
        if not _num_bitmap_valid(src_nbm, start + i):
            validity[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
    return validity


cdef Vector _build_temporal_from_int64(int64_t* src, uint8_t* src_nbm,
                                       int64_t start, int64_t length,
                                       DrakenType out_type, bytes unit) except *:
    """Build a typed temporal Vector from int64 results [start, start+length).

    Copies into fresh draken_malloc'd buffers at the source's NATIVE width
    (4-byte for DATE32/TIME32, 8-byte for TIMESTAMP64/TIME64) and hands them to
    the Python-free extern-C builders (which carry the unit). Does NOT touch the
    source buffer's ownership. No Python objects on this path.
    """
    cdef size_t alloc_n = <size_t>(length if length > 0 else 1)
    cdef uint8_t* validity = _validity_from_slice(src_nbm, start, length)
    cdef int32_t* d32
    cdef int64_t* d64
    cdef int64_t i
    cdef PyObject* raw
    cdef Vector result
    if out_type == DRAKEN_DATE32:
        d32 = <int32_t*>draken_malloc(alloc_n * sizeof(int32_t))
        if d32 == NULL:
            if validity != NULL: draken_free(validity)
            raise MemoryError()
        for i in range(length):
            d32[i] = <int32_t>src[start + i]
        # DATE32 has no unit — from_decoded is the Python-free builder.
        return _vector_from_decoded(d32, validity, <uint32_t>length, DRAKEN_DATE32)
    if out_type == DRAKEN_TIME32:
        d32 = <int32_t*>draken_malloc(alloc_n * sizeof(int32_t))
        if d32 == NULL:
            if validity != NULL: draken_free(validity)
            raise MemoryError()
        for i in range(length):
            d32[i] = <int32_t>src[start + i]
        raw = draken_vector_own_time32(d32, validity, <uint32_t>length, <const char*>unit)
    else:
        d64 = <int64_t*>draken_malloc(alloc_n * sizeof(int64_t))
        if d64 == NULL:
            if validity != NULL: draken_free(validity)
            raise MemoryError()
        for i in range(length):
            d64[i] = src[start + i]
        if out_type == DRAKEN_TIMESTAMP64:
            raw = draken_vector_own_timestamp(d64, validity, <uint32_t>length, <const char*>unit)
        else:  # DRAKEN_TIME64
            raw = draken_vector_own_time64(d64, validity, <uint32_t>length, <const char*>unit)
    if raw == NULL:
        raise MemoryError("temporal vector build failed")
    result = <Vector>(<object>raw)   # Cython incref → 2
    _cn_decref(raw)                  # balance the NEW ref → 1
    return result


cdef Vector _build_float32_from_float64(double* src, uint8_t* src_nbm,
                                        int64_t start, int64_t length) except *:
    """Narrow double results [start, start+length) to a fresh FLOAT32 buffer and
    wrap via from_decoded (Python-free). Does NOT touch the source's ownership.
    """
    cdef size_t alloc_n = <size_t>(length if length > 0 else 1)
    cdef uint8_t* validity = _validity_from_slice(src_nbm, start, length)
    cdef float* f32 = <float*>draken_malloc(alloc_n * sizeof(float))
    cdef int64_t i
    if f32 == NULL:
        if validity != NULL: draken_free(validity)
        raise MemoryError()
    for i in range(length):
        f32[i] = <float>src[start + i]
    return _vector_from_decoded(f32, validity, <uint32_t>length, DRAKEN_FLOAT32)


cdef inline Vector _slice_int64_buffer(
    DrakenFixedBuffer* src,
    int64_t start,
    int64_t stop,
) except *:
    return _materialize_fixed_buffer(src, start, stop, DRAKEN_INT64, sizeof(int64_t))


cdef inline Vector _slice_float64_buffer(
    DrakenFixedBuffer* src,
    int64_t start,
    int64_t stop,
) except *:
    return _materialize_fixed_buffer(src, start, stop, DRAKEN_FLOAT64, sizeof(double))


# ---------------------------------------------------------------------------
# COUNT(*) — no column, counts every row
# ---------------------------------------------------------------------------

cdef class CountStarCollector(BaseCollector):
    cdef DrakenFixedBuffer* _counts
    cdef int64_t _capacity
    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0

    def __dealloc__(self):
        if self._counts != NULL:
            free_fixed_buffer(self._counts, True)
            self._counts = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._counts, self._capacity, target)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
        with nogil:
            for i in range(n_rows):
                counts[state_indices[i]] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef long long start_ns = _now_ns()
        cdef Vector out = _slice_int64_buffer(self._counts, start, stop)
        self._time_finalize_ns += _now_ns() - start_ns
        return out

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        cdef CountStarCollector o = <CountStarCollector>other
        (<int64_t*>self._counts.data)[self_idx] += (<int64_t*>o._counts.data)[other_idx]

# ---------------------------------------------------------------------------
# COUNT(col) — skip NULLs
# ---------------------------------------------------------------------------

cdef class CountValueCollector(BaseCollector):
    cdef DrakenFixedBuffer* _counts
    cdef int64_t _capacity
    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0

    def __dealloc__(self):
        if self._counts != NULL:
            free_fixed_buffer(self._counts, True)
            self._counts = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._counts, self._capacity, target)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
        cdef uint8_t* nulls = vec.unified().validity
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    counts[state_indices[i]] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef long long start_ns = _now_ns()
        cdef Vector out = _slice_int64_buffer(self._counts, start, stop)
        self._time_finalize_ns += _now_ns() - start_ns
        return out

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        cdef CountValueCollector o = <CountValueCollector>other
        (<int64_t*>self._counts.data)[self_idx] += (<int64_t*>o._counts.data)[other_idx]

# ---------------------------------------------------------------------------
# SUM(int64)
# ---------------------------------------------------------------------------

cdef class SumInt64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _sums
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_bitmap(&self._seen, self._capacity, target, False)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef int64_t* sums = <int64_t*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef DrakenVector* uv
        cdef DrakenType t

        uv = vec.unified()
        sel = uv.selection
        nulls = uv.validity
        t = uv.type
        # Width-aware read: narrow ints are sign-extended into the int64 sum.
        with nogil:
            if t == DRAKEN_INT8:
                _sum_accumulate_int(<const int8_t*>uv.data, sel, nulls, state_indices, sums, seen, n_rows)
            elif t == DRAKEN_INT16:
                _sum_accumulate_int(<const int16_t*>uv.data, sel, nulls, state_indices, sums, seen, n_rows)
            elif t == DRAKEN_INT32:
                _sum_accumulate_int(<const int32_t*>uv.data, sel, nulls, state_indices, sums, seen, n_rows)
            else:
                _sum_accumulate_int(<const int64_t*>uv.data, sel, nulls, state_indices, sums, seen, n_rows)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef long long start_ns = _now_ns()
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

        if seen != NULL:
            free(seen)

        self._time_finalize_ns += _now_ns() - start_ns
        return _consume_int64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef long long start_ns = _now_ns()
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
            if seen != NULL:
                free(seen)
                self._seen = NULL

        cdef Vector result = _slice_int64_buffer(out, start, stop)
        self._time_finalize_ns += _now_ns() - start_ns
        return result

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        # SUM partials add. _seen marks groups that saw a non-null value; OR it in
        # so a group seen in either partition finalizes non-null (other's slot is 0
        # when unseen, so the add is harmless).
        cdef SumInt64Collector o = <SumInt64Collector>other
        (<int64_t*>self._sums.data)[self_idx] += (<int64_t*>o._sums.data)[other_idx]
        if o._seen != NULL and _num_bitmap_valid(o._seen, other_idx) and self._seen != NULL:
            _bitmap_set(self._seen, self_idx)

# ---------------------------------------------------------------------------
# SUM(float64)
# ---------------------------------------------------------------------------

cdef class SumFloat64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _sums
    cdef uint8_t* _seen
    cdef int64_t _capacity

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_bitmap(&self._seen, self._capacity, target, False)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef double* sums = <double*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef double* data
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef DrakenVector* uv

        uv = vec.unified()
        data = <double*>uv.data
        sel = uv.selection
        nulls = uv.validity
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += data[sel[i]]
                    _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        self._sums = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

        if seen != NULL:
            free(seen)

        return _consume_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
            if seen != NULL:
                free(seen)
                self._seen = NULL

        return _slice_float64_buffer(out, start, stop)

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        # Float SUM partials add; cross-partition order may differ from serial in
        # the last ULP (the property test allows a relative tolerance for floats).
        cdef SumFloat64Collector o = <SumFloat64Collector>other
        (<double*>self._sums.data)[self_idx] += (<double*>o._sums.data)[other_idx]
        if o._seen != NULL and _num_bitmap_valid(o._seen, other_idx) and self._seen != NULL:
            _bitmap_set(self._seen, self_idx)

# ---------------------------------------------------------------------------
# MIN/MAX(int64)   direction: +1 = MIN, -1 = MAX
# ---------------------------------------------------------------------------

cdef class MinMaxInt64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN (use INT64_MAX as init), -1 = MAX (INT64_MIN)
    # Type-preserving finalize: the min/max is computed on the raw int representation
    # (correct ordering for INT and all int-backed temporal types within one unit),
    # then the int64 result buffer is reinterpreted back to the source type. Default
    # INT64 → no reinterpret. _out_unit carries the TIMESTAMP/TIME unit ("s"/"ms"/...).
    cdef DrakenType _out_type
    cdef bytes _out_unit       # TIMESTAMP/TIME unit (b"s"/b"ms"/b"us"/b"ns")

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._out_type = DRAKEN_INT64
        self._out_unit = b"us"

    def __dealloc__(self):
        if self._values != NULL:
            free_fixed_buffer(self._values, True)
            self._values = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t sentinel = INT64_MAX if self._direction == 1 else INT64_MIN
        cdef int64_t old_count = self._capacity
        cdef int64_t target
        cdef int64_t* values
        cdef int64_t i

        if new_count > old_count:
            target = _grow_target(old_count, new_count)
            _grow_fixed_buffer(self._values, old_count, target)
            _grow_bitmap(&self._seen, old_count, target, False)
            values = <int64_t*>self._values.data
            for i in range(old_count, target):
                values[i] = sentinel
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef int64_t* values = <int64_t*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef DrakenVector* uv
        cdef DrakenType t

        uv = vec.unified()
        sel = uv.selection
        nulls = uv.validity
        t = uv.type
        cdef int8_t direction = self._direction
        # Width-aware read: narrow ints (and the 4-byte temporal types DATE32/TIME32)
        # are sign-extended before compare; 8-byte TIMESTAMP64/TIME64 read as int64.
        with nogil:
            if t == DRAKEN_INT8:
                _minmax_accumulate_int(<const int8_t*>uv.data, sel, nulls, state_indices, values, seen, n_rows, direction)
            elif t == DRAKEN_INT16:
                _minmax_accumulate_int(<const int16_t*>uv.data, sel, nulls, state_indices, values, seen, n_rows, direction)
            elif t == DRAKEN_INT32 or t == DRAKEN_DATE32 or t == DRAKEN_TIME32:
                _minmax_accumulate_int(<const int32_t*>uv.data, sel, nulls, state_indices, values, seen, n_rows, direction)
            else:
                _minmax_accumulate_int(<const int64_t*>uv.data, sel, nulls, state_indices, values, seen, n_rows, direction)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        self._values = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

        if seen != NULL:
            free(seen)

        # INT64: hand the buffer off directly (Python-free). Int-backed temporal:
        # build the typed vector from the same buffer, carrying the unit — also
        # Python-free (extern-C builders), then free the int64 source.
        if self._out_type == DRAKEN_INT64:
            return _consume_int64_buffer(out)
        cdef Vector r = _build_temporal_from_int64(
            <int64_t*>out.data, out.null_bitmap, 0, num_groups,
            self._out_type, self._out_unit)
        free_fixed_buffer(out, True)
        return r

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
            if seen != NULL:
                free(seen)
                self._seen = NULL

        if self._out_type == DRAKEN_INT64:
            return _slice_int64_buffer(out, start, stop)
        # Temporal slice: build [start, stop) from the int64 buffer; the buffer is
        # retained (self._values), so the typed builder only copies, never frees it.
        return _build_temporal_from_int64(
            <int64_t*>out.data, out.null_bitmap, start, stop - start,
            self._out_type, self._out_unit)

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        # MIN/MAX is seen-aware on the raw int representation (correct ordering for
        # INT and int-backed temporals within one unit); finalize re-tags the type.
        # direction 0 (ANY_VALUE): keep self's existing value, take other's only if
        # self has none — matching the per-row "first value per group" semantics.
        cdef MinMaxInt64Collector o = <MinMaxInt64Collector>other
        if o._seen == NULL or not _num_bitmap_valid(o._seen, other_idx):
            return
        cdef int64_t v = (<int64_t*>o._values.data)[other_idx]
        cdef int64_t* sv = <int64_t*>self._values.data
        cdef int8_t direction = self._direction
        if self._seen == NULL or not _num_bitmap_valid(self._seen, self_idx):
            sv[self_idx] = v
            if self._seen != NULL:
                _bitmap_set(self._seen, self_idx)
            return
        if direction == 1:
            if v < sv[self_idx]:
                sv[self_idx] = v
        elif direction == -1:
            if v > sv[self_idx]:
                sv[self_idx] = v

# ---------------------------------------------------------------------------
# MIN/MAX(float64)
# ---------------------------------------------------------------------------

cdef class MinMaxFloat64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX
    # FLOAT32 sources accumulate in double then narrow back at finalize so the
    # result emerges FLOAT32, not FLOAT64. Default FLOAT64 → no narrow.
    cdef DrakenType _out_type

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._out_type = DRAKEN_FLOAT64

    def __dealloc__(self):
        if self._values != NULL:
            free_fixed_buffer(self._values, True)
            self._values = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef double sentinel = HUGE_VAL if self._direction == 1 else -HUGE_VAL
        cdef int64_t old_count = self._capacity
        cdef int64_t target
        cdef double* values
        cdef int64_t i

        if new_count > old_count:
            target = _grow_target(old_count, new_count)
            _grow_fixed_buffer(self._values, old_count, target)
            _grow_bitmap(&self._seen, old_count, target, False)
            values = <double*>self._values.data
            for i in range(old_count, target):
                values[i] = sentinel
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef double* values = <double*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef double* data
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double v
        cdef DrakenVector* uv

        uv = vec.unified()
        sel = uv.selection
        nulls = uv.validity
        cdef int8_t direction = self._direction
        cdef const float* fdata
        # FLOAT32 source: read 4-byte floats, widen to double for compare/store.
        # The float64 hot path below is left untouched.
        if uv.type == DRAKEN_FLOAT32:
            fdata = <const float*>uv.data
            with nogil:
                if direction == 0:      # ANY_VALUE — first non-null per group
                    for i in range(n_rows):
                        if _num_bitmap_valid(nulls, i):
                            si = state_indices[i]
                            if not _num_bitmap_valid(seen, si):
                                values[si] = <double>fdata[sel[i]]
                                _bitmap_set(seen, si)
                elif direction == 1:
                    for i in range(n_rows):
                        if _num_bitmap_valid(nulls, i):
                            si = state_indices[i]
                            v = <double>fdata[sel[i]]
                            if not _num_bitmap_valid(seen, si) or v < values[si]:
                                values[si] = v
                            _bitmap_set(seen, si)
                else:
                    for i in range(n_rows):
                        if _num_bitmap_valid(nulls, i):
                            si = state_indices[i]
                            v = <double>fdata[sel[i]]
                            if not _num_bitmap_valid(seen, si) or v > values[si]:
                                values[si] = v
                            _bitmap_set(seen, si)
            return
        data = <double*>uv.data
        with nogil:
            if direction == 0:       # ANY_VALUE — first non-null per group
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si):
                            values[si] = data[sel[i]]
                            _bitmap_set(seen, si)
            elif direction == 1:   # MIN
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            else:                # MAX
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i
        cdef Vector f32r

        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        self._values = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

        if seen != NULL:
            free(seen)

        if self._out_type == DRAKEN_FLOAT32:
            f32r = _build_float32_from_float64(<double*>out.data, out.null_bitmap, 0, num_groups)
            free_fixed_buffer(out, True)
            return f32r
        return _consume_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
            if seen != NULL:
                free(seen)
                self._seen = NULL

        if self._out_type == DRAKEN_FLOAT32:
            return _build_float32_from_float64(<double*>out.data, out.null_bitmap, start, stop - start)
        return _slice_float64_buffer(out, start, stop)

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        # As MinMaxInt64Collector but in double; FLOAT32 sources stored as double
        # and narrowed back at finalize, so the merge is identical.
        cdef MinMaxFloat64Collector o = <MinMaxFloat64Collector>other
        if o._seen == NULL or not _num_bitmap_valid(o._seen, other_idx):
            return
        cdef double v = (<double*>o._values.data)[other_idx]
        cdef double* sv = <double*>self._values.data
        cdef int8_t direction = self._direction
        if self._seen == NULL or not _num_bitmap_valid(self._seen, self_idx):
            sv[self_idx] = v
            if self._seen != NULL:
                _bitmap_set(self._seen, self_idx)
            return
        if direction == 1:
            if v < sv[self_idx]:
                sv[self_idx] = v
        elif direction == -1:
            if v > sv[self_idx]:
                sv[self_idx] = v

# ---------------------------------------------------------------------------
# MIN/MAX(bool) — false < true: MIN = AND-reduce, MAX = OR-reduce over non-nulls.
# One 0/1 byte per group (+ a seen byte). Per-row accumulate is nogil (reads the
# bit-packed source); finalize boxes over #groups (bool MIN/MAX is rare, and the
# result must round-trip through the bool-vector builder).
# ---------------------------------------------------------------------------

cdef class MinMaxBoolCollector(BaseCollector):
    cdef uint8_t* _values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    def __cinit__(self):
        self._values = NULL
        self._seen = NULL
        self._capacity = 0

    def __dealloc__(self):
        if self._values != NULL:
            free(self._values)
            self._values = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        cdef void* p
        if new_count <= self._capacity:
            return
        target = _grow_target(self._capacity, new_count)
        p = malloc(<size_t>target)
        if self._values != NULL:
            memcpy(p, self._values, <size_t>self._capacity)
            free(self._values)
        self._values = <uint8_t*>p
        memset(self._values + self._capacity, 0, <size_t>(target - self._capacity))
        p = malloc(<size_t>target)
        if self._seen != NULL:
            memcpy(p, self._seen, <size_t>self._capacity)
            free(self._seen)
        self._seen = <uint8_t*>p
        memset(self._seen + self._capacity, 0, <size_t>(target - self._capacity))
        self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef const uint8_t* bits = <const uint8_t*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef uint8_t* values = self._values
        cdef uint8_t* seen = self._seen
        cdef int8_t direction = self._direction
        cdef Py_ssize_t i
        cdef int64_t si
        cdef uint32_t phys
        cdef uint8_t b
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    phys = sel[i]
                    b = (bits[phys >> 3] >> (phys & 7)) & 1
                    if seen[si] == 0:
                        values[si] = b
                        seen[si] = 1
                    elif direction == 1:        # MIN: false dominates
                        if b < values[si]:
                            values[si] = b
                    elif direction == -1:       # MAX: true dominates
                        if b > values[si]:
                            values[si] = b
                    # direction 0 (ANY_VALUE): keep the first value, no update

    cdef Vector _build(self, int64_t start, int64_t stop):
        # Build a bit-packed DRAKEN_BOOL buffer + validity directly (Python-free).
        cdef int64_t length = stop - start
        cdef size_t nbytes = <size_t>((length + 7) >> 3)
        cdef uint8_t* data = <uint8_t*>draken_malloc(nbytes if nbytes > 0 else 1)
        cdef uint8_t* validity = <uint8_t*>draken_malloc(nbytes if nbytes > 0 else 1)
        cdef int64_t g, idx
        cdef bint has_null = False
        if data == NULL or validity == NULL:
            if data != NULL: draken_free(data)
            if validity != NULL: draken_free(validity)
            raise MemoryError()
        memset(data, 0, nbytes)
        memset(validity, 0xFF, nbytes)
        for g in range(length):
            idx = start + g
            if idx >= self._capacity or self._seen[idx] == 0:
                validity[g >> 3] &= ~(<uint8_t>(1 << (g & 7)))
                has_null = True
            elif self._values[idx] != 0:
                data[g >> 3] |= <uint8_t>(1 << (g & 7))
        if not has_null:
            draken_free(validity)
            validity = NULL
        return _vector_from_decoded(data, validity, <uint32_t>length, DRAKEN_BOOL)

    cpdef Vector finalize(self, int64_t num_groups):
        return self._build(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        return self._build(start, stop)

# ---------------------------------------------------------------------------
# MIN/MAX(interval) — ordered by the approximate scalar fold (months*30d + ms),
# the same order the engine uses everywhere else for intervals. The winning
# row's ORIGINAL (months, ms) slot is kept, so the result is exact for the slot
# even though the ORDERING is approximate. Per-row accumulate is nogil; finalize
# boxes (months, ms) tuples over #groups (interval MIN/MAX is rare).
# ---------------------------------------------------------------------------

cdef class MinMaxIntervalCollector(BaseCollector):
    cdef int64_t* _months
    cdef int64_t* _ms
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    def __cinit__(self):
        self._months = NULL
        self._ms = NULL
        self._seen = NULL
        self._capacity = 0

    def __dealloc__(self):
        if self._months != NULL:
            free(self._months)
            self._months = NULL
        if self._ms != NULL:
            free(self._ms)
            self._ms = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        cdef void* p
        if new_count <= self._capacity:
            return
        target = _grow_target(self._capacity, new_count)
        p = malloc(<size_t>target * sizeof(int64_t))
        if self._months != NULL:
            memcpy(p, self._months, <size_t>self._capacity * sizeof(int64_t))
            free(self._months)
        self._months = <int64_t*>p
        p = malloc(<size_t>target * sizeof(int64_t))
        if self._ms != NULL:
            memcpy(p, self._ms, <size_t>self._capacity * sizeof(int64_t))
            free(self._ms)
        self._ms = <int64_t*>p
        p = malloc(<size_t>target)
        if self._seen != NULL:
            memcpy(p, self._seen, <size_t>self._capacity)
            free(self._seen)
        self._seen = <uint8_t*>p
        memset(self._seen + self._capacity, 0, <size_t>(target - self._capacity))
        self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef const DrakenIntervalSlot* slots = <const DrakenIntervalSlot*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef int64_t* months = self._months
        cdef int64_t* ms = self._ms
        cdef uint8_t* seen = self._seen
        cdef int8_t direction = self._direction
        cdef Py_ssize_t i
        cdef int64_t si
        cdef uint32_t phys
        cdef int64_t sm, sms, src_norm, grp_norm
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    phys = sel[i]
                    sm = slots[phys].months
                    sms = slots[phys].ms
                    src_norm = sm * INTERVAL_MONTH_MS + sms
                    if seen[si] == 0:
                        months[si] = sm
                        ms[si] = sms
                        seen[si] = 1
                    elif direction == 1:
                        grp_norm = months[si] * INTERVAL_MONTH_MS + ms[si]
                        if src_norm < grp_norm:
                            months[si] = sm
                            ms[si] = sms
                    elif direction == -1:
                        grp_norm = months[si] * INTERVAL_MONTH_MS + ms[si]
                        if src_norm > grp_norm:
                            months[si] = sm
                            ms[si] = sms
                    # direction 0 (ANY_VALUE): keep the first value, no update

    cdef Vector _build(self, int64_t start, int64_t stop):
        # Build a DrakenIntervalSlot buffer + validity directly (Python-free).
        cdef int64_t length = stop - start
        cdef size_t nbytes = <size_t>(length if length > 0 else 1) * sizeof(DrakenIntervalSlot)
        cdef DrakenIntervalSlot* slots = <DrakenIntervalSlot*>draken_malloc(nbytes)
        cdef uint8_t* validity = NULL
        cdef size_t bm_bytes = <size_t>((length + 7) >> 3)
        cdef int64_t g, idx
        cdef bint has_null = False
        if slots == NULL:
            raise MemoryError()
        for g in range(length):
            idx = start + g
            if idx >= self._capacity or self._seen[idx] == 0:
                slots[g].months = 0
                slots[g].ms = 0
                has_null = True
            else:
                slots[g].months = self._months[idx]
                slots[g].ms = self._ms[idx]
        if has_null:
            validity = <uint8_t*>draken_malloc(bm_bytes if bm_bytes > 0 else 1)
            if validity == NULL:
                draken_free(slots)
                raise MemoryError()
            memset(validity, 0xFF, bm_bytes)
            for g in range(length):
                idx = start + g
                if idx >= self._capacity or self._seen[idx] == 0:
                    validity[g >> 3] &= ~(<uint8_t>(1 << (g & 7)))
        return _vector_from_decoded(slots, validity, <uint32_t>length, DRAKEN_INTERVAL)

    cpdef Vector finalize(self, int64_t num_groups):
        return self._build(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        return self._build(start, stop)

# ---------------------------------------------------------------------------
# MIN/MAX for VARCHAR / NVARCHAR / VARBINARY
#
# State: one DrakenStringSlot per group + a shared arena for long payloads.
# Accumulate: zero Python objects, zero to_pylist() calls.
#
# Comparison fast-path:
#   Short strings (len ≤ 12): fully inline, two 64-bit compares, no arena.
#   Long  strings (len  > 12): str_prefix4() gives big-endian uint32 of first
#   4 bytes — uint32 comparison gives lexicographic order directly. Arena read
#   (memcmp) only when length AND prefix match, i.e. strings agree on ≥4 chars.
# ---------------------------------------------------------------------------

cdef class MinMaxVarcharCollector(BaseCollector):
    cdef DrakenStringSlot* _slots   # one slot per group; draken_malloc'd
    cdef uint8_t*   _arena          # long-string payload arena; draken_malloc'd
    cdef uint8_t*   _seen           # 1 byte per group: 0 = not yet set
    cdef size_t     _capacity       # allocated group count
    cdef size_t     _arena_cap      # bytes allocated in _arena
    cdef size_t     _arena_used     # bytes written in _arena
    cdef int8_t     _direction      # +1 = MIN, -1 = MAX
    cdef DrakenType _col_type       # source type; preserved for finalize

    _SLOT_BYTES = sizeof(DrakenStringSlot)  # 16

    def __cinit__(self):
        self._slots = NULL
        self._arena = NULL
        self._seen  = NULL
        self._capacity   = 0
        self._arena_cap  = 0
        self._arena_used = 0
        self._col_type   = DRAKEN_VARCHAR

    def __dealloc__(self):
        if self._slots != NULL:
            draken_free(self._slots)
            self._slots = NULL
        if self._arena != NULL:
            draken_free(self._arena)
            self._arena = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef size_t nc = <size_t>new_count
        cdef size_t target, i
        cdef void* p
        if nc <= self._capacity:
            return
        target = _grow_target(self._capacity, new_count)
        # Grow slot array
        p = draken_malloc(target * sizeof(DrakenStringSlot))
        if self._slots != NULL:
            memcpy(p, self._slots, self._capacity * sizeof(DrakenStringSlot))
            draken_free(self._slots)
        self._slots = <DrakenStringSlot*>p
        # Zero-init new slots (str_init_null semantics: length=0, all bytes zero)
        memset(self._slots + self._capacity, 0,
               (target - self._capacity) * sizeof(DrakenStringSlot))
        # Grow seen array
        p = malloc(target)
        if self._seen != NULL:
            memcpy(p, self._seen, self._capacity)
            free(self._seen)
        self._seen = <uint8_t*>p
        memset(self._seen + self._capacity, 0, target - self._capacity)
        self._capacity = target

    cdef inline void _ensure_arena(self, size_t need) noexcept nogil:
        cdef size_t new_cap
        cdef void* p
        if self._arena_used + need <= self._arena_cap:
            return
        new_cap = self._arena_cap * 2 if self._arena_cap > 0 else 65536
        while new_cap < self._arena_used + need:
            new_cap *= 2
        p = draken_malloc(new_cap)
        if self._arena != NULL:
            memcpy(p, self._arena, self._arena_used)
            draken_free(self._arena)
        self._arena = <uint8_t*>p
        self._arena_cap = new_cap

    cdef inline void _copy_slot(
        self,
        size_t si,
        const DrakenStringSlot* src,
        const uint8_t* src_arena,
    ) noexcept nogil:
        """Copy src slot into group si, appending to arena if needed."""
        cdef uint32_t slen = str_length(src)
        cdef uint32_t new_off
        if slen <= <uint32_t>STR_INLINE_MAX:
            str_clone_with_offset(&self._slots[si], src, 0)
        else:
            self._ensure_arena(<size_t>slen)
            new_off = <uint32_t>self._arena_used
            memcpy(self._arena + new_off, src_arena + (<uint32_t*>src)[3], slen)
            self._arena_used += slen
            str_clone_with_offset(&self._slots[si], src, new_off)

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        self._col_type = uv.type
        cdef DrakenStringArena* garena = <DrakenStringArena*>uv.data
        cdef DrakenStringSlot* src_slots = garena.slots
        cdef uint8_t* src_arena = garena.arena
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef DrakenStringSlot* grp_slot
        cdef const DrakenStringSlot* src_slot
        cdef uint32_t src_p4, grp_p4
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i
        cdef size_t si
        cdef int cmp
        cdef int8_t direction = self._direction

        with nogil:
            if direction == 0:  # ANY_VALUE — first non-null per group
                for i in range(n_rows):
                    if nulls != NULL and not _num_bitmap_valid(nulls, i):
                        continue
                    si = <size_t>state_indices[i]
                    if not seen[si]:
                        self._copy_slot(si, &src_slots[sel[i]], src_arena)
                        seen[si] = 1
            elif direction == 1:  # MIN
                for i in range(n_rows):
                    if nulls != NULL and not _num_bitmap_valid(nulls, i):
                        continue
                    si = <size_t>state_indices[i]
                    src_slot = &src_slots[sel[i]]
                    if not seen[si]:
                        self._copy_slot(si, src_slot, src_arena)
                        seen[si] = 1
                        continue
                    grp_slot = &self._slots[si]
                    src_p4 = str_prefix4(src_slot)
                    grp_p4 = str_prefix4(grp_slot)
                    if src_p4 < grp_p4:
                        self._copy_slot(si, src_slot, src_arena)
                    elif src_p4 == grp_p4:
                        cmp = str_compare(src_slot, src_arena, grp_slot, self._arena)
                        if cmp < 0:
                            self._copy_slot(si, src_slot, src_arena)
            else:               # MAX
                for i in range(n_rows):
                    if nulls != NULL and not _num_bitmap_valid(nulls, i):
                        continue
                    si = <size_t>state_indices[i]
                    src_slot = &src_slots[sel[i]]
                    if not seen[si]:
                        self._copy_slot(si, src_slot, src_arena)
                        seen[si] = 1
                        continue
                    grp_slot = &self._slots[si]
                    src_p4 = str_prefix4(src_slot)
                    grp_p4 = str_prefix4(grp_slot)
                    if src_p4 > grp_p4:
                        self._copy_slot(si, src_slot, src_arena)
                    elif src_p4 == grp_p4:
                        cmp = str_compare(src_slot, src_arena, grp_slot, self._arena)
                        if cmp > 0:
                            self._copy_slot(si, src_slot, src_arena)

    cpdef Vector finalize(self, int64_t num_groups):
        # Hand the accumulated German-string slots + arena straight to the
        # string-family builder (Python-free — no per-group bytes boxing). The
        # builder takes ownership of slots/arena/validity, so we null our pointers
        # afterwards to avoid a double free in __dealloc__.
        cdef uint8_t* seen = self._seen
        cdef size_t bm_bytes = <size_t>((num_groups + 7) >> 3)
        cdef uint8_t* validity = NULL
        cdef size_t g
        cdef bint has_null = False
        cdef PyObject* raw
        cdef Vector result
        if num_groups > 0 and seen != NULL:
            for g in range(<size_t>num_groups):
                if seen[g] == 0:
                    has_null = True
                    break
            if has_null:
                validity = <uint8_t*>draken_malloc(bm_bytes if bm_bytes > 0 else 1)
                if validity == NULL:
                    raise MemoryError()
                memset(validity, 0xFF, bm_bytes)
                for g in range(<size_t>num_groups):
                    if seen[g] == 0:
                        validity[g >> 3] &= ~(<uint8_t>(1 << (g & 7)))
        raw = draken_vector_own_string(
            self._slots, self._arena, self._arena_used, validity,
            <uint32_t>num_groups, self._col_type)
        self._slots = NULL
        self._arena = NULL
        self._capacity = 0
        self._arena_used = 0
        self._arena_cap = 0
        if seen != NULL:
            free(seen)
            self._seen = NULL
        if raw == NULL:
            raise MemoryError("draken_vector_own_string failed")
        result = <Vector>(<object>raw)
        _cn_decref(raw)
        return result

# ---------------------------------------------------------------------------
# AVG — always works in float64
# ---------------------------------------------------------------------------

cdef class AvgCollector(BaseCollector):
    cdef DrakenFixedBuffer* _sums
    cdef DrakenFixedBuffer* _counts
    cdef int64_t _capacity

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._counts != NULL:
            free_fixed_buffer(self._counts, True)
            self._counts = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_fixed_buffer(self._counts, self._capacity, target)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        # Per-row template: one type-dispatch per morsel, typed pointers
        # cached from vec.unified(), pure-C inner loop.
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector raw = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = raw.unified()
        cdef DrakenType t = uv.type
        cdef double* sums = <double*>self._sums.data
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double* f64
        cdef uint8_t* nulls = uv.validity
        cdef const uint32_t* sel = uv.selection

        # DECIMAL columns are routed to AvgDecimalCollector (exact int64 sum) by the
        # deferred resolver, so they never reach here. Integer widths accumulate in
        # double (overflow-safe for large-magnitude columns like AVG(UserID)) and are
        # read at their true width; FLOAT64 reads as double directly.
        if t == DRAKEN_INT8:
            with nogil:
                _avg_accumulate_int(<const int8_t*>uv.data, sel, nulls, state_indices, sums, counts, n_rows)
        elif t == DRAKEN_INT16:
            with nogil:
                _avg_accumulate_int(<const int16_t*>uv.data, sel, nulls, state_indices, sums, counts, n_rows)
        elif t == DRAKEN_INT32:
            with nogil:
                _avg_accumulate_int(<const int32_t*>uv.data, sel, nulls, state_indices, sums, counts, n_rows)
        elif t == DRAKEN_INT64:
            with nogil:
                _avg_accumulate_int(<const int64_t*>uv.data, sel, nulls, state_indices, sums, counts, n_rows)
        else:
            # FLOAT64 (and other 8-byte numerics via reinterpret).
            f64 = <double*>uv.data
            with nogil:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        sums[si] += f64[sel[i]]
                        counts[si] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._sums
        cdef DrakenFixedBuffer* counts = self._counts
        cdef double* sums_data = <double*>out.data
        cdef int64_t* counts_data = <int64_t*>counts.data
        cdef Py_ssize_t i

        out.length = <size_t>num_groups
        for i in range(num_groups):
            if counts_data[i] > 0:
                sums_data[i] = sums_data[i] / counts_data[i]
            else:
                _ensure_validity_bitmap(out)
                _bitmap_clear(out.null_bitmap, i)

        self._sums = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0

        free_fixed_buffer(counts, True)
        return _consume_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._sums
        cdef DrakenFixedBuffer* counts = self._counts
        cdef double* sums_data = <double*>out.data
        cdef int64_t* counts_data = <int64_t*>counts.data
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        for i in range(self._capacity):
            if counts_data[i] > 0:
                sums_data[i] = sums_data[i] / counts_data[i]
            else:
                _ensure_validity_bitmap(out)
                _bitmap_clear(out.null_bitmap, i)

        return _slice_float64_buffer(out, start, stop)

    cdef bint is_mergeable(self) noexcept:
        return True

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        # AVG holds (sum, count) per group; both add. finalize recomputes sum/count
        # so the merged ratio equals the serial average exactly (modulo float order).
        cdef AvgCollector o = <AvgCollector>other
        (<double*>self._sums.data)[self_idx] += (<double*>o._sums.data)[other_idx]
        (<int64_t*>self._counts.data)[self_idx] += (<int64_t*>o._counts.data)[other_idx]


cdef class AvgDecimalCollector(BaseCollector):
    """AVG for DECIMAL columns.

    Accumulates the RAW UNSCALED int64 sum EXACTLY per group, then produces a
    FLOAT64 average `sum / 10^scale / count` (AVG is a ratio → DOUBLE, matching
    DuckDB and the binder's AVG type). The generic AvgCollector summed
    `(double)unscaled * 10^-scale` per row, losing precision before the divide
    (the same float-accumulation error that hurt SUM). int64 accumulation matches
    SumDecimalCollector's overflow profile; int128 accumulation is the future step.
    """
    cdef DrakenFixedBuffer* _sums      # int64 raw unscaled sums
    cdef DrakenFixedBuffer* _counts    # int64 counts
    cdef int64_t _capacity
    cdef int _scale                    # column scale, set via factory resolve

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0
        self._scale = 0

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._counts != NULL:
            free_fixed_buffer(self._counts, True)
            self._counts = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_fixed_buffer(self._counts, self._capacity, target)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector raw = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = raw.unified()
        cdef int64_t* sums = <int64_t*>self._sums.data
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef int64_t* data = <int64_t*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef Py_ssize_t i
        cdef int64_t si
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += data[sel[i]]   # raw unscaled — exact
                    counts[si] += 1

    cdef DrakenFixedBuffer* _build_averages(self, int64_t count) except NULL:
        """Produce a FLOAT64 buffer of `sum / 10^scale / count` for [0, count)."""
        cdef DrakenFixedBuffer* sums = self._sums
        cdef DrakenFixedBuffer* counts = self._counts
        cdef int64_t* sums_data = <int64_t*>sums.data
        cdef int64_t* counts_data = <int64_t*>counts.data
        cdef DrakenFixedBuffer* out = alloc_fixed_buffer(DRAKEN_FLOAT64, <size_t>count, 8)
        cdef double* out_data = <double*>out.data
        cdef double divisor = 10.0 ** self._scale
        cdef Py_ssize_t i
        out.length = <size_t>count
        for i in range(count):
            if counts_data[i] > 0:
                out_data[i] = (<double>sums_data[i]) / divisor / (<double>counts_data[i])
            else:
                _ensure_validity_bitmap(out)
                _bitmap_clear(out.null_bitmap, i)
        return out

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._build_averages(num_groups)
        free_fixed_buffer(self._sums, True)
        free_fixed_buffer(self._counts, True)
        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0
        return _consume_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._build_averages(self._capacity)
        cdef Vector result = _slice_float64_buffer(out, start, stop)
        free_fixed_buffer(out, True)
        return result


# ---------------------------------------------------------------------------
# Decimal collectors — SumDecimalCollector and MinMaxDecimalCollector
# Accumulate unscaled int64 values, apply scale factor at finalize time.
# ---------------------------------------------------------------------------

cdef class SumDecimalCollector(BaseCollector):
    """
    SUM collector for DecimalVector columns.

    Accumulates the RAW UNSCALED int64 values EXACTLY per group (no float
    conversion — the old float64 path lost precision: q01/q05/q09), then finalizes
    to a DECIMAL vector at the column's scale via a DECIMAL-typed materialize +
    `set_decimal_descriptor`. int64 accumulation matches SumInt64Collector's overflow
    characteristics; int128 grouped accumulation (for sums exceeding int64) is a
    future step, mirroring the deferred ungrouped DECIMAL128 promotion.
    """
    cdef DrakenFixedBuffer* _sums
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int _scale       # column scale, set from DecimalVector on first resolve
    cdef int _precision   # result precision (int64-decimal max = 18)

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._scale = 0
        self._precision = 18

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_bitmap(&self._seen, self._capacity, target, False)
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef int64_t* sums = <int64_t*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef DrakenVector* uv

        uv = vec.unified()
        data = <int64_t*>uv.data
        sel = uv.selection
        nulls = uv.validity
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += data[sel[i]]   # raw unscaled — exact
                    _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        self._sums = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

        # Materialize the int64 sums as a DECIMAL-typed Vector and attach the
        # (precision, scale) descriptor; _materialize_fixed_buffer copies (does not
        # consume), so the source buffer is freed afterwards.
        cdef Vector dec_vec = _materialize_fixed_buffer(
            out, 0, num_groups, DRAKEN_DECIMAL, sizeof(int64_t)
        )
        free_fixed_buffer(out, True)
        if seen != NULL:
            free(seen)
        if num_groups > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
            if seen != NULL:
                free(seen)
                self._seen = NULL

        return _slice_float64_buffer(out, start, stop)


cdef class MinMaxDecimalCollector(BaseCollector):
    """
    MIN/MAX collector for DecimalVector columns.

    Compares unscaled int64 values directly (valid since all values share the
    same scale). Applies the scale factor (10^-scale) at finalize time to
    produce a Float64Vector.
    """
    cdef DrakenFixedBuffer* _values  # stores unscaled int64 min/max values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX
    cdef int _scale           # column scale, set from DecimalVector via factory
    cdef int _precision       # column precision (MIN/MAX keep the column's p)

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._scale = 0
        self._precision = 18

    def __dealloc__(self):
        if self._values != NULL:
            free_fixed_buffer(self._values, True)
            self._values = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t sentinel = INT64_MAX if self._direction == 1 else INT64_MIN
        cdef int64_t old_count = self._capacity
        cdef int64_t target
        cdef int64_t* values
        cdef int64_t i

        if new_count > old_count:
            target = _grow_target(old_count, new_count)
            _grow_fixed_buffer(self._values, old_count, target)
            _grow_bitmap(&self._seen, old_count, target, False)
            values = <int64_t*>self._values.data
            for i in range(old_count, target):
                values[i] = sentinel
            self._capacity = target

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef int64_t* values = <int64_t*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef const uint32_t* sel
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, v
        cdef DrakenVector* uv

        uv = vec.unified()
        data = <int64_t*>uv.data
        sel = uv.selection
        nulls = uv.validity
        cdef int8_t direction = self._direction
        with nogil:
            if direction == 0:   # ANY_VALUE — first non-null per group
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si):
                            values[si] = data[sel[i]]
                            _bitmap_set(seen, si)
            elif direction == 1:   # MIN
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            else:                # MAX
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        """Emit the exact unscaled int64 min/max as a DECIMAL vector (was lossy float)."""
        cdef DrakenFixedBuffer* src = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i

        src.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    src.null_bitmap = seen   # attach so materialize copies validity
                    break

        cdef Vector dec_vec = _materialize_fixed_buffer(
            src, 0, num_groups, DRAKEN_DECIMAL, sizeof(int64_t)
        )
        src.null_bitmap = NULL  # detach; _values is freed (with no bitmap) in __dealloc__
        if seen != NULL:
            free(seen)
        self._seen = NULL
        self._capacity = 0
        if num_groups > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        """Emit a DECIMAL slice (was lossy float). _materialize copies src.null_bitmap."""
        cdef DrakenFixedBuffer* src = self._values
        cdef int64_t length = stop - start
        cdef Vector dec_vec = _materialize_fixed_buffer(
            src, start, stop, DRAKEN_DECIMAL, sizeof(int64_t)
        )
        if length > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec


# ---------------------------------------------------------------------------
# DECIMAL128 (int128) grouped collectors — the 16-byte siblings of the DECIMAL
# collectors above. Accumulate raw unscaled int128 values per group; finalize to a
# DECIMAL128 vector (SUM/MIN/MAX) or FLOAT64 (AVG). Like the int64 collectors, the
# sum accumulators do not check for int128 overflow (astronomically unlikely for real
# data; matches SumDecimalCollector's posture).
# ---------------------------------------------------------------------------

cdef class SumDecimal128Collector(BaseCollector):
    cdef DrakenFixedBuffer* _sums    # int128 raw unscaled sums (16-byte slots)
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int _scale
    cdef int _precision

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_DECIMAL128, 0, 16)
        self._seen = NULL
        self._capacity = 0
        self._scale = 0
        self._precision = 38

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_bitmap(&self._seen, self._capacity, target, False)
            self._capacity = target

    cdef void accumulate(self, Morsel morsel, const uint32_t* state_indices, Py_ssize_t n_rows):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef int128_t* sums = <int128_t*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef int128_t* data = <int128_t*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef Py_ssize_t i
        cdef int64_t si
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += data[sel[i]]   # raw unscaled int128 — exact
                    _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i
        out.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break
        self._sums = alloc_fixed_buffer(DRAKEN_DECIMAL128, 0, 16)
        self._seen = NULL
        self._capacity = 0
        cdef Vector dec_vec = _materialize_fixed_buffer(out, 0, num_groups, DRAKEN_DECIMAL128, 16)
        out.null_bitmap = NULL
        free_fixed_buffer(out, True)
        if seen != NULL:
            free(seen)
        if num_groups > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._sums
        cdef uint8_t* seen = self._seen
        cdef int64_t length = stop - start
        cdef Py_ssize_t i
        out.length = <size_t>self._capacity
        if out.null_bitmap == NULL and seen != NULL:
            for i in range(self._capacity):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    self._seen = NULL
                    break
        cdef Vector dec_vec = _materialize_fixed_buffer(out, start, stop, DRAKEN_DECIMAL128, 16)
        if length > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec


cdef class MinMaxDecimal128Collector(BaseCollector):
    cdef DrakenFixedBuffer* _values   # int128 unscaled min/max (16-byte slots)
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX
    cdef int _scale
    cdef int _precision

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_DECIMAL128, 0, 16)
        self._seen = NULL
        self._capacity = 0
        self._scale = 0
        self._precision = 38

    def __dealloc__(self):
        if self._values != NULL:
            free_fixed_buffer(self._values, True)
            self._values = NULL
        if self._seen != NULL:
            free(self._seen)
            self._seen = NULL

    cdef void grow(self, int64_t new_count):
        # No sentinel fill: the _seen bitmap guards first-touch, and untouched slots
        # are masked null at finalize (so any garbage value is never observed).
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._values, self._capacity, target)
            _grow_bitmap(&self._seen, self._capacity, target, False)
            self._capacity = target

    cdef void accumulate(self, Morsel morsel, const uint32_t* state_indices, Py_ssize_t n_rows):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef int128_t* values = <int128_t*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef int128_t* data = <int128_t*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef Py_ssize_t i
        cdef int64_t si
        cdef int128_t v
        cdef int8_t direction = self._direction
        with nogil:
            if direction == 0:   # ANY_VALUE — first non-null per group
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si):
                            values[si] = data[sel[i]]
                            _bitmap_set(seen, si)
            elif direction == 1:   # MIN
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            else:                # MAX
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        v = data[sel[i]]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* src = self._values
        cdef uint8_t* seen = self._seen
        cdef Py_ssize_t i
        src.length = <size_t>num_groups
        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    src.null_bitmap = seen
                    break
        cdef Vector dec_vec = _materialize_fixed_buffer(src, 0, num_groups, DRAKEN_DECIMAL128, 16)
        src.null_bitmap = NULL
        if seen != NULL:
            free(seen)
        self._seen = NULL
        self._capacity = 0
        if num_groups > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* src = self._values
        cdef int64_t length = stop - start
        cdef Vector dec_vec = _materialize_fixed_buffer(src, start, stop, DRAKEN_DECIMAL128, 16)
        if length > 0:
            dec_vec._nb.set_decimal_descriptor(self._precision, self._scale)
        return dec_vec


cdef class AvgDecimal128Collector(BaseCollector):
    cdef DrakenFixedBuffer* _sums      # int128 raw unscaled sums (16-byte slots)
    cdef DrakenFixedBuffer* _counts    # int64 counts
    cdef int64_t _capacity
    cdef int _scale

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_DECIMAL128, 0, 16)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0
        self._scale = 0

    def __dealloc__(self):
        if self._sums != NULL:
            free_fixed_buffer(self._sums, True)
            self._sums = NULL
        if self._counts != NULL:
            free_fixed_buffer(self._counts, True)
            self._counts = NULL

    cdef void grow(self, int64_t new_count):
        cdef int64_t target
        if new_count > self._capacity:
            target = _grow_target(self._capacity, new_count)
            _grow_fixed_buffer(self._sums, self._capacity, target)
            _grow_fixed_buffer(self._counts, self._capacity, target)
            self._capacity = target

    cdef void accumulate(self, Morsel morsel, const uint32_t* state_indices, Py_ssize_t n_rows):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef int128_t* sums = <int128_t*>self._sums.data
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef int128_t* data = <int128_t*>uv.data
        cdef const uint32_t* sel = uv.selection
        cdef uint8_t* nulls = uv.validity
        cdef Py_ssize_t i
        cdef int64_t si
        with nogil:
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += data[sel[i]]
                    counts[si] += 1

    cdef DrakenFixedBuffer* _build_averages(self, int64_t count) except NULL:
        cdef int128_t* sums_data = <int128_t*>self._sums.data
        cdef int64_t* counts_data = <int64_t*>self._counts.data
        cdef DrakenFixedBuffer* out = alloc_fixed_buffer(DRAKEN_FLOAT64, <size_t>count, 8)
        cdef double* out_data = <double*>out.data
        cdef double divisor = 10.0 ** self._scale
        cdef Py_ssize_t i
        out.length = <size_t>count
        for i in range(count):
            if counts_data[i] > 0:
                out_data[i] = (<double>sums_data[i]) / divisor / (<double>counts_data[i])
            else:
                _ensure_validity_bitmap(out)
                _bitmap_clear(out.null_bitmap, i)
        return out

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* out = self._build_averages(num_groups)
        free_fixed_buffer(self._sums, True)
        free_fixed_buffer(self._counts, True)
        self._sums = alloc_fixed_buffer(DRAKEN_DECIMAL128, 0, 16)
        self._counts = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._capacity = 0
        return _consume_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef DrakenFixedBuffer* out = self._build_averages(self._capacity)
        cdef Vector result = _slice_float64_buffer(out, start, stop)
        free_fixed_buffer(out, True)
        return result
