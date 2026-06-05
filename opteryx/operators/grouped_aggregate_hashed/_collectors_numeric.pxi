# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

# Numeric collectors — COUNT, SUM, MIN, MAX, AVG.
# Numeric state lives in Draken-owned fixed buffers so finalize can hand off
# buffers without copying. No Python in accumulate().
#

from libc.stdint cimport int64_t, uint8_t, uint32_t, INT64_MAX, INT64_MIN
from libc.stddef cimport size_t
from libc.math cimport HUGE_VAL
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector, DrakenType
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_DECIMAL
from draken.core.buffers cimport DRAKEN_DECIMAL128
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY
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
    cdef void* new_data
    cdef Py_ssize_t old_bytes
    cdef Py_ssize_t new_bytes

    if new_count <= old_count:
        buf.length = <size_t>new_count
        return

    old_bytes = <Py_ssize_t>(old_count * <int64_t>buf.itemsize)
    new_bytes = <Py_ssize_t>(new_count * <int64_t>buf.itemsize)

    new_data = malloc(new_bytes) if new_bytes > 0 else NULL
    if new_bytes > 0 and new_data == NULL:
        raise MemoryError()

    if old_bytes > 0 and buf.data != NULL:
        memcpy(new_data, buf.data, old_bytes)
    if new_bytes > old_bytes:
        memset(<uint8_t*>new_data + old_bytes, 0, new_bytes - old_bytes)

    if buf.data != NULL:
        free(buf.data)
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

    new_bitmap = <uint8_t*>malloc(new_bytes)
    if new_bitmap == NULL:
        raise MemoryError()

    memset(new_bitmap, fill_byte, new_bytes)
    if old_bytes > 0 and bitmap_ref[0] != NULL:
        memcpy(new_bitmap, bitmap_ref[0], old_bytes)
        free(bitmap_ref[0])

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

    cpdef BaseCollector _clone_empty(self):
        cdef CountStarCollector c = CountStarCollector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef SumInt64Collector c = SumInt64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        return c


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

    cpdef BaseCollector _clone_empty(self):
        cdef CountValueCollector c = CountValueCollector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef SumInt64Collector c = SumInt64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        return c


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
                    sums[si] += data[sel[i]]
                    _bitmap_set(seen, si)

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

    cpdef BaseCollector _clone_empty(self):
        cdef SumInt64Collector c = SumInt64Collector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef SumInt64Collector c = SumInt64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        return c


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

    cpdef BaseCollector _clone_empty(self):
        cdef SumFloat64Collector c = SumFloat64Collector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef SumFloat64Collector c = SumFloat64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        return c


# ---------------------------------------------------------------------------
# MIN/MAX(int64)   direction: +1 = MIN, -1 = MAX
# ---------------------------------------------------------------------------

cdef class MinMaxInt64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN (use INT64_MAX as init), -1 = MAX (INT64_MIN)

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

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
            if direction == 1:   # MIN
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

        return _consume_int64_buffer(out)

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

        return _slice_int64_buffer(out, start, stop)

    cpdef BaseCollector _clone_empty(self):
        cdef MinMaxInt64Collector c = MinMaxInt64Collector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef MinMaxInt64Collector c = MinMaxInt64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c


# ---------------------------------------------------------------------------
# MIN/MAX(float64)
# ---------------------------------------------------------------------------

cdef class MinMaxFloat64Collector(BaseCollector):
    cdef DrakenFixedBuffer* _values
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0

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
        data = <double*>uv.data
        sel = uv.selection
        nulls = uv.validity
        cdef int8_t direction = self._direction
        with nogil:
            if direction == 1:   # MIN
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

        return _slice_float64_buffer(out, start, stop)

    cpdef BaseCollector _clone_empty(self):
        cdef MinMaxFloat64Collector c = MinMaxFloat64Collector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef MinMaxFloat64Collector c = MinMaxFloat64Collector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c


# ---------------------------------------------------------------------------
# MIN/MAX on generic (non-numeric) columns — Python comparison via to_pylist
# ---------------------------------------------------------------------------

cdef class MinMaxObjectCollector(BaseCollector):
    """MIN/MAX for non-numeric types via Python comparison."""
    cdef vector[vector[uint8_t]] _values  # C-level string storage per group
    cdef vector[uint8_t] _seen
    cdef int8_t _direction    # +1 = MIN, -1 = MAX
    cdef DrakenType _col_type # source column type, captured on first accumulate

    def __cinit__(self):
        self._col_type = DRAKEN_VARBINARY

    cdef void grow(self, int64_t new_count):
        while <int64_t>self._values.size() < new_count:
            self._values.push_back(vector[uint8_t]())
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef uint8_t* seen = self._seen.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef bytes cur, v
        cdef object v_obj
        cdef list col
        cdef vector[uint8_t]* cur_vec

        # Preserve the source column type so finalize emits the same type.
        self._col_type = vec.unified().type

        col = vec.to_pylist()

        if self._direction == 1:   # MIN
            for i in range(n_rows):
                v_obj = col[i]
                if v_obj is None:
                    continue
                v = v_obj if isinstance(v_obj, bytes) else str(v_obj).encode('utf-8')
                si = state_indices[i]
                cur_vec = &self._values[si]
                if cur_vec.empty():
                    self._values[si] = v
                else:
                    cur = bytes(cur_vec.data()[:cur_vec.size()])
                    if v < cur:
                        self._values[si] = v
                seen[si] = 1
        else:                      # MAX
            for i in range(n_rows):
                v_obj = col[i]
                if v_obj is None:
                    continue
                v = v_obj if isinstance(v_obj, bytes) else str(v_obj).encode('utf-8')
                si = state_indices[i]
                cur_vec = &self._values[si]
                if cur_vec.empty():
                    self._values[si] = v
                else:
                    cur = bytes(cur_vec.data()[:cur_vec.size()])
                    if v > cur:
                        self._values[si] = v
                seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        cdef list result = []
        cdef int64_t i
        cdef int64_t limit = min(<int64_t>self._values.size(), num_groups)
        cdef vector[uint8_t]* vec
        for i in range(limit):
            vec = &self._values[i]
            if vec.empty():
                result.append(None)
            else:
                result.append(bytes(vec.data()[:vec.size()]))
        # Emit with the source column's type (raw bytes, no decode).
        nb = _draken_native.vector_string_family_from_bytes(result, <int>self._col_type)
        return _V(nb)

    cpdef BaseCollector _clone_empty(self):
        cdef MinMaxObjectCollector c = MinMaxObjectCollector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef MinMaxObjectCollector c = MinMaxObjectCollector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        c._direction = self._direction
        return c


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
            if direction == 1:  # MIN
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
        cdef list result = []
        cdef size_t i
        cdef uint8_t* seen = self._seen
        cdef DrakenStringSlot* slot
        cdef const uint8_t* payload
        cdef uint32_t slen
        for i in range(<size_t>num_groups):
            if not seen[i]:
                result.append(None)
            else:
                slot = &self._slots[i]
                slen = str_length(slot)
                payload = str_data(slot, self._arena)
                result.append(bytes(payload[:slen]))
        nb = _draken_native.vector_string_family_from_bytes(result, <int>self._col_type)
        return _V(nb)

    cpdef BaseCollector _clone_empty(self):
        cdef MinMaxVarcharCollector c = MinMaxVarcharCollector()
        c.column_name = self.column_name
        c.result_name = self.result_name
        c._direction  = self._direction
        return c

    cpdef BaseCollector _clone_as_merge(self):
        cdef MinMaxVarcharCollector c = MinMaxVarcharCollector()
        c.column_name = self.result_name
        c.result_name = self.result_name
        c._direction  = self._direction
        return c


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
        cdef int64_t* i64
        cdef double* f64
        cdef uint8_t* nulls = uv.validity
        cdef const uint32_t* sel = uv.selection

        # DECIMAL columns are routed to AvgDecimalCollector (exact int64 sum) by the
        # deferred resolver, so they never reach here. INT64 accumulates in double
        # (overflow-safe for large-magnitude columns like AVG(UserID)); everything
        # else reinterprets as FLOAT64.
        if t == DRAKEN_INT64:
            i64 = <int64_t*>uv.data
            with nogil:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        si = state_indices[i]
                        sums[si] += i64[sel[i]]
                        counts[si] += 1
        else:
            # Default to FLOAT64 (also handles other numerics via reinterpret).
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
            if direction == 1:   # MIN
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
            if direction == 1:   # MIN
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
