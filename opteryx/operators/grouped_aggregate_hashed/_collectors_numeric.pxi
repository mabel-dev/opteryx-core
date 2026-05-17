# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

# Numeric collectors — COUNT, SUM, MIN, MAX, AVG.
# Numeric state lives in Draken-owned fixed buffers so finalize can hand off
# buffers without copying. No Python in accumulate().
#
# Constant-encoding: dense_ptr() returns NULL for DRAKEN_ENCODING_CONSTANT vectors.
# Every collector that calls dense_ptr() must check _has_const first and use
# _const_value instead.

from libc.stdint cimport int64_t, uint8_t, INT64_MAX, INT64_MIN
from libc.math cimport HUGE_VAL
from libc.string cimport memset, memcpy, memcmp
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_FLOAT64
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport Vector
from draken.vectors.int64_vector cimport Int64Vector, _materialize_dict_int64, _refresh_unified_int64
from draken.vectors.float64_vector cimport Float64Vector, _materialize_dict_float64, _refresh_unified_float64
from draken.vectors.string_vector cimport StringVector, _StringVectorCIterator, StringElement, _materialize_dict_string
from draken.vectors._decimal_vector cimport DecimalVector


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


cdef inline Int64Vector _wrap_int64_buffer(DrakenFixedBuffer* buf) except *:
    cdef Int64Vector vec = Int64Vector(0, True)
    vec.ptr = buf
    vec.owns_data = True
    vec._dict_values = NULL
    vec._dict_codes = NULL
    vec._dict_code_width = 0
    vec._dict_ordered = 0
    vec._dict_accessor.codes = NULL
    vec._dict_accessor.code_width = 0
    vec._dict_accessor.row_nulls = NULL
    vec._dict_accessor.length = 0
    vec._dict_accessor.dict_values = NULL
    vec._dict_accessor.value_type = DRAKEN_INT64
    vec._const_accessor.length = 0
    vec._const_accessor.value_type = DRAKEN_INT64
    vec._const_accessor.value_ptr = NULL
    vec._const_accessor.is_null = 0
    vec._const_value = 0
    vec._has_const = False
    vec._const_is_null = False
    _refresh_unified_int64(vec)
    return vec


cdef inline Float64Vector _wrap_float64_buffer(DrakenFixedBuffer* buf) except *:
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = buf
    vec.owns_data = True
    vec._dict_values = NULL
    vec._dict_codes = NULL
    vec._dict_code_width = 0
    vec._dict_ordered = 0
    vec._dict_accessor.codes = NULL
    vec._dict_accessor.code_width = 0
    vec._dict_accessor.row_nulls = NULL
    vec._dict_accessor.length = 0
    vec._dict_accessor.dict_values = NULL
    vec._dict_accessor.value_type = DRAKEN_FLOAT64
    vec._const_accessor.length = 0
    vec._const_accessor.value_type = DRAKEN_FLOAT64
    vec._const_accessor.value_ptr = NULL
    vec._const_accessor.is_null = 0
    vec._const_value = 0.0
    vec._has_const = False
    vec._const_is_null = False
    _refresh_unified_float64(vec)
    return vec


cdef inline Int64Vector _slice_int64_buffer(
    DrakenFixedBuffer* src,
    int64_t start,
    int64_t stop,
) except *:
    cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
    cdef Int64Vector out = Int64Vector(<size_t>length)
    cdef int64_t* src_data = <int64_t*>src.data
    cdef int64_t* out_data = <int64_t*>out.ptr.data
    cdef Py_ssize_t i

    if length <= 0:
        return out

    memcpy(out_data, src_data + start, length * sizeof(int64_t))

    if src.null_bitmap != NULL:
        out.ptr.null_bitmap = _alloc_all_valid_bitmap(length)
        for i in range(length):
            if not _num_bitmap_valid(src.null_bitmap, start + i):
                _bitmap_clear(out.ptr.null_bitmap, i)

    return out


cdef inline Float64Vector _slice_float64_buffer(
    DrakenFixedBuffer* src,
    int64_t start,
    int64_t stop,
) except *:
    cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
    cdef Float64Vector out = Float64Vector(<size_t>length)
    cdef double* src_data = <double*>src.data
    cdef double* out_data = <double*>out.ptr.data
    cdef Py_ssize_t i

    if length <= 0:
        return out

    memcpy(out_data, src_data + start, length * sizeof(double))

    if src.null_bitmap != NULL:
        out.ptr.null_bitmap = _alloc_all_valid_bitmap(length)
        for i in range(length):
            if not _num_bitmap_valid(src.null_bitmap, start + i):
                _bitmap_clear(out.ptr.null_bitmap, i)

    return out


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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector vec = morsel.column(self.column_name)
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
        cdef uint8_t* nulls
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef DrakenVector* uv

        # Constant-encoded vector: either all-null or all-non-null
        if isinstance(vec, Int64Vector):
            iv = <Int64Vector>vec
            uv = iv.unified()
            if uv.data_length == 1:
                if uv.validity == NULL:
                    for i in range(n_rows):
                        counts[state_indices[i]] += 1
                return
        elif isinstance(vec, Float64Vector):
            fv = <Float64Vector>vec
            uv = fv.unified()
            if uv.data_length == 1:
                if uv.validity == NULL:
                    for i in range(n_rows):
                        counts[state_indices[i]] += 1
                return

        nulls = vec.null_bitmap_ptr()
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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Int64Vector vec = <Int64Vector>morsel.column(self.column_name)
        cdef int64_t* sums = <int64_t*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, const_val
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                const_val = (<int64_t*>uv.data)[0]
                for i in range(n_rows):
                    si = state_indices[i]
                    sums[si] += const_val
                    _bitmap_set(seen, si)
            return

        if uv.selection != NULL:
            vec = _materialize_dict_int64(vec)
        data = <int64_t*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                sums[si] += data[i]
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
        return _wrap_int64_buffer(out)

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Float64Vector vec = <Float64Vector>morsel.column(self.column_name)
        cdef double* sums = <double*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef double* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double const_val
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                const_val = (<double*>uv.data)[0]
                for i in range(n_rows):
                    si = state_indices[i]
                    sums[si] += const_val
                    _bitmap_set(seen, si)
            return

        if uv.selection != NULL:
            vec = _materialize_dict_float64(vec)
        data = <double*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                sums[si] += data[i]
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

        return _wrap_float64_buffer(out)

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Int64Vector vec = <Int64Vector>morsel.column(self.column_name)
        cdef int64_t* values = <int64_t*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, v
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                v = (<int64_t*>uv.data)[0]
                if self._direction == 1:   # MIN
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
                else:                      # MAX
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            return

        if uv.selection != NULL:
            vec = _materialize_dict_int64(vec)
        data = <int64_t*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not _num_bitmap_valid(seen, si) or v < values[si]:
                        values[si] = v
                    _bitmap_set(seen, si)
        else:                      # MAX
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
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

        return _wrap_int64_buffer(out)

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Float64Vector vec = <Float64Vector>morsel.column(self.column_name)
        cdef double* values = <double*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef double* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double v
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                v = (<double*>uv.data)[0]
                if self._direction == 1:   # MIN
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
                else:                      # MAX
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            return

        if uv.selection != NULL:
            vec = _materialize_dict_float64(vec)
        data = <double*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not _num_bitmap_valid(seen, si) or v < values[si]:
                        values[si] = v
                    _bitmap_set(seen, si)
        else:                      # MAX
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
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

        return _wrap_float64_buffer(out)

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
    """Native MIN/MAX using C-level buffers (no Python in accumulate loop)."""
    cdef vector[vector[uint8_t]] _values  # C-level string storage per group
    cdef vector[uint8_t] _seen
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    def __cinit__(self):
        pass

    cdef void grow(self, int64_t new_count):
        while <int64_t>self._values.size() < new_count:
            self._values.push_back(vector[uint8_t]())
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector vec = morsel.column(self.column_name)
        cdef uint8_t* seen = self._seen.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef bytes cur, v
        cdef object v_obj
        cdef list col
        cdef StringVector sv_native

        # Fast path: StringVector with C-level iteration (no Python materialization)
        if isinstance(vec, StringVector):
            sv_native = <StringVector>vec
            if sv_native.unified().selection != NULL:
                sv_native = _materialize_dict_string(sv_native)
            self._accumulate_string_vector_native(sv_native, state_indices, n_rows, seen)
        else:
            # Fallback for other types (dates, etc.)
            col = vec.to_pylist()

            if self._direction == 1:   # MIN
                for i in range(n_rows):
                    v_obj = col[i]
                    if v_obj is None:
                        continue
                    v = v_obj if isinstance(v_obj, bytes) else str(v_obj).encode('utf-8')
                    si = state_indices[i]
                    cur = self._values[si]
                    if not cur or v < cur:
                        self._values[si] = v
                    seen[si] = 1
            else:                      # MAX
                for i in range(n_rows):
                    v_obj = col[i]
                    if v_obj is None:
                        continue
                    v = v_obj if isinstance(v_obj, bytes) else str(v_obj).encode('utf-8')
                    si = state_indices[i]
                    cur = self._values[si]
                    if not cur or v > cur:
                        self._values[si] = v
                    seen[si] = 1

    cdef void _accumulate_string_vector_native(
        self,
        StringVector vec,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
        uint8_t* seen,
    ):
        cdef _StringVectorCIterator it = _StringVectorCIterator._from_ptr(vec.ptr)
        cdef StringElement elem
        cdef Py_ssize_t i
        cdef int64_t si
        cdef Py_ssize_t cur_len
        cdef int cmp_result
        cdef Py_ssize_t min_len
        cdef vector[uint8_t]* cur_vec
        cdef uint8_t* cur_ptr

        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if not it.next(&elem):
                    break
                if elem.is_null:
                    continue
                si = state_indices[i]
                cur_vec = &self._values[si]
                if cur_vec.empty():
                    cur_vec.assign(elem.ptr, elem.ptr + elem.length)
                else:
                    cur_len = cur_vec.size()
                    min_len = elem.length if elem.length < cur_len else cur_len
                    cmp_result = memcmp(elem.ptr, cur_vec.data(), min_len)
                    if cmp_result < 0 or (cmp_result == 0 and elem.length < cur_len):
                        cur_vec.clear()
                        cur_vec.assign(elem.ptr, elem.ptr + elem.length)
                seen[si] = 1
        else:                      # MAX
            for i in range(n_rows):
                if not it.next(&elem):
                    break
                if elem.is_null:
                    continue
                si = state_indices[i]
                cur_vec = &self._values[si]
                if cur_vec.empty():
                    cur_vec.assign(elem.ptr, elem.ptr + elem.length)
                else:
                    cur_len = cur_vec.size()
                    min_len = elem.length if elem.length < cur_len else cur_len
                    cmp_result = memcmp(elem.ptr, cur_vec.data(), min_len)
                    if cmp_result > 0 or (cmp_result == 0 and elem.length > cur_len):
                        cur_vec.clear()
                        cur_vec.assign(elem.ptr, elem.ptr + elem.length)
                seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from draken.interop.arrow import vector_from_sequence
        cdef list result = []
        cdef int64_t i
        cdef int64_t limit = min(<int64_t>self._values.size(), num_groups)
        cdef vector[uint8_t]* vec
        for i in range(limit):
            vec = &self._values[i]
            if vec.empty():
                result.append(None)
            else:
                result.append(bytes(vec.data()[:vec.size()]).decode('utf-8'))
        return vector_from_sequence(result)

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector raw = morsel.column(self.column_name)
        cdef double* sums = <double*>self._sums.data
        cdef int64_t* counts = <int64_t*>self._counts.data
        cdef Py_ssize_t i
        cdef int64_t si
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef DecimalVector dv
        cdef int64_t* i64
        cdef int64_t* dec_data
        cdef double* f64
        cdef uint8_t* nulls
        cdef double const_f64
        cdef double dec_factor
        cdef int64_t const_i64
        cdef DrakenVector* uv

        if isinstance(raw, Int64Vector):
            iv = <Int64Vector>raw
            uv = iv.unified()
            if uv.data_length == 1:
                if uv.validity == NULL:
                    const_i64 = (<int64_t*>uv.data)[0]
                    for i in range(n_rows):
                        si = state_indices[i]
                        sums[si] += const_i64
                        counts[si] += 1
                return
            if uv.selection != NULL:
                iv = _materialize_dict_int64(iv)
            i64 = <int64_t*>iv.dense_ptr()
            nulls = iv.null_bitmap_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += i64[i]
                    counts[si] += 1
        elif isinstance(raw, DecimalVector):
            dv = <DecimalVector>raw
            dec_factor = 10.0 ** (-dv._scale)
            uv = dv.unified()
            if uv.data_length == 1:
                if uv.validity == NULL:
                    const_f64 = <double>(<int64_t*>uv.data)[0] * dec_factor
                    for i in range(n_rows):
                        si = state_indices[i]
                        sums[si] += const_f64
                        counts[si] += 1
                return
            dec_data = <int64_t*>dv.ptr.data
            nulls = dv.null_bitmap_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += <double>dec_data[i] * dec_factor
                    counts[si] += 1
        else:
            fv = <Float64Vector>raw
            uv = fv.unified()
            if uv.data_length == 1:
                if uv.validity == NULL:
                    const_f64 = (<double*>uv.data)[0]
                    for i in range(n_rows):
                        si = state_indices[i]
                        sums[si] += const_f64
                        counts[si] += 1
                return
            if uv.selection != NULL:
                fv = _materialize_dict_float64(fv)
            f64 = <double*>fv.dense_ptr()
            nulls = fv.null_bitmap_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += f64[i]
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
        return _wrap_float64_buffer(out)

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


# ---------------------------------------------------------------------------
# Decimal collectors — SumDecimalCollector and MinMaxDecimalCollector
# Accumulate unscaled int64 values, apply scale factor at finalize time.
# ---------------------------------------------------------------------------

cdef class SumDecimalCollector(BaseCollector):
    """
    SUM collector for DecimalVector columns.

    Accumulates unscaled int64 sums in a float64 buffer (to avoid int64 overflow
    on large datasets). The scale factor (10^-scale) is applied once on first
    accumulate, converting unscaled int64 to actual decimal value before summing.
    Finalize returns a Float64Vector.
    """
    cdef DrakenFixedBuffer* _sums
    cdef uint8_t* _seen
    cdef int64_t _capacity
    cdef double _factor   # 10 ^ (-scale), set from DecimalVector on first accumulate

    def __cinit__(self):
        self._sums = alloc_fixed_buffer(DRAKEN_FLOAT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._factor = 1.0

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef DecimalVector vec = <DecimalVector>morsel.column(self.column_name)
        cdef double* sums = <double*>self._sums.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double const_val
        cdef double factor = self._factor
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                const_val = <double>(<int64_t*>uv.data)[0] * factor
                for i in range(n_rows):
                    si = state_indices[i]
                    sums[si] += const_val
                    _bitmap_set(seen, si)
            return

        data = <int64_t*>vec.ptr.data
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                sums[si] += <double>data[i] * factor
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

        return _wrap_float64_buffer(out)

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
    cdef double _factor       # 10 ^ (-scale), set from DecimalVector on first accumulate

    def __cinit__(self):
        self._values = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
        self._seen = NULL
        self._capacity = 0
        self._factor = 1.0

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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef DecimalVector vec = <DecimalVector>morsel.column(self.column_name)
        cdef int64_t* values = <int64_t*>self._values.data
        cdef uint8_t* seen = self._seen
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, v
        cdef DrakenVector* uv

        uv = vec.unified()
        if uv.data_length == 1:
            if uv.validity == NULL:
                v = (<int64_t*>uv.data)[0]
                if self._direction == 1:   # MIN
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v < values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
                else:                      # MAX
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not _num_bitmap_valid(seen, si) or v > values[si]:
                            values[si] = v
                        _bitmap_set(seen, si)
            return

        data = <int64_t*>vec.ptr.data
        nulls = vec.null_bitmap_ptr()
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not _num_bitmap_valid(seen, si) or v < values[si]:
                        values[si] = v
                    _bitmap_set(seen, si)
        else:                      # MAX
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not _num_bitmap_valid(seen, si) or v > values[si]:
                        values[si] = v
                    _bitmap_set(seen, si)

    cpdef Vector finalize(self, int64_t num_groups):
        """Apply scale factor and return Float64Vector."""
        cdef DrakenFixedBuffer* src = self._values
        cdef uint8_t* seen = self._seen
        cdef DrakenFixedBuffer* out
        cdef int64_t* src_data
        cdef double* out_data
        cdef double factor = self._factor
        cdef Py_ssize_t i

        out = alloc_fixed_buffer(DRAKEN_FLOAT64, <size_t>num_groups, 8)
        src_data = <int64_t*>src.data
        out_data = <double*>out.data

        for i in range(num_groups):
            out_data[i] = <double>src_data[i] * factor

        if seen != NULL:
            for i in range(num_groups):
                if not _num_bitmap_valid(seen, i):
                    out.null_bitmap = seen
                    seen = NULL
                    break

        if seen != NULL:
            free(seen)
        self._seen = NULL
        self._capacity = 0

        return _wrap_float64_buffer(out)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        """Apply scale factor and return slice as Float64Vector."""
        cdef DrakenFixedBuffer* src = self._values
        cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
        cdef DrakenFixedBuffer* out
        cdef int64_t* src_data
        cdef double* out_data
        cdef double factor = self._factor
        cdef Py_ssize_t i

        out = alloc_fixed_buffer(DRAKEN_FLOAT64, <size_t>length, 8)
        src_data = <int64_t*>src.data
        out_data = <double*>out.data

        for i in range(length):
            out_data[i] = <double>src_data[start + i] * factor

        if src.null_bitmap != NULL:
            out.null_bitmap = _alloc_all_valid_bitmap(length)
            for i in range(length):
                if not _num_bitmap_valid(src.null_bitmap, start + i):
                    _bitmap_clear(out.null_bitmap, i)

        return _wrap_float64_buffer(out)
