# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

# Numeric collectors — COUNT, SUM, MIN, MAX, AVG.
# All state lives in C++ typed vectors.  No Python in accumulate().
#
# Constant-encoding: dense_ptr() returns NULL for DRAKEN_ENCODING_CONSTANT vectors.
# Every collector that calls dense_ptr() must check _has_const first and use
# _const_value instead.

from libc.stdint cimport int64_t, uint8_t, INT64_MAX, INT64_MIN
from libc.math cimport HUGE_VAL
from libcpp.vector cimport vector

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector


cdef inline bint _num_bitmap_valid(uint8_t* bm, Py_ssize_t i) noexcept nogil:
    if bm == NULL:
        return True
    return ((bm[i >> 3] >> (i & 7)) & 1) != 0


# ---------------------------------------------------------------------------
# COUNT(*) — no column, counts every row
# ---------------------------------------------------------------------------

cdef class CountStarCollector(BaseCollector):
    cdef vector[int64_t] _counts
    cdef long long _time_finalize_ns

    cdef void grow(self, int64_t new_count):
        while self._counts.size() < <size_t>new_count:
            self._counts.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef int64_t* counts = self._counts.data()
        cdef Py_ssize_t i
        for i in range(n_rows):
            counts[state_indices[i]] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._counts[i])
        self._time_finalize_ns += _now_ns() - start_ns
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# COUNT(col) — skip NULLs
# ---------------------------------------------------------------------------

cdef class CountValueCollector(BaseCollector):
    cdef vector[int64_t] _counts
    cdef long long _time_finalize_ns

    cdef void grow(self, int64_t new_count):
        while self._counts.size() < <size_t>new_count:
            self._counts.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector vec = morsel.column(self.column_name)
        cdef int64_t* counts = self._counts.data()
        cdef Py_ssize_t i
        cdef uint8_t* nulls
        cdef Int64Vector iv
        cdef Float64Vector fv

        # Constant-encoded vector: either all-null or all-non-null
        if isinstance(vec, Int64Vector):
            iv = <Int64Vector>vec
            if iv._has_const:
                if not iv._const_is_null:
                    for i in range(n_rows):
                        counts[state_indices[i]] += 1
                return
        elif isinstance(vec, Float64Vector):
            fv = <Float64Vector>vec
            if fv._has_const:
                if not fv._const_is_null:
                    for i in range(n_rows):
                        counts[state_indices[i]] += 1
                return

        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                counts[state_indices[i]] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._counts[i])
        self._time_finalize_ns += _now_ns() - start_ns
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# SUM(int64)
# ---------------------------------------------------------------------------

cdef class SumInt64Collector(BaseCollector):
    cdef vector[int64_t] _sums
    cdef vector[uint8_t] _seen   # 1 = at least one non-NULL row seen
    cdef long long _time_finalize_ns

    cdef void grow(self, int64_t new_count):
        while self._sums.size() < <size_t>new_count:
            self._sums.push_back(0)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Int64Vector vec = <Int64Vector>morsel.column(self.column_name)
        cdef int64_t* sums = self._sums.data()
        cdef uint8_t* seen = self._seen.data()
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, const_val

        if vec._has_const:
            if not vec._const_is_null:
                const_val = vec._const_value
                for i in range(n_rows):
                    si = state_indices[i]
                    sums[si] += const_val
                    seen[si] = 1
            return

        data = <int64_t*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                sums[si] += data[i]
                seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._sums[i] if self._seen[i] else None)
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# SUM(float64)
# ---------------------------------------------------------------------------

cdef class SumFloat64Collector(BaseCollector):
    cdef vector[double] _sums
    cdef vector[uint8_t] _seen

    cdef void grow(self, int64_t new_count):
        while self._sums.size() < <size_t>new_count:
            self._sums.push_back(0.0)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Float64Vector vec = <Float64Vector>morsel.column(self.column_name)
        cdef double* sums = self._sums.data()
        cdef uint8_t* seen = self._seen.data()
        cdef double* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double const_val

        if vec._has_const:
            if not vec._const_is_null:
                const_val = vec._const_value
                for i in range(n_rows):
                    si = state_indices[i]
                    sums[si] += const_val
                    seen[si] = 1
            return

        data = <double*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                sums[si] += data[i]
                seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._sums[i] if self._seen[i] else None)
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# MIN/MAX(int64)   direction: +1 = MIN, -1 = MAX
# ---------------------------------------------------------------------------

cdef class MinMaxInt64Collector(BaseCollector):
    cdef vector[int64_t] _values
    cdef vector[uint8_t] _seen
    cdef int8_t _direction    # +1 = MIN (use INT64_MAX as init), -1 = MAX (INT64_MIN)

    cdef void grow(self, int64_t new_count):
        cdef int64_t sentinel = INT64_MAX if self._direction == 1 else INT64_MIN
        while self._values.size() < <size_t>new_count:
            self._values.push_back(sentinel)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Int64Vector vec = <Int64Vector>morsel.column(self.column_name)
        cdef int64_t* values = self._values.data()
        cdef uint8_t* seen = self._seen.data()
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si, v

        if vec._has_const:
            if not vec._const_is_null:
                v = vec._const_value
                if self._direction == 1:   # MIN
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not seen[si] or v < values[si]:
                            values[si] = v
                        seen[si] = 1
                else:                      # MAX
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not seen[si] or v > values[si]:
                            values[si] = v
                        seen[si] = 1
            return

        data = <int64_t*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not seen[si] or v < values[si]:
                        values[si] = v
                    seen[si] = 1
        else:                      # MAX
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not seen[si] or v > values[si]:
                        values[si] = v
                    seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._values[i] if self._seen[i] else None)
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# MIN/MAX(float64)
# ---------------------------------------------------------------------------

cdef class MinMaxFloat64Collector(BaseCollector):
    cdef vector[double] _values
    cdef vector[uint8_t] _seen
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    cdef void grow(self, int64_t new_count):
        cdef double sentinel = HUGE_VAL if self._direction == 1 else -HUGE_VAL
        while self._values.size() < <size_t>new_count:
            self._values.push_back(sentinel)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Float64Vector vec = <Float64Vector>morsel.column(self.column_name)
        cdef double* values = self._values.data()
        cdef uint8_t* seen = self._seen.data()
        cdef double* data
        cdef uint8_t* nulls
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double v

        if vec._has_const:
            if not vec._const_is_null:
                v = vec._const_value
                if self._direction == 1:   # MIN
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not seen[si] or v < values[si]:
                            values[si] = v
                        seen[si] = 1
                else:                      # MAX
                    for i in range(n_rows):
                        si = state_indices[i]
                        if not seen[si] or v > values[si]:
                            values[si] = v
                        seen[si] = 1
            return

        data = <double*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not seen[si] or v < values[si]:
                        values[si] = v
                    seen[si] = 1
        else:                      # MAX
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    v = data[i]
                    if not seen[si] or v > values[si]:
                        values[si] = v
                    seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._values[i] if self._seen[i] else None)
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# MIN/MAX on generic (non-numeric) columns — Python comparison via to_pylist
# ---------------------------------------------------------------------------

cdef class MinMaxObjectCollector(BaseCollector):
    """Fallback for date/time/string MIN/MAX — uses Python comparison."""
    cdef list _values
    cdef vector[uint8_t] _seen
    cdef int8_t _direction    # +1 = MIN, -1 = MAX

    def __cinit__(self):
        self._values = []

    cdef void grow(self, int64_t new_count):
        while len(self._values) < new_count:
            self._values.append(None)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef list col = morsel.column(self.column_name).to_pylist()
        cdef uint8_t* seen = self._seen.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef object v, cur
        if self._direction == 1:   # MIN
            for i in range(n_rows):
                v = col[i]
                if v is None:
                    continue
                si = state_indices[i]
                cur = self._values[si]
                if cur is None or v < cur:
                    self._values[si] = v
                seen[si] = 1
        else:                      # MAX
            for i in range(n_rows):
                v = col[i]
                if v is None:
                    continue
                si = state_indices[i]
                cur = self._values[si]
                if cur is None or v > cur:
                    self._values[si] = v
                seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        return vector_from_sequence(self._values[:num_groups])


# ---------------------------------------------------------------------------
# AVG — always works in float64
# ---------------------------------------------------------------------------

cdef class AvgCollector(BaseCollector):
    cdef vector[double] _sums
    cdef vector[int64_t] _counts

    cdef void grow(self, int64_t new_count):
        while self._sums.size() < <size_t>new_count:
            self._sums.push_back(0.0)
            self._counts.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector raw = morsel.column(self.column_name)
        cdef double* sums = self._sums.data()
        cdef int64_t* counts = self._counts.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef int64_t* i64
        cdef double* f64
        cdef uint8_t* nulls
        cdef double const_f64
        cdef int64_t const_i64

        if isinstance(raw, Int64Vector):
            iv = <Int64Vector>raw
            if iv._has_const:
                if not iv._const_is_null:
                    const_i64 = iv._const_value
                    for i in range(n_rows):
                        si = state_indices[i]
                        sums[si] += const_i64
                        counts[si] += 1
                return
            i64 = <int64_t*>iv.dense_ptr()
            nulls = iv.null_bitmap_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += i64[i]
                    counts[si] += 1
        else:
            fv = <Float64Vector>raw
            if fv._has_const:
                if not fv._const_is_null:
                    const_f64 = fv._const_value
                    for i in range(n_rows):
                        si = state_indices[i]
                        sums[si] += const_f64
                        counts[si] += 1
                return
            f64 = <double*>fv.dense_ptr()
            nulls = fv.null_bitmap_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    sums[si] += f64[i]
                    counts[si] += 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        cdef int64_t cnt
        for i in range(num_groups):
            cnt = self._counts[i]
            vals.append(self._sums[i] / cnt if cnt > 0 else None)
        return vector_from_sequence(vals)
