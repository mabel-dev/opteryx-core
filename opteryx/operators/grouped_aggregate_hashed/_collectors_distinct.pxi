# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# COUNT(DISTINCT col) and ANY_VALUE collectors.
# CountDistinctCollector: one CarcharSet* per group — pure C++.
# AnyValueInt64/Float64Collector: vector[int64/double] + seen — pure C++.
# AnyValueObjectCollector: fallback for non-numeric types (string, date, etc.)
#   — Python list for values, but grow()/accumulate() stay in Cython.

from libc.stdint cimport int64_t, uint8_t, uint64_t
from libc.stdlib cimport malloc, free
from libcpp.vector cimport vector

from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer


cdef extern from "carchar_set.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharSet:
        CarcharSet() except +
        size_t size() const
        bint insert_or_ignore(uint64_t key) except +
        size_t insert_many(const uint64_t* keys, size_t length) except +
        void tighten() noexcept


# ---------------------------------------------------------------------------
# COUNT(DISTINCT col) — one CarcharSet* per group, no Python
# ---------------------------------------------------------------------------

cdef class CountDistinctCollector(BaseCollector):
    cdef vector[CarcharSet*] _sets
    cdef vector[vector[uint64_t]] _scratch_per_group

    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._scratch_per_group = []

    def __dealloc__(self):
        cdef Py_ssize_t i
        for i in range(self._sets.size()):
            if self._sets[i] != NULL:
                del self._sets[i]
        self._sets.clear()
        self._scratch_per_group.clear()

    cdef void grow(self, int64_t new_count):
        while self._sets.size() < <size_t>new_count:
            self._sets.push_back(new CarcharSet())
            self._scratch_per_group.push_back(vector[uint64_t]())

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
     ):
         # Hash the value column once, then batch-insert by group for better cache locality
        cdef long long start_ns = _now_ns()
        cdef uint64_t[::1] hashes = morsel.hash([self.column_name])
        cdef CarcharSet** sets = self._sets.data()
        cdef vector[uint64_t]* scratch
        cdef Py_ssize_t i

        # Collect hashes per group (batching)
        for i in range(n_rows):
            scratch = &self._scratch_per_group[state_indices[i]]
            scratch.push_back(hashes[i])

        # Now bulk-insert each group's collected hashes using insert_many()
        cdef Py_ssize_t g
        cdef vector[uint64_t]* per_group
        for g in range(self._sets.size()):
            per_group = &self._scratch_per_group[g]
            if per_group.size() > 0:
                sets[g].insert_many(per_group.data(), per_group.size())
                per_group.clear()  # Reset for next batch

        self._time_finalize_ns += _now_ns() - start_ns

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i

        # Tighten each set to exact size before counting
        for i in range(num_groups):
            self._sets[i].tighten()
            vals.append(<int64_t>self._sets[i].size())

        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# ANY_VALUE(int64) — first non-NULL value per group
# ---------------------------------------------------------------------------

cdef class AnyValueInt64Collector(BaseCollector):
    cdef vector[int64_t] _values
    cdef vector[uint8_t] _seen
    cdef long long _time_finalize_ns

    cdef void grow(self, int64_t new_count):
        while self._values.size() < <size_t>new_count:
            self._values.push_back(0)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Int64Vector vec = <Int64Vector>morsel.column(self.column_name)
        cdef int64_t* data
        cdef uint8_t* nulls
        cdef int64_t* values = self._values.data()
        cdef uint8_t* seen = self._seen.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef int64_t const_val

        if vec._has_const:
            if not vec._const_is_null:
                const_val = vec._const_value
                for i in range(n_rows):
                    si = state_indices[i]
                    if not seen[si]:
                        values[si] = const_val
                        seen[si] = 1
            return

        data = <int64_t*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                if not seen[si]:
                    values[si] = data[i]
                    seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._values[i] if self._seen[i] else None)
        self._time_finalize_ns += _now_ns() - start_ns
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# ANY_VALUE(float64)
# ---------------------------------------------------------------------------

cdef class AnyValueFloat64Collector(BaseCollector):
    cdef vector[double] _values
    cdef vector[uint8_t] _seen
    cdef long long _time_finalize_ns

    cdef void grow(self, int64_t new_count):
        while self._values.size() < <size_t>new_count:
            self._values.push_back(0.0)
            self._seen.push_back(0)

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Float64Vector vec = <Float64Vector>morsel.column(self.column_name)
        cdef double* data
        cdef uint8_t* nulls
        cdef double* values = self._values.data()
        cdef uint8_t* seen = self._seen.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef double const_val

        if vec._has_const:
            if not vec._const_is_null:
                const_val = vec._const_value
                for i in range(n_rows):
                    si = state_indices[i]
                    if not seen[si]:
                        values[si] = const_val
                        seen[si] = 1
            return

        data = <double*>vec.dense_ptr()
        nulls = vec.null_bitmap_ptr()
        for i in range(n_rows):
            if _num_bitmap_valid(nulls, i):
                si = state_indices[i]
                if not seen[si]:
                    values[si] = data[i]
                    seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(self._values[i] if self._seen[i] else None)
        self._time_finalize_ns += _now_ns() - start_ns
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# ANY_VALUE(object) — string, date, time, etc.
# Uses a Python list for per-group storage — the column type requires it.
# grow()/accumulate() are Cython loops. No Python inside the inner loop
# beyond the list index assignment.
# ---------------------------------------------------------------------------

cdef class AnyValueObjectCollector(BaseCollector):
    cdef list _values     # one Python object per group (None until first non-NULL)
    cdef vector[uint8_t] _seen
    cdef long long _time_finalize_ns

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
        cdef object v
        for i in range(n_rows):
            v = col[i]
            if v is not None:
                si = state_indices[i]
                if not seen[si]:
                    self._values[si] = v
                    seen[si] = 1

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        result = vector_from_sequence(self._values[:num_groups])
        self._time_finalize_ns += _now_ns() - start_ns
        return result
