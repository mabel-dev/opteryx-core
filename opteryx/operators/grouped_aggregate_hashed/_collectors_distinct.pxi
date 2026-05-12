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

from draken.vectors.vector cimport Vector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.string_vector cimport StringVector
from draken.core.buffers cimport DrakenVarBuffer, DRAKEN_ENCODING_DICTIONARY


cdef extern from "carchar_set.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharSet:
        CarcharSet() except +
        size_t size() const
        bint insert_or_ignore(uint64_t key) except +
        size_t insert_many(const uint64_t* keys, size_t length) except +
        void tighten() noexcept


# ---------------------------------------------------------------------------
# COUNT(DISTINCT col) — one CarcharSet* per group
#
# Hot-path summary (accumulate):
#   Dict-encoded StringVector: precompute dict-entry hashes once per morsel,
#   then scatter via dict-code lookup — no intermediate _scratch_buf write.
#   All other vectors: zero _scratch_buf, call c_hash_into() nogil, scatter
#   non-null hashes to per-group scratch vectors, bulk-insert into CarcharSets.
#   NULL values (SQL92: not counted) are filtered by null_marker comparison.
# ---------------------------------------------------------------------------

cdef class CountDistinctCollector(BaseCollector):
    cdef vector[CarcharSet*] _sets
    cdef vector[vector[uint64_t]] _scratch_per_group
    cdef Py_ssize_t _col_idx          # cached column index (-1 = unresolved)
    cdef uint64_t* _scratch_buf       # reusable per-row hash buffer
    cdef Py_ssize_t _scratch_capacity

    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._scratch_per_group = []
        self._col_idx = -1
        self._scratch_buf = NULL
        self._scratch_capacity = 0

    def __dealloc__(self):
        cdef Py_ssize_t i
        for i in range(self._sets.size()):
            if self._sets[i] != NULL:
                del self._sets[i]
        self._sets.clear()
        self._scratch_per_group.clear()
        if self._scratch_buf != NULL:
            free(self._scratch_buf)
        self._scratch_buf = NULL

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
        cdef long long start_ns = _now_ns()
        cdef Morsel m = <Morsel>morsel
        cdef StringVector svec
        cdef Vector raw
        cdef CarcharSet** sets = self._sets.data()
        cdef vector[uint64_t]* scratch
        cdef Py_ssize_t i, g, dict_size, di
        cdef vector[uint64_t]* per_group
        cdef uint64_t null_marker = mix_hash(0, NULL_HASH)
        cdef uint64_t h
        cdef uint64_t* dict_hashes
        cdef const uint8_t* dict_codes
        cdef uint8_t code_width
        cdef const uint8_t* row_nulls
        cdef uint32_t code

        # Resolve column index on first call
        if self._col_idx < 0:
            self._col_idx = m._column_index_from_name(self.column_name)
        raw = <Vector>m._columns[self._col_idx]

        # Dict-encoded StringVector fast path: precompute one hash per dict entry,
        # then scatter via dict-code table lookup — no _scratch_buf write.
        if isinstance(raw, StringVector):
            svec = <StringVector>raw
            if (
                svec._encoding == DRAKEN_ENCODING_DICTIONARY
                and svec._dict_codes != NULL
                and svec._dict_values != NULL
            ):
                dict_size = svec.c_dict_size()
                dict_hashes = <uint64_t*>malloc(<size_t>dict_size * sizeof(uint64_t))
                if dict_hashes == NULL:
                    raise MemoryError()
                with nogil:
                    for di in range(dict_size):
                        if svec.c_dict_value_is_null(di):
                            dict_hashes[di] = null_marker
                        else:
                            dict_hashes[di] = svec.c_dict_value_hash(di)
                dict_codes = svec.c_dict_codes_ptr()
                code_width = svec.c_dict_code_width()
                row_nulls = svec.c_row_null_bitmap()
                with nogil:
                    for i in range(n_rows):
                        if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
                            continue
                        if code_width == 1:
                            code = (<const uint8_t*>dict_codes)[i]
                        elif code_width == 2:
                            code = (<const uint16_t*>dict_codes)[i]
                        else:
                            code = (<const uint32_t*>dict_codes)[i]
                        h = dict_hashes[code]
                        if h == null_marker:
                            continue
                        scratch = &self._scratch_per_group[state_indices[i]]
                        scratch.push_back(h)
                free(dict_hashes)
                # Bulk-insert per group
                for g in range(self._sets.size()):
                    per_group = &self._scratch_per_group[g]
                    if per_group.size() > 0:
                        sets[g].insert_many(per_group.data(), per_group.size())
                        per_group.clear()
                self._time_finalize_ns += _now_ns() - start_ns
                return

        # Generic path: hash into scratch buffer, scatter non-null hashes.
        if n_rows > self._scratch_capacity:
            if self._scratch_buf != NULL:
                free(self._scratch_buf)
            self._scratch_buf = <uint64_t*>malloc(<size_t>n_rows * sizeof(uint64_t))
            if self._scratch_buf == NULL:
                raise MemoryError()
            self._scratch_capacity = n_rows

        memset(self._scratch_buf, 0, <size_t>n_rows * sizeof(uint64_t))
        raw.c_hash_into(self._scratch_buf, n_rows)

        with nogil:
            for i in range(n_rows):
                if self._scratch_buf[i] == null_marker:
                    continue
                scratch = &self._scratch_per_group[state_indices[i]]
                scratch.push_back(self._scratch_buf[i])

        for g in range(self._sets.size()):
            per_group = &self._scratch_per_group[g]
            if per_group.size() > 0:
                sets[g].insert_many(per_group.data(), per_group.size())
                per_group.clear()

        self._time_finalize_ns += _now_ns() - start_ns

    cpdef Vector finalize(self, int64_t num_groups):
        from draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i

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
        from draken.interop.arrow import vector_from_sequence
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
        from draken.interop.arrow import vector_from_sequence
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
        from draken.interop.arrow import vector_from_sequence
        cdef long long start_ns = _now_ns()
        result = vector_from_sequence(self._values[:num_groups])
        self._time_finalize_ns += _now_ns() - start_ns
        return result
