# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# COUNT(DISTINCT col) collector.
# CountDistinctCollector: one CarcharSet* per group — pure C++.
# (ANY_VALUE now routes through the type-preserving MIN/MAX collectors in
#  _collectors_numeric.pxi with direction 0 = "keep first value per group".)

from libc.stdint cimport int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.vector cimport vector

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector


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
    cdef vector[uint32_t] _touched       # group ids touched this morsel (sparse path)
    cdef uint8_t* _in_touched            # dirty bitmap, one byte per group, always zeroed
    cdef size_t   _in_touched_cap        # allocated size of _in_touched
    cdef uint64_t* _scratch_buf          # reusable per-row hash buffer
    cdef Py_ssize_t _scratch_capacity

    cdef long long _time_finalize_ns

    def __cinit__(self):
        self._scratch_per_group = []
        self._col_idx = -1
        self._scratch_buf = NULL
        self._scratch_capacity = 0
        self._in_touched = NULL
        self._in_touched_cap = 0

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
        if self._in_touched != NULL:
            free(self._in_touched)
        self._in_touched = NULL

    cdef void grow(self, int64_t new_count):
        cdef size_t nc = <size_t>new_count
        cdef void* p
        while self._sets.size() < nc:
            self._sets.push_back(new CarcharSet())
            self._scratch_per_group.push_back(vector[uint64_t]())
        # Grow dirty bitmap, zero-fill new entries.
        if nc > self._in_touched_cap:
            p = malloc(nc)
            if self._in_touched != NULL:
                memcpy(p, self._in_touched, self._in_touched_cap)
                free(self._in_touched)
            memset(<uint8_t*>p + self._in_touched_cap, 0, nc - self._in_touched_cap)
            self._in_touched = <uint8_t*>p
            self._in_touched_cap = nc

    cdef void accumulate(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef long long start_ns = _now_ns()
        cdef Vector raw
        cdef CarcharSet** sets = self._sets.data()
        cdef vector[uint64_t]* scratch
        cdef Py_ssize_t i, g
        cdef vector[uint64_t]* per_group
        cdef uint64_t null_marker = mix_hash(0, NULL_HASH)

        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        raw = morsel._get_column(self._col_idx)

        # Generic path: hash into scratch buffer, scatter non-null hashes.
        if n_rows > self._scratch_capacity:
            if self._scratch_buf != NULL:
                free(self._scratch_buf)
            self._scratch_buf = <uint64_t*>malloc(<size_t>n_rows * sizeof(uint64_t))
            if self._scratch_buf == NULL:
                raise MemoryError()
            self._scratch_capacity = n_rows

        raw.c_hash_single(self._scratch_buf, n_rows)

        cdef uint8_t* null_bitmap = raw.null_bitmap_ptr()
        cdef uint8_t* in_touched = self._in_touched
        cdef size_t gi
        cdef bint sparse = self._sets.size() > <size_t>n_rows

        with nogil:
            if null_bitmap == NULL:
                for i in range(n_rows):
                    scratch = &self._scratch_per_group[state_indices[i]]
                    scratch.push_back(self._scratch_buf[i])
            else:
                for i in range(n_rows):
                    if self._scratch_buf[i] == null_marker:
                        continue
                    scratch = &self._scratch_per_group[state_indices[i]]
                    scratch.push_back(self._scratch_buf[i])

            if sparse:
                # Collect unique touched groups via a sequential si_buf pass.
                # O(n_rows) sequential reads instead of O(num_groups) scan.
                self._touched.clear()
                for i in range(n_rows):
                    gi = <size_t>state_indices[i]
                    if not in_touched[gi]:
                        in_touched[gi] = 1
                        self._touched.push_back(<uint32_t>gi)

        if sparse:
            for i in range(<Py_ssize_t>self._touched.size()):
                gi = <size_t>self._touched[i]
                per_group = &self._scratch_per_group[gi]
                if per_group.size() > 0:
                    sets[gi].insert_many(per_group.data(), per_group.size())
                    per_group.clear()
                in_touched[gi] = 0   # reset as we go — no separate memset pass
        else:
            for g in range(<Py_ssize_t>self._sets.size()):
                per_group = &self._scratch_per_group[g]
                if per_group.size() > 0:
                    sets[g].insert_many(per_group.data(), per_group.size())
                    per_group.clear()

        self._time_finalize_ns += _now_ns() - start_ns

    cpdef Vector finalize(self, int64_t num_groups):
        cdef DrakenFixedBuffer* buf = alloc_fixed_buffer(DRAKEN_INT64, <size_t>num_groups, 8)
        cdef int64_t* out = <int64_t*>buf.data
        cdef Py_ssize_t i
        for i in range(num_groups):
            self._sets[i].tighten()
            out[i] = <int64_t>self._sets[i].size()
        buf.length = <size_t>num_groups
        return _consume_int64_buffer(buf)
