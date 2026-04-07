# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# APPROX_COUNT_DISTINCT (HyperLogLog), APPROX_PERCENTILE (t-digest), ARRAY_AGG.
# HllppSketch* and td_histogram_t* declared directly here.
# The Python wrapper classes (ApproximateCountState etc.) are NOT used.
# ARRAY_AGG is the one principled Python exception — output is a list of SQL values
# whose type is unknown at compile time.

from libc.stddef cimport size_t
from libc.stdint cimport int64_t, uint64_t, uint8_t
from libcpp.vector cimport vector


# ---------------------------------------------------------------------------
# HyperLogLog declarations (from src/cpp/hllpp.h)
# ---------------------------------------------------------------------------

cdef extern from "hllpp.h":
    cdef cppclass HllppSketch:
        HllppSketch(int precision, size_t explicit_threshold, size_t sparse_threshold) except +
        void add_hash(uint64_t hash) nogil
        void add_hashes(const uint64_t* hashes, size_t count) nogil
        uint64_t estimate() const


# ---------------------------------------------------------------------------
# t-digest declarations (from third_party/tdigest-c/src/tdigest.h)
# ---------------------------------------------------------------------------

cdef extern from "tdigest.h":
    ctypedef struct td_histogram_t:
        pass
    td_histogram_t* td_new(double compression)
    void td_free(td_histogram_t* h)
    int td_add(td_histogram_t* h, double val, long long weight) nogil
    double td_quantile(td_histogram_t* h, double q)
    long long td_size(td_histogram_t* h)


# ---------------------------------------------------------------------------
# APPROX_COUNT_DISTINCT — one HllppSketch* per group, no Python
# ---------------------------------------------------------------------------

cdef class ApproxCountDistinctCollector(BaseCollector):
    cdef vector[HllppSketch*] _sketches

    def __dealloc__(self):
        cdef Py_ssize_t i
        for i in range(self._sketches.size()):
            if self._sketches[i] != NULL:
                del self._sketches[i]
        self._sketches.clear()

    cdef void grow(self, int64_t new_count):
        while self._sketches.size() < <size_t>new_count:
            self._sketches.push_back(new HllppSketch(14, 0, 0))

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        # Hash the column in one shot, then dispatch per row
        cdef uint64_t[::1] hashes = morsel.hash([self.column_name])
        cdef HllppSketch** sketches = self._sketches.data()
        cdef Py_ssize_t i
        for i in range(n_rows):
            sketches[state_indices[i]].add_hash(hashes[i])

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        for i in range(num_groups):
            vals.append(<int64_t>self._sketches[i].estimate())
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# APPROX_PERCENTILE — one td_histogram_t* per group, no Python
# ---------------------------------------------------------------------------

cdef class ApproxPercentileCollector(BaseCollector):
    cdef vector[td_histogram_t*] _hists
    cdef double _percentile

    def __cinit__(self, double percentile=0.5):
        self._percentile = percentile

    def __dealloc__(self):
        cdef Py_ssize_t i
        for i in range(self._hists.size()):
            if self._hists[i] != NULL:
                td_free(self._hists[i])
        self._hists.clear()

    cdef void grow(self, int64_t new_count):
        while self._hists.size() < <size_t>new_count:
            self._hists.push_back(td_new(100.0))

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector raw = morsel.column(self.column_name)
        cdef uint8_t* nulls = raw.null_bitmap_ptr()
        cdef td_histogram_t** hists = self._hists.data()
        cdef Py_ssize_t i
        cdef int64_t si
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef int64_t* i64
        cdef double* f64

        if isinstance(raw, Int64Vector):
            iv = <Int64Vector>raw
            i64 = <int64_t*>iv.dense_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    td_add(hists[si], <double>i64[i], 1)
        else:
            fv = <Float64Vector>raw
            f64 = <double*>fv.dense_ptr()
            for i in range(n_rows):
                if _num_bitmap_valid(nulls, i):
                    si = state_indices[i]
                    td_add(hists[si], f64[i], 1)

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list vals = []
        cdef Py_ssize_t i
        cdef td_histogram_t* h
        for i in range(num_groups):
            h = self._hists[i]
            if td_size(h) == 0:
                vals.append(None)
            else:
                vals.append(td_quantile(h, self._percentile))
        return vector_from_sequence(vals)


# ---------------------------------------------------------------------------
# ARRAY_AGG — Python-backed, one list per group.
# The principled Python exception: output is a list of arbitrary SQL values.
# grow() is called once per morsel (not per row).
# accumulate() is a Cython loop with Python list appends.
# ---------------------------------------------------------------------------

cdef class ArrayAggCollector(BaseCollector):
    cdef list _per_group    # list of lists
    cdef object _options    # dict or None

    def __cinit__(self, object options=None):
        self._per_group = []
        self._options = options or {}

    cdef void grow(self, int64_t new_count):
        while len(self._per_group) < new_count:
            self._per_group.append([])

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef list col = morsel.column(self.column_name).to_pylist()
        cdef list groups = self._per_group
        cdef Py_ssize_t i
        for i in range(n_rows):
            groups[state_indices[i]].append(col[i])

    cpdef Vector finalize(self, int64_t num_groups):
        from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        cdef list result = []
        cdef list vals
        cdef Py_ssize_t i
        cdef object limit = self._options.get("limit")
        cdef bint ordered = bool(self._options.get("ordered", False))
        cdef bint descending = bool(self._options.get("descending", False))
        cdef bint distinct = bool(self._options.get("distinct", False))

        for i in range(num_groups):
            vals = self._per_group[i]
            if distinct:
                seen_set = set()
                deduped = []
                for v in vals:
                    k = repr(v) if not isinstance(v, (int, float, str, bytes, bool, type(None))) else v
                    if k not in seen_set:
                        seen_set.add(k)
                        deduped.append(v)
                vals = deduped
            if ordered:
                non_nulls = [v for v in vals if v is not None]
                nulls_count = len(vals) - len(non_nulls)
                non_nulls.sort(reverse=descending)
                vals = non_nulls
                if nulls_count:
                    vals.extend([None] * nulls_count)
            if limit is not None:
                vals = vals[:limit]
            result.append(vals)
        return vector_from_sequence(result)
