# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# APPROX_COUNT_DISTINCT (HyperLogLog), APPROX_PERCENTILE (t-digest), ARRAY_AGG.
# HllppSketch* and td_histogram_t* declared directly here.
# The Python wrapper classes (ApproximateCountState etc.) are NOT used.
# ARRAY_AGG is the one principled Python exception — output is a list of SQL values
# whose type is unknown at compile time.

from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint32_t, uint64_t, uint8_t

from libc.string cimport memset

from draken.vectors.vector cimport from_decoded as _vector_from_decoded
from draken.core.buffers cimport DRAKEN_INT64, DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_FLOAT32

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

# Hoist the shim Vector import to module level to avoid hot-path inline imports (E.30a)
from draken.vectors.vector import Vector as _V


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
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        # Hash the column in one shot, then dispatch per row
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef uint64_t[::1] hashes = morsel.hash([self.column_name])
        cdef HllppSketch** sketches = self._sketches.data()
        cdef const uint64_t* hp = &hashes[0]
        cdef Py_ssize_t i
        with nogil:
            for i in range(n_rows):
                sketches[state_indices[i]].add_hash(hp[i])

    cpdef Vector finalize(self, int64_t num_groups):
        # Estimates are counts ≥ 0 (never null) → write straight into an INT64
        # buffer and hand it to from_decoded. No Python list, no boxing.
        cdef int64_t* out = <int64_t*>draken_malloc(
            <size_t>(num_groups if num_groups > 0 else 1) * sizeof(int64_t))
        if out == NULL:
            raise MemoryError()
        cdef Py_ssize_t i
        for i in range(num_groups):
            out[i] = <int64_t>self._sketches[i].estimate()
        return _vector_from_decoded(<void*>out, NULL, <uint32_t>num_groups, DRAKEN_INT64)


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
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        # Per-row template: type-dispatch once per morsel via the unified
        # DrakenVector, typed pointers cached for the inner loop.
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector raw = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = raw.unified()
        cdef DrakenType t = uv.type
        cdef uint8_t* nulls = uv.validity
        cdef const uint32_t* sel = uv.selection
        cdef td_histogram_t** hists = self._hists.data()
        cdef Py_ssize_t i
        # Width-aware read: feed the raw value into the sketch off-GIL. (The old
        # code read every non-INT64 type at double stride — wrong for narrow ints
        # and FLOAT32.) Numeric only; non-numeric fails loud before the nogil loop.
        if t != DRAKEN_INT64 and t != DRAKEN_INT32 and t != DRAKEN_INT16 and \
           t != DRAKEN_INT8 and t != DRAKEN_FLOAT64 and t != DRAKEN_FLOAT32:
            raise NotImplementedError(
                f"APPROX_PERCENTILE over column type {t} is not supported (numeric only)")
        with nogil:
            if t == DRAKEN_INT64:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], <double>(<int64_t*>uv.data)[sel[i]], 1)
            elif t == DRAKEN_INT32:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], <double>(<int32_t*>uv.data)[sel[i]], 1)
            elif t == DRAKEN_INT16:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], <double>(<int16_t*>uv.data)[sel[i]], 1)
            elif t == DRAKEN_INT8:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], <double>(<int8_t*>uv.data)[sel[i]], 1)
            elif t == DRAKEN_FLOAT64:
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], (<double*>uv.data)[sel[i]], 1)
            else:  # DRAKEN_FLOAT32
                for i in range(n_rows):
                    if _num_bitmap_valid(nulls, i):
                        td_add(hists[state_indices[i]], <double>(<float*>uv.data)[sel[i]], 1)

    cpdef Vector finalize(self, int64_t num_groups):
        # One quantile per group straight into a FLOAT64 buffer (+ validity for
        # empty groups). No Python list. td_quantile/td_size aren't nogil, so the
        # loop runs GIL-held — but touches no Python objects.
        cdef double* out = <double*>draken_malloc(
            <size_t>(num_groups if num_groups > 0 else 1) * sizeof(double))
        if out == NULL:
            raise MemoryError()
        cdef Py_ssize_t bitmap_bytes = (num_groups + 7) >> 3
        cdef uint8_t* validity = <uint8_t*>draken_malloc(<size_t>(bitmap_bytes if bitmap_bytes > 0 else 1))
        if validity == NULL:
            draken_free(out)
            raise MemoryError()
        memset(validity, 0xFF, bitmap_bytes)
        cdef Py_ssize_t i
        cdef td_histogram_t* h
        cdef bint any_null = False
        for i in range(num_groups):
            h = self._hists[i]
            if td_size(h) == 0:
                out[i] = 0.0
                validity[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
                any_null = True
            else:
                out[i] = td_quantile(h, self._percentile)
        if not any_null:
            draken_free(validity)
            validity = NULL
        return _vector_from_decoded(<void*>out, validity, <uint32_t>num_groups, DRAKEN_FLOAT64)


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
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef list col = morsel._get_column(self._col_idx).to_pylist()
        cdef list groups = self._per_group
        cdef Py_ssize_t i
        for i in range(n_rows):
            groups[state_indices[i]].append(col[i])

    cpdef Vector finalize(self, int64_t num_groups):
        from draken.interop.vector_sequence import vector_from_sequence
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
        nb = vector_from_sequence(result)
        return _V(nb)
