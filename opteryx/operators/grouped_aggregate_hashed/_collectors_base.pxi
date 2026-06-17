# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# BaseCollector — abstract base for all per-aggregate state collectors.
# All subclasses must hold ONLY C++ state (typed vectors, C pointers).
# grow() and accumulate() are cdef — called from Cython only.

from libc.stdint cimport int64_t, uint32_t

from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel


cdef class BaseCollector:
    """
    Abstract base for a grouped aggregate collector.

    Lifecycle:
      1. create_collectors() instantiates one per aggregation spec.
      2. engine.ingest() calls grow(new_num_groups) once per morsel
         after all new groups for that morsel are counted.
      3. engine.ingest() calls accumulate(morsel, state_indices, n_rows)
         once per morsel to update per-group state.
      4. engine.finalize_morsels() may call finalize_slice(start, stop)
         repeatedly to produce chunked output vectors.
      5. finalize(num_groups) remains as a compatibility wrapper for
         collectors that only implement whole-column finalization.
    """

    cdef public bytes column_name    # source column, or b"*" for COUNT(*)
    cdef public bytes result_name    # output column alias
    cdef public bint telemetry_enabled
    cdef public long long time_finalize_ns
    cdef Py_ssize_t _col_idx         # cached column index; -1 = unresolved (bound at resolve)
    # S-B.3: nogil-capable collectors read their value column from a pre-resolved
    # DrakenVector* in `accumulate` (noexcept nogil) — the engine's nogil ingest_cxx
    # path can drive them with the GIL released. Complex collectors that touch Python
    # (median/count-distinct/approx/array_agg) set this False and override
    # `accumulate_gil(Morsel, …)` instead; an engine containing any such collector
    # stays on the GIL ingest path.
    cdef bint _nogil_capable

    def __cinit__(self, *args, **kwargs):
        self._nogil_capable = True
        # -1 = unresolved. Cython zero-initialises Py_ssize_t to 0, which would
        # silently resolve every collector to column 0 (the first group key) and
        # skip the name lookup in accumulate (guarded by `if self._col_idx < 0`),
        # so SUM/AVG/MIN/MAX read the wrong column. Cython runs this base
        # __cinit__ for every subclass (with the constructor args, hence *args),
        # so all collectors start unresolved and bind their real source column on
        # first accumulate.
        self._col_idx = -1

    cdef void grow(self, int64_t new_count):
        """Resize internal state to hold new_count groups."""
        pass

    cdef inline void _telemetry_begin_finalize(self) noexcept:
        if self.telemetry_enabled:
            self.time_finalize_ns = 0

    cdef inline void _telemetry_end_finalize(self, long long elapsed_ns) noexcept:
        if self.telemetry_enabled:
            self.time_finalize_ns += elapsed_ns

    cdef void accumulate(
        self,
        const DrakenVector* value_view,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ) noexcept nogil:
        """Update aggregate state (nogil-capable collectors). `value_view` is the
        collector's pre-resolved source column (a DrakenVector* into the morsel's
        substrate; NULL for COUNT(*) which reads no column). The engine resolves it
        from the cached `_col_idx` — under the GIL on the fallback ingest path, from
        the CxxMorsel* on the nogil ingest_cxx path. state_indices[i] = group slot
        for row i; n_rows rows. Read value_view.data/validity/selection/type."""
        pass

    cdef void accumulate_gil(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        """GIL-only accumulate for collectors that touch Python (median /
        count-distinct / approx / array_agg). Overridden by those collectors;
        the engine calls this (under the GIL) when `_nogil_capable` is False."""
        pass

    cpdef Vector finalize(self, int64_t num_groups):
        """Return an output Vector with one value per group."""
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        """Return an output Vector for groups in [start, stop)."""
        return None

    # ---- WP-7 partition-parallel merge -----------------------------------
    # A parallel engine runs one GroupHashEngine per worker over a disjoint
    # partition, then combines per-group collector state with merge_group_state
    # before a single finalize. A collector is mergeable only if combining two
    # partial per-group accumulators is exact (COUNT→add, SUM→add, MIN/MAX→
    # seen-aware, AVG→add sums+counts). Collectors whose partials cannot be
    # combined without the original rows (MEDIAN) or via an API not yet exposed
    # (COUNT DISTINCT), and the collectors not yet wired for merge (decimal /
    # string / bool / interval MIN/MAX, decimal AVG/SUM), report is_mergeable()
    # ==False so the engine keeps the whole aggregation serial rather than
    # producing a wrong answer.
    cdef bint is_mergeable(self) noexcept:
        return False

    cdef void merge_group_state(self, BaseCollector other, int64_t other_idx, int64_t self_idx) except *:
        """Combine other's group `other_idx` accumulator into self's group
        `self_idx`. The caller (GroupHashEngine.merge) guarantees
        type(self) is type(other) BEFORE calling, because the <Type>other cast in
        the overrides is UNCHECKED — a type mismatch would read the wrong struct
        layout (memory corruption)."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support partition-parallel merge"
        )

