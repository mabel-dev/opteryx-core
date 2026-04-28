# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# BaseCollector — abstract base for all per-aggregate state collectors.
# All subclasses must hold ONLY C++ state (typed vectors, C pointers).
# grow() and accumulate() are cdef — called from Cython only.

from libc.stdint cimport int64_t

from draken.vectors.vector cimport Vector


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
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        """
        Update aggregate state.
        state_indices[i] is the group slot for row i.
        Called once per morsel — n_rows rows to process.
        """
        pass

    cpdef Vector finalize(self, int64_t num_groups):
        """Return an output Vector with one value per group."""
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        """Return an output Vector for groups in [start, stop)."""
        return None
