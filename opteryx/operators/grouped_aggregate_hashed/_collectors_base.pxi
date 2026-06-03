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
    cdef Py_ssize_t _col_idx         # cached column index; -1 = unresolved

    def __cinit__(self, *args, **kwargs):
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
        Morsel morsel,
        const uint32_t* state_indices,
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

    cpdef BaseCollector _clone_empty(self):
        """Return a fresh zero-state instance of this collector with the same config.
        Returns None for collectors that cannot participate in partial aggregation."""
        return None

    cpdef BaseCollector _clone_as_merge(self):
        """Return a collector that accumulates partial aggregates from a finalised morsel.
        Reads from result_name instead of column_name.  Returns None if non-mergeable."""
        return None
