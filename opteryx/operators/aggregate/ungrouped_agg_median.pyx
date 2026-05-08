# included by ungrouped_agg.pyx — do not compile standalone
#
# MedianFloat64Aggregate
#
# Buffers all non-null values into a typed double[] and runs std::nth_element
# at finalize.  Result type is always Float64 — even-count inputs interpolate,
# odd-count return the middle value cast to double.
#
# Input vector types accepted: integer (any width), float64. Decimal is
# rejected at the dispatch site (aggregate_node.pyx) with a CAST suggestion.
#
# Memory: hard-capped at MEDIAN_MAX_VALUES_PER_GROUP (default 1000); the
# state's overflowed() flag is checked on apply() and raises immediately.


cdef extern from "_agg_kernels.hpp" namespace "opteryx::ungrouped":
    cdef cppclass MedianState:
        double* buf
        size_t  size
        size_t  cap
        size_t  max_size
        bint    overflowed
        MedianState() except +
        bint append(double v) noexcept
        double finalize_median() noexcept


cdef class MedianFloat64Aggregate(UngroupedAggregate):
    cdef MedianState* _state

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._state      = new MedianState()

    def __dealloc__(self):
        if self._state != NULL:
            del self._state
            self._state = NULL

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed     = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if nrows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]
        if raw is None:
            return

        cdef double v
        for val_py in raw.to_pylist():
            if val_py is None:
                continue
            v = <double>val_py
            if not self._state.append(v):
                if self._state.overflowed:
                    raise ValueError(
                        f"MEDIAN exceeded the per-group cap of "
                        f"{self._state.max_size} non-null values. "
                        "Use APPROX_PERCENTILE for larger inputs."
                    )
                raise MemoryError("MEDIAN buffer allocation failed")

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._state.finalize_median()

    cdef double get_result_f64(self) noexcept:
        return self._state.finalize_median()

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return self._state.size == 0

    cpdef object get_result(self):
        if self._state.size == 0:
            return None
        return self._state.finalize_median()
