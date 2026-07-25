# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

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

        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const double*      fdata
        cdef const int64_t*     idata64
        cdef const int32_t*     idata32
        cdef const int16_t*     idata16
        cdef const int8_t*      idata8
        cdef const uint8_t*     nulls
        cdef const uint32_t*    sel
        cdef DrakenVector*      uv
        cdef Py_ssize_t i

        if self._col_type == _VTYPE_FLOAT64:
            uv = (<Vector>raw).unified()
            fdata = <const double*>uv.data
            sel   = uv.selection
            nulls = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                if not self._state.append(fdata[sel[i]]):
                    self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INT64:
            uv = (<Vector>raw).unified()
            idata64 = <const int64_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                if not self._state.append(<double>idata64[sel[i]]):
                    self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INT8:
            uv = (<Vector>raw).unified()
            idata8 = <const int8_t*>uv.data
            sel    = uv.selection
            nulls  = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                if not self._state.append(<double>idata8[sel[i]]):
                    self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INT16:
            uv = (<Vector>raw).unified()
            idata16 = <const int16_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                if not self._state.append(<double>idata16[sel[i]]):
                    self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INT32:
            uv = (<Vector>raw).unified()
            idata32 = <const int32_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                if not self._state.append(<double>idata32[sel[i]]):
                    self._raise_append_failure()
            return

        raise TypeError(
            f"MedianFloat64Aggregate cannot scan column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef void _raise_append_failure(self) except *:
        if self._state.overflowed:
            raise ValueError(
                f"MEDIAN — too many values in one group (cap: "
                f"{self._state.max_size}). Use APPROX_PERCENTILE(x, 0.5) for "
                "approximate median over large sets of values."
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
