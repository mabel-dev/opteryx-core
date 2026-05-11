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

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const double*      fdata
        cdef const int64_t*     idata
        cdef const uint8_t*     nulls
        cdef DrakenFixedBuffer* ibuf
        cdef Py_ssize_t i
        cdef double v

        if self._col_type == _VTYPE_FLOAT64:
            vec_f = <Float64Vector>raw
            if vec_f._has_const:
                if not vec_f._const_is_null:
                    for i in range(nrows):
                        if not self._state.append(vec_f._const_value):
                            self._raise_append_failure()
                return
            fdata = <const double*>vec_f.dense_ptr()
            nulls = vec_f.null_bitmap_ptr()
            if nulls == NULL:
                for i in range(nrows):
                    if not self._state.append(fdata[i]):
                        self._raise_append_failure()
                return
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    if not self._state.append(fdata[i]):
                        self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INT64:
            vec_i = <Int64Vector>raw
            if vec_i._has_const:
                if not vec_i._const_is_null:
                    v = <double>vec_i._const_value
                    for i in range(nrows):
                        if not self._state.append(v):
                            self._raise_append_failure()
                return
            idata = <const int64_t*>vec_i.dense_ptr()
            nulls = vec_i.null_bitmap_ptr()
            if nulls == NULL:
                for i in range(nrows):
                    if not self._state.append(<double>idata[i]):
                        self._raise_append_failure()
                return
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    if not self._state.append(<double>idata[i]):
                        self._raise_append_failure()
            return

        if self._col_type == _VTYPE_INTEGER:
            vec_n = <IntegerVector>raw
            if vec_n._has_const:
                if not vec_n._const_is_null:
                    v = <double>vec_n._const_value
                    for i in range(nrows):
                        if not self._state.append(v):
                            self._raise_append_failure()
                return
            ibuf  = vec_n.ptr
            nulls = <const uint8_t*>ibuf.null_bitmap
            if nulls == NULL:
                for i in range(nrows):
                    if not self._state.append(<double>_read_integer_value(ibuf, i)):
                        self._raise_append_failure()
                return
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    if not self._state.append(<double>_read_integer_value(ibuf, i)):
                        self._raise_append_failure()
            return

        raise TypeError(
            f"MedianFloat64Aggregate cannot scan column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef void _raise_append_failure(self) except *:
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
