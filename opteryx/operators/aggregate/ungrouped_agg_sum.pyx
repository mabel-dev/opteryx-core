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


cdef class SumInt64Aggregate(UngroupedAggregate):
    cdef int64_t _total
    cdef bint    _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._total      = 0
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        # Per-morsel work: classify the vector once, then delegate to the
        # Vector's cpdef sum(), which routes through the C++ reduction
        # kernel for dense paths and encoding-specific helpers for
        # dict/RLE/const. No encoding handling duplicated here.
        cdef Morsel typed = <Morsel>morsel
        if typed.num_rows == 0:
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

        if self._col_type == _VTYPE_INT64:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return
        if self._col_type == _VTYPE_INT8:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return
        if self._col_type == _VTYPE_INT16:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return
        if self._col_type == _VTYPE_INT32:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return

        raise TypeError(
            f"SumInt64Aggregate cannot sum column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef int64_t get_result_i64(self) noexcept:
        return self._total

    cdef double get_result_f64(self) noexcept:
        return <double>self._total

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._total if self._seen else None


cdef class SumFloat64Aggregate(UngroupedAggregate):
    cdef double _total
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._total      = 0.0
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.num_rows == 0:
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

        if self._col_type == _VTYPE_FLOAT64:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return
        if self._col_type == _VTYPE_INT64:
            self._total += <double>((<Vector>raw).sum())
            self._seen = True
            return
        if self._col_type == _VTYPE_INT8:
            self._total += <double>((<Vector>raw).sum())
            self._seen = True
            return
        if self._col_type == _VTYPE_INT16:
            self._total += <double>((<Vector>raw).sum())
            self._seen = True
            return
        if self._col_type == _VTYPE_INT32:
            self._total += <double>((<Vector>raw).sum())
            self._seen = True
            return

        raise TypeError(
            f"SumFloat64Aggregate cannot sum column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._total

    cdef double get_result_f64(self) noexcept:
        return self._total

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._total if self._seen else None
