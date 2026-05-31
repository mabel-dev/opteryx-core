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


cdef class CountStarAggregate(UngroupedAggregate):
    """COUNT(*) — counts every row unconditionally."""
    cdef int64_t _count

    def __cinit__(self, bytes alias):
        self.column_name = b""
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._count      = 0

    cdef void apply(self, Morsel morsel) except *:
        self._count += <int64_t>(<Morsel>morsel).ptr.num_rows

    cdef int64_t get_result_i64(self) noexcept:
        return self._count

    cdef double get_result_f64(self) noexcept:
        return <double>self._count

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return False

    cpdef object get_result(self):
        return self._count


cdef class CountAggregate(UngroupedAggregate):
    """COUNT(col) — counts non-null values in the named column."""
    cdef int64_t _count

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._count      = 0

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        # Cached column index — list[i] on subsequent morsels, no dict hash
        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = typed._get_column(self._col_idx)

        # Classify once; integer compare on all subsequent morsels
        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const uint8_t* nulls
        cdef Py_ssize_t i
        cdef DrakenVector* uv

        if self._col_type == _VTYPE_INT64:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:  # not null constant
                    self._count += nrows
                return
            if uv.validity == NULL:
                self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        if self._col_type == _VTYPE_STRING:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:  # not null constant
                    self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        if self._col_type == _VTYPE_FLOAT64:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:
                    self._count += nrows
                return
            if uv.validity == NULL:
                self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        if self._col_type == _VTYPE_INT8:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:
                    self._count += nrows
                return
            if uv.validity == NULL:
                self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        if self._col_type == _VTYPE_INT16:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:
                    self._count += nrows
                return
            if uv.validity == NULL:
                self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        if self._col_type == _VTYPE_INT32:
            uv = (<Vector>raw).unified()
            if uv.data_length == 1 and uv.length > 1:  # constant
                if uv.validity == NULL:
                    self._count += nrows
                return
            if uv.validity == NULL:
                self._count += nrows
                return
            self._count += <int64_t>nrows - _count_nulls(uv.validity, nrows)
            return

        # Generic fallback — pay per-row only for unknown vector types
        nulls = raw.null_bitmap_ptr()
        if nulls == NULL:
            self._count += nrows
            return
        self._count += <int64_t>nrows - _count_nulls(nulls, nrows)

    cdef int64_t get_result_i64(self) noexcept:
        return self._count

    cdef double get_result_f64(self) noexcept:
        return <double>self._count

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return False

    cpdef object get_result(self):
        return self._count
