# included by ungrouped_agg.pyx — do not compile standalone


cdef class SumInt64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._total      = 0
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        if not isinstance(morsel, Morsel):
            return

        cdef Morsel typed    = <Morsel>morsel

        if typed.ptr is NULL or typed.ptr.num_rows == 0:
            return

        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        if hasattr(raw, 'sum'):
            try:
                val = raw.sum()
                if val is not None:
                    self._total += <int64_t>val
                    self._seen   = True
                return
            except (ValueError, NotImplementedError):
                pass

        for val_py in raw.to_pylist():
            if val_py is not None:
                self._total += <int64_t>val_py
                self._seen   = True

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

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._total      = 0.0
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        if not isinstance(morsel, Morsel):
            return

        cdef Morsel typed    = <Morsel>morsel

        if typed.ptr is NULL or typed.ptr.num_rows == 0:
            return

        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        if hasattr(raw, 'sum'):
            try:
                val = raw.sum()
                if val is not None:
                    self._total += <double>val
                    self._seen   = True
                return
            except (ValueError, NotImplementedError):
                pass

        for val_py in raw.to_pylist():
            if val_py is not None:
                self._total += <double>val_py
                self._seen   = True

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
