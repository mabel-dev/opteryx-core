# included by ungrouped_agg.pyx — do not compile standalone


cdef class SumInt64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._total      = 0
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const int64_t*   data
        cdef const uint8_t*   nulls
        cdef DictAccessor*    dacc
        cdef DrakenFixedBuffer* buf
        cdef Py_ssize_t i
        cdef int64_t val

        if self._col_type == _VTYPE_INT64:
            vec_i = <Int64Vector>raw
            if vec_i._has_const:
                if not vec_i._const_is_null:
                    self._total += vec_i._const_value * nrows
                    self._seen   = True
                return
            if vec_i._dict_codes != NULL:
                dacc  = vec_i.dict_accessor()
                nulls = dacc.row_nulls
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        val = _dict_accessor_read_int_value(dacc, i)
                        self._total += val
                        self._seen   = True
                return
            data  = <const int64_t*>vec_i.dense_ptr()
            nulls = vec_i.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        self._total += data[i]
                self._seen = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        self._total += data[i]
                        self._seen   = True
            return

        if self._col_type == _VTYPE_INTEGER:
            vec_n = <IntegerVector>raw
            if vec_n._has_const:
                if not vec_n._const_is_null:
                    self._total += vec_n._const_value * nrows
                    self._seen   = True
                return
            buf   = vec_n.ptr
            nulls = <const uint8_t*>buf.null_bitmap
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        self._total += _read_integer_value(buf, i)
                self._seen = True
                return
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    self._total += _read_integer_value(buf, i)
                    self._seen   = True
            return

        # Generic fallback (handles floats masquerading as ints, etc.)
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
        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const double*  data
        cdef const uint8_t* nulls
        cdef Py_ssize_t i

        if self._col_type == _VTYPE_FLOAT64:
            vec_f = <Float64Vector>raw
            if vec_f._has_const:
                if not vec_f._const_is_null:
                    self._total += vec_f._const_value * nrows
                    self._seen   = True
                return
            data  = <const double*>vec_f.dense_ptr()
            nulls = vec_f.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        self._total += data[i]
                self._seen = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        self._total += data[i]
                        self._seen   = True
            return

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
