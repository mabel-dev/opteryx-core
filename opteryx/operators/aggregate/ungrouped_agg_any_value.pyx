# included by ungrouped_agg.pyx — do not compile standalone


cdef class AnyValueAggregate(UngroupedAggregate):
    """
    ANY_VALUE(col) — returns the first non-null value seen.

    Once a value is found, all subsequent morsels are skipped entirely
    without even touching the column.
    """

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_OBJECT
        self._value      = None
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        if self._seen:
            return  # already have a value — skip the whole morsel

        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const int64_t*   idata
        cdef const double*    fdata
        cdef const uint8_t*   nulls
        cdef DrakenVarBuffer* buf
        cdef Py_ssize_t i
        cdef const char* ptr_c
        cdef Py_ssize_t  length_c

        if self._col_type == _VTYPE_INT64:
            vec_i = <Int64Vector>raw
            if vec_i._has_const:
                if not vec_i._const_is_null:
                    self._value = vec_i._const_value; self._seen = True
                return
            idata = <const int64_t*>vec_i.dense_ptr()
            nulls = vec_i.null_bitmap_ptr()
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    self._value = idata[i]; self._seen = True; return
            return

        if self._col_type == _VTYPE_FLOAT64:
            vec_f = <Float64Vector>raw
            if vec_f._has_const:
                if not vec_f._const_is_null:
                    self._value = vec_f._const_value; self._seen = True
                return
            fdata = <const double*>vec_f.dense_ptr()
            nulls = vec_f.null_bitmap_ptr()
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    self._value = fdata[i]; self._seen = True; return
            return

        if self._col_type == _VTYPE_STRING:
            svec = <StringVector>raw
            if svec._has_const:
                if not svec._const_is_null:
                    ptr_c    = <const char*>svec._const_value.data
                    length_c = <Py_ssize_t>svec._const_value.length
                    self._value = ptr_c[:length_c]; self._seen = True
                return
            buf = svec.ptr
            nulls = buf.null_bitmap
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    ptr_c    = <const char*>(buf.data + buf.offsets[i])
                    length_c = <Py_ssize_t>(buf.offsets[i + 1] - buf.offsets[i])
                    self._value = ptr_c[:length_c]; self._seen = True; return
            return

        if self._col_type == _VTYPE_INTEGER:
            vec_n = <IntegerVector>raw
            if vec_n._has_const:
                if not vec_n._const_is_null:
                    self._value = vec_n._const_value; self._seen = True
                return
            nulls = vec_n.null_bitmap_ptr()
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    self._value = vec_n.to_pylist()[i]; self._seen = True; return
            return

        # Generic fallback
        for val_py in raw.to_pylist():
            if val_py is not None:
                self._value = val_py; self._seen = True; return

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._value
