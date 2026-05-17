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


cdef class AnyValueAggregate(UngroupedAggregate):
    """
    ANY_VALUE(col) — returns the first non-null value seen.

    Once a value is found, all subsequent morsels are skipped entirely
    without even touching the column.
    """
    cdef object _value
    cdef bint   _seen

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

        cdef const int64_t*     idata
        cdef const double*      fdata
        cdef const uint8_t*     nulls
        cdef DrakenVarBuffer*   buf
        cdef DrakenFixedBuffer* ibuf
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
                if nulls == NULL or _bitmap_is_valid(nulls, i):
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
                if nulls == NULL or _bitmap_is_valid(nulls, i):
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
            # Dict-encoded fast path: find the first dict entry that has a
            # referenced, non-null row, and return its value.
            if (
                svec._encoding == DRAKEN_ENCODING_DICTIONARY
                and svec._dict_codes != NULL
                and svec._dict_values != NULL
            ):
                if self._take_first_dict(svec):
                    self._seen = True
                return
            buf = svec.ptr
            nulls = buf.null_bitmap
            for i in range(nrows):
                if nulls == NULL or _bitmap_is_valid(nulls, i):
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
            ibuf  = vec_n.ptr
            nulls = <const uint8_t*>ibuf.null_bitmap
            if nulls == NULL:
                if nrows > 0:
                    self._value = _read_integer_value(ibuf, 0); self._seen = True
                return
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    self._value = _read_integer_value(ibuf, i); self._seen = True; return
            return

        raise TypeError(
            f"AnyValueAggregate cannot scan column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef bint _take_first_dict(self, StringVector svec) except *:
        """Walk the dict codes once; on the first valid (non-null) row whose
        dict entry is itself not null, capture its value and return True."""
        cdef Py_ssize_t n = svec.c_length()
        cdef const uint8_t* codes = svec.c_dict_codes_ptr()
        cdef uint8_t code_width = svec.c_dict_code_width()
        cdef const uint8_t* row_nulls = svec.c_row_null_bitmap()
        cdef Py_ssize_t dict_size = svec.c_dict_size()
        cdef Py_ssize_t i, vlen
        cdef uint32_t code
        cdef const uint8_t* vptr
        for i in range(n):
            if row_nulls != NULL and not _bitmap_is_valid(row_nulls, i):
                continue
            if code_width == 1:
                code = (<const uint8_t*>codes)[i]
            elif code_width == 2:
                code = (<const uint16_t*>codes)[i]
            else:
                code = (<const uint32_t*>codes)[i]
            if <Py_ssize_t>code >= dict_size:
                raise ValueError(f"dictionary index out of bounds at row {i}: {code}")
            if svec.c_dict_value_is_null(<Py_ssize_t>code):
                continue
            vptr = svec.c_dict_value_ptr(<Py_ssize_t>code, &vlen)
            if vptr == NULL:
                continue
            self._value = (<const char*>vptr)[:vlen]
            return True
        return False

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
