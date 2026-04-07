# included by ungrouped_agg.pyx — do not compile standalone


# ---------------------------------------------------------------------------
# MinInt64Aggregate
# ---------------------------------------------------------------------------

cdef class MinInt64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._result     = INT64_MAX
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
                    val = vec_i._const_value
                    if not self._seen or val < self._result:
                        self._result = val
                        self._seen   = True
                return
            if vec_i._dict_codes != NULL:
                dacc  = vec_i.dict_accessor()
                nulls = dacc.row_nulls
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        val = _dict_accessor_read_int_value(dacc, i)
                        if not self._seen or val < self._result:
                            self._result = val
                            self._seen   = True
                return
            data  = <const int64_t*>vec_i.dense_ptr()
            nulls = vec_i.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        if not self._seen or data[i] < self._result:
                            self._result = data[i]
                            self._seen   = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        if not self._seen or data[i] < self._result:
                            self._result = data[i]
                            self._seen   = True
            return

        if self._col_type == _VTYPE_INTEGER:
            vec_n = <IntegerVector>raw
            if vec_n._has_const:
                if not vec_n._const_is_null:
                    val = vec_n._const_value
                    if not self._seen or val < self._result:
                        self._result = val; self._seen = True
                return
            buf   = vec_n.ptr
            nulls = <const uint8_t*>buf.null_bitmap
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    val = _read_integer_value(buf, i)
                    if not self._seen or val < self._result:
                        self._result = val; self._seen = True
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                val = <int64_t>val_py
                if not self._seen or val < self._result:
                    self._result = val; self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return self._result

    cdef double get_result_f64(self) noexcept:
        return <double>self._result

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None


# ---------------------------------------------------------------------------
# MaxInt64Aggregate
# ---------------------------------------------------------------------------

cdef class MaxInt64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._result     = INT64_MIN
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
                    val = vec_i._const_value
                    if not self._seen or val > self._result:
                        self._result = val; self._seen = True
                return
            if vec_i._dict_codes != NULL:
                dacc  = vec_i.dict_accessor()
                nulls = dacc.row_nulls
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        val = _dict_accessor_read_int_value(dacc, i)
                        if not self._seen or val > self._result:
                            self._result = val; self._seen = True
                return
            data  = <const int64_t*>vec_i.dense_ptr()
            nulls = vec_i.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        if not self._seen or data[i] > self._result:
                            self._result = data[i]; self._seen = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        if not self._seen or data[i] > self._result:
                            self._result = data[i]; self._seen = True
            return

        if self._col_type == _VTYPE_INTEGER:
            vec_n = <IntegerVector>raw
            if vec_n._has_const:
                if not vec_n._const_is_null:
                    val = vec_n._const_value
                    if not self._seen or val > self._result:
                        self._result = val; self._seen = True
                return
            buf   = vec_n.ptr
            nulls = <const uint8_t*>buf.null_bitmap
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    val = _read_integer_value(buf, i)
                    if not self._seen or val > self._result:
                        self._result = val; self._seen = True
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                val = <int64_t>val_py
                if not self._seen or val > self._result:
                    self._result = val; self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return self._result

    cdef double get_result_f64(self) noexcept:
        return <double>self._result

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None


# ---------------------------------------------------------------------------
# MinFloat64Aggregate
# ---------------------------------------------------------------------------

cdef class MinFloat64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._result     = DBL_MAX
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
        cdef double val

        if self._col_type == _VTYPE_FLOAT64:
            vec_f = <Float64Vector>raw
            if vec_f._has_const:
                if not vec_f._const_is_null:
                    val = vec_f._const_value
                    if not self._seen or val < self._result:
                        self._result = val; self._seen = True
                return
            data  = <const double*>vec_f.dense_ptr()
            nulls = vec_f.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        if not self._seen or data[i] < self._result:
                            self._result = data[i]; self._seen = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        if not self._seen or data[i] < self._result:
                            self._result = data[i]; self._seen = True
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                val = <double>val_py
                if not self._seen or val < self._result:
                    self._result = val; self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._result

    cdef double get_result_f64(self) noexcept:
        return self._result

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None


# ---------------------------------------------------------------------------
# MaxFloat64Aggregate
# ---------------------------------------------------------------------------

cdef class MaxFloat64Aggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._result     = -DBL_MAX
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
        cdef double val

        if self._col_type == _VTYPE_FLOAT64:
            vec_f = <Float64Vector>raw
            if vec_f._has_const:
                if not vec_f._const_is_null:
                    val = vec_f._const_value
                    if not self._seen or val > self._result:
                        self._result = val; self._seen = True
                return
            data  = <const double*>vec_f.dense_ptr()
            nulls = vec_f.null_bitmap_ptr()
            if nulls == NULL:
                with nogil:
                    for i in range(nrows):
                        if not self._seen or data[i] > self._result:
                            self._result = data[i]; self._seen = True
                return
            with nogil:
                for i in range(nrows):
                    if _bitmap_is_valid(nulls, i):
                        if not self._seen or data[i] > self._result:
                            self._result = data[i]; self._seen = True
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                val = <double>val_py
                if not self._seen or val > self._result:
                    self._result = val; self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._result

    cdef double get_result_f64(self) noexcept:
        return self._result

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None


# ---------------------------------------------------------------------------
# MinBytesAggregate / MaxBytesAggregate  (string columns, SIMD compare)
# ---------------------------------------------------------------------------

cdef class MinBytesAggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_BYTES
        self._result     = None

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef DrakenVarBuffer* buf
        cdef const uint8_t*   nulls
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <StringVector>raw
            if svec._has_const:
                if not svec._const_is_null:
                    ptr_b = <const char*>svec._const_value.data
                    len_b = <size_t>svec._const_value.length
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) < 0:
                            self._result = ptr_b[:len_b]
                return
            buf   = svec.ptr
            nulls = buf.null_bitmap
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    ptr_b = <const char*>(buf.data + buf.offsets[i])
                    len_b = <size_t>(buf.offsets[i + 1] - buf.offsets[i])
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) < 0:
                            self._result = ptr_b[:len_b]
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                b = val_py if isinstance(val_py, bytes) else str(val_py).encode()
                if self._result is None or b < self._result:
                    self._result = b

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        if self._result is None:
            out_ptr[0] = NULL; out_len[0] = 0
        else:
            out_ptr[0] = self._result; out_len[0] = len(self._result)

    cdef bint is_null(self) noexcept:
        return self._result is None

    cpdef object get_result(self):
        return self._result


cdef class MaxBytesAggregate(UngroupedAggregate):

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_BYTES
        self._result     = None

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed    = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef DrakenVarBuffer* buf
        cdef const uint8_t*   nulls
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <StringVector>raw
            if svec._has_const:
                if not svec._const_is_null:
                    ptr_b = <const char*>svec._const_value.data
                    len_b = <size_t>svec._const_value.length
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) > 0:
                            self._result = ptr_b[:len_b]
                return
            buf   = svec.ptr
            nulls = buf.null_bitmap
            for i in range(nrows):
                if _bitmap_is_valid(nulls, i):
                    ptr_b = <const char*>(buf.data + buf.offsets[i])
                    len_b = <size_t>(buf.offsets[i + 1] - buf.offsets[i])
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) > 0:
                            self._result = ptr_b[:len_b]
            return

        for val_py in raw.to_pylist():
            if val_py is not None:
                b = val_py if isinstance(val_py, bytes) else str(val_py).encode()
                if self._result is None or b > self._result:
                    self._result = b

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        if self._result is None:
            out_ptr[0] = NULL; out_len[0] = 0
        else:
            out_ptr[0] = self._result; out_len[0] = len(self._result)

    cdef bint is_null(self) noexcept:
        return self._result is None

    cpdef object get_result(self):
        return self._result
