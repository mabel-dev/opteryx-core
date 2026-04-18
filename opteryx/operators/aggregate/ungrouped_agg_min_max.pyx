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

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        # Skip native fast paths due to segfault - use fallback instead
        cdef int64_t val
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

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        # Skip native fast paths due to segfault - use fallback instead
        cdef int64_t val
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

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        # Skip native fast paths due to segfault - use fallback instead
        cdef double val
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

        if self._col_idx < 0 or self._col_idx >= len(typed._columns):
            return

        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        if raw is None:
            return

        # Skip native fast paths due to segfault - use fallback instead
        cdef double val
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
