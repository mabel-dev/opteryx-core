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


# ---------------------------------------------------------------------------
# MinInt64Aggregate
# ---------------------------------------------------------------------------

cdef class MinInt64Aggregate(UngroupedAggregate):
    cdef int64_t _result
    cdef bint    _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._result     = INT64_MAX
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        # Delegates to Vector.min() which dispatches by encoding (dense via
        # C++ kernel, dict / RLE / const via dedicated helpers).
        cdef Morsel typed = <Morsel>morsel
        if typed.ptr is NULL or typed.ptr.num_rows == 0:
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

        cdef int64_t val
        try:
            if self._col_type == _VTYPE_INT64:
                val = (<Integer64Vector>raw).min()
            elif self._col_type == _VTYPE_INT8:
                val = (<Integer8Vector>raw).min()
            elif self._col_type == _VTYPE_INT16:
                val = (<Integer16Vector>raw).min()
            elif self._col_type == _VTYPE_INT32:
                val = (<Integer32Vector>raw).min()
            else:
                raise TypeError(
                    f"MinInt64Aggregate cannot scan column {self.column_name!r}: "
                    f"unsupported vector type {type(raw).__name__}"
                )
        except ValueError:
            # all-null morsel — skip
            return

        if not self._seen or val < self._result:
            self._result = val
            self._seen = True

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
    cdef int64_t _result
    cdef bint    _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._result     = INT64_MIN
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.ptr is NULL or typed.ptr.num_rows == 0:
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

        cdef int64_t val
        try:
            if self._col_type == _VTYPE_INT64:
                val = (<Integer64Vector>raw).max()
            elif self._col_type == _VTYPE_INT8:
                val = (<Integer8Vector>raw).max()
            elif self._col_type == _VTYPE_INT16:
                val = (<Integer16Vector>raw).max()
            elif self._col_type == _VTYPE_INT32:
                val = (<Integer32Vector>raw).max()
            else:
                raise TypeError(
                    f"MaxInt64Aggregate cannot scan column {self.column_name!r}: "
                    f"unsupported vector type {type(raw).__name__}"
                )
        except ValueError:
            return

        if not self._seen or val > self._result:
            self._result = val
            self._seen = True

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
    cdef double _result
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._result     = DBL_MAX
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.ptr is NULL or typed.ptr.num_rows == 0:
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

        cdef double val
        try:
            if self._col_type == _VTYPE_FLOAT64:
                val = (<Float64Vector>raw).min()
            else:
                raise TypeError(
                    f"MinFloat64Aggregate cannot scan column {self.column_name!r}: "
                    f"unsupported vector type {type(raw).__name__}"
                )
        except ValueError:
            return

        if not self._seen or val < self._result:
            self._result = val
            self._seen = True

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
    cdef double _result
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_F64
        self._result     = -DBL_MAX
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.ptr is NULL or typed.ptr.num_rows == 0:
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

        cdef double val
        try:
            if self._col_type == _VTYPE_FLOAT64:
                val = (<Float64Vector>raw).max()
            else:
                raise TypeError(
                    f"MaxFloat64Aggregate cannot scan column {self.column_name!r}: "
                    f"unsupported vector type {type(raw).__name__}"
                )
        except ValueError:
            return

        if not self._seen or val > self._result:
            self._result = val
            self._seen = True

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
    cdef bytes _result

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
        cdef DrakenVector*    uv
        cdef const uint8_t*   nulls
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <StringVector>raw
            uv = svec.unified()
            if svec.ptr.offsets == NULL:  # constant (offsets always allocated for dense/dict)
                if uv.validity == NULL:
                    ptr_b = <const char*>(<DrakenConstantStringPayload*>uv.data).data
                    len_b = <size_t>(<DrakenConstantStringPayload*>uv.data).length
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) < 0:
                            self._result = ptr_b[:len_b]
                return
            # Dict-encoded fast path: scan referenced, non-null dict entries
            # only.  Only worthwhile when K << N — at K ~ N this loses to
            # the dense path's tighter inner loop.
            if (
                uv.selection != NULL
                and svec.c_dict_size() <= (nrows >> 2)
            ):
                self._scan_dict_min(svec)
                return
            buf   = <DrakenVarBuffer*>uv.data
            nulls = uv.validity
            for i in range(nrows):
                if nulls == NULL or _bitmap_is_valid(nulls, i):
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

    cdef void _scan_dict_min(self, StringVector svec) except *:
        cdef const int64_t* counts = svec.c_dict_code_counts_ptr()
        cdef Py_ssize_t dict_size = svec.c_dict_size()
        cdef Py_ssize_t di, vlen, best_len
        cdef const uint8_t* vptr
        cdef const char* best_ptr
        for di in range(dict_size):
            if counts[di] <= 0:
                continue
            if svec.c_dict_value_is_null(di):
                continue
            vptr = svec.c_dict_value_ptr(di, &vlen)
            if vptr == NULL:
                continue
            if self._result is None:
                self._result = (<const char*>vptr)[:vlen]
            else:
                best_ptr = self._result
                best_len = len(self._result)
                if compare_bytes(<const char*>vptr, <size_t>vlen, best_ptr, <size_t>best_len) < 0:
                    self._result = (<const char*>vptr)[:vlen]

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
    cdef bytes _result

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
        cdef DrakenVector*    uv
        cdef const uint8_t*   nulls
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <StringVector>raw
            uv = svec.unified()
            if svec.ptr.offsets == NULL:  # constant (offsets always allocated for dense/dict)
                if uv.validity == NULL:
                    ptr_b = <const char*>(<DrakenConstantStringPayload*>uv.data).data
                    len_b = <size_t>(<DrakenConstantStringPayload*>uv.data).length
                    if self._result is None:
                        self._result = ptr_b[:len_b]
                    else:
                        ptr_a = self._result; len_a = len(self._result)
                        if compare_bytes(ptr_b, len_b, ptr_a, len_a) > 0:
                            self._result = ptr_b[:len_b]
                return
            if (
                uv.selection != NULL
                and svec.c_dict_size() <= (nrows >> 2)
            ):
                self._scan_dict_max(svec)
                return
            buf   = <DrakenVarBuffer*>uv.data
            nulls = uv.validity
            for i in range(nrows):
                if nulls == NULL or _bitmap_is_valid(nulls, i):
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

    cdef void _scan_dict_max(self, StringVector svec) except *:
        cdef const int64_t* counts = svec.c_dict_code_counts_ptr()
        cdef Py_ssize_t dict_size = svec.c_dict_size()
        cdef Py_ssize_t di, vlen, best_len
        cdef const uint8_t* vptr
        cdef const char* best_ptr
        for di in range(dict_size):
            if counts[di] <= 0:
                continue
            if svec.c_dict_value_is_null(di):
                continue
            vptr = svec.c_dict_value_ptr(di, &vlen)
            if vptr == NULL:
                continue
            if self._result is None:
                self._result = (<const char*>vptr)[:vlen]
            else:
                best_ptr = self._result
                best_len = len(self._result)
                if compare_bytes(<const char*>vptr, <size_t>vlen, best_ptr, <size_t>best_len) > 0:
                    self._result = (<const char*>vptr)[:vlen]

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
