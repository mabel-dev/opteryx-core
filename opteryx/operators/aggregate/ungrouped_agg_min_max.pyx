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
        if typed.num_rows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef int64_t val
        try:
            if self._col_type == _VTYPE_INT64:
                val = (<Vector>raw).min()
            elif self._col_type == _VTYPE_INT8:
                val = (<Vector>raw).min()
            elif self._col_type == _VTYPE_INT16:
                val = (<Vector>raw).min()
            elif self._col_type == _VTYPE_INT32:
                val = (<Vector>raw).min()
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
        if typed.num_rows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef int64_t val
        try:
            if self._col_type == _VTYPE_INT64:
                val = (<Vector>raw).max()
            elif self._col_type == _VTYPE_INT8:
                val = (<Vector>raw).max()
            elif self._col_type == _VTYPE_INT16:
                val = (<Vector>raw).max()
            elif self._col_type == _VTYPE_INT32:
                val = (<Vector>raw).max()
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
        if typed.num_rows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef double val
        try:
            if self._col_type == _VTYPE_FLOAT64:
                val = (<Vector>raw).min()
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
        if typed.num_rows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef double val
        try:
            if self._col_type == _VTYPE_FLOAT64:
                val = (<Vector>raw).max()
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
        cdef Vector raw = typed._get_column(self._col_idx)

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef DrakenStringArena* arena
        cdef DrakenVector*      uv
        cdef const uint8_t*     nulls
        cdef const uint32_t*    sel
        cdef DrakenStringSlot*  slot
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <Vector>raw
            uv    = svec.unified()
            arena = <DrakenStringArena*>uv.data
            sel   = <const uint32_t*>uv.selection
            nulls = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                slot  = &arena.slots[sel[i]]
                ptr_b = <const char*>str_data(slot, arena.arena)
                len_b = <size_t>str_length(slot)
                if self._result is None:
                    self._result = ptr_b[:len_b]
                else:
                    ptr_a = self._result; len_a = len(self._result)
                    if compare_bytes(ptr_b, len_b, ptr_a, len_a) < 0:
                        self._result = ptr_b[:len_b]
            return

        raise TypeError(
            f"MinBytesAggregate: unsupported column type {self._col_type} "
            f"for column {self.column_name!r}"
        )

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
        cdef Vector raw = typed._get_column(self._col_idx)

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef DrakenStringArena* arena
        cdef DrakenVector*      uv
        cdef const uint8_t*     nulls
        cdef const uint32_t*    sel
        cdef DrakenStringSlot*  slot
        cdef Py_ssize_t i
        cdef const char* ptr_a
        cdef const char* ptr_b
        cdef size_t      len_a, len_b

        if self._col_type == _VTYPE_STRING:
            svec = <Vector>raw
            uv    = svec.unified()
            arena = <DrakenStringArena*>uv.data
            sel   = <const uint32_t*>uv.selection
            nulls = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                slot  = &arena.slots[sel[i]]
                ptr_b = <const char*>str_data(slot, arena.arena)
                len_b = <size_t>str_length(slot)
                if self._result is None:
                    self._result = ptr_b[:len_b]
                else:
                    ptr_a = self._result; len_a = len(self._result)
                    if compare_bytes(ptr_b, len_b, ptr_a, len_a) > 0:
                        self._result = ptr_b[:len_b]
            return

        raise TypeError(
            f"MaxBytesAggregate: unsupported column type {self._col_type} "
            f"for column {self.column_name!r}"
        )

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


# ---------------------------------------------------------------------------
# MinDecimalAggregate / MaxDecimalAggregate — exact decimal MIN/MAX.
# Vector.min()/max() return a Python Decimal for DECIMAL/DECIMAL128 columns
# (preserving scale); we track the running min/max Decimal across morsels and
# emit a DECIMAL vector (AGG_RESULT_OBJECT → _decimal_result_vector in the engine).
# The float aggregates lost precision (Decimal → C double).
# ---------------------------------------------------------------------------

cdef class MinDecimalAggregate(UngroupedAggregate):
    cdef object _result   # Decimal or None
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_OBJECT
        self._result     = None
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.num_rows == 0:
            return
        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return
        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return
        cdef object v
        try:
            v = (<Vector>raw).min()   # Decimal; raises on all-null morsel
        except ValueError:
            return
        if self._result is None or v < self._result:
            self._result = v
        self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None


cdef class MaxDecimalAggregate(UngroupedAggregate):
    cdef object _result   # Decimal or None
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_OBJECT
        self._result     = None
        self._seen       = False

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed = <Morsel>morsel
        if typed.num_rows == 0:
            return
        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return
        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return
        cdef object v
        try:
            v = (<Vector>raw).max()   # Decimal; raises on all-null morsel
        except ValueError:
            return
        if self._result is None or v > self._result:
            self._result = v
        self._seen = True

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return self._result if self._seen else None
