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


cdef extern from *:
    # 128-bit accumulator: an integer column's per-morsel sum can exceed int64
    # (e.g. a large-magnitude id column), so the int64 reduction kernel wraps.
    # Accumulating in __int128 over the uniform data[selection[i]] access is exact
    # and is correct on dict-encoded morsels (where to_float64_vector().sum() is
    # not). The final cast to double matches AVG's double semantics.
    ctypedef long long _int128_t "__int128"


cdef inline bint _uagg_row_valid(const uint8_t* validity, uint32_t i) noexcept nogil:
    if validity == NULL:
        return True
    return ((validity[i >> 3] >> (i & 7)) & 1) != 0


cdef double _exact_int_sum_as_double(DrakenVector* dv) noexcept nogil:
    """Exact integer SUM (128-bit accumulator) over data[selection[i]], as double.

    Handles INT8/16/32/64 via native width. NULL rows (per-logical-row validity)
    contribute 0. Used by AVG so the sum neither wraps (int64 kernel) nor goes
    through the dict-buggy float-conversion path.
    """
    cdef uint32_t n = dv.length
    cdef const uint32_t* sel = dv.selection
    cdef const uint8_t* validity = dv.validity
    cdef _int128_t total = 0
    cdef uint32_t i
    cdef DrakenType t = dv.type
    cdef const int64_t* d64
    cdef const int32_t* d32
    cdef const int16_t* d16
    cdef const int8_t*  d8

    if t == DRAKEN_INT64:
        d64 = <const int64_t*>dv.data
        for i in range(n):
            if _uagg_row_valid(validity, i):
                total += <_int128_t>d64[sel[i]]
    elif t == DRAKEN_INT32:
        d32 = <const int32_t*>dv.data
        for i in range(n):
            if _uagg_row_valid(validity, i):
                total += <_int128_t>d32[sel[i]]
    elif t == DRAKEN_INT16:
        d16 = <const int16_t*>dv.data
        for i in range(n):
            if _uagg_row_valid(validity, i):
                total += <_int128_t>d16[sel[i]]
    elif t == DRAKEN_INT8:
        d8 = <const int8_t*>dv.data
        for i in range(n):
            if _uagg_row_valid(validity, i):
                total += <_int128_t>d8[sel[i]]
    return <double>total


cdef class SumInt64Aggregate(UngroupedAggregate):
    # Use a Python big-int accumulator to avoid C int64 overflow during
    # diagnostic / ASAN runs. This is intentionally conservative: we'll
    # revisit the aggregate return type/migration once the native UAF is
    # resolved and we discuss desired SUM semantics.
    cdef object _total_py
    cdef bint    _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._total_py   = 0
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
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        # Use Python integer arithmetic for the per-morsel addition. The
        # Vector.sum() cpdef returns a Python int so this stays in Python
        # space and never truncates silently.
        if self._col_type in (_VTYPE_INT64, _VTYPE_INT8, _VTYPE_INT16, _VTYPE_INT32):
            self._total_py = self._total_py + (<Vector>raw).sum()
            self._seen = True
            return

        raise TypeError(
            f"SumInt64Aggregate cannot sum column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

    cdef int64_t get_result_i64(self) noexcept:
        # Best-effort conversion: if the Python total fits in int64_t, return it,
        # otherwise raise OverflowError so callers don't silently receive a
        # truncated value. This keeps behaviour explicit during diagnostic runs.
        # Use pure-Python bounds check to avoid direct C-API calls in Cython.
        try:
            val_py = int(self._total_py)
        except Exception:
            raise
        cdef long long minv = -9223372036854775808LL
        cdef long long maxv =  9223372036854775807LL
        if val_py < minv or val_py > maxv:
            raise OverflowError("Sum result does not fit in int64")
        return <int64_t>val_py

    cdef double get_result_f64(self) noexcept:
        return <double>float(self._total_py)

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return not self._seen

    cpdef object get_result(self):
        return int(self._total_py) if self._seen else None


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
        if self._col_idx < 0 or self._col_idx >= typed._num_columns():
            return

        cdef Vector raw = typed._get_column(self._col_idx)
        if raw is None:
            return

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        if self._col_type == _VTYPE_FLOAT64:
            self._total += (<Vector>raw).sum()
            self._seen = True
            return
        if self._col_type in (_VTYPE_INT64, _VTYPE_INT8, _VTYPE_INT16, _VTYPE_INT32):
            # Exact 128-bit integer sum (as double): the int64 reduction kernel
            # wraps when a morsel's sum exceeds int64, and to_float64_vector().sum()
            # is wrong on dict-encoded morsels. _exact_int_sum_as_double reads the
            # uniform data[selection[i]] in a 128-bit accumulator — correct for both.
            self._total += _exact_int_sum_as_double((<Vector>raw).unified())
            self._seen = True
            return
        if self._col_type == _VTYPE_DECIMAL:
            # SUM(DECIMAL) is routed here by the planner (_is_float_type treats
            # DECIMAL as float). Convert to float64 (value = unscaled / 10^scale)
            # and accumulate in the double total.
            self._total += (<Vector>raw).to_float64_vector().sum()
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


cdef class SumDecimalAggregate(UngroupedAggregate):
    # Exact decimal SUM. Vector.sum() returns a Python Decimal for DECIMAL/DECIMAL128
    # columns (preserving scale), so we accumulate Decimals — never converting to
    # float64. Result is DECIMAL; emitted via AGG_RESULT_OBJECT + the Decimal-aware
    # path in the engine (_decimal_result_vector), which builds a real DECIMAL vector.
    cdef object _total_dec
    cdef bint   _seen

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_OBJECT
        self._total_dec  = None
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
        cdef object s = (<Vector>raw).sum()   # Decimal for DECIMAL / DECIMAL128
        if self._total_dec is None:
            self._total_dec = s
        else:
            self._total_dec = self._total_dec + s
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
        return self._total_dec if self._seen else None
