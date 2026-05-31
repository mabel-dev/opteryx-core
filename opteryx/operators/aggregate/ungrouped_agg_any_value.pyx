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
        cdef Vector raw = typed._get_column(self._col_idx)

        if self._col_type == _VTYPE_UNKNOWN:
            self._col_type = _classify_vector(raw)

        cdef const int64_t*      idata64
        cdef const int32_t*      idata32
        cdef const int16_t*      idata16
        cdef const int8_t*       idata8
        cdef const double*       fdata
        cdef const uint8_t*      nulls
        cdef DrakenStringArena*  arena
        cdef DrakenVector*       uv
        cdef Py_ssize_t i
        cdef const char*         ptr_c
        cdef Py_ssize_t          length_c
        cdef const uint32_t*     sel
        cdef DrakenStringSlot*   slot

        if self._col_type == _VTYPE_INT64:
            uv = (<Vector>raw).unified()
            idata64 = <const int64_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                self._value = idata64[sel[i]]; self._seen = True; return
            return

        if self._col_type == _VTYPE_FLOAT64:
            uv = (<Vector>raw).unified()
            fdata = <const double*>uv.data
            sel   = uv.selection
            nulls = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                self._value = fdata[sel[i]]; self._seen = True; return
            return

        if self._col_type == _VTYPE_STRING:
            uv = (<Vector>raw).unified()
            arena = <DrakenStringArena*>uv.data
            sel   = uv.selection
            nulls = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                slot     = &arena.slots[sel[i]]
                ptr_c    = <const char*>str_data(slot, arena.arena)
                length_c = <Py_ssize_t>str_length(slot)
                self._value = ptr_c[:length_c]; self._seen = True; return
            return

        if self._col_type == _VTYPE_INT8:
            uv = (<Vector>raw).unified()
            idata8 = <const int8_t*>uv.data
            sel    = uv.selection
            nulls  = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                self._value = <int64_t>idata8[sel[i]]; self._seen = True; return
            return

        if self._col_type == _VTYPE_INT16:
            uv = (<Vector>raw).unified()
            idata16 = <const int16_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                self._value = <int64_t>idata16[sel[i]]; self._seen = True; return
            return

        if self._col_type == _VTYPE_INT32:
            uv = (<Vector>raw).unified()
            idata32 = <const int32_t*>uv.data
            sel     = uv.selection
            nulls   = uv.validity
            for i in range(nrows):
                if nulls != NULL and not _bitmap_is_valid(nulls, i):
                    continue
                self._value = <int64_t>idata32[sel[i]]; self._seen = True; return
            return

        raise TypeError(
            f"AnyValueAggregate cannot scan column {self.column_name!r}: "
            f"unsupported vector type {type(raw).__name__}"
        )

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
