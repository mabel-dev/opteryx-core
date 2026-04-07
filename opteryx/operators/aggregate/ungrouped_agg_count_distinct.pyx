# included by ungrouped_agg.pyx — do not compile standalone


cdef class CountDistinctAggregate(UngroupedAggregate):
    """
    COUNT(DISTINCT col).

    Hashes are written directly into a malloc'd buffer via c_hash_into()
    (no Python memoryview) then inserted into a CarcharSetWrapper under nogil.
    """

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._set        = CarcharSetWrapper()

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed     = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        cdef uint64_t* hbuf = <uint64_t*>malloc(nrows * sizeof(uint64_t))
        if hbuf == NULL:
            raise MemoryError()

        cdef CarcharSetWrapper the_set = self._set
        cdef Py_ssize_t i

        try:
            raw.c_hash_into(hbuf, nrows)   # C-level hash, no memoryview
            with nogil:
                for i in range(nrows):
                    the_set.insert(hbuf[i])
        finally:
            free(hbuf)

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._set.size()

    cdef double get_result_f64(self) noexcept:
        return <double>self._set.size()

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return False

    cpdef object get_result(self):
        return <int64_t>self._set.size()
