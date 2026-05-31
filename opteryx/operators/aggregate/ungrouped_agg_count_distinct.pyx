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


cdef class CountDistinctAggregate(UngroupedAggregate):
    """
    COUNT(DISTINCT col).

    Hashes are written directly into a malloc'd buffer via c_hash_single()
    (no Python memoryview) then inserted into a CarcharSetWrapper under nogil.
    """
    cdef CarcharSetWrapper _set
    cdef uint64_t* _scratch_buf
    cdef Py_ssize_t _scratch_capacity

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._set        = CarcharSetWrapper()
        self._scratch_buf = NULL
        self._scratch_capacity = 0

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed      = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = typed._get_column(self._col_idx)

        cdef CarcharSetWrapper the_set = self._set

        # Reuse scratch buffer if large enough
        if nrows > self._scratch_capacity:
            if self._scratch_buf != NULL:
                free(self._scratch_buf)
            self._scratch_buf = <uint64_t*>malloc(nrows * sizeof(uint64_t))
            if self._scratch_buf == NULL:
                raise MemoryError()
            self._scratch_capacity = nrows

        raw.c_hash_single(self._scratch_buf, nrows)

        cdef Py_ssize_t write_idx
        cdef Py_ssize_t read_idx
        cdef uint64_t null_marker = mix_hash(0, NULL_HASH)
        cdef uint8_t* null_bitmap = raw.null_bitmap_ptr()

        if null_bitmap == NULL:
            with nogil:
                the_set._insert_many_nogil(self._scratch_buf, <size_t>nrows)
        else:
            with nogil:
                write_idx = 0
                for read_idx in range(nrows):
                    if self._scratch_buf[read_idx] != null_marker:
                        self._scratch_buf[write_idx] = self._scratch_buf[read_idx]
                        write_idx += 1
            with nogil:
                the_set._insert_many_nogil(self._scratch_buf, <size_t>write_idx)

    cdef int64_t get_result_i64(self) noexcept:
        return <int64_t>self._set.size()

    cdef double get_result_f64(self) noexcept:
        return <double>self._set.size()

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL; out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return False

    cpdef object get_result(self):
        self._set.tighten()
        return <int64_t>self._set.size()
