# included by ungrouped_agg.pyx — do not compile standalone


cdef class CountDistinctAggregate(UngroupedAggregate):
    """
    COUNT(DISTINCT col).

    Hashes are written directly into a malloc'd buffer via c_hash_into()
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

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed      = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = <Vector>typed._columns[self._col_idx]

        cdef CarcharSetWrapper the_set = self._set
        cdef StringVector svec
        cdef Py_ssize_t dict_size, di
        cdef const int64_t* counts
        cdef uint64_t h

        # Dict / RLE encoded fast paths: only worthwhile when the unique
        # value count is meaningfully smaller than the row count.  At K ~ N
        # the per-entry hash cost (one at a time) loses to the dense path's
        # SIMD-batched hashing.  Threshold K <= N/4 — empirically chosen to
        # preserve the big wins on low-cardinality data without regressing
        # high-cardinality URL-style columns.
        cdef Py_ssize_t run_count
        cdef Py_ssize_t card_threshold = nrows >> 2
        if isinstance(raw, StringVector):
            svec = <StringVector>raw
            if (
                svec._encoding == DRAKEN_ENCODING_DICTIONARY
                and svec._dict_codes != NULL
                and svec._dict_values != NULL
            ):
                dict_size = svec.c_dict_size()
                if dict_size <= card_threshold:
                    counts = svec.c_dict_code_counts_ptr()
                    with nogil:
                        for di in range(dict_size):
                            if counts[di] <= 0:
                                continue
                            if svec.c_dict_value_is_null(di):
                                continue
                            h = svec.c_dict_value_hash(di)
                            the_set._insert_many_nogil(&h, 1)
                    return
            elif (
                svec._encoding == DRAKEN_ENCODING_RLE
                and svec.c_rle_null_bitmap() == NULL
            ):
                # No cardinality gate here: c_hash_into has no RLE branch,
                # so the dense fallback would silently produce wrong results.
                run_count = svec.c_rle_run_count()
                with nogil:
                    for di in range(run_count):
                        h = svec.c_rle_run_value_hash(di)
                        the_set._insert_many_nogil(&h, 1)
                return

        # Reuse scratch buffer if large enough
        if nrows > self._scratch_capacity:
            if self._scratch_buf != NULL:
                free(self._scratch_buf)
            self._scratch_buf = <uint64_t*>malloc(nrows * sizeof(uint64_t))
            if self._scratch_buf == NULL:
                raise MemoryError()
            self._scratch_capacity = nrows

        # simd_mix_hash accumulates into dest, so zero before each call
        memset(self._scratch_buf, 0, <size_t>nrows * sizeof(uint64_t))
        # Hash into scratch buffer, then batch insert
        raw.c_hash_into(self._scratch_buf, nrows)    # C-level hash, no memoryview

        # Compact out null entries.  c_hash_into writes NULL_HASH for null
        # rows, then simd_mix_hash mixes that with the (zeroed) destination,
        # so the final marker for a null row is mix_hash(0, NULL_HASH).
        # Excluding these honours ANSI SQL: COUNT(DISTINCT col) ignores NULL.
        cdef uint64_t null_marker = mix_hash(0, NULL_HASH)
        cdef Py_ssize_t write_idx = 0
        cdef Py_ssize_t read_idx
        with nogil:
            for read_idx in range(nrows):
                if self._scratch_buf[read_idx] != null_marker:
                    self._scratch_buf[write_idx] = self._scratch_buf[read_idx]
                    write_idx += 1
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
        # Tighten memory before counting
        self._set.tighten()
        return <int64_t>self._set.size()
