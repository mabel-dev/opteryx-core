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

# draken_is_compressed: data_length < length (dict OR constant shape). A static
# inline in buffers.h; safe to call on any DrakenVector.
cdef extern from "core/buffers.h" nogil:
    int draken_is_compressed(const DrakenVector* v)


cdef class CountDistinctAggregate(UngroupedAggregate):
    """
    COUNT(DISTINCT col).

    Two paths, same result and same hashing kernel:

      * Compressed (dict/constant) morsel — hash only the data_length DISTINCT
        values once, mark which are referenced by a non-null row in a single
        cheap O(nrows) pass (no set probe), then insert just the referenced
        hashes. A dict-encoded string column repeats each value ~2.4× on the
        ClickBench `hits` data, so this collapses ~92M per-row set probes to
        ~37.6M unique inserts and drops the per-row scatter/null-compaction
        passes entirely. Values referenced only by null rows are excluded
        (COUNT(DISTINCT) ignores NULL).

      * Dense morsel — hash every row (c_hash_single), drop null rows by the
        baked NULL_HASH marker, bulk-insert.

    Because both paths hash a value through the SAME per-type kernel (the
    compressed path via draken_hash_distinct's dense view), a given string
    produces a byte-identical 64-bit hash regardless of which shape it arrived
    in, so the set dedups correctly across a mix of dense and dict morsels.

    CLAUDE.md §11: the compressed fast path is an architect-approved (F2)
    shape-specialized dispatch; its result is identical to the uniform path.
    """
    cdef CarcharSetWrapper _set
    cdef uint64_t* _scratch_buf
    cdef Py_ssize_t _scratch_capacity
    cdef uint8_t* _mask_buf
    cdef Py_ssize_t _mask_capacity

    def __cinit__(self, bytes column_name, bytes alias):
        self.column_name = column_name
        self.alias       = alias
        self.result_type = AGG_RESULT_I64
        self._set        = CarcharSetWrapper()
        self._scratch_buf = NULL
        self._scratch_capacity = 0
        self._mask_buf = NULL
        self._mask_capacity = 0

    def __dealloc__(self):
        if self._scratch_buf != NULL:
            free(self._scratch_buf)
            self._scratch_buf = NULL
        if self._mask_buf != NULL:
            free(self._mask_buf)
            self._mask_buf = NULL

    cdef void apply(self, Morsel morsel) except *:
        cdef Morsel typed      = <Morsel>morsel
        cdef Py_ssize_t nrows = <Py_ssize_t>typed.ptr.num_rows
        if nrows == 0:
            return

        if self._col_idx < 0:
            self._col_idx = typed._column_index_from_name(self.column_name)
        cdef Vector raw = typed._get_column(self._col_idx)
        cdef CarcharSetWrapper the_set = self._set
        cdef DrakenVector* dv = raw.unified()
        cdef Py_ssize_t nd = <Py_ssize_t>dv.data_length

        # Reuse hash scratch if large enough. Sized to nrows; the dense path
        # writes nrows entries and a compressed vector has nd <= nrows.
        if nrows > self._scratch_capacity:
            if self._scratch_buf != NULL:
                free(self._scratch_buf)
            self._scratch_buf = <uint64_t*>malloc(nrows * sizeof(uint64_t))
            if self._scratch_buf == NULL:
                raise MemoryError()
            self._scratch_capacity = nrows

        cdef const uint32_t* sel
        cdef uint8_t* validity
        cdef Py_ssize_t i
        cdef Py_ssize_t write_idx
        cdef Py_ssize_t read_idx

        # ── Compressed (dict/constant) fast path ─────────────────────────────
        if (draken_is_compressed(dv)
                and dv.type != DRAKEN_ARRAY
                and dv.type != DRAKEN_NULL
                and dv.type != DRAKEN_VECTOR_FP16):
            # Hash the nd distinct values into the front of the scratch buffer.
            raw.c_hash_distinct(self._scratch_buf)

            if nd > self._mask_capacity:
                if self._mask_buf != NULL:
                    free(self._mask_buf)
                self._mask_buf = <uint8_t*>malloc(nd * sizeof(uint8_t))
                if self._mask_buf == NULL:
                    raise MemoryError()
                self._mask_capacity = nd

            sel      = dv.selection
            validity = dv.validity
            with nogil:
                memset(self._mask_buf, 0, <size_t>nd)
                # Mark every distinct value referenced by a non-null row.
                if validity == NULL:
                    for i in range(nrows):
                        self._mask_buf[sel[i]] = 1
                else:
                    for i in range(nrows):
                        if (validity[i >> 3] >> (i & 7)) & 1:
                            self._mask_buf[sel[i]] = 1
                # Compact referenced hashes to the front (write_idx <= read_idx,
                # so the in-place move never clobbers an unread slot), then
                # bulk-insert just those.
                write_idx = 0
                for read_idx in range(nd):
                    if self._mask_buf[read_idx]:
                        self._scratch_buf[write_idx] = self._scratch_buf[read_idx]
                        write_idx += 1
                the_set._insert_many_nogil(self._scratch_buf, <size_t>write_idx)
            return

        # ── Dense path (data_length == length) ───────────────────────────────
        raw.c_hash_single(self._scratch_buf, nrows)
        validity = raw.null_bitmap_ptr()

        if validity == NULL:
            with nogil:
                the_set._insert_many_nogil(self._scratch_buf, <size_t>nrows)
        else:
            # Drop null rows by the validity bitmap, NOT a hash sentinel. The
            # kernel mixes null rows via simd_hash_i64(NULL_HASH), whose output
            # does not equal the scalar mix_hash(0, NULL_HASH); comparing against
            # that sentinel silently kept NULL as one extra distinct value.
            # COUNT(DISTINCT) ignores NULL — filter on validity, which the
            # compressed path above already does.
            with nogil:
                write_idx = 0
                for read_idx in range(nrows):
                    if (validity[read_idx >> 3] >> (read_idx & 7)) & 1:
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
        self._set.tighten()
        return <int64_t>self._set.size()
