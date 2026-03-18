cdef class Int64CountStarKernel:
    cdef object _key_column
    cdef bint _hash_keys_mode
    cdef bint _seen_null
    cdef int64_t _null_count
    cdef flat_hash_map[uint64_t, int64_t] _counts
    cdef dict _hash_keys

    def __cinit__(self, object key_column):
        self._key_column = key_column
        self._hash_keys_mode = False
        self._seen_null = False
        self._null_count = 0
        self._hash_keys = {}

    cpdef bint ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef object key_vector
        cdef Int64Vector key_int64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef DictAccessor* key_dict_ptr
        cdef int64_t* key_data
        cdef uint8_t* key_nulls
        cdef uint64_t* key_hashes = NULL
        cdef int64_t key_value
        cdef uint32_t key_code
        cdef uint64_t key_u64
        cdef object key_obj
        cdef uint64_t* dptr = <uint64_t*>NULL
        cdef uint8_t* nptr = <uint8_t*>NULL
        cdef flat_hash_map[uint64_t, int64_t]* cmap
        cdef bint seen
        cdef int64_t nulls

        key_vector = morsel.column(self._key_column)
        row_count = morsel.num_rows

        if self._counts.size() == 0 and row_count > 0:
            self._counts.reserve(<size_t>(row_count * 2))

        if isinstance(key_vector, Int64Vector):
            self._hash_keys_mode = False
            key_int64_vector = <Int64Vector>key_vector
            key_ptr = key_int64_vector.ptr
            key_data = <int64_t*>key_ptr.data
            key_nulls = <uint8_t*>key_ptr.null_bitmap

            # perform the tight counting loop without touching Python objects
            seen = self._seen_null
            nulls = self._null_count
            dptr = <uint64_t*>key_data
            nptr = key_nulls
            cmap = &self._counts

            with nogil:
                if nptr == NULL:
                    for row_idx in range(row_count):
                        cmap[0][dptr[row_idx]] += 1
                else:
                    for row_idx in range(row_count):
                        if (nptr[row_idx >> 3] >> (row_idx & 7)) & 1:
                            cmap[0][dptr[row_idx]] += 1
                        else:
                            seen = True
                            nulls += 1

            # write back the bits that had to live under the GIL
            self._seen_null = seen
            self._null_count = nulls
            return True

        if isinstance(key_vector, Vector):
            key_dict_ptr = (<Vector>key_vector).dict_accessor()
        else:
            key_dict_ptr = NULL

        if key_dict_ptr != NULL:
            self._hash_keys_mode = True
            if key_dict_ptr.dict_values == NULL:
                return False

            key_hashes = _build_dict_hashes(key_dict_ptr)
            try:
                for row_idx in range(row_count):
                    key_code = _dict_read_code(key_dict_ptr, row_idx)
                    if key_code >= key_dict_ptr.dict_values.length:
                        raise ValueError("Dictionary key code out of bounds in COUNT(*) kernel")
                    if _dict_row_null(key_dict_ptr, key_code, row_idx):
                        self._seen_null = True
                        self._null_count += 1
                        continue
                    key_u64 = key_hashes[key_code]
                    self._counts[key_u64] += 1
                    key_obj = self._hash_keys.get(key_u64, _KERNEL_MISSING)
                    if key_obj is _KERNEL_MISSING:
                        self._hash_keys[key_u64] = _dict_code_to_object(key_dict_ptr, key_code)
                return True
            finally:
                if key_hashes != NULL:
                    free(key_hashes)

        return False

    # finalize_rows is the slow, fully‑Python path; we keep it for
    # compatibility but it's only invoked if the caller cannot consume
    # the fast columns returned by ``finalize_fast_columns``.
    cpdef list finalize_rows(self):
        # delegate to fast columns and then convert to Python list in a single
        # loop; this avoids repeated append() calls in the Python layer.
        cdef object res
        cdef list rows
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef object keys, counts
        cdef uint64_t key_u64
        cdef object key_obj

        res = self.finalize_fast_columns()
        if res is None:
            # nulls present; fall back to the old behaviour for correctness
            rows = []
            count_it = self._counts.begin()
            while count_it != self._counts.end():
                key_u64 = dereference(count_it).first
                if self._hash_keys_mode:
                    key_obj = self._hash_keys.get(key_u64, <int64_t>key_u64)
                else:
                    key_obj = <int64_t>key_u64
                rows.append(
                    (
                        (key_obj,),
                        [dereference(count_it).second],
                    )
                )
                preincrement(count_it)
            if self._seen_null:
                rows.append(((None,), [self._null_count]))
            return rows
        else:
            # res is (keys, counts) tuple
            keys, counts = res
            return [( (k,), [c] ) for k, c in zip(keys, counts)]

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef object keys
        cdef object counts
        cdef int64_t[::1] key_view
        cdef int64_t[::1] count_view

        if self._seen_null or self._hash_keys_mode:
            return None

        n = <Py_ssize_t>self._counts.size()
        keys = array("q", [0]) * n
        counts = array("q", [0]) * n
        key_view = keys
        count_view = counts

        count_it = self._counts.begin()
        idx = 0
        # iterate over the C++ map while holding the GIL; the body does not
        # perform any Python calls so the loop is already very tight.  if
        # desired this could be executed nogil, but the improvement is
        # negligible compared to the cost of the aggregation itself.
        while count_it != self._counts.end():
            key_view[idx] = <int64_t>dereference(count_it).first
            count_view[idx] = dereference(count_it).second
            idx += 1
            preincrement(count_it)

        return keys, counts
