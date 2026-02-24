cdef class Int64CountStarKernel:
    cdef object _key_column
    cdef bint _seen_null
    cdef int64_t _null_count
    cdef flat_hash_map[uint64_t, int64_t] _counts

    def __cinit__(self, object key_column):
        self._key_column = key_column
        self._seen_null = False
        self._null_count = 0

    cpdef bint ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef object key_vector
        cdef Int64Vector key_int64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef int64_t* key_data
        cdef uint8_t* key_nulls
        cdef int64_t key_value

        key_vector = morsel.column(self._key_column)
        if not isinstance(key_vector, Int64Vector):
            return False

        key_int64_vector = <Int64Vector>key_vector
        key_ptr = key_int64_vector.ptr
        key_data = <int64_t*>key_ptr.data
        key_nulls = <uint8_t*>key_ptr.null_bitmap
        row_count = morsel.num_rows

        if self._counts.size() == 0 and row_count > 0:
            self._counts.reserve(<size_t>(row_count * 2))

        if key_nulls == NULL:
            for row_idx in range(row_count):
                key_value = key_data[row_idx]
                self._counts[<uint64_t>key_value] += 1
            return True

        for row_idx in range(row_count):
            if (key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1:
                key_value = key_data[row_idx]
                self._counts[<uint64_t>key_value] += 1
            else:
                self._seen_null = True
                self._null_count += 1
        return True

    cpdef list finalize_rows(self):
        cdef list rows
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it

        if self._counts.size() == 0 and not self._seen_null:
            return []

        rows = []
        count_it = self._counts.begin()
        while count_it != self._counts.end():
            rows.append(
                (
                    (<int64_t>dereference(count_it).first,),
                    [dereference(count_it).second],
                )
            )
            preincrement(count_it)
        if self._seen_null:
            rows.append(((None,), [self._null_count]))
        return rows

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef object keys
        cdef object counts
        cdef int64_t[::1] key_view
        cdef int64_t[::1] count_view

        if self._seen_null:
            return None

        n = <Py_ssize_t>self._counts.size()
        keys = array("q", [0]) * n
        counts = array("q", [0]) * n
        key_view = keys
        count_view = counts

        count_it = self._counts.begin()
        idx = 0
        while count_it != self._counts.end():
            key_view[idx] = <int64_t>dereference(count_it).first
            count_view[idx] = dereference(count_it).second
            idx += 1
            preincrement(count_it)

        return keys, counts
