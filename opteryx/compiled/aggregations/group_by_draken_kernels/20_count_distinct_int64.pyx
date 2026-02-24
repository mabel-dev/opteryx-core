cdef class Int64CountDistinctInt64Kernel:
    cdef object _key_column
    cdef object _value_column
    cdef bint _seen_null_key
    cdef int64_t _null_key_count
    cdef flat_hash_map[uint64_t, int64_t] _counts
    cdef flat_hash_map[uint64_t, flat_hash_set[uint64_t, IdentityHash]] _seen
    cdef flat_hash_set[uint64_t, IdentityHash] _null_key_seen

    def __cinit__(self, object key_column, object value_column):
        self._key_column = key_column
        self._value_column = value_column
        self._seen_null_key = False
        self._null_key_count = 0

    cpdef bint ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef object key_vector
        cdef object value_vector
        cdef Int64Vector key_int64_vector
        cdef Int64Vector value_i64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef DrakenFixedBuffer* value_ptr
        cdef int64_t* key_data
        cdef int64_t* value_i64_data
        cdef uint8_t* key_nulls
        cdef uint8_t* value_nulls
        cdef uint64_t key_u64
        cdef uint64_t distinct_value_u64

        key_vector = morsel.column(self._key_column)
        value_vector = morsel.column(self._value_column)
        if not isinstance(key_vector, Int64Vector) or not isinstance(value_vector, Int64Vector):
            return False

        key_int64_vector = <Int64Vector>key_vector
        key_ptr = key_int64_vector.ptr
        key_data = <int64_t*>key_ptr.data
        key_nulls = <uint8_t*>key_ptr.null_bitmap

        value_i64_vector = <Int64Vector>value_vector
        value_ptr = value_i64_vector.ptr
        value_i64_data = <int64_t*>value_ptr.data
        value_nulls = <uint8_t*>value_ptr.null_bitmap
        row_count = morsel.num_rows

        if self._counts.size() == 0 and row_count > 0:
            self._counts.reserve(<size_t>(row_count * 2))

        if key_nulls == NULL and value_nulls == NULL:
            for row_idx in range(row_count):
                key_u64 = <uint64_t>key_data[row_idx]
                distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                if self._seen[key_u64].insert(distinct_value_u64).second:
                    self._counts[key_u64] += 1
            return True

        for row_idx in range(row_count):
            if value_nulls != NULL and not ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                continue
            distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
            if key_nulls == NULL or ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                key_u64 = <uint64_t>key_data[row_idx]
                if self._seen[key_u64].insert(distinct_value_u64).second:
                    self._counts[key_u64] += 1
            else:
                self._seen_null_key = True
                if self._null_key_seen.insert(distinct_value_u64).second:
                    self._null_key_count += 1
        return True

    cpdef list finalize_rows(self):
        cdef list rows
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it

        if self._counts.size() == 0 and not self._seen_null_key:
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
        if self._seen_null_key:
            rows.append(((None,), [self._null_key_count]))
        return rows

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef object keys
        cdef object counts
        cdef int64_t[::1] key_view
        cdef int64_t[::1] count_view

        if self._seen_null_key:
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
