cdef class Int64AvgFloat64Kernel:
    cdef object _key_column
    cdef object _value_column
    cdef bint _seen_null_key
    cdef double _null_key_sum
    cdef int64_t _null_key_count
    cdef flat_hash_map[uint64_t, int64_t] _rows
    cdef flat_hash_map[uint64_t, double] _sums
    cdef flat_hash_map[uint64_t, int64_t] _counts

    def __cinit__(self, object key_column, object value_column):
        self._key_column = key_column
        self._value_column = value_column
        self._seen_null_key = False
        self._null_key_sum = 0.0
        self._null_key_count = 0

    cpdef bint ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef object key_vector
        cdef object value_vector
        cdef Int64Vector key_int64_vector
        cdef Int64Vector value_i64_vector
        cdef Float64Vector value_f64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef DrakenFixedBuffer* value_ptr
        cdef int64_t* key_data
        cdef int64_t* value_i64_data
        cdef double* value_f64_data
        cdef uint8_t* key_nulls
        cdef uint8_t* value_nulls
        cdef uint64_t key_u64
        cdef double value_f64

        key_vector = morsel.column(self._key_column)
        value_vector = morsel.column(self._value_column)
        if not isinstance(key_vector, Int64Vector):
            return False

        key_int64_vector = <Int64Vector>key_vector
        key_ptr = key_int64_vector.ptr
        key_data = <int64_t*>key_ptr.data
        key_nulls = <uint8_t*>key_ptr.null_bitmap
        row_count = morsel.num_rows

        if self._rows.size() == 0 and row_count > 0:
            self._rows.reserve(<size_t>(row_count * 2))
            self._sums.reserve(<size_t>(row_count * 2))
            self._counts.reserve(<size_t>(row_count * 2))

        if isinstance(value_vector, Int64Vector):
            value_i64_vector = <Int64Vector>value_vector
            value_ptr = value_i64_vector.ptr
            value_i64_data = <int64_t*>value_ptr.data
            value_nulls = <uint8_t*>value_ptr.null_bitmap

            if key_nulls == NULL and value_nulls == NULL:
                for row_idx in range(row_count):
                    key_u64 = <uint64_t>key_data[row_idx]
                    self._rows[key_u64] += 1
                    self._sums[key_u64] += <double>value_i64_data[row_idx]
                    self._counts[key_u64] += 1
                return True

            for row_idx in range(row_count):
                if key_nulls != NULL and not ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                    self._seen_null_key = True
                    if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        self._null_key_sum += <double>value_i64_data[row_idx]
                        self._null_key_count += 1
                    continue

                key_u64 = <uint64_t>key_data[row_idx]
                self._rows[key_u64] += 1
                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                    self._sums[key_u64] += <double>value_i64_data[row_idx]
                    self._counts[key_u64] += 1
            return True

        if isinstance(value_vector, Float64Vector):
            value_f64_vector = <Float64Vector>value_vector
            value_ptr = value_f64_vector.ptr
            value_f64_data = <double*>value_ptr.data
            value_nulls = <uint8_t*>value_ptr.null_bitmap

            if key_nulls == NULL and value_nulls == NULL:
                for row_idx in range(row_count):
                    key_u64 = <uint64_t>key_data[row_idx]
                    self._rows[key_u64] += 1
                    self._sums[key_u64] += value_f64_data[row_idx]
                    self._counts[key_u64] += 1
                return True

            for row_idx in range(row_count):
                if key_nulls != NULL and not ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                    self._seen_null_key = True
                    if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        self._null_key_sum += value_f64_data[row_idx]
                        self._null_key_count += 1
                    continue

                key_u64 = <uint64_t>key_data[row_idx]
                self._rows[key_u64] += 1
                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                    value_f64 = value_f64_data[row_idx]
                    self._sums[key_u64] += value_f64
                    self._counts[key_u64] += 1
            return True

        return False

    cpdef list finalize_rows(self):
        cdef list rows
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef uint64_t key_u64
        cdef int64_t c
        cdef object finalized_value

        if self._rows.size() == 0 and not self._seen_null_key:
            return []

        rows = []
        rows_it = self._rows.begin()
        while rows_it != self._rows.end():
            key_u64 = dereference(rows_it).first
            c = self._counts[key_u64]
            finalized_value = None if c == 0 else (self._sums[key_u64] / c)
            rows.append(
                (
                    (<int64_t>key_u64,),
                    [finalized_value],
                )
            )
            preincrement(rows_it)
        if self._seen_null_key:
            finalized_value = None if self._null_key_count == 0 else (self._null_key_sum / self._null_key_count)
            rows.append(((None,), [finalized_value]))
        return rows

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef object keys
        cdef object averages
        cdef int64_t[::1] key_view
        cdef double[::1] avg_view
        cdef uint64_t key_u64
        cdef int64_t c

        if self._seen_null_key:
            return None

        if self._rows.size() != self._counts.size():
            return None

        n = <Py_ssize_t>self._rows.size()
        keys = array("q", [0]) * n
        averages = array("d", [0.0]) * n
        key_view = keys
        avg_view = averages

        rows_it = self._rows.begin()
        idx = 0
        while rows_it != self._rows.end():
            key_u64 = dereference(rows_it).first
            c = self._counts[key_u64]
            if c == 0:
                return None
            key_view[idx] = <int64_t>key_u64
            avg_view[idx] = self._sums[key_u64] / c
            idx += 1
            preincrement(rows_it)

        return keys, averages
