cdef class Int64CountDistinctInt64Kernel:
    cdef object _key_column
    cdef object _value_column
    cdef bint _hash_keys_mode
    cdef bint _seen_null_key
    cdef int64_t _null_key_count
    cdef flat_hash_map[uint64_t, int64_t] _counts
    cdef flat_hash_map[uint64_t, flat_hash_set[uint64_t, IdentityHash]] _seen
    cdef flat_hash_set[uint64_t, IdentityHash] _null_key_seen
    cdef dict _hash_keys

    def __cinit__(self, object key_column, object value_column):
        self._key_column = key_column
        self._value_column = value_column
        self._hash_keys_mode = False
        self._seen_null_key = False
        self._null_key_count = 0
        self._hash_keys = {}

    cpdef bint ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef object key_vector
        cdef object value_vector
        cdef bint key_is_dict = False
        cdef bint value_is_dict = False
        cdef Int64Vector key_int64_vector
        cdef Int64Vector value_i64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef DrakenFixedBuffer* value_ptr
        cdef DictAccessor* key_dict_ptr
        cdef DictAccessor* value_dict_ptr
        cdef DrakenVarBuffer* value_dict_values
        cdef int64_t* key_data
        cdef int64_t* value_i64_data
        cdef uint8_t* key_nulls
        cdef uint8_t* value_nulls
        cdef uint8_t* value_dict_nulls
        cdef uint64_t* key_hashes = NULL
        cdef uint64_t* value_hashes = NULL
        cdef uint64_t key_u64
        cdef uint64_t distinct_value_u64
        cdef uint64_t local_code_u64
        cdef uint32_t key_code
        cdef uint32_t value_code
        cdef flat_hash_map[uint64_t, flat_hash_set[uint64_t, IdentityHash]] morsel_local_codes
        cdef flat_hash_set[uint64_t, IdentityHash] morsel_null_local_codes
        cdef flat_hash_map[uint64_t, flat_hash_set[uint64_t, IdentityHash]].iterator local_codes_it
        cdef flat_hash_set[uint64_t, IdentityHash].iterator local_code_it
        cdef object key_obj
        key_vector = morsel.column(self._key_column)
        value_vector = morsel.column(self._value_column)
        row_count = morsel.num_rows

        if isinstance(key_vector, Int64Vector):
            key_is_dict = False
            self._hash_keys_mode = False
            key_int64_vector = <Int64Vector>key_vector
            key_ptr = key_int64_vector.ptr
            key_data = <int64_t*>key_ptr.data
            key_nulls = <uint8_t*>key_ptr.null_bitmap
        elif isinstance(key_vector, Vector):
            key_dict_ptr = (<Vector>key_vector).dict_accessor()
            if key_dict_ptr == NULL:
                return False
            key_is_dict = True
            self._hash_keys_mode = True
            if key_dict_ptr.dict_values == NULL:
                return False
            key_nulls = <uint8_t*>key_dict_ptr.row_nulls
            key_hashes = _build_dict_hashes(key_dict_ptr)
        else:
            return False

        if isinstance(value_vector, Int64Vector):
            value_is_dict = False
            value_i64_vector = <Int64Vector>value_vector
            value_ptr = value_i64_vector.ptr
            value_i64_data = <int64_t*>value_ptr.data
            value_nulls = <uint8_t*>value_ptr.null_bitmap
        elif isinstance(value_vector, Vector):
            value_dict_ptr = (<Vector>value_vector).dict_accessor()
            if value_dict_ptr == NULL:
                if key_hashes != NULL:
                    free(key_hashes)
                return False
            value_is_dict = True
            if value_dict_ptr.dict_values == NULL:
                if key_hashes != NULL:
                    free(key_hashes)
                return False
            value_nulls = <uint8_t*>value_dict_ptr.row_nulls
            value_dict_values = value_dict_ptr.dict_values
            value_dict_nulls = <uint8_t*>value_dict_values.null_bitmap
            value_hashes = _build_dict_hashes(value_dict_ptr)
        else:
            if key_hashes != NULL:
                free(key_hashes)
            return False

        try:
            if self._counts.size() == 0 and row_count > 0:
                self._counts.reserve(<size_t>(row_count * 2))
            if self._seen.size() == 0 and row_count > 0:
                self._seen.reserve(<size_t>(row_count * 2))

            # Fast path for int64/distinct-int64 without dictionary values.
            if not key_is_dict and not value_is_dict:
                if key_nulls == NULL and value_nulls == NULL:
                    for row_idx in range(row_count):
                        key_u64 = <uint64_t>key_data[row_idx]
                        distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                        if not self._seen[key_u64].contains(distinct_value_u64):
                            self._seen[key_u64].insert(distinct_value_u64)
                            self._counts[key_u64] += 1
                    return True

                for row_idx in range(row_count):
                    if value_nulls != NULL and not ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        continue
                    distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                    if key_nulls == NULL or ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        key_u64 = <uint64_t>key_data[row_idx]
                        if not self._seen[key_u64].contains(distinct_value_u64):
                            self._seen[key_u64].insert(distinct_value_u64)
                            self._counts[key_u64] += 1
                    else:
                        self._seen_null_key = True
                        if not self._null_key_seen.contains(distinct_value_u64):
                            self._null_key_seen.insert(distinct_value_u64)
                            self._null_key_count += 1
                return True

            # Path with dictionary-valued DISTINCT input:
            # 1) Accumulate local dictionary codes per group key for this morsel.
            # 2) Expand local code sets to pre-hashed dictionary values once per
            #    key at morsel boundary and merge into global distinct state.
            if value_is_dict:
                if row_count > 0:
                    morsel_local_codes.reserve(<size_t>(row_count * 2))
                    morsel_null_local_codes.reserve(<int64_t>row_count)

                for row_idx in range(row_count):
                    if value_nulls != NULL and not ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        continue
                    value_code = _dict_read_code(value_dict_ptr, row_idx)
                    if value_code >= value_dict_values.length:
                        raise ValueError("Dictionary DISTINCT value code out of bounds")
                    if value_dict_nulls != NULL and ((value_dict_nulls[value_code >> 3] >> (value_code & 7)) & 1) == 0:
                        continue

                    if key_is_dict:
                        key_code = _dict_read_code(key_dict_ptr, row_idx)
                        if key_code >= key_dict_ptr.dict_values.length:
                            raise ValueError("Dictionary key code out of bounds in COUNT(DISTINCT) kernel")
                        if _dict_row_null(key_dict_ptr, key_code, row_idx):
                            self._seen_null_key = True
                            morsel_null_local_codes.insert(<uint64_t>value_code)
                        else:
                            key_u64 = key_hashes[key_code]
                            morsel_local_codes[key_u64].insert(<uint64_t>value_code)
                            key_obj = self._hash_keys.get(key_u64, _KERNEL_MISSING)
                            if key_obj is _KERNEL_MISSING:
                                self._hash_keys[key_u64] = _dict_code_to_object(key_dict_ptr, key_code)
                    else:
                        if key_nulls != NULL and ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) == 0:
                            self._seen_null_key = True
                            morsel_null_local_codes.insert(<uint64_t>value_code)
                        else:
                            key_u64 = <uint64_t>key_data[row_idx]
                            morsel_local_codes[key_u64].insert(<uint64_t>value_code)

                local_codes_it = morsel_local_codes.begin()
                while local_codes_it != morsel_local_codes.end():
                    key_u64 = dereference(local_codes_it).first
                    local_code_it = dereference(local_codes_it).second.begin()
                    while local_code_it != dereference(local_codes_it).second.end():
                        local_code_u64 = dereference(local_code_it)
                        distinct_value_u64 = value_hashes[<uint32_t>local_code_u64]
                        if not self._seen[key_u64].contains(distinct_value_u64):
                            self._seen[key_u64].insert(distinct_value_u64)
                            self._counts[key_u64] += 1
                        preincrement(local_code_it)
                    preincrement(local_codes_it)

                local_code_it = morsel_null_local_codes.begin()
                while local_code_it != morsel_null_local_codes.end():
                    local_code_u64 = dereference(local_code_it)
                    distinct_value_u64 = value_hashes[<uint32_t>local_code_u64]
                    if not self._null_key_seen.contains(distinct_value_u64):
                        self._null_key_seen.insert(distinct_value_u64)
                        self._null_key_count += 1
                    preincrement(local_code_it)

                return True

            # Mixed path: dictionary key + int64 distinct value.
            for row_idx in range(row_count):
                if value_nulls != NULL and not ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                    continue
                distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                if key_is_dict:
                    key_code = _dict_read_code(key_dict_ptr, row_idx)
                    if key_code >= key_dict_ptr.dict_values.length:
                        raise ValueError("Dictionary key code out of bounds in COUNT(DISTINCT) kernel")
                    if _dict_row_null(key_dict_ptr, key_code, row_idx):
                        self._seen_null_key = True
                        if not self._null_key_seen.contains(distinct_value_u64):
                            self._null_key_seen.insert(distinct_value_u64)
                            self._null_key_count += 1
                    else:
                        key_u64 = key_hashes[key_code]
                        if not self._seen[key_u64].contains(distinct_value_u64):
                            self._seen[key_u64].insert(distinct_value_u64)
                            self._counts[key_u64] += 1
                        key_obj = self._hash_keys.get(key_u64, _KERNEL_MISSING)
                        if key_obj is _KERNEL_MISSING:
                            self._hash_keys[key_u64] = _dict_code_to_object(key_dict_ptr, key_code)
                else:
                    if key_nulls != NULL and not ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                        self._seen_null_key = True
                        if not self._null_key_seen.contains(distinct_value_u64):
                            self._null_key_seen.insert(distinct_value_u64)
                            self._null_key_count += 1
                    else:
                        key_u64 = <uint64_t>key_data[row_idx]
                        if not self._seen[key_u64].contains(distinct_value_u64):
                            self._seen[key_u64].insert(distinct_value_u64)
                            self._counts[key_u64] += 1
            return True
        finally:
            if key_hashes != NULL:
                free(key_hashes)
            if value_hashes != NULL:
                free(value_hashes)

    cpdef list finalize_rows(self):
        cdef list rows
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef uint64_t key_u64
        cdef object key_obj

        if self._counts.size() == 0 and not self._seen_null_key:
            return []

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

        if self._seen_null_key or self._hash_keys_mode:
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
