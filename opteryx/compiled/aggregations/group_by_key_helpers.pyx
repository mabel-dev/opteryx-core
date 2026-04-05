# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT8
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT16
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_STRING
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.integer_vector cimport IntegerVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.time_vector cimport TimeVector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.aggregations.key_codec cimport append_multi_key_record
from opteryx.compiled.aggregations.key_codec cimport append_single_encoded_key_record
from opteryx.compiled.aggregations.key_codec cimport append_single_fixed_key_record
from opteryx.compiled.aggregations.vector_readers cimport _dict_read_code
from opteryx.compiled.aggregations.vector_readers cimport _vector_dict_accessor
from opteryx.exceptions import UnsupportedSyntaxError


cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t index) noexcept:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline int64_t _read_integer_value(DrakenFixedBuffer* ptr, Py_ssize_t index) noexcept:
    if ptr.itemsize == 1:
        return (<char*> ptr.data)[index]
    if ptr.itemsize == 2:
        return (<short*> ptr.data)[index]
    if ptr.itemsize == 4:
        return (<int*> ptr.data)[index]
    return (<int64_t*> ptr.data)[index]


cdef inline int64_t _read_dictionary_fixed_key(
    object self,
    object key_vector,
    Py_ssize_t row_idx,
    int64_t* key_valid_flag,
) except *:
    cdef DictAccessor* dict_accessor = _vector_dict_accessor(key_vector)
    cdef DrakenVarBuffer* dict_values = NULL
    cdef uint8_t* nulls = NULL
    cdef uint32_t code = 0

    key_valid_flag[0] = 0
    if dict_accessor == NULL or dict_accessor.dict_values == NULL:
        return 0
    if not _bitmap_is_valid(dict_accessor.row_nulls, row_idx):
        return 0

    dict_values = dict_accessor.dict_values
    code = _dict_read_code(dict_accessor, row_idx)
    if code >= dict_values.length:
        raise IndexError("Dictionary code out of range")

    nulls = <uint8_t*>dict_values.null_bitmap
    if not _bitmap_is_valid(nulls, code):
        return 0

    key_valid_flag[0] = 1
    if dict_accessor.value_type == DRAKEN_INT8:
        return (<int8_t*>dict_values.data)[code]
    if dict_accessor.value_type == DRAKEN_INT16:
        return (<int16_t*>dict_values.data)[code]
    if (
        dict_accessor.value_type == DRAKEN_INT32
        or dict_accessor.value_type == DRAKEN_DATE32
        or dict_accessor.value_type == DRAKEN_TIME32
    ):
        return (<int32_t*>dict_values.data)[code]
    if (
        dict_accessor.value_type == DRAKEN_INT64
        or dict_accessor.value_type == DRAKEN_TIME64
        or dict_accessor.value_type == DRAKEN_TIMESTAMP64
    ):
        return (<int64_t*>dict_values.data)[code]
    if dict_accessor.value_type == DRAKEN_BOOL:
        return 1 if (<uint8_t*>dict_values.data)[code] != 0 else 0

    raise UnsupportedSyntaxError(
        "Carchar group-state engine only supports fixed-width and string dictionary keys."
    )


cdef inline void _append_single_encoded_key(
    object self,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *:
    cdef Py_ssize_t idx
    cdef int32_t next_offset = self._encoded_key_offsets[self._encoded_key_offsets.size() - 1]

    if valid_flag != 0 and data_len > 0:
        for idx in range(data_len):
            self._encoded_key_bytes.push_back(<uint8_t> data_ptr[idx])
        next_offset += <int32_t> data_len
    self._encoded_key_offsets.push_back(next_offset)
    self._encoded_key_valid.push_back(valid_flag)


cdef inline void _append_multi_encoded_key(
    object self,
    Py_ssize_t key_idx,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *:
    cdef Py_ssize_t idx
    cdef int32_t next_offset = self._multi_encoded_key_offsets[key_idx][
        self._multi_encoded_key_offsets[key_idx].size() - 1
    ]

    if valid_flag != 0 and data_len > 0:
        for idx in range(data_len):
            self._multi_encoded_key_bytes[key_idx].push_back(<uint8_t> data_ptr[idx])
        next_offset += <int32_t> data_len
    self._multi_encoded_key_offsets[key_idx].push_back(next_offset)
    self._multi_encoded_key_valid[key_idx].push_back(valid_flag)


cdef inline int64_t _extract_stringlike_key(
    object self,
    object key_vector,
    Py_ssize_t row_idx,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *:
    cdef DrakenVarBuffer* str_ptr
    cdef DictAccessor* dict_accessor = NULL
    cdef DrakenVarBuffer* dict_values
    cdef uint8_t* nulls
    cdef uint32_t code
    cdef int32_t start
    cdef int32_t stop

    if isinstance(key_vector, StringVector):
        str_ptr = (<StringVector> key_vector).ptr
        nulls = <uint8_t*> str_ptr.null_bitmap
        if not _bitmap_is_valid(nulls, row_idx):
            data_ptr[0] = NULL
            data_len[0] = 0
            return 0
        start = str_ptr.offsets[row_idx]
        stop = str_ptr.offsets[row_idx + 1]
        data_ptr[0] = <const char*> str_ptr.data + start
        data_len[0] = stop - start
        return 1

    dict_accessor = _vector_dict_accessor(key_vector)
    if dict_accessor != NULL:
        nulls = dict_accessor.row_nulls
        if not _bitmap_is_valid(nulls, row_idx):
            data_ptr[0] = NULL
            data_len[0] = 0
            return 0
        dict_values = dict_accessor.dict_values
        if dict_values == NULL or dict_accessor.value_type != DRAKEN_STRING:
            raise UnsupportedSyntaxError(
                "Carchar group-state engine only supports string dictionary keys on the native encoded-key path."
            )
        code = _dict_read_code(dict_accessor, row_idx)
        nulls = <uint8_t*> dict_values.null_bitmap
        if not _bitmap_is_valid(nulls, code):
            data_ptr[0] = NULL
            data_len[0] = 0
            return 0
        start = dict_values.offsets[code]
        stop = dict_values.offsets[code + 1]
        data_ptr[0] = <const char*> dict_values.data + start
        data_len[0] = stop - start
        return 1

    raise UnsupportedSyntaxError(
        "Carchar group-state engine only supports native encoded storage for string-like group keys."
    )


cdef inline void _append_single_payload_key(
    object self,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *:
    if not append_single_encoded_key_record(
        self._key_payload_bytes,
        self._key_payload_offsets,
        data_ptr,
        data_len,
        key_valid_flag,
    ):
        raise RuntimeError("failed to serialize encoded group key record")


cdef inline void _append_single_fixed_payload_key(
    object self,
    int64_t key_value,
    int64_t key_valid_flag,
) except *:
    if not append_single_fixed_key_record(
        self._key_payload_bytes,
        self._key_payload_offsets,
        key_value,
        key_valid_flag,
    ):
        raise RuntimeError("failed to serialize fixed group key record")


cdef inline void _append_multi_fixed_payload_key_from_vectors(
    object self,
    list key_vectors,
    Py_ssize_t row_idx,
) except *:
    cdef Py_ssize_t key_idx
    cdef object key_vector
    cdef DrakenFixedBuffer* key_ptr
    cdef uint8_t* key_null_bitmap
    cdef int64_t key_value
    cdef int64_t key_valid_flag
    cdef vector[int64_t] fixed_values
    cdef vector[int64_t] fixed_valids
    cdef vector[string] encoded_values
    cdef vector[int64_t] encoded_valids

    for key_idx in range(len(key_vectors)):
        key_vector = key_vectors[key_idx]

        if isinstance(key_vector, Int64Vector):
            key_ptr = (<Int64Vector> key_vector).ptr
            key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
        elif isinstance(key_vector, Date32Vector):
            key_ptr = (<Date32Vector> key_vector).ptr
            key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
        elif isinstance(key_vector, TimeVector):
            key_ptr = (<TimeVector> key_vector).ptr
            key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
        elif isinstance(key_vector, TimestampVector):
            key_ptr = (<TimestampVector> key_vector).ptr
            key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
        else:
            key_ptr = (<IntegerVector> key_vector).ptr
            key_null_bitmap = <uint8_t*> key_ptr.null_bitmap

        key_valid_flag = 1 if _bitmap_is_valid(key_null_bitmap, row_idx) else 0
        key_value = _read_integer_value(key_ptr, row_idx) if key_valid_flag != 0 else 0
        fixed_values.push_back(key_value)
        fixed_valids.push_back(key_valid_flag)

    if not append_multi_key_record(
        self._key_payload_bytes,
        self._key_payload_offsets,
        fixed_values,
        fixed_valids,
        encoded_values,
        encoded_valids,
    ):
        raise RuntimeError("failed to serialize multi fixed group key record")


cdef inline void _append_multi_payload_key(
    object self,
    list key_vectors,
    Py_ssize_t row_idx,
) except *:
    cdef Py_ssize_t key_idx
    cdef int64_t key_kind
    cdef int64_t key_valid_flag
    cdef int64_t key_value
    cdef const char* data_ptr = NULL
    cdef Py_ssize_t data_len = 0
    cdef object key_vector
    cdef vector[int64_t] fixed_values
    cdef vector[int64_t] fixed_valids
    cdef vector[string] encoded_values
    cdef vector[int64_t] encoded_valids
    cdef string encoded_value

    for key_idx in range(len(key_vectors)):
        key_vector = key_vectors[key_idx]
        key_kind = self._multi_group_key_kinds[key_idx]
        if self._is_multi_fixed_kind(key_kind):
            if _vector_dict_accessor(key_vector) != NULL:
                key_value = _read_dictionary_fixed_key(self, key_vector, row_idx, &key_valid_flag)
            elif isinstance(key_vector, Int64Vector):
                key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<Int64Vector> key_vector).ptr.null_bitmap, row_idx) else 0
                key_value = _read_integer_value((<Int64Vector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
            elif isinstance(key_vector, IntegerVector):
                key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<IntegerVector> key_vector).ptr.null_bitmap, row_idx) else 0
                key_value = _read_integer_value((<IntegerVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
            elif isinstance(key_vector, Date32Vector):
                key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<Date32Vector> key_vector).ptr.null_bitmap, row_idx) else 0
                key_value = _read_integer_value((<Date32Vector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
            elif isinstance(key_vector, TimeVector):
                key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<TimeVector> key_vector).ptr.null_bitmap, row_idx) else 0
                key_value = _read_integer_value((<TimeVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
            elif isinstance(key_vector, TimestampVector):
                key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<TimestampVector> key_vector).ptr.null_bitmap, row_idx) else 0
                key_value = _read_integer_value((<TimestampVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
            else:
                raise UnsupportedSyntaxError(
                    "Unsupported fixed-width group key vector in Carchar payload arena."
                )
            fixed_values.push_back(key_value)
            fixed_valids.push_back(key_valid_flag)
            continue

        key_valid_flag = _extract_stringlike_key(self, key_vector, row_idx, &data_ptr, &data_len)
        if key_valid_flag != 0:
            encoded_value.assign(data_ptr, <size_t> data_len)
        else:
            encoded_value.clear()
        encoded_values.push_back(encoded_value)
        encoded_valids.push_back(key_valid_flag)

    if not append_multi_key_record(
        self._key_payload_bytes,
        self._key_payload_offsets,
        fixed_values,
        fixed_valids,
        encoded_values,
        encoded_valids,
    ):
        raise RuntimeError("failed to serialize multi group key record")
