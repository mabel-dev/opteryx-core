# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False

"""Dictionary-key helpers for grouped aggregation ingest paths."""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT8
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT16
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_STRING
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.integer_vector cimport IntegerVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.vector cimport Vector


cdef int KEY_MULTI_ENCODED_STRING = 6


cdef inline uint32_t _dict_read_code(const DictAccessor* ptr, Py_ssize_t row_idx) noexcept nogil:
    if ptr.code_width == 1:
        return (<uint8_t*>ptr.codes)[row_idx]
    if ptr.code_width == 2:
        return (<uint16_t*>ptr.codes)[row_idx]
    return (<uint32_t*>ptr.codes)[row_idx]


cdef inline DictAccessor* _vector_dict_accessor(object vec) noexcept:
    if isinstance(vec, Int64Vector):
        return (<Int64Vector>vec).dict_accessor()
    if isinstance(vec, IntegerVector):
        return (<IntegerVector>vec).dict_accessor()
    if isinstance(vec, StringVector):
        return (<StringVector>vec).dict_accessor()
    if isinstance(vec, Float64Vector):
        return (<Float64Vector>vec).dict_accessor()
    return NULL


cdef inline DictAccessor* _vector_value_dict_accessor(object vec) noexcept:
    cdef DictAccessor* dict_accessor = _vector_dict_accessor(vec)
    if dict_accessor == NULL or dict_accessor.dict_values == NULL:
        return NULL
    return dict_accessor


cdef inline int64_t _dict_accessor_key_kind(const DictAccessor* dict_accessor) noexcept:
    if dict_accessor == NULL:
        return KEY_MULTI_ENCODED_STRING
    if dict_accessor.value_type == DRAKEN_STRING:
        return KEY_MULTI_ENCODED_STRING
    if dict_accessor.value_type == DRAKEN_DATE32:
        return 2
    if dict_accessor.value_type == DRAKEN_TIME32:
        return 3
    if dict_accessor.value_type == DRAKEN_TIME64:
        return 4
    if dict_accessor.value_type == DRAKEN_TIMESTAMP64:
        return 5
    return 1


cdef inline int _dict_accessor_value_kind(const DictAccessor* dict_accessor) noexcept:
    if dict_accessor == NULL:
        return 0
    if dict_accessor.value_type == DRAKEN_STRING:
        return 3
    if dict_accessor.value_type == DRAKEN_FLOAT32 or dict_accessor.value_type == DRAKEN_FLOAT64:
        return 2
    return 1


cdef inline double _dict_accessor_read_float_value(
    const DictAccessor* dict_accessor,
    Py_ssize_t row_idx,
) except *:
    cdef uint32_t code

    code = _dict_read_code(dict_accessor, row_idx)
    if code >= dict_accessor.dict_values.length:
        raise IndexError("Dictionary code out of range")
    if dict_accessor.value_type == DRAKEN_FLOAT32:
        return (<float*>dict_accessor.dict_values.data)[code]
    return (<double*>dict_accessor.dict_values.data)[code]


cdef inline int64_t _dict_accessor_read_int_value(
    const DictAccessor* dict_accessor,
    Py_ssize_t row_idx,
) except *:
    cdef uint32_t code

    code = _dict_read_code(dict_accessor, row_idx)
    if code >= dict_accessor.dict_values.length:
        raise IndexError("Dictionary code out of range")
    if dict_accessor.value_type == DRAKEN_INT8:
        return (<int8_t*>dict_accessor.dict_values.data)[code]
    if dict_accessor.value_type == DRAKEN_INT16:
        return (<int16_t*>dict_accessor.dict_values.data)[code]
    if (
        dict_accessor.value_type == DRAKEN_INT32
        or dict_accessor.value_type == DRAKEN_DATE32
        or dict_accessor.value_type == DRAKEN_TIME32
    ):
        return (<int32_t*>dict_accessor.dict_values.data)[code]
    if (
        dict_accessor.value_type == DRAKEN_INT64
        or dict_accessor.value_type == DRAKEN_TIME64
        or dict_accessor.value_type == DRAKEN_TIMESTAMP64
    ):
        return (<int64_t*>dict_accessor.dict_values.data)[code]
    if dict_accessor.value_type == DRAKEN_BOOL:
        return 1 if (<uint8_t*>dict_accessor.dict_values.data)[code] != 0 else 0
    raise UnsupportedSyntaxError(
        "Carchar group-state engine only supports fixed-width and string dictionary values."
    )


cdef inline int64_t _read_dictionary_fixed_key(
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
    if dict_accessor.row_nulls != NULL and ((dict_accessor.row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) == 0:
        return 0

    dict_values = dict_accessor.dict_values
    code = _dict_read_code(dict_accessor, row_idx)
    if code >= dict_values.length:
        raise IndexError("Dictionary code out of range")

    nulls = <uint8_t*>dict_values.null_bitmap
    if nulls != NULL and ((nulls[code >> 3] >> (code & 7)) & 1) == 0:
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


cdef inline int64_t _extract_stringlike_key(
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
        if nulls != NULL and ((nulls[row_idx >> 3] >> (row_idx & 7)) & 1) == 0:
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
        if nulls != NULL and ((nulls[row_idx >> 3] >> (row_idx & 7)) & 1) == 0:
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
        if nulls != NULL and ((nulls[code >> 3] >> (code & 7)) & 1) == 0:
            data_ptr[0] = NULL
            data_len[0] = 0
            return 0
        start = dict_values.offsets[code]
        stop = dict_values.offsets[code + 1]
        data_ptr[0] = <const char*> dict_values.data + start
        data_len[0] = stop - start
        return 1

    raise UnsupportedSyntaxError(
        "Carchar group-state engine only supports string keys on the native encoded-key path."
    )
