# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.math cimport isinf
from libc.math cimport isnan
from libc.stdio cimport snprintf
from libc.stdint cimport int8_t
from libc.stdint cimport int16_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.stdlib cimport realloc
from libc.string cimport memcpy

from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.buffers cimport DRAKEN_INT8
from opteryx.draken.core.buffers cimport DRAKEN_INT16
from opteryx.draken.core.buffers cimport DRAKEN_INT32
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.constant_vector cimport ConstantVector
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.string_vector cimport StringVectorBuilder
from opteryx.draken.vectors.string_vector cimport _StringVectorView


cdef enum EncoderKind:
    ENC_INT64 = 1
    ENC_INTEGER = 2
    ENC_FLOAT64 = 3
    ENC_BOOL = 4
    ENC_STRING = 5
    ENC_RAW_STRING = 6
    ENC_CONST_INT = 7
    ENC_CONST_FLOAT = 8
    ENC_CONST_BOOL = 9
    ENC_CONST_STRING = 10
    ENC_CONST_RAW_STRING = 11
    ENC_GENERIC = 12


cdef bytes _LIT_NULL = b"null"
cdef bytes _LIT_TRUE = b"true"
cdef bytes _LIT_FALSE = b"false"
cdef inline bint _is_valid(const uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline bint _bool_at(const uint8_t* bits, Py_ssize_t index) noexcept nogil:
    return ((bits[index >> 3] >> (index & 7)) & 1) != 0


cdef inline int64_t _read_integer_value(IntegerVector vec, Py_ssize_t index) noexcept nogil:
    cdef DrakenFixedBuffer* ptr = vec.ptr
    if ptr.type == DRAKEN_INT8:
        return (<int8_t*>ptr.data)[index]
    if ptr.type == DRAKEN_INT16:
        return (<int16_t*>ptr.data)[index]
    if ptr.type == DRAKEN_INT32:
        return (<int32_t*>ptr.data)[index]
    if ptr.type == DRAKEN_INT64:
        return (<int64_t*>ptr.data)[index]
    return 0


cdef inline int _int64_to_ascii(int64_t value, char* buf) noexcept nogil:
    cdef unsigned long long uval
    cdef int i = 20
    cdef bint neg = value < 0

    if value == 0:
        buf[19] = 48
        return 1

    uval = <unsigned long long>(-value) if neg else <unsigned long long>value
    while uval != 0:
        i -= 1
        buf[i] = <char>(48 + (uval % 10))
        uval //= 10

    if neg:
        i -= 1
        buf[i] = 45

    return 20 - i


cdef inline Py_ssize_t _measure_int64(int64_t value) noexcept nogil:
    cdef char buf[21]
    return _int64_to_ascii(value, buf)


cdef inline Py_ssize_t _write_int64(char* dst, int64_t value) noexcept nogil:
    cdef char buf[21]
    cdef int length = _int64_to_ascii(value, buf)
    memcpy(dst, buf + (20 - length), length)
    return length


cdef inline Py_ssize_t _measure_float64(double value) noexcept nogil:
    cdef char buf[32]
    if isnan(value) or isinf(value):
        return 4
    return snprintf(buf, 32, "%.17g", value)


cdef inline Py_ssize_t _write_float64(char* dst, double value) noexcept nogil:
    if isnan(value) or isinf(value):
        memcpy(dst, b"null", 4)
        return 4
    return snprintf(dst, 32, "%.17g", value)


cdef inline Py_ssize_t _measure_escaped_content(const char* src, Py_ssize_t length) noexcept nogil:
    cdef Py_ssize_t i
    cdef unsigned char ch
    cdef Py_ssize_t total = 0

    for i in range(length):
        ch = <unsigned char>src[i]
        if ch == 34 or ch == 92:
            total += 2
        elif ch == 8 or ch == 9 or ch == 10 or ch == 12 or ch == 13:
            total += 2
        elif ch < 32:
            total += 6
        else:
            total += 1

    return total


cdef inline Py_ssize_t _measure_json_string(const char* src, Py_ssize_t length) noexcept nogil:
    return _measure_escaped_content(src, length) + 2


cdef inline Py_ssize_t _write_json_string(char* dst, const char* src, Py_ssize_t length) noexcept nogil:
    cdef Py_ssize_t i
    cdef Py_ssize_t pos = 0
    cdef unsigned char ch
    cdef const char* hexchars = "0123456789abcdef"

    dst[pos] = 34
    pos += 1

    for i in range(length):
        ch = <unsigned char>src[i]
        if ch == 34:
            dst[pos] = 92
            dst[pos + 1] = 34
            pos += 2
        elif ch == 92:
            dst[pos] = 92
            dst[pos + 1] = 92
            pos += 2
        elif ch == 8:
            dst[pos] = 92
            dst[pos + 1] = 98
            pos += 2
        elif ch == 9:
            dst[pos] = 92
            dst[pos + 1] = 116
            pos += 2
        elif ch == 10:
            dst[pos] = 92
            dst[pos + 1] = 110
            pos += 2
        elif ch == 12:
            dst[pos] = 92
            dst[pos + 1] = 102
            pos += 2
        elif ch == 13:
            dst[pos] = 92
            dst[pos + 1] = 114
            pos += 2
        elif ch < 32:
            dst[pos] = 92
            dst[pos + 1] = 117
            dst[pos + 2] = 48
            dst[pos + 3] = 48
            dst[pos + 4] = hexchars[ch >> 4]
            dst[pos + 5] = hexchars[ch & 15]
            pos += 6
        else:
            dst[pos] = <char>ch
            pos += 1

    dst[pos] = 34
    pos += 1
    return pos


cdef bytes _build_key_prefix(bytes name, bint include_leading_comma):
    cdef char* name_ptr = NULL
    cdef Py_ssize_t name_len = 0
    cdef Py_ssize_t total
    cdef bytes out
    cdef char* out_ptr
    cdef Py_ssize_t pos = 0

    PyBytes_AsStringAndSize(name, &name_ptr, &name_len)
    total = _measure_json_string(name_ptr, name_len) + 1 + (1 if include_leading_comma else 0)
    out = PyBytes_FromStringAndSize(NULL, total)
    out_ptr = PyBytes_AS_STRING(out)

    if include_leading_comma:
        out_ptr[0] = 44
        pos = 1

    pos += _write_json_string(out_ptr + pos, name_ptr, name_len)
    out_ptr[pos] = 58
    return out


cdef void _ensure_scratch_capacity(char** scratch, Py_ssize_t* capacity, Py_ssize_t needed) except *:
    cdef char* resized
    cdef Py_ssize_t new_capacity

    if needed <= capacity[0]:
        return

    new_capacity = capacity[0] if capacity[0] > 0 else 64
    while new_capacity < needed:
        new_capacity <<= 1

    resized = <char*>realloc(scratch[0], new_capacity)
    if resized == NULL:
        raise MemoryError()
    scratch[0] = resized
    capacity[0] = new_capacity


cdef bytes _normalize_name(object name):
    if isinstance(name, bytes):
        return name
    if isinstance(name, str):
        return name.encode("utf-8")
    raise TypeError("column names must be str or bytes")


cdef list _normalize_columns(Morsel morsel, object columns):
    cdef list normalized
    if columns is None:
        return list(morsel.column_names)
    normalized = []
    for col in columns:
        normalized.append(_normalize_name(col))
    return normalized


cdef set _normalize_raw_columns(object raw_json_columns):
    cdef set normalized = set()
    if raw_json_columns is None:
        return normalized
    for col in raw_json_columns:
        normalized.add(_normalize_name(col))
    return normalized


cdef int _constant_encoder(ConstantVector vec, bint raw_json):
    if vec.ptr.value_type == DRAKEN_INT64:
        return ENC_CONST_INT
    if vec.ptr.value_type == DRAKEN_FLOAT64:
        return ENC_CONST_FLOAT
    if vec.ptr.value_type == DRAKEN_BOOL:
        return ENC_CONST_BOOL
    if vec.ptr.value_type == DRAKEN_STRING:
        return ENC_CONST_RAW_STRING if raw_json else ENC_CONST_STRING
    return 0


cdef Py_ssize_t _estimate_value_bytes(
    int encoder,
    object vec_obj,
    object aux_obj,
    Py_ssize_t num_rows,
) except -1:
    cdef StringVector string_vec
    cdef ConstantVector const_vec
    cdef DrakenConstantStringPayload* payload
    cdef Py_ssize_t total_bytes

    if encoder == ENC_INT64 or encoder == ENC_INTEGER or encoder == ENC_CONST_INT:
        return 20
    if encoder == ENC_FLOAT64 or encoder == ENC_CONST_FLOAT:
        return 24
    if encoder == ENC_BOOL or encoder == ENC_CONST_BOOL:
        return 5
    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
        string_vec = <StringVector>vec_obj
        if num_rows <= 0:
            return 8
        total_bytes = string_vec.ptr.offsets[string_vec.ptr.length]
        total_bytes = total_bytes // num_rows if num_rows > 0 else 0
        if encoder == ENC_RAW_STRING:
            return total_bytes if total_bytes > 0 else 8
        return total_bytes + 6
    if encoder == ENC_CONST_STRING or encoder == ENC_CONST_RAW_STRING:
        const_vec = <ConstantVector>vec_obj
        payload = <DrakenConstantStringPayload*>const_vec.ptr.value
        if encoder == ENC_CONST_RAW_STRING:
            return payload.length if payload.length > 0 else 8
        return payload.length + 6
    if encoder == ENC_GENERIC:
        return 24
    raise NotImplementedError("unsupported encoder")


cdef Py_ssize_t _measure_value(
    int encoder,
    object vec_obj,
    Py_ssize_t row_index,
) except -1:
    cdef Int64Vector int64_vec
    cdef IntegerVector integer_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec
    cdef StringVector string_vec
    cdef _StringVectorView string_view
    cdef ConstantVector const_vec
    cdef const char* ptr
    cdef Py_ssize_t length
    cdef DrakenConstantStringPayload* payload

    if encoder == ENC_INT64:
        int64_vec = <Int64Vector>vec_obj
        return _measure_int64((<int64_t*>int64_vec.ptr.data)[row_index])

    if encoder == ENC_INTEGER:
        integer_vec = <IntegerVector>vec_obj
        return _measure_int64(_read_integer_value(integer_vec, row_index))

    if encoder == ENC_FLOAT64:
        float_vec = <Float64Vector>vec_obj
        return _measure_float64((<double*>float_vec.ptr.data)[row_index])

    if encoder == ENC_BOOL:
        bool_vec = <BoolVector>vec_obj
        return 4 if _bool_at(<uint8_t*>bool_vec.ptr.data, row_index) else 5

    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
        string_vec = <StringVector>vec_obj
        string_view = <_StringVectorView>string_vec.view()
        ptr = <const char*>string_view.value_ptr(row_index)
        length = string_view.value_len(row_index)
        if encoder == ENC_RAW_STRING:
            return length
        return _measure_json_string(ptr, length)

    if encoder == ENC_CONST_INT:
        const_vec = <ConstantVector>vec_obj
        return _measure_int64((<int64_t*>const_vec.ptr.value)[0])

    if encoder == ENC_CONST_FLOAT:
        const_vec = <ConstantVector>vec_obj
        return _measure_float64((<double*>const_vec.ptr.value)[0])

    if encoder == ENC_CONST_BOOL:
        const_vec = <ConstantVector>vec_obj
        return 4 if (<uint8_t*>const_vec.ptr.value)[0] else 5

    if encoder == ENC_CONST_STRING or encoder == ENC_CONST_RAW_STRING:
        const_vec = <ConstantVector>vec_obj
        payload = <DrakenConstantStringPayload*>const_vec.ptr.value
        ptr = <const char*>payload.data
        length = payload.length
        if encoder == ENC_CONST_RAW_STRING:
            return length
        return _measure_json_string(ptr, length)
    if encoder == ENC_GENERIC:
        value = vec_obj[row_index]
        if value is None:
            return 4
        if isinstance(value, bytes):
            generic_bytes = value
            return _measure_json_string(PyBytes_AS_STRING(generic_bytes), len(generic_bytes))
        if isinstance(value, str):
            generic_bytes = value.encode("utf8")
            return _measure_json_string(PyBytes_AS_STRING(generic_bytes), len(generic_bytes))
        if isinstance(value, bool):
            return 4 if value else 5
        return len(str(value).encode("utf8"))

    raise NotImplementedError("unsupported encoder")


cdef Py_ssize_t _write_value(
    int encoder,
    object vec_obj,
    object aux_obj,
    Py_ssize_t row_index,
    char* dst,
) except -1:
    cdef Int64Vector int64_vec
    cdef IntegerVector integer_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec
    cdef _StringVectorView string_view
    cdef ConstantVector const_vec
    cdef const char* ptr
    cdef Py_ssize_t length
    cdef DrakenConstantStringPayload* payload
    cdef object generic_value
    cdef bytes generic_bytes

    if encoder == ENC_INT64:
        int64_vec = <Int64Vector>vec_obj
        return _write_int64(dst, (<int64_t*>int64_vec.ptr.data)[row_index])

    if encoder == ENC_INTEGER:
        integer_vec = <IntegerVector>vec_obj
        return _write_int64(dst, _read_integer_value(integer_vec, row_index))

    if encoder == ENC_FLOAT64:
        float_vec = <Float64Vector>vec_obj
        return _write_float64(dst, (<double*>float_vec.ptr.data)[row_index])

    if encoder == ENC_BOOL:
        bool_vec = <BoolVector>vec_obj
        if _bool_at(<uint8_t*>bool_vec.ptr.data, row_index):
            memcpy(dst, b"true", 4)
            return 4
        memcpy(dst, b"false", 5)
        return 5

    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
        string_view = <_StringVectorView>aux_obj
        ptr = <const char*>string_view.value_ptr(row_index)
        length = string_view.value_len(row_index)
        if encoder == ENC_RAW_STRING:
            memcpy(dst, ptr, length)
            return length
        return _write_json_string(dst, ptr, length)

    if encoder == ENC_CONST_INT:
        const_vec = <ConstantVector>vec_obj
        return _write_int64(dst, (<int64_t*>const_vec.ptr.value)[0])

    if encoder == ENC_CONST_FLOAT:
        const_vec = <ConstantVector>vec_obj
        return _write_float64(dst, (<double*>const_vec.ptr.value)[0])

    if encoder == ENC_CONST_BOOL:
        const_vec = <ConstantVector>vec_obj
        if (<uint8_t*>const_vec.ptr.value)[0]:
            memcpy(dst, b"true", 4)
            return 4
        memcpy(dst, b"false", 5)
        return 5

    if encoder == ENC_CONST_STRING or encoder == ENC_CONST_RAW_STRING:
        const_vec = <ConstantVector>vec_obj
        payload = <DrakenConstantStringPayload*>const_vec.ptr.value
        ptr = <const char*>payload.data
        length = payload.length
        if encoder == ENC_CONST_RAW_STRING:
            memcpy(dst, ptr, length)
            return length
        return _write_json_string(dst, ptr, length)
    if encoder == ENC_GENERIC:
        generic_value = aux_obj[row_index]
        if generic_value is None:
            memcpy(dst, b"null", 4)
            return 4
        if isinstance(generic_value, bytes):
            generic_bytes = generic_value
            return _write_json_string(dst, PyBytes_AS_STRING(generic_bytes), len(generic_bytes))
        if isinstance(generic_value, str):
            generic_bytes = generic_value.encode("utf8")
            return _write_json_string(dst, PyBytes_AS_STRING(generic_bytes), len(generic_bytes))
        if isinstance(generic_value, bool):
            if generic_value:
                memcpy(dst, b"true", 4)
                return 4
            memcpy(dst, b"false", 5)
            return 5
        generic_bytes = str(generic_value).encode("utf8")
        memcpy(dst, PyBytes_AS_STRING(generic_bytes), len(generic_bytes))
        return len(generic_bytes)

    raise NotImplementedError("unsupported encoder")


cdef bint _value_is_null(int encoder, object vec_obj, Py_ssize_t row_index) except? False:
    cdef Int64Vector int64_vec
    cdef IntegerVector integer_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec
    cdef StringVector string_vec
    cdef _StringVectorView string_view
    cdef ConstantVector const_vec

    if encoder == ENC_INT64:
        int64_vec = <Int64Vector>vec_obj
        return not _is_valid(int64_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_INTEGER:
        integer_vec = <IntegerVector>vec_obj
        return not _is_valid(integer_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_FLOAT64:
        float_vec = <Float64Vector>vec_obj
        return not _is_valid(float_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_BOOL:
        bool_vec = <BoolVector>vec_obj
        return not _is_valid(bool_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
        string_vec = <StringVector>vec_obj
        string_view = <_StringVectorView>string_vec.view()
        return string_view.is_null(row_index)

    if (
        encoder == ENC_CONST_INT
        or encoder == ENC_CONST_FLOAT
        or encoder == ENC_CONST_BOOL
        or encoder == ENC_CONST_STRING
        or encoder == ENC_CONST_RAW_STRING
    ):
        const_vec = <ConstantVector>vec_obj
        return not _is_valid(const_vec.ptr.null_bitmap, row_index)

    raise NotImplementedError("unsupported encoder")


cdef bint _value_is_null_cached(int encoder, object vec_obj, object aux_obj, Py_ssize_t row_index) except? False:
    cdef Int64Vector int64_vec
    cdef IntegerVector integer_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec
    cdef ConstantVector const_vec

    if encoder == ENC_INT64:
        int64_vec = <Int64Vector>vec_obj
        return not _is_valid(int64_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_INTEGER:
        integer_vec = <IntegerVector>vec_obj
        return not _is_valid(integer_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_FLOAT64:
        float_vec = <Float64Vector>vec_obj
        return not _is_valid(float_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_BOOL:
        bool_vec = <BoolVector>vec_obj
        return not _is_valid(bool_vec.ptr.null_bitmap, row_index)

    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
        return (<_StringVectorView>aux_obj).is_null(row_index)

    if (
        encoder == ENC_CONST_INT
        or encoder == ENC_CONST_FLOAT
        or encoder == ENC_CONST_BOOL
        or encoder == ENC_CONST_STRING
        or encoder == ENC_CONST_RAW_STRING
    ):
        const_vec = <ConstantVector>vec_obj
        return not _is_valid(const_vec.ptr.null_bitmap, row_index)
    if encoder == ENC_GENERIC:
        return aux_obj[row_index] is None

    raise NotImplementedError("unsupported encoder")


cpdef StringVector morsel_to_json_rows(
    Morsel morsel,
    object columns=None,
    object raw_json_columns=None,
    bint omit_null_fields=False,
):
    cdef list selected_columns = _normalize_columns(morsel, columns)
    cdef set raw_columns = _normalize_raw_columns(raw_json_columns)
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t num_cols = len(selected_columns)
    cdef list vectors = []
    cdef list aux_objects = []
    cdef list key_first_objs = []
    cdef list key_next_objs = []
    cdef int* encoders = NULL
    cdef const char** key_first_ptrs = NULL
    cdef const char** key_next_ptrs = NULL
    cdef Py_ssize_t* key_first_lens = NULL
    cdef Py_ssize_t* key_next_lens = NULL
    cdef char* scratch = NULL
    cdef Py_ssize_t scratch_capacity = 0
    cdef Py_ssize_t estimated_row_bytes = 2
    cdef Py_ssize_t row_index
    cdef Py_ssize_t col_index
    cdef Py_ssize_t pos
    cdef Py_ssize_t field_count
    cdef object vec_obj
    cdef object aux_obj
    cdef bytes col_name
    cdef bint raw_json
    cdef int encoder
    cdef StringVectorBuilder builder
    cdef Py_ssize_t reserve_needed
    cdef _StringVectorView string_view

    try:
        encoders = <int*>malloc(sizeof(int) * max(num_cols, 1))
        key_first_ptrs = <const char**>malloc(sizeof(const char*) * max(num_cols, 1))
        key_next_ptrs = <const char**>malloc(sizeof(const char*) * max(num_cols, 1))
        key_first_lens = <Py_ssize_t*>malloc(sizeof(Py_ssize_t) * max(num_cols, 1))
        key_next_lens = <Py_ssize_t*>malloc(sizeof(Py_ssize_t) * max(num_cols, 1))
        if (
            encoders == NULL
            or key_first_ptrs == NULL
            or key_next_ptrs == NULL
            or key_first_lens == NULL
            or key_next_lens == NULL
        ):
            raise MemoryError()

        for col_index in range(num_cols):
            col_name = <bytes>selected_columns[col_index]
            vec_obj = morsel.column(col_name)
            raw_json = col_name in raw_columns

            if isinstance(vec_obj, Int64Vector):
                encoder = ENC_INT64
            elif isinstance(vec_obj, IntegerVector):
                encoder = ENC_INTEGER
            elif isinstance(vec_obj, Float64Vector):
                encoder = ENC_FLOAT64
            elif isinstance(vec_obj, BoolVector):
                encoder = ENC_BOOL
            elif isinstance(vec_obj, StringVector):
                encoder = ENC_RAW_STRING if raw_json else ENC_STRING
            elif isinstance(vec_obj, ConstantVector):
                encoder = _constant_encoder(<ConstantVector>vec_obj, raw_json)
                if encoder == 0:
                    raise NotImplementedError(
                        f"json serialization does not support ConstantVector value type for column {col_name!r}"
                    )
            elif isinstance(vec_obj, DictionaryVector):
                encoder = ENC_GENERIC
            else:
                raise NotImplementedError(
                    f"json serialization does not support vector type {type(vec_obj).__name__} for column {col_name!r}"
                )

            vectors.append(vec_obj)
            encoders[col_index] = encoder
            if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
                aux_obj = vec_obj.view()
            elif encoder == ENC_GENERIC:
                aux_obj = vec_obj.to_pylist()
            else:
                aux_obj = None
            aux_objects.append(aux_obj)

            key_first_objs.append(_build_key_prefix(col_name, False))
            key_next_objs.append(_build_key_prefix(col_name, True))
            PyBytes_AsStringAndSize(key_first_objs[col_index], <char**>&key_first_ptrs[col_index], &key_first_lens[col_index])
            PyBytes_AsStringAndSize(key_next_objs[col_index], <char**>&key_next_ptrs[col_index], &key_next_lens[col_index])
            estimated_row_bytes += key_next_lens[col_index]
            estimated_row_bytes += _estimate_value_bytes(encoder, vec_obj, aux_obj, num_rows)

        if estimated_row_bytes < 8:
            estimated_row_bytes = 8

        scratch_capacity = estimated_row_bytes * 2
        if scratch_capacity < 64:
            scratch_capacity = 64
        scratch = <char*>malloc(scratch_capacity)
        if scratch == NULL and scratch_capacity > 0:
            raise MemoryError()

        builder = StringVectorBuilder.with_estimate(num_rows, estimated_row_bytes)

        for row_index in range(num_rows):
            pos = 0
            field_count = 0
            scratch[pos] = 123
            pos += 1

            for col_index in range(num_cols):
                vec_obj = vectors[col_index]
                aux_obj = aux_objects[col_index]
                encoder = encoders[col_index]

                if _value_is_null_cached(encoder, vec_obj, aux_obj, row_index):
                    if omit_null_fields:
                        continue
                    if field_count == 0:
                        reserve_needed = pos + key_first_lens[col_index] + 4 + 1
                        _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                        memcpy(scratch + pos, key_first_ptrs[col_index], key_first_lens[col_index])
                        pos += key_first_lens[col_index]
                    else:
                        reserve_needed = pos + key_next_lens[col_index] + 4 + 1
                        _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                        memcpy(scratch + pos, key_next_ptrs[col_index], key_next_lens[col_index])
                        pos += key_next_lens[col_index]
                    memcpy(scratch + pos, b"null", 4)
                    pos += 4
                    field_count += 1
                    continue

                if field_count == 0:
                    reserve_needed = pos + key_first_lens[col_index] + 32
                    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
                        string_view = <_StringVectorView>aux_obj
                        reserve_needed = pos + key_first_lens[col_index] + (
                            string_view.value_len(row_index) if encoder == ENC_RAW_STRING else (string_view.value_len(row_index) * 6 + 2)
                        ) + 1
                    elif encoder == ENC_CONST_STRING or encoder == ENC_CONST_RAW_STRING:
                        reserve_needed = pos + key_first_lens[col_index] + (
                            (<DrakenConstantStringPayload*>(<ConstantVector>vec_obj).ptr.value).length
                            if encoder == ENC_CONST_RAW_STRING
                            else ((<DrakenConstantStringPayload*>(<ConstantVector>vec_obj).ptr.value).length * 6 + 2)
                        ) + 1
                    _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                    memcpy(scratch + pos, key_first_ptrs[col_index], key_first_lens[col_index])
                    pos += key_first_lens[col_index]
                else:
                    reserve_needed = pos + key_next_lens[col_index] + 32
                    if encoder == ENC_STRING or encoder == ENC_RAW_STRING:
                        string_view = <_StringVectorView>aux_obj
                        reserve_needed = pos + key_next_lens[col_index] + (
                            string_view.value_len(row_index) if encoder == ENC_RAW_STRING else (string_view.value_len(row_index) * 6 + 2)
                        ) + 1
                    elif encoder == ENC_CONST_STRING or encoder == ENC_CONST_RAW_STRING:
                        reserve_needed = pos + key_next_lens[col_index] + (
                            (<DrakenConstantStringPayload*>(<ConstantVector>vec_obj).ptr.value).length
                            if encoder == ENC_CONST_RAW_STRING
                            else ((<DrakenConstantStringPayload*>(<ConstantVector>vec_obj).ptr.value).length * 6 + 2)
                        ) + 1
                    _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                    memcpy(scratch + pos, key_next_ptrs[col_index], key_next_lens[col_index])
                    pos += key_next_lens[col_index]

                pos += _write_value(encoder, vec_obj, aux_obj, row_index, scratch + pos)
                field_count += 1

            _ensure_scratch_capacity(&scratch, &scratch_capacity, pos + 1)
            scratch[pos] = 125
            pos += 1
            builder.append_bytes(scratch, pos)

        return builder.finish()
    finally:
        if scratch != NULL:
            free(scratch)
        if key_next_lens != NULL:
            free(key_next_lens)
        if key_first_lens != NULL:
            free(key_first_lens)
        if key_next_ptrs != NULL:
            free(key_next_ptrs)
        if key_first_ptrs != NULL:
            free(key_first_ptrs)
        if encoders != NULL:
            free(encoders)


cpdef list morsel_to_json_strings(
    Morsel morsel,
    object columns=None,
    object raw_json_columns=None,
    bint omit_null_fields=False,
):
    cdef StringVector rows = morsel_to_json_rows(
        morsel,
        columns=columns,
        raw_json_columns=raw_json_columns,
        omit_null_fields=omit_null_fields,
    )
    return [value.decode("utf-8") for value in rows.to_pylist()]
