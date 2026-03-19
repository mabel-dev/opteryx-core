# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_AS_STRING
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
from opteryx.draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from opteryx.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.constant_vector cimport ConstantVector
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
    ENC_CONST_INT = 6
    ENC_CONST_FLOAT = 7
    ENC_CONST_BOOL = 8
    ENC_CONST_STRING = 9
    ENC_GENERIC = 10


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


cdef inline Py_ssize_t _write_int64(char* dst, int64_t value) noexcept nogil:
    cdef char buf[21]
    cdef int length = _int64_to_ascii(value, buf)
    memcpy(dst, buf + (20 - length), length)
    return length


cdef inline Py_ssize_t _write_float64(char* dst, double value) noexcept nogil:
    if isnan(value) or isinf(value):
        return snprintf(dst, 32, "%g", value)
    return snprintf(dst, 32, "%.17g", value)


cdef inline Py_ssize_t _write_csv_field(char* dst, const char* src, Py_ssize_t length, char separator) noexcept nogil:
    cdef Py_ssize_t i
    cdef Py_ssize_t pos = 0
    cdef char ch

    i = 0
    while i < length:
        ch = src[i]
        if ch == 34 or ch == separator or ch == 10 or ch == 13:
            break
        i += 1

    if i == length:
        if length > 0:
            memcpy(dst, src, length)
        return length

    dst[pos] = 34
    pos += 1
    if i > 0:
        memcpy(dst + pos, src, i)
        pos += i

    while i < length:
        ch = src[i]
        if ch == 34:
            dst[pos] = 34
            dst[pos + 1] = 34
            pos += 2
        else:
            dst[pos] = ch
            pos += 1
        i += 1

    dst[pos] = 34
    return pos + 1


cdef inline Py_ssize_t _estimate_csv_field_bytes(const char* src, Py_ssize_t length) noexcept nogil:
    return (length * 2) + 2


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


cdef char _normalize_separator(object separator):
    cdef char* ptr = NULL
    cdef Py_ssize_t length = 0
    cdef bytes value

    if separator is None:
        return <char>44
    if isinstance(separator, str):
        value = separator.encode("utf-8")
    elif isinstance(separator, bytes):
        value = separator
    else:
        raise TypeError("separator must be a one-byte str or bytes")

    PyBytes_AsStringAndSize(value, &ptr, &length)
    if length != 1:
        raise ValueError("separator must be exactly one byte")
    return ptr[0]


cdef int _constant_encoder(ConstantVector vec):
    if vec.ptr.value_type == DRAKEN_INT64:
        return ENC_CONST_INT
    if vec.ptr.value_type == DRAKEN_FLOAT64:
        return ENC_CONST_FLOAT
    if vec.ptr.value_type == DRAKEN_BOOL:
        return ENC_CONST_BOOL
    if vec.ptr.value_type == DRAKEN_STRING:
        return ENC_CONST_STRING
    return 0


cdef Py_ssize_t _estimate_value_bytes(int encoder, object vec_obj, object aux_obj, Py_ssize_t num_rows) except -1:
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
    if encoder == ENC_STRING:
        string_vec = <StringVector>vec_obj
        if num_rows <= 0:
            return 8
        total_bytes = string_vec.ptr.offsets[string_vec.ptr.length]
        total_bytes = total_bytes // num_rows if num_rows > 0 else 0
        return total_bytes + 4
    if encoder == ENC_CONST_STRING:
        const_vec = <ConstantVector>vec_obj
        payload = <DrakenConstantStringPayload*>const_vec.ptr.value
        return payload.length + 4
    if encoder == ENC_GENERIC:
        return 24
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
    if encoder == ENC_STRING:
        return (<_StringVectorView>aux_obj).is_null(row_index)
    if encoder == ENC_CONST_INT or encoder == ENC_CONST_FLOAT or encoder == ENC_CONST_BOOL or encoder == ENC_CONST_STRING:
        const_vec = <ConstantVector>vec_obj
        return not _is_valid(const_vec.ptr.null_bitmap, row_index)
    if encoder == ENC_GENERIC:
        return aux_obj[row_index] is None
    raise NotImplementedError("unsupported encoder")


cdef inline bytes _generic_csv_bytes(object value):
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf8")
    if isinstance(value, bool):
        return b"true" if value else b"false"
    return str(value).encode("utf8")


cdef Py_ssize_t _write_value(int encoder, object vec_obj, object aux_obj, Py_ssize_t row_index, char* dst, char separator) except -1:
    cdef Int64Vector int64_vec
    cdef IntegerVector integer_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec
    cdef _StringVectorView string_view
    cdef ConstantVector const_vec
    cdef const char* ptr
    cdef Py_ssize_t length
    cdef Py_ssize_t offset
    cdef Py_ssize_t next_offset
    cdef DrakenConstantStringPayload* payload
    cdef bytes generic_bytes
    cdef char* generic_ptr = NULL
    cdef Py_ssize_t generic_len = 0

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
    if encoder == ENC_STRING:
        string_view = <_StringVectorView>aux_obj
        # Bypass bounds checks for speed; caller guarantees row_index in range
        offset = string_view._offsets[row_index]
        next_offset = string_view._offsets[row_index + 1]
        ptr = <const char*>(string_view._data + offset)
        length = next_offset - offset
        return _write_csv_field(dst, ptr, length, separator)
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
    if encoder == ENC_CONST_STRING:
        const_vec = <ConstantVector>vec_obj
        payload = <DrakenConstantStringPayload*>const_vec.ptr.value
        ptr = <const char*>payload.data
        length = payload.length
        return _write_csv_field(dst, ptr, length, separator)
    if encoder == ENC_GENERIC:
        generic_bytes = _generic_csv_bytes(aux_obj[row_index])
        PyBytes_AsStringAndSize(generic_bytes, &generic_ptr, &generic_len)
        return _write_csv_field(dst, generic_ptr, generic_len, separator)
    raise NotImplementedError("unsupported encoder")


cpdef StringVector morsel_to_csv_rows(
    Morsel morsel,
    object columns=None,
    bint include_header=False,
    object separator=b",",
):
    cdef list selected_columns = _normalize_columns(morsel, columns)
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t num_cols = len(selected_columns)
    cdef Py_ssize_t output_rows = num_rows + (1 if include_header else 0)
    cdef char sep = _normalize_separator(separator)
    cdef list vectors = []
    cdef list aux_objects = []
    cdef int* encoders = NULL
    cdef const uint8_t** null_bitmaps = NULL
    cdef char* scratch = NULL
    cdef Py_ssize_t scratch_capacity = 0
    cdef Py_ssize_t estimated_row_bytes = max(1, num_cols)
    cdef Py_ssize_t row_index
    cdef Py_ssize_t col_index
    cdef Py_ssize_t pos
    cdef Py_ssize_t reserve_needed
    cdef object vec_obj
    cdef object aux_obj
    cdef bytes col_name
    cdef int encoder
    cdef StringVectorBuilder builder
    cdef _StringVectorView string_view
    cdef ConstantVector const_vec
    cdef DrakenConstantStringPayload* payload
    cdef Py_ssize_t offset
    cdef Py_ssize_t next_offset
    cdef char* header_ptr = NULL
    cdef Py_ssize_t header_len = 0

    try:
        encoders = <int*>malloc(sizeof(int) * max(num_cols, 1))
        if encoders == NULL:
            raise MemoryError()

        null_bitmaps = <const uint8_t**>malloc(sizeof(const uint8_t*) * max(num_cols, 1))
        if null_bitmaps == NULL:
            raise MemoryError()

        for col_index in range(num_cols):
            col_name = <bytes>selected_columns[col_index]
            vec_obj = morsel.column(col_name)

            if isinstance(vec_obj, Int64Vector):
                encoder = ENC_INT64
                aux_obj = None
                null_bitmaps[col_index] = (<Int64Vector>vec_obj).ptr.null_bitmap
            elif isinstance(vec_obj, IntegerVector):
                encoder = ENC_INTEGER
                aux_obj = None
                null_bitmaps[col_index] = (<IntegerVector>vec_obj).ptr.null_bitmap
            elif isinstance(vec_obj, Float64Vector):
                encoder = ENC_FLOAT64
                aux_obj = None
                null_bitmaps[col_index] = (<Float64Vector>vec_obj).ptr.null_bitmap
            elif isinstance(vec_obj, BoolVector):
                encoder = ENC_BOOL
                aux_obj = None
                null_bitmaps[col_index] = (<BoolVector>vec_obj).ptr.null_bitmap
            elif isinstance(vec_obj, StringVector):
                encoder = ENC_STRING
                aux_obj = vec_obj.view()
                null_bitmaps[col_index] = NULL
            elif isinstance(vec_obj, ConstantVector):
                encoder = _constant_encoder(<ConstantVector>vec_obj)
                if encoder == 0:
                    raise NotImplementedError(
                        f"csv serialization does not support ConstantVector value type for column {col_name!r}"
                    )
                aux_obj = None
                null_bitmaps[col_index] = (<ConstantVector>vec_obj).ptr.null_bitmap
            elif getattr(vec_obj, "encoding", None) == DRAKEN_ENCODING_DICTIONARY:
                encoder = ENC_GENERIC
                aux_obj = vec_obj.to_pylist()
                null_bitmaps[col_index] = NULL
            else:
                raise NotImplementedError(
                    f"csv serialization does not support vector type {type(vec_obj).__name__} for column {col_name!r}"
                )

            vectors.append(vec_obj)
            aux_objects.append(aux_obj)
            encoders[col_index] = encoder
            estimated_row_bytes += _estimate_csv_field_bytes(PyBytes_AS_STRING(col_name), len(col_name))
            estimated_row_bytes += _estimate_value_bytes(encoder, vec_obj, aux_obj, num_rows)

        if estimated_row_bytes < 16:
            estimated_row_bytes = 16

        scratch_capacity = estimated_row_bytes * 2
        if scratch_capacity < 64:
            scratch_capacity = 64
        scratch = <char*>malloc(scratch_capacity)
        if scratch == NULL:
            raise MemoryError()

        builder = StringVectorBuilder.with_estimate(output_rows, estimated_row_bytes)

        if include_header:
            pos = 0
            for col_index in range(num_cols):
                col_name = <bytes>selected_columns[col_index]
                PyBytes_AsStringAndSize(col_name, &header_ptr, &header_len)
                reserve_needed = pos + _estimate_csv_field_bytes(header_ptr, header_len) + 2
                _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                if col_index > 0:
                    scratch[pos] = sep
                    pos += 1
                pos += _write_csv_field(scratch + pos, header_ptr, header_len, sep)
            builder.append_bytes(scratch, pos)

        for row_index in range(num_rows):
            pos = 0
            for col_index in range(num_cols):
                vec_obj = vectors[col_index]
                aux_obj = aux_objects[col_index]
                encoder = encoders[col_index]

                if col_index > 0:
                    _ensure_scratch_capacity(&scratch, &scratch_capacity, pos + 2)
                    scratch[pos] = sep
                    pos += 1

                if encoder == ENC_STRING:
                    string_view = <_StringVectorView>aux_obj
                    if string_view._nulls != NULL and not _is_valid(string_view._nulls, row_index):
                        continue
                else:
                    if null_bitmaps[col_index] != NULL and not _is_valid(null_bitmaps[col_index], row_index):
                        continue

                reserve_needed = pos + 32
                if encoder == ENC_STRING:
                    string_view = <_StringVectorView>aux_obj
                    # Bypass bounds checks for speed; caller guarantees row_index in range
                    offset = string_view._offsets[row_index]
                    next_offset = string_view._offsets[row_index + 1]
                    reserve_needed = pos + _estimate_csv_field_bytes(
                        <char*>string_view._data + offset,
                        next_offset - offset,
                    ) + 1
                elif encoder == ENC_CONST_STRING:
                    const_vec = <ConstantVector>vec_obj
                    payload = <DrakenConstantStringPayload*>const_vec.ptr.value
                    reserve_needed = pos + _estimate_csv_field_bytes(<char*>payload.data, payload.length) + 1
                _ensure_scratch_capacity(&scratch, &scratch_capacity, reserve_needed)
                pos += _write_value(encoder, vec_obj, aux_obj, row_index, scratch + pos, sep)

            builder.append_bytes(scratch, pos)

        return builder.finish()
    finally:
        if scratch != NULL:
            free(scratch)
        if encoders != NULL:
            free(encoders)
        if null_bitmaps != NULL:
            free(<void*>null_bitmaps)


cpdef list morsel_to_csv_strings(
    Morsel morsel,
    object columns=None,
    bint include_header=False,
    object separator=b",",
):
    cdef StringVector rows = morsel_to_csv_rows(
        morsel,
        columns=columns,
        include_header=include_header,
        separator=separator,
    )
    return [value.decode("utf-8") for value in rows.to_pylist()]
