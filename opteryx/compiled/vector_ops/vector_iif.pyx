# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Vectorized IIF(condition, true_value, false_value)."""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from opteryx.draken.core.buffers cimport (
    DRAKEN_BOOL,
    DRAKEN_DATE32,
    DRAKEN_FLOAT64,
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_STRING,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
    DRAKEN_TIMESTAMP64,
    DrakenFixedBuffer,
)
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.constant_vector cimport ConstantVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector
from opteryx.draken.vectors.string_vector cimport (
    StringVector,
    StringVectorBuilder,
    _StringVectorView,
)
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.vector cimport Vector


cdef int BRANCH_UNSUPPORTED = 0


cdef int _infer_value_type(object value) noexcept:
    # A simplified version of the logic in ConstantVector._infer_value_type.
    # Used to help normalize scalar inputs for IIF.
    if value is None:
        return -1
    if isinstance(value, bool):
        return DRAKEN_BOOL
    if isinstance(value, int):
        return DRAKEN_INT64
    if isinstance(value, float):
        return DRAKEN_FLOAT64
    if isinstance(value, (bytes, str)):
        return DRAKEN_STRING
    return -1
cdef int BRANCH_VECTOR = 1
cdef int BRANCH_CONSTANT = 2
cdef int BRANCH_SCALAR = 3


def _is_numpy_array_like(value):
    module_name = value.__class__.__module__
    return module_name == "numpy" or module_name.startswith("numpy.")


def _is_arrow_value(value):
    module_name = value.__class__.__module__
    return module_name == "pyarrow.lib" or module_name.startswith("pyarrow.")


def _sequence_length(value):
    if isinstance(value, Vector):
        return len(value)
    if _is_numpy_array_like(value):
        return None if value.shape == () else len(value)
    if isinstance(value, (list, tuple)):
        return len(value)
    if _is_arrow_value(value) and hasattr(value, "__len__"):
        try:
            return len(value)
        except TypeError:
            return None
    return None


def _infer_length(*values):
    lengths = [length for length in (_sequence_length(value) for value in values) if length is not None]
    return max(lengths) if lengths else 1


def _normalize_value(value):
    if isinstance(value, Vector):
        return value

    if _is_arrow_value(value):
        if hasattr(value, "combine_chunks"):
            try:
                value = value.combine_chunks()
            except Exception:
                pass

        # Prefer Draken-native conversion if possible (works for strings, ints, etc.)
        try:
            from opteryx.draken.interop.arrow import vector_from_arrow

            return vector_from_arrow(value)
        except Exception:
            pass

        if hasattr(value, "to_pylist"):
            try:
                return value.to_pylist()
            except Exception:
                pass

        if hasattr(value, "as_py"):
            try:
                return value.as_py()
            except Exception:
                pass

    if hasattr(value, "as_py"):
        try:
            return value.as_py()
        except Exception:
            pass

    if _is_numpy_array_like(value):
        # Treat scalar arrays as scalars to make constant-folding compatible
        # with functions (e.g. IIF) that expect fixed-width vectors.
        if value.shape == () or value.shape == (1,):
            value = value.item()
        else:
            return value.tolist()

    # Scalars are supported via ConstantVector so that scalar expressions
    # (e.g. IIF(1=1, 1, 0)) can be handled by the Draken kernel.
    if value is None or isinstance(value, (bool, int, float, bytes, str)):
        value_type = _infer_value_type(value)
        if value_type != -1:
            return ConstantVector(1, value_type, value)

    # Normalize Python sequences into Draken vectors so the Draken IIF kernel
    # can operate on them directly.
    from opteryx.draken.interop.arrow import vector_from_sequence

    if isinstance(value, (tuple, list)):
        if len(value) == 1:
            return _normalize_value(value[0])
        try:
            return vector_from_sequence(value)
        except Exception:
            # fallback to list (will error later with a helpful message)
            return list(value)

    return value


def _validate_length(value, length):
    value_length = _sequence_length(value)
    if value_length is None:
        return
    if value_length not in (1, length):
        raise ValueError(f"IIF arguments must be length 1 or {length}, got {value_length}")


def _python_values(value, length):
    if isinstance(value, Vector):
        values = value.to_pylist()
    elif _is_numpy_array_like(value):
        values = [value.item()] if value.shape == () else value.tolist()
    elif isinstance(value, (list, tuple)):
        values = list(value)
    elif hasattr(value, "to_pylist"):
        values = value.to_pylist()
    else:
        values = [value]

    if len(values) == length:
        return values
    if len(values) == 1:
        return values * length
    raise ValueError(f"IIF arguments must be length 1 or {length}, got {len(values)}")


cdef inline bint _bit_is_set(uint8_t* bits, Py_ssize_t index) noexcept nogil:
    return ((bits[index >> 3] >> (index & 7)) & 1) != 0


cdef inline bint _is_valid(uint8_t* null_bitmap, Py_ssize_t index) noexcept nogil:
    if null_bitmap == NULL:
        return True
    return _bit_is_set(null_bitmap, index)


cdef inline void _set_true_bit(uint8_t* bits, Py_ssize_t index) noexcept nogil:
    bits[index >> 3] |= <uint8_t>(1 << (index & 7))


cdef inline Py_ssize_t _row_index(Py_ssize_t source_length, Py_ssize_t index) noexcept nogil:
    if source_length == 1:
        return 0
    return index


cdef inline int _fixed_vector_type(object value) noexcept:
    cdef int vt
    if isinstance(value, Int64Vector):
        return DRAKEN_INT64
    if isinstance(value, Float64Vector):
        return DRAKEN_FLOAT64
    if isinstance(value, IntegerVector):
        return (<IntegerVector>value).ptr.type
    if isinstance(value, Date32Vector):
        return DRAKEN_DATE32
    if isinstance(value, TimeVector):
        return (<TimeVector>value).ptr.type
    if isinstance(value, TimestampVector):
        return DRAKEN_TIMESTAMP64
    if isinstance(value, ConstantVector):
        # ConstantVector supports fixed-width types (int, float, bool) and string.
        # String values should be handled by the string path, not the fixed-width path.
        vt = (<ConstantVector>value).ptr.value_type
        if vt == DRAKEN_STRING:
            return -1
        return vt
    return -1


cdef inline DrakenFixedBuffer* _fixed_ptr(object value) noexcept:
    if isinstance(value, Int64Vector):
        return (<Int64Vector>value).ptr
    if isinstance(value, Float64Vector):
        return (<Float64Vector>value).ptr
    if isinstance(value, IntegerVector):
        return (<IntegerVector>value).ptr
    if isinstance(value, Date32Vector):
        return (<Date32Vector>value).ptr
    if isinstance(value, TimeVector):
        return (<TimeVector>value).ptr
    if isinstance(value, TimestampVector):
        return (<TimestampVector>value).ptr
    return NULL


cdef inline bint _is_fixed_scalar_type(int output_type) noexcept nogil:
    return output_type in (
        DRAKEN_INT8,
        DRAKEN_INT16,
        DRAKEN_INT32,
        DRAKEN_INT64,
        DRAKEN_FLOAT64,
        DRAKEN_DATE32,
        DRAKEN_TIME32,
        DRAKEN_TIME64,
        DRAKEN_TIMESTAMP64,
    )


cdef bint _scalar_matches_fixed_type(object value, int output_type):
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if output_type == DRAKEN_FLOAT64:
        return isinstance(value, (int, float))
    if _is_fixed_scalar_type(output_type):
        return isinstance(value, int)
    return False


cdef object _coerce_string_scalar(object value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf8")
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


cdef inline bint _is_bool_scalar(object value):
    return value is None or isinstance(value, bool)


cdef inline bint _is_string_branch(object value):
    if isinstance(value, StringVector):
        return True
    if isinstance(value, ConstantVector):
        return (<ConstantVector>value).ptr.value_type == DRAKEN_STRING
    return _coerce_string_scalar(value) is not None or value is None


cdef inline int _fixed_branch_kind(object value, int output_type):
    if isinstance(value, ConstantVector):
        if (<ConstantVector>value).ptr.value_type == output_type:
            return BRANCH_CONSTANT
        return BRANCH_UNSUPPORTED
    if _fixed_vector_type(value) == output_type:
        return BRANCH_VECTOR
    if _scalar_matches_fixed_type(value, output_type):
        return BRANCH_SCALAR
    return BRANCH_UNSUPPORTED


cdef inline int _bool_branch_kind(object value):
    if isinstance(value, ConstantVector):
        if (<ConstantVector>value).ptr.value_type == DRAKEN_BOOL:
            return BRANCH_CONSTANT
        return BRANCH_UNSUPPORTED
    if isinstance(value, BoolVector):
        return BRANCH_VECTOR
    if _is_bool_scalar(value):
        return BRANCH_SCALAR
    return BRANCH_UNSUPPORTED


cdef inline int _string_branch_kind(object value):
    if isinstance(value, ConstantVector):
        if (<ConstantVector>value).ptr.value_type == DRAKEN_STRING:
            return BRANCH_CONSTANT
        return BRANCH_UNSUPPORTED
    if isinstance(value, StringVector):
        return BRANCH_VECTOR
    if value is None or _coerce_string_scalar(value) is not None:
        return BRANCH_SCALAR
    return BRANCH_UNSUPPORTED


cdef inline object _new_fixed_vector(int output_type, Py_ssize_t length, object template):
    cdef object result
    if output_type == DRAKEN_INT64:
        return Int64Vector(length)
    if output_type == DRAKEN_FLOAT64:
        return Float64Vector(length)
    if output_type in (DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32):
        return IntegerVector(output_type, length)
    if output_type == DRAKEN_DATE32:
        return Date32Vector(length)
    if output_type == DRAKEN_TIME32:
        return TimeVector(length, False)
    if output_type == DRAKEN_TIME64:
        return TimeVector(length, True)
    if output_type == DRAKEN_TIMESTAMP64:
        result = TimestampVector(length)
        if isinstance(template, TimestampVector):
            (<TimestampVector>result).timestamp_unit = (<TimestampVector>template).timestamp_unit
        return result
    raise TypeError(f"Unsupported fixed-width IIF output type {output_type}")


cdef void _write_fixed_scalar(DrakenFixedBuffer* out_ptr, Py_ssize_t row, int output_type, object value) except *:
    if output_type == DRAKEN_INT8:
        (<int8_t*>out_ptr.data)[row] = <int8_t>value
        return
    if output_type == DRAKEN_INT16:
        (<int16_t*>out_ptr.data)[row] = <int16_t>value
        return
    if output_type in (DRAKEN_INT32, DRAKEN_DATE32, DRAKEN_TIME32):
        (<int32_t*>out_ptr.data)[row] = <int32_t>value
        return
    if output_type in (DRAKEN_INT64, DRAKEN_TIME64, DRAKEN_TIMESTAMP64):
        (<int64_t*>out_ptr.data)[row] = <int64_t>value
        return
    if output_type == DRAKEN_FLOAT64:
        (<double*>out_ptr.data)[row] = <double>value
        return
    raise TypeError(f"Unsupported fixed-width IIF scalar type {output_type}")


cdef bint _condition_is_true(object condition, Py_ssize_t row, Py_ssize_t length) except *:
    cdef BoolVector bool_vec
    cdef ConstantVector const_vec
    cdef Py_ssize_t source_row
    cdef object value

    if isinstance(condition, BoolVector):
        bool_vec = <BoolVector>condition
        source_row = _row_index(len(bool_vec), row)
        if not _is_valid(bool_vec.ptr.null_bitmap, source_row):
            return False
        return _bit_is_set(<uint8_t*>bool_vec.ptr.data, source_row)

    if isinstance(condition, ConstantVector) and (<ConstantVector>condition).ptr.value_type == DRAKEN_BOOL:
        const_vec = <ConstantVector>condition
        source_row = _row_index(len(const_vec), row)
        if not _is_valid(const_vec.ptr.null_bitmap, source_row):
            return False
        value = const_vec.scalar_value()
        return False if value is None else bool(value)

    if isinstance(condition, Vector):
        source_row = _row_index(len(condition), row)
        value = condition[source_row]
        return False if value is None else bool(value)

    if isinstance(condition, (list, tuple)):
        source_row = _row_index(len(condition), row)
        value = condition[source_row]
        return False if value is None else bool(value)

    return False if condition is None else bool(condition)


cdef object _select_fixed(
    object condition,
    object when_true,
    object when_false,
    Py_ssize_t length,
    int output_type,
):
    cdef object template = when_true if _fixed_vector_type(when_true) == output_type else when_false
    cdef object result = _new_fixed_vector(output_type, length, template)
    cdef DrakenFixedBuffer* out_ptr = _fixed_ptr(result)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef bint choose_true

    cdef int true_kind = _fixed_branch_kind(when_true, output_type)
    cdef int false_kind = _fixed_branch_kind(when_false, output_type)
    cdef DrakenFixedBuffer* true_ptr = _fixed_ptr(when_true) if true_kind == BRANCH_VECTOR else NULL
    cdef DrakenFixedBuffer* false_ptr = _fixed_ptr(when_false) if false_kind == BRANCH_VECTOR else NULL
    cdef Py_ssize_t true_length = len(when_true) if true_kind == BRANCH_VECTOR else 1
    cdef Py_ssize_t false_length = len(when_false) if false_kind == BRANCH_VECTOR else 1
    cdef ConstantVector true_const
    cdef ConstantVector false_const
    cdef object true_scalar = None
    cdef object false_scalar = None
    cdef Py_ssize_t source_row
    cdef char* out_data = <char*>out_ptr.data
    cdef char* source_data

    if true_kind == BRANCH_UNSUPPORTED or false_kind == BRANCH_UNSUPPORTED:
        raise TypeError("unsupported fixed-width branch combination")

    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    if true_kind == BRANCH_CONSTANT:
        true_const = <ConstantVector>when_true
        true_scalar = true_const.scalar_value()
    elif true_kind == BRANCH_SCALAR:
        true_scalar = when_true

    if false_kind == BRANCH_CONSTANT:
        false_const = <ConstantVector>when_false
        false_scalar = false_const.scalar_value()
    elif false_kind == BRANCH_SCALAR:
        false_scalar = when_false

    for row in range(length):
        choose_true = _condition_is_true(condition, row, length)

        if choose_true:
            if true_kind == BRANCH_VECTOR:
                source_row = _row_index(true_length, row)
                if not _is_valid(true_ptr.null_bitmap, source_row):
                    any_null = True
                    continue
                source_data = <char*>true_ptr.data
                memcpy(
                    out_data + (row * out_ptr.itemsize),
                    source_data + (source_row * true_ptr.itemsize),
                    out_ptr.itemsize,
                )
            elif true_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(true_const), row)
                if not _is_valid(true_const.ptr.null_bitmap, source_row) or true_scalar is None:
                    any_null = True
                    continue
                _write_fixed_scalar(out_ptr, row, output_type, true_scalar)
            else:
                if true_scalar is None:
                    any_null = True
                    continue
                _write_fixed_scalar(out_ptr, row, output_type, true_scalar)
        else:
            if false_kind == BRANCH_VECTOR:
                source_row = _row_index(false_length, row)
                if not _is_valid(false_ptr.null_bitmap, source_row):
                    any_null = True
                    continue
                source_data = <char*>false_ptr.data
                memcpy(
                    out_data + (row * out_ptr.itemsize),
                    source_data + (source_row * false_ptr.itemsize),
                    out_ptr.itemsize,
                )
            elif false_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(false_const), row)
                if not _is_valid(false_const.ptr.null_bitmap, source_row) or false_scalar is None:
                    any_null = True
                    continue
                _write_fixed_scalar(out_ptr, row, output_type, false_scalar)
            else:
                if false_scalar is None:
                    any_null = True
                    continue
                _write_fixed_scalar(out_ptr, row, output_type, false_scalar)

        if out_null != NULL:
            _set_true_bit(out_null, row)

    if out_null == NULL or not any_null:
        if out_null != NULL:
            free(out_null)
        out_ptr.null_bitmap = NULL
    else:
        out_ptr.null_bitmap = out_null

    return result


cdef object _select_bool(object condition, object when_true, object when_false, Py_ssize_t length):
    cdef BoolVector result = BoolVector(length)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t row
    cdef bint choose_true
    cdef bint any_null = False

    cdef int true_kind = _bool_branch_kind(when_true)
    cdef int false_kind = _bool_branch_kind(when_false)
    cdef BoolVector true_vec
    cdef BoolVector false_vec
    cdef ConstantVector true_const
    cdef ConstantVector false_const
    cdef Py_ssize_t source_row
    cdef object true_scalar = None
    cdef object false_scalar = None
    cdef bint value

    if true_kind == BRANCH_UNSUPPORTED or false_kind == BRANCH_UNSUPPORTED:
        raise TypeError("unsupported boolean branch combination")

    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    if true_kind == BRANCH_VECTOR:
        true_vec = <BoolVector>when_true
    elif true_kind == BRANCH_CONSTANT:
        true_const = <ConstantVector>when_true
        true_scalar = true_const.scalar_value()
    else:
        true_scalar = when_true

    if false_kind == BRANCH_VECTOR:
        false_vec = <BoolVector>when_false
    elif false_kind == BRANCH_CONSTANT:
        false_const = <ConstantVector>when_false
        false_scalar = false_const.scalar_value()
    else:
        false_scalar = when_false

    for row in range(length):
        choose_true = _condition_is_true(condition, row, length)

        if choose_true:
            if true_kind == BRANCH_VECTOR:
                source_row = _row_index(len(true_vec), row)
                if not _is_valid(true_vec.ptr.null_bitmap, source_row):
                    any_null = True
                    continue
                value = _bit_is_set(<uint8_t*>true_vec.ptr.data, source_row)
            elif true_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(true_const), row)
                if not _is_valid(true_const.ptr.null_bitmap, source_row) or true_scalar is None:
                    any_null = True
                    continue
                value = bool(true_scalar)
            else:
                if true_scalar is None:
                    any_null = True
                    continue
                value = bool(true_scalar)
        else:
            if false_kind == BRANCH_VECTOR:
                source_row = _row_index(len(false_vec), row)
                if not _is_valid(false_vec.ptr.null_bitmap, source_row):
                    any_null = True
                    continue
                value = _bit_is_set(<uint8_t*>false_vec.ptr.data, source_row)
            elif false_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(false_const), row)
                if not _is_valid(false_const.ptr.null_bitmap, source_row) or false_scalar is None:
                    any_null = True
                    continue
                value = bool(false_scalar)
            else:
                if false_scalar is None:
                    any_null = True
                    continue
                value = bool(false_scalar)

        if value:
            _set_true_bit(out_bits, row)
        if out_null != NULL:
            _set_true_bit(out_null, row)

    if out_null == NULL or not any_null:
        if out_null != NULL:
            free(out_null)
        result.ptr.null_bitmap = NULL
    else:
        result.ptr.null_bitmap = out_null

    return result


cdef object _select_string(object condition, object when_true, object when_false, Py_ssize_t length):
    cdef int true_kind = _string_branch_kind(when_true)
    cdef int false_kind = _string_branch_kind(when_false)
    cdef StringVector true_vec
    cdef StringVector false_vec
    cdef _StringVectorView true_view
    cdef _StringVectorView false_view
    cdef ConstantVector true_const
    cdef ConstantVector false_const
    cdef object true_scalar = None
    cdef object false_scalar = None
    cdef Py_ssize_t row
    cdef Py_ssize_t source_row
    cdef Py_ssize_t total_bytes = 0
    cdef bint choose_true
    cdef Py_ssize_t value_len
    cdef const char* value_ptr
    cdef StringVectorBuilder builder

    if true_kind == BRANCH_UNSUPPORTED or false_kind == BRANCH_UNSUPPORTED:
        raise TypeError("unsupported string branch combination")

    if true_kind == BRANCH_VECTOR:
        true_vec = <StringVector>when_true
        true_view = <_StringVectorView>true_vec.view()
    elif true_kind == BRANCH_CONSTANT:
        true_const = <ConstantVector>when_true
        true_scalar = _coerce_string_scalar(true_const.scalar_value())
    else:
        true_scalar = _coerce_string_scalar(when_true)

    if false_kind == BRANCH_VECTOR:
        false_vec = <StringVector>when_false
        false_view = <_StringVectorView>false_vec.view()
    elif false_kind == BRANCH_CONSTANT:
        false_const = <ConstantVector>when_false
        false_scalar = _coerce_string_scalar(false_const.scalar_value())
    else:
        false_scalar = _coerce_string_scalar(when_false)

    for row in range(length):
        choose_true = _condition_is_true(condition, row, length)
        if choose_true:
            if true_kind == BRANCH_VECTOR:
                source_row = _row_index(len(true_vec), row)
                if true_view.is_null(source_row):
                    continue
                total_bytes += true_view.value_len(source_row)
            elif true_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(true_const), row)
                if not _is_valid(true_const.ptr.null_bitmap, source_row) or true_scalar is None:
                    continue
                total_bytes += len(true_scalar)
            elif true_scalar is not None:
                total_bytes += len(true_scalar)
        else:
            if false_kind == BRANCH_VECTOR:
                source_row = _row_index(len(false_vec), row)
                if false_view.is_null(source_row):
                    continue
                total_bytes += false_view.value_len(source_row)
            elif false_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(false_const), row)
                if not _is_valid(false_const.ptr.null_bitmap, source_row) or false_scalar is None:
                    continue
                total_bytes += len(false_scalar)
            elif false_scalar is not None:
                total_bytes += len(false_scalar)

    builder = StringVectorBuilder(length, total_bytes, False, True)

    for row in range(length):
        choose_true = _condition_is_true(condition, row, length)
        if choose_true:
            if true_kind == BRANCH_VECTOR:
                source_row = _row_index(len(true_vec), row)
                if true_view.is_null(source_row):
                    builder.append_null()
                    continue
                value_len = true_view.value_len(source_row)
                value_ptr = <const char*>true_view.value_ptr(source_row)
                builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)
            elif true_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(true_const), row)
                if not _is_valid(true_const.ptr.null_bitmap, source_row) or true_scalar is None:
                    builder.append_null()
                    continue
                builder.append(<bytes>true_scalar)
            else:
                if true_scalar is None:
                    builder.append_null()
                    continue
                builder.append(<bytes>true_scalar)
        else:
            if false_kind == BRANCH_VECTOR:
                source_row = _row_index(len(false_vec), row)
                if false_view.is_null(source_row):
                    builder.append_null()
                    continue
                value_len = false_view.value_len(source_row)
                value_ptr = <const char*>false_view.value_ptr(source_row)
                builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)
            elif false_kind == BRANCH_CONSTANT:
                source_row = _row_index(len(false_const), row)
                if not _is_valid(false_const.ptr.null_bitmap, source_row) or false_scalar is None:
                    builder.append_null()
                    continue
                builder.append(<bytes>false_scalar)
            else:
                if false_scalar is None:
                    builder.append_null()
                    continue
                builder.append(<bytes>false_scalar)

    return builder.finish()


cpdef Vector vector_iif(Vector condition, Vector when_true, Vector when_false):
    """Return row-wise selected values using SQL IIF semantics.

    This function assumes it is handed Draken vectors (including
    ConstantVector). Callers are responsible for coercing scalars/lists into
    Draken vectors before dispatching here.
    """

    length = _infer_length(condition, when_true, when_false)
    _validate_length(condition, length)
    _validate_length(when_true, length)
    _validate_length(when_false, length)

    true_fixed_type = _fixed_vector_type(when_true)
    false_fixed_type = _fixed_vector_type(when_false)

    if true_fixed_type != -1 and false_fixed_type == true_fixed_type:
        return <Vector>_select_fixed(condition, when_true, when_false, length, true_fixed_type)
    if true_fixed_type != -1 and _fixed_branch_kind(when_false, true_fixed_type) != BRANCH_UNSUPPORTED:
        return <Vector>_select_fixed(condition, when_true, when_false, length, true_fixed_type)
    if false_fixed_type != -1 and _fixed_branch_kind(when_true, false_fixed_type) != BRANCH_UNSUPPORTED:
        return <Vector>_select_fixed(condition, when_true, when_false, length, false_fixed_type)

    if _bool_branch_kind(when_true) != BRANCH_UNSUPPORTED and _bool_branch_kind(when_false) != BRANCH_UNSUPPORTED:
        return <Vector>_select_bool(condition, when_true, when_false, length)

    if _string_branch_kind(when_true) != BRANCH_UNSUPPORTED and _string_branch_kind(when_false) != BRANCH_UNSUPPORTED:
        return <Vector>_select_string(condition, when_true, when_false, length)

    raise TypeError(
        f"vector_iif only supports Draken fixed-width, boolean, and string vector families; "
        f"got condition={type(condition).__name__}, when_true={type(when_true).__name__}, when_false={type(when_false).__name__}"
    )
