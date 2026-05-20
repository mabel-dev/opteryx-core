# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Shared helpers for selection-style kernels (IIF, COALESCE, CASE).
#
# Operations whose output is a per-row choice between multiple Draken vectors
# share the same primitives: bitmap manipulation, vector type classification,
# constant-value reading, fixed-width vector construction.
#
# All consumers MUST treat their inputs as Draken vectors. These helpers do
# not accept Python lists, scalars, or PyArrow surfaces.

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t

from draken.core.buffers cimport (
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
    DrakenVector,
)
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.date32_vector cimport Date32Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.integer8_vector cimport Integer8Vector
from draken.vectors.integer16_vector cimport Integer16Vector
from draken.vectors.integer32_vector cimport Integer32Vector
from draken.vectors.string_vector cimport StringVector
from draken.vectors.time_vector cimport TimeVector
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.vector cimport Vector


# ---------------------------------------------------------------------------
# Bitmap primitives (Draken convention: bit=1 valid, bit=0 null)
# ---------------------------------------------------------------------------

cdef inline bint _sel_bit_is_set(uint8_t* bits, Py_ssize_t index) noexcept nogil:
    return ((bits[index >> 3] >> (index & 7)) & 1) != 0


cdef inline bint _sel_is_valid(uint8_t* null_bitmap, Py_ssize_t index) noexcept nogil:
    if null_bitmap == NULL:
        return True
    return _sel_bit_is_set(null_bitmap, index)


cdef inline void _sel_set_true_bit(uint8_t* bits, Py_ssize_t index) noexcept nogil:
    bits[index >> 3] |= <uint8_t>(1 << (index & 7))


# ---------------------------------------------------------------------------
# Type-family classification for selection dispatch
# ---------------------------------------------------------------------------

cdef inline int _sel_bool_family(Vector value) noexcept:
    """Return DRAKEN_BOOL when value is a BOOL vector."""
    if isinstance(value, BoolVector):
        return DRAKEN_BOOL
    return -1


cdef inline int _sel_string_family(Vector value) noexcept:
    if isinstance(value, StringVector):
        return DRAKEN_STRING
    return -1


cdef inline int _sel_fixed_family(Vector value) noexcept:
    """Return Draken type code for fixed-width vectors, -1 otherwise.

    BOOL and STRING are excluded — they have dedicated kernels.
    """
    if isinstance(value, Integer64Vector):
        return DRAKEN_INT64
    if isinstance(value, Float64Vector):
        return DRAKEN_FLOAT64
    if isinstance(value, Integer8Vector):
        return DRAKEN_INT8
    if isinstance(value, Integer16Vector):
        return DRAKEN_INT16
    if isinstance(value, Integer32Vector):
        return DRAKEN_INT32
    if isinstance(value, Date32Vector):
        return DRAKEN_DATE32
    if isinstance(value, TimeVector):
        return (<TimeVector>value).ptr.type
    if isinstance(value, TimestampVector):
        return DRAKEN_TIMESTAMP64
    return -1


cdef inline DrakenFixedBuffer* _sel_fixed_ptr(Vector value) noexcept:
    if isinstance(value, Integer64Vector):
        return (<Integer64Vector>value).ptr
    if isinstance(value, Float64Vector):
        return (<Float64Vector>value).ptr
    if isinstance(value, Integer8Vector):
        return (<Integer8Vector>value).ptr
    if isinstance(value, Integer16Vector):
        return (<Integer16Vector>value).ptr
    if isinstance(value, Integer32Vector):
        return (<Integer32Vector>value).ptr
    if isinstance(value, Date32Vector):
        return (<Date32Vector>value).ptr
    if isinstance(value, TimeVector):
        return (<TimeVector>value).ptr
    if isinstance(value, TimestampVector):
        return (<TimestampVector>value).ptr
    return NULL


# ---------------------------------------------------------------------------
# Constant value extraction
# ---------------------------------------------------------------------------

cdef object _sel_const_scalar(Vector value):
    """Return the Python value of a const-encoded vector, or None if null."""
    cdef DrakenVector* uv = value.unified()
    if uv.validity != NULL:
        return None
    if uv.type == DRAKEN_BOOL:
        return bool((<uint8_t*>uv.data)[0])
    if uv.type == DRAKEN_INT8:
        return int((<int8_t*>uv.data)[0])
    if uv.type == DRAKEN_INT16:
        return int((<int16_t*>uv.data)[0])
    if uv.type in (DRAKEN_INT32, DRAKEN_DATE32, DRAKEN_TIME32):
        return int((<int32_t*>uv.data)[0])
    if uv.type in (DRAKEN_INT64, DRAKEN_TIME64, DRAKEN_TIMESTAMP64):
        return int((<int64_t*>uv.data)[0])
    if uv.type == DRAKEN_FLOAT64:
        return float((<double*>uv.data)[0])
    raise TypeError(
        f"_sel_const_scalar: unsupported constant value_type {uv.type}"
    )


# ---------------------------------------------------------------------------
# Output vector construction
# ---------------------------------------------------------------------------

cdef Vector _sel_new_fixed_vector(int output_type, Py_ssize_t length, Vector template):
    cdef TimestampVector ts_result
    if output_type == DRAKEN_INT64:
        return Integer64Vector(length)
    if output_type == DRAKEN_FLOAT64:
        return Float64Vector(length)
    if output_type == DRAKEN_INT8:
        return Integer8Vector(length)
    if output_type == DRAKEN_INT16:
        return Integer16Vector(length)
    if output_type == DRAKEN_INT32:
        return Integer32Vector(length)
    if output_type == DRAKEN_DATE32:
        return Date32Vector(length)
    if output_type == DRAKEN_TIME32:
        return TimeVector(length, False)
    if output_type == DRAKEN_TIME64:
        return TimeVector(length, True)
    if output_type == DRAKEN_TIMESTAMP64:
        ts_result = TimestampVector(length)
        if isinstance(template, TimestampVector):
            ts_result.timestamp_unit = (<TimestampVector>template).timestamp_unit
        return ts_result
    raise TypeError(
        f"_sel_new_fixed_vector: unsupported fixed-width output type {output_type}"
    )


cdef void _sel_write_fixed_scalar(
    DrakenFixedBuffer* out_ptr,
    Py_ssize_t row,
    int output_type,
    object value,
) except *:
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
    raise TypeError(
        f"_sel_write_fixed_scalar: unsupported fixed-width scalar type {output_type}"
    )
