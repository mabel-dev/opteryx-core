# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Draken-native math kernels.

Provides element-wise implementations of:
  - CEILING / FLOOR / TRUNCATE  (with optional scale factor)
  - POWER
  - RANDOM()
  - RANDOM_NORMAL()

All kernels follow the same structure as vector_round.pyx:
  - dict-accessor (dictionary-encoded) fast path
  - Int64Vector path
  - Float64Vector path
  - TypeError for anything else

The random kernels reuse the _xorshift32 / _rng_state already defined
in vector_random_string.pyx (included earlier in vector_ops.pyx).
A separate seeded state (_normal_rng_state) is used by vector_random_normal
so that the sequence is reproducible across calls.
"""

from libc.math cimport ceil  as c_ceil
from libc.math cimport floor as c_floor
from libc.math cimport trunc as c_trunc
from libc.math cimport pow   as c_pow
from libc.math cimport log   as c_log
from libc.math cimport cos   as c_cos
from libc.math cimport sin   as c_sin
from libc.math cimport sqrt  as c_sqrt

from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t, int32_t, int16_t, int8_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport (
    DRAKEN_FLOAT64, DRAKEN_FLOAT32,
    DRAKEN_INT64,  DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8,
)
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector   cimport Int64Vector
from opteryx.compiled.draken.vectors.scalar_constructors cimport from_scalar
from opteryx.compiled.draken.vectors.vector cimport Vector


# ---------------------------------------------------------------------------
# Seeded RNG for RANDOM_NORMAL — separate from the time-seeded string RNG
# so that RANDOM_NORMAL remains reproducible across process lifetime.
# ---------------------------------------------------------------------------
cdef unsigned int _normal_rng_state = 674162347314
# coordinates of Apollo 11's moonlanding
# https://geohack.toolforge.org/geohack.php?pagename=Apollo_11&params=0.67416_N_23.47314_E_globe:moon


cdef inline unsigned int _xorshift32_normal() nogil:
    global _normal_rng_state
    cdef unsigned int x = _normal_rng_state
    x ^= x << 13
    x ^= x >> 17
    x ^= x << 5
    _normal_rng_state = x
    return x


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

cdef inline double _apply_scale(double v, int scale) nogil:
    """Apply scale-factor arithmetic around a pre-computed value.

    Used by ceil/floor/trunc so the caller can pass the raw element and get
    back the scaled result without branching in the per-element loop.
    """
    cdef double sf
    if scale == 0:
        return v
    sf = c_pow(10.0, <double>(scale if scale > 0 else -scale))
    if scale > 0:
        return v / sf          # post-divide (ceil/floor/trunc already applied to v*sf)
    else:
        return v * sf          # post-multiply


cdef inline double _ceil_scaled(double x, int scale) nogil:
    cdef double sf
    if scale == 0:
        return c_ceil(x)
    sf = c_pow(10.0, <double>(scale if scale > 0 else -scale))
    if scale > 0:
        return c_ceil(x * sf) / sf
    else:
        return c_ceil(x / sf) * sf


cdef inline double _floor_scaled(double x, int scale) nogil:
    cdef double sf
    if scale == 0:
        return c_floor(x)
    sf = c_pow(10.0, <double>(scale if scale > 0 else -scale))
    if scale > 0:
        return c_floor(x * sf) / sf
    else:
        return c_floor(x / sf) * sf


cdef inline double _trunc_scaled(double x, int scale) nogil:
    cdef double sf
    if scale == 0:
        return c_trunc(x)
    sf = c_pow(10.0, <double>(scale if scale > 0 else -scale))
    if scale > 0:
        return c_trunc(x * sf) / sf
    else:
        return c_trunc(x / sf) * sf


# ---------------------------------------------------------------------------
# CEILING
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_ceil(object values, int scale=0):
    """CEILING(values [, scale]): element-wise ceiling with optional scale factor."""

    cdef size_t n = <size_t>len(values)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data

    cdef uint8_t*  in_null  = NULL
    cdef uint8_t*  out_null = NULL
    cdef Py_ssize_t i

    cdef double*   in_data   = NULL
    cdef int64_t*  in_data_i = NULL
    cdef Float64Vector fvals
    cdef Int64Vector   ivals

    cdef DictAccessor*  d_ptr    = NULL
    cdef DrakenVarBuffer* dict_buf
    cdef int   d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf   = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null    = <uint8_t*>d_ptr.row_nulls

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = _ceil_scaled((<double*>dict_buf.data)[code], scale)
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = _ceil_scaled(<double>((<float*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = _ceil_scaled(<double>((<int64_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = _ceil_scaled(<double>((<int32_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = _ceil_scaled(<double>((<int16_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = _ceil_scaled(<double>((<int8_t*>dict_buf.data)[code]), scale)
            else:
                out_data[i] = 0.0

    elif isinstance(values, Int64Vector):
        ivals    = <Int64Vector>values
        in_data_i = <int64_t*>ivals.ptr.data
        in_null   = <uint8_t*>ivals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _ceil_scaled(<double>in_data_i[i], scale)

    elif isinstance(values, Float64Vector):
        fvals   = <Float64Vector>values
        in_data = <double*>fvals.ptr.data
        in_null = <uint8_t*>fvals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _ceil_scaled(in_data[i], scale)

    else:
        raise TypeError(f"vector_ceil: unsupported vector type {type(values)}")

    return out_vec


# ---------------------------------------------------------------------------
# FLOOR
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_floor(object values, int scale=0):
    """FLOOR(values [, scale]): element-wise floor with optional scale factor."""

    cdef size_t n = <size_t>len(values)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data

    cdef uint8_t*  in_null  = NULL
    cdef uint8_t*  out_null = NULL
    cdef Py_ssize_t i

    cdef double*   in_data   = NULL
    cdef int64_t*  in_data_i = NULL
    cdef Float64Vector fvals
    cdef Int64Vector   ivals

    cdef DictAccessor*  d_ptr    = NULL
    cdef DrakenVarBuffer* dict_buf
    cdef int   d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf   = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null    = <uint8_t*>d_ptr.row_nulls

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = _floor_scaled((<double*>dict_buf.data)[code], scale)
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = _floor_scaled(<double>((<float*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = _floor_scaled(<double>((<int64_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = _floor_scaled(<double>((<int32_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = _floor_scaled(<double>((<int16_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = _floor_scaled(<double>((<int8_t*>dict_buf.data)[code]), scale)
            else:
                out_data[i] = 0.0

    elif isinstance(values, Int64Vector):
        ivals    = <Int64Vector>values
        in_data_i = <int64_t*>ivals.ptr.data
        in_null   = <uint8_t*>ivals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _floor_scaled(<double>in_data_i[i], scale)

    elif isinstance(values, Float64Vector):
        fvals   = <Float64Vector>values
        in_data = <double*>fvals.ptr.data
        in_null = <uint8_t*>fvals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _floor_scaled(in_data[i], scale)

    else:
        raise TypeError(f"vector_floor: unsupported vector type {type(values)}")

    return out_vec


# ---------------------------------------------------------------------------
# TRUNCATE
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_trunc(object values, int scale=0):
    """TRUNCATE(values [, scale]): element-wise truncation toward zero with optional scale."""

    cdef size_t n = <size_t>len(values)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data

    cdef uint8_t*  in_null  = NULL
    cdef uint8_t*  out_null = NULL
    cdef Py_ssize_t i

    cdef double*   in_data   = NULL
    cdef int64_t*  in_data_i = NULL
    cdef Float64Vector fvals
    cdef Int64Vector   ivals

    cdef DictAccessor*  d_ptr    = NULL
    cdef DrakenVarBuffer* dict_buf
    cdef int   d_val_type
    cdef uint32_t code

    if isinstance(values, Vector):
        d_ptr = (<Vector>values).dict_accessor()

    if d_ptr != NULL:
        dict_buf   = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null    = <uint8_t*>d_ptr.row_nulls

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = _trunc_scaled((<double*>dict_buf.data)[code], scale)
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = _trunc_scaled(<double>((<float*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = _trunc_scaled(<double>((<int64_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = _trunc_scaled(<double>((<int32_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = _trunc_scaled(<double>((<int16_t*>dict_buf.data)[code]), scale)
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = _trunc_scaled(<double>((<int8_t*>dict_buf.data)[code]), scale)
            else:
                out_data[i] = 0.0

    elif isinstance(values, Int64Vector):
        ivals    = <Int64Vector>values
        in_data_i = <int64_t*>ivals.ptr.data
        in_null   = <uint8_t*>ivals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _trunc_scaled(<double>in_data_i[i], scale)

    elif isinstance(values, Float64Vector):
        fvals   = <Float64Vector>values
        in_data = <double*>fvals.ptr.data
        in_null = <uint8_t*>fvals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = _trunc_scaled(in_data[i], scale)

    else:
        raise TypeError(f"vector_trunc: unsupported vector type {type(values)}")

    return out_vec


# ---------------------------------------------------------------------------
# POWER
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_power(object base_array, double exponent):
    """POWER(base, exponent): element-wise power, always returns Float64Vector.

    The exponent is a scalar double.  The caller is responsible for validating
    that all values in an exponent vector are identical and extracting [0].
    """

    cdef size_t n = <size_t>len(base_array)
    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data

    cdef uint8_t*  in_null  = NULL
    cdef uint8_t*  out_null = NULL
    cdef Py_ssize_t i

    cdef double*   in_data   = NULL
    cdef int64_t*  in_data_i = NULL
    cdef Float64Vector fvals
    cdef Int64Vector   ivals

    cdef DictAccessor*  d_ptr    = NULL
    cdef DrakenVarBuffer* dict_buf
    cdef int   d_val_type
    cdef uint32_t code

    if isinstance(base_array, Vector):
        d_ptr = (<Vector>base_array).dict_accessor()

    if d_ptr != NULL:
        dict_buf   = d_ptr.dict_values
        d_val_type = dict_buf.type
        in_null    = <uint8_t*>d_ptr.row_nulls

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            if d_ptr.code_width == 1:
                code = (<uint8_t*>d_ptr.codes)[i]
            elif d_ptr.code_width == 2:
                code = (<uint16_t*>d_ptr.codes)[i]
            else:
                code = (<uint32_t*>d_ptr.codes)[i]
            if d_val_type == DRAKEN_FLOAT64:
                out_data[i] = c_pow((<double*>dict_buf.data)[code], exponent)
            elif d_val_type == DRAKEN_FLOAT32:
                out_data[i] = c_pow(<double>((<float*>dict_buf.data)[code]), exponent)
            elif d_val_type == DRAKEN_INT64:
                out_data[i] = c_pow(<double>((<int64_t*>dict_buf.data)[code]), exponent)
            elif d_val_type == DRAKEN_INT32:
                out_data[i] = c_pow(<double>((<int32_t*>dict_buf.data)[code]), exponent)
            elif d_val_type == DRAKEN_INT16:
                out_data[i] = c_pow(<double>((<int16_t*>dict_buf.data)[code]), exponent)
            elif d_val_type == DRAKEN_INT8:
                out_data[i] = c_pow(<double>((<int8_t*>dict_buf.data)[code]), exponent)
            else:
                out_data[i] = 0.0

    elif isinstance(base_array, Int64Vector):
        ivals    = <Int64Vector>base_array
        in_data_i = <int64_t*>ivals.ptr.data
        in_null   = <uint8_t*>ivals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = c_pow(<double>in_data_i[i], exponent)

    elif isinstance(base_array, Float64Vector):
        fvals   = <Float64Vector>base_array
        in_data = <double*>fvals.ptr.data
        in_null = <uint8_t*>fvals.ptr.null_bitmap

        if in_null != NULL and n > 0:
            out_null = <uint8_t*>malloc((n + 7) >> 3)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, in_null, (n + 7) >> 3)
            out_vec.ptr.null_bitmap = out_null

        for i in range(n):
            if in_null != NULL and ((in_null[i >> 3] >> (i & 7)) & 1) == 0:
                out_data[i] = 0.0
                continue
            out_data[i] = c_pow(in_data[i], exponent)

    else:
        raise TypeError(f"vector_power: unsupported base type {type(base_array)}")

    return out_vec


# ---------------------------------------------------------------------------
# RANDOM()  —  uniform [0, 1)
# Uses the same xorshift32 / _rng_state defined in vector_random_string.pyx.
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_random(size_t n):
    """RANDOM(): generate n uniform random doubles in [0, 1)."""

    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data
    cdef unsigned int rv
    cdef Py_ssize_t i

    for i in range(n):
        rv = _xorshift32()
        out_data[i] = (rv & 0x7FFFFFFF) * (1.0 / 2147483648.0)

    return out_vec


# ---------------------------------------------------------------------------
# RANDOM_NORMAL()  —  standard normal via Box-Muller, reproducible seed
# ---------------------------------------------------------------------------

cpdef Float64Vector vector_random_normal(size_t n):
    """
    RANDOM_NORMAL(): generate n standard-normal doubles using Box-Muller.
    """

    cdef Float64Vector out_vec = Float64Vector(n)
    cdef double* out_data = <double*>out_vec.ptr.data

    cdef unsigned int rv1, rv2
    cdef double u1, u2, mag
    cdef double scale = 1.0 / 2147483648.0
    cdef Py_ssize_t i
    cdef Py_ssize_t pairs = <Py_ssize_t>(n >> 1)

    for i in range(pairs):
        rv1 = _xorshift32_normal()
        rv2 = _xorshift32_normal()
        u1 = (rv1 & 0x7FFFFFFF) * scale
        u2 = (rv2 & 0x7FFFFFFF) * scale
        if u1 < 1e-300:
            u1 = 1e-300
        mag = c_sqrt(-2.0 * c_log(u1))
        out_data[2 * i]     = mag * c_cos(6.283185307179586 * u2)
        out_data[2 * i + 1] = mag * c_sin(6.283185307179586 * u2)

    # Handle odd n: generate one more pair, keep the first value
    if n & 1:
        rv1 = _xorshift32_normal()
        rv2 = _xorshift32_normal()
        u1 = (rv1 & 0x7FFFFFFF) * scale
        u2 = (rv2 & 0x7FFFFFFF) * scale
        if u1 < 1e-300:
            u1 = 1e-300
        out_data[n - 1] = c_sqrt(-2.0 * c_log(u1)) * c_cos(6.283185307179586 * u2)

    return out_vec
