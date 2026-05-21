# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Sequence-to-Draken vector conversion helpers.

This module provides generic sequence and scalar conversion helpers that are not
specific to Arrow. Arrow-specific conversion remains in the Arrow interop module.

The primary entry point is vector_from_sequence(), which accepts:
  - Typed C-contiguous memoryviews (int64, float64, uint8) — zero-copy wrap
  - Python lists/tuples of a uniform type — builds an owned Draken Vector
  - Scalar / constant sequences — delegates to scalar_constructors

Supported element types for Python list input:
  - str / bytes                   -> StringVector  (via StringVectorBuilder)
  - int (non-bool)                -> Integer64Vector   (with optional null bitmap)
  - float                         -> Float64Vector (with optional null bitmap)
  - decimal.Decimal               -> DecimalVector (with optional null bitmap)
  - bool                          -> Integer64Vector   (treated as 0/1)

The dtype hint (OrsoTypes enum or plain string) is used for dispatch when
the list contains only None values, or when type sniffing is ambiguous.
"""

from libc.stdint cimport int8_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc
from libc.string cimport memset

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType, DRAKEN_INT64
from draken.core.buffers cimport draken_vector_from_dense
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.bool_vector cimport from_sequence as bool_from_sequence
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.float64_vector cimport from_sequence as float64_from_sequence
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.integer64_vector cimport from_sequence as int64_from_sequence
from draken.vectors.decimal_vector cimport DecimalVector
from draken.vectors.scalar_constructors cimport from_sequence as constant_from_sequence
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from draken.vectors.vector_vector cimport from_float_pylist


# ---------------------------------------------------------------------------
# Private helpers: build owned Draken Vectors from Python lists
# ---------------------------------------------------------------------------

cdef Integer64Vector _int64_from_pylist(list data):
    """
    Build an Integer64Vector from a Python list that may contain None values.

    Allocates an owned DrakenFixedBuffer. If any element is None, also
    allocates a null bitmap (Arrow convention: 1 = valid, 0 = null).
    The bitmap and data buffer are both owned by the returned vector and
    will be freed by free_fixed_buffer() in __dealloc__.
    """
    cdef Py_ssize_t n = len(data)
    cdef Integer64Vector vec = Integer64Vector(n)
    cdef int64_t* data_ptr = <int64_t*> vec.ptr.data
    cdef uint8_t* null_bm = NULL
    cdef Py_ssize_t i, nb_size
    cdef object item

    for i in range(n):
        item = data[i]
        if item is None:
            if null_bm == NULL:
                nb_size = (n + 7) >> 3
                null_bm = <uint8_t*> malloc(nb_size)
                if null_bm == NULL:
                    raise MemoryError()
                # Initialise all bits to 1 (all valid), then clear nulls below
                memset(null_bm, 0xFF, nb_size)
                vec.ptr.null_bitmap = null_bm
            # Clear bit i -> mark row as null (1=valid, 0=null)
            null_bm[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
            data_ptr[i] = 0
        else:
            data_ptr[i] = <int64_t> int(item)

    return vec


cdef Float64Vector _float64_from_pylist(list data):
    """
    Build a Float64Vector from a Python list that may contain None values.

    Same ownership semantics as _int64_from_pylist.
    """
    cdef Py_ssize_t n = len(data)
    cdef Float64Vector vec = Float64Vector(n)
    cdef double* data_ptr = <double*> vec.ptr.data
    cdef uint8_t* null_bm = NULL
    cdef Py_ssize_t i, nb_size
    cdef object item

    for i in range(n):
        item = data[i]
        if item is None:
            if null_bm == NULL:
                nb_size = (n + 7) >> 3
                null_bm = <uint8_t*> malloc(nb_size)
                if null_bm == NULL:
                    raise MemoryError()
                memset(null_bm, 0xFF, nb_size)
                vec.ptr.null_bitmap = null_bm
            null_bm[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
            data_ptr[i] = 0.0
        else:
            data_ptr[i] = <double> float(item)

    return vec


cdef StringVector _string_from_pylist(list data):
    """
    Build a StringVector from a Python list of str, bytes, or None values.

    Uses StringVectorBuilder with a resizable backing buffer (estimate 16
    bytes per row). str values are UTF-8 encoded; bytes values are stored
    as-is.
    """
    cdef Py_ssize_t n = len(data)
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 16)
    cdef Py_ssize_t i
    cdef object item

    for i in range(n):
        item = data[i]
        if item is None:
            builder.append_null()
        elif isinstance(item, bytes):
            builder.append(item)
        else:
            builder.append(str(item).encode('utf-8'))

    return builder.finish()


cdef DecimalVector _decimal_from_pylist(list data):
    """
    Build a DecimalVector from a Python list of decimal.Decimal or None.

    Scale is determined by a first pass over all non-None elements, taking
    the maximum scale found. Precision is stored as 18 (the int64 maximum).
    """
    import decimal as _decimal

    cdef Py_ssize_t n = len(data)
    cdef DecimalVector vec = DecimalVector(n)
    cdef int64_t* data_ptr = <int64_t*> vec.ptr.data
    cdef uint8_t* null_bm = NULL
    cdef Py_ssize_t i, nb_size
    cdef object item
    cdef int scale = 0, item_scale
    cdef object sign, digits, exp_obj
    cdef object multiplier

    # First pass: determine the maximum scale across all non-None values
    for i in range(n):
        item = data[i]
        if item is None:
            continue
        if not isinstance(item, _decimal.Decimal):
            item = _decimal.Decimal(str(item))
        sign, digits, exp_obj = item.as_tuple()
        item_scale = max(0, -int(exp_obj))
        if item_scale > scale:
            scale = item_scale

    vec._scale = <int8_t> min(scale, 18)
    vec._precision = <int8_t> 18
    multiplier = _decimal.Decimal(10) ** scale

    # Second pass: convert values to scaled int64
    for i in range(n):
        item = data[i]
        if item is None:
            if null_bm == NULL:
                nb_size = (n + 7) >> 3
                null_bm = <uint8_t*> malloc(nb_size)
                if null_bm == NULL:
                    raise MemoryError()
                memset(null_bm, 0xFF, nb_size)
                vec.ptr.null_bitmap = null_bm
            null_bm[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
            data_ptr[i] = 0
        else:
            if not isinstance(item, _decimal.Decimal):
                item = _decimal.Decimal(str(item))
            data_ptr[i] = <int64_t> int(item * multiplier)

    vec.ptr.null_bitmap = null_bm
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>n, DRAKEN_INT64, null_bm)
    return vec


# ---------------------------------------------------------------------------
# dtype hint helpers
# ---------------------------------------------------------------------------

cdef str _resolve_dtype_str(object dtype):
    """
    Return an uppercase string identifying the dtype, or '' if unknown.

    Handles:
      - OrsoTypes enum members (have a .value attribute that is a plain str)
      - Plain strings (e.g. "VARCHAR", "INTEGER")
      - Anything else returns ''
    """
    if dtype is None:
        return ''
    # OrsoTypes (and any Enum with a string .value)
    cdef object val = getattr(dtype, 'value', None)
    if val is not None and isinstance(val, str):
        return (<str> val).upper()
    # Plain string dtype
    if isinstance(dtype, str):
        return (<str> dtype).upper()
    return ''


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

cpdef object vector_from_sequence(object data, object dtype=None):
    """
    Create a Draken Vector from a typed memoryview or Python sequence.

    Resolution order
    ----------------
    1. Typed C-contiguous memoryviews (int64, float64, uint8/bool) — zero-copy.
    2. Constant-vector fast path via scalar_constructors.from_sequence().
    3. Python list / tuple with a known dtype hint — dispatch to the
       appropriate native builder (_int64_from_pylist, _float64_from_pylist,
       _string_from_pylist, _decimal_from_pylist).
    4. Python list / tuple — sniff element type from the first non-None value
       and dispatch to the appropriate builder.
    5. Return data unchanged (scalar, unsupported type, etc.).

    Args:
        data:  int64[::1], double[::1], uint8[::1] typed memoryview,
               Python list/tuple, or any scalar.
        dtype: Optional OrsoTypes (or plain uppercase string) hint for type
               resolution when the list is empty or all-None.

    Returns:
        An appropriate Draken Vector subclass, or the original data if no
        conversion is possible.
    """
    cdef int64_t[::1] int64_view
    cdef double[::1] float64_view
    cdef uint8_t[::1] bool_view
    cdef Py_ssize_t n
    cdef object first_val
    cdef str dtype_val
    cdef object item

    # ------------------------------------------------------------------
    # 1. Typed memoryview fast paths (zero-copy wrapping)
    # ------------------------------------------------------------------
    try:
        int64_view = data
        return int64_from_sequence(int64_view)
    except (TypeError, ValueError, BufferError):
        pass

    try:
        float64_view = data
        return float64_from_sequence(float64_view)
    except (TypeError, ValueError, BufferError):
        pass

    try:
        bool_view = data
        return bool_from_sequence(bool_view)
    except (TypeError, ValueError, BufferError):
        pass

    # ------------------------------------------------------------------
    # 2. Constant-vector fast path (all elements equal)
    # ------------------------------------------------------------------
    const_vec = constant_from_sequence(data, dtype)
    if const_vec is not None:
        return const_vec

    # ------------------------------------------------------------------
    # 3 & 4. Python list / tuple — native builder dispatch
    # ------------------------------------------------------------------
    if isinstance(data, (list, tuple)):
        data = list(data) if isinstance(data, tuple) else data
        n = len(data)

        if n == 0:
            # Return a proper empty typed Vector — never return a raw Python list,
            # since downstream C code will try to dereference it as a Vector ptr.
            dtype_val = _resolve_dtype_str(dtype)
            if dtype_val in ('VARCHAR', 'BLOB', 'STRING', 'BINARY', 'LARGE_STRING', 'LARGE_BINARY'):
                return StringVectorBuilder.with_estimate(0, 1).finish()
            if dtype_val in ('DOUBLE', 'FLOAT', 'FLOAT32', 'FLOAT64'):
                return Float64Vector(0)
            if dtype_val == 'DECIMAL':
                return DecimalVector(0)
            # INTEGER, BOOLEAN, unknown, or no hint — Integer64Vector is a safe default
            return Integer64Vector(0)

        # -- Dispatch by dtype hint --
        dtype_val = _resolve_dtype_str(dtype)

        if dtype_val in ('VARCHAR', 'BLOB', 'STRING', 'BINARY', 'LARGE_STRING', 'LARGE_BINARY'):
            return _string_from_pylist(data)

        if dtype_val in ('INTEGER', 'INT', 'INT8', 'INT16', 'INT32', 'INT64',
                         'UINT8', 'UINT16', 'UINT32', 'UINT64', 'BIGINT', 'BOOLEAN'):
            return _int64_from_pylist(data)

        if dtype_val in ('DOUBLE', 'FLOAT', 'FLOAT32', 'FLOAT64'):
            return _float64_from_pylist(data)

        if dtype_val == 'DECIMAL':
            return _decimal_from_pylist(data)

        if dtype_val == 'VECTOR':
            return from_float_pylist(data)

        # -- Sniff element type from first non-None value --
        import decimal as _decimal

        first_val = None
        for item in data:
            if item is not None:
                first_val = item
                break

        if first_val is None:
            # All elements are None — return a typed null-valued constant vector.
            # Without a dtype hint, default to Integer64Vector (consistent with the
            # empty-list path above). Returning the raw list here is unsafe:
            # downstream Cython code will treat it as a Vector and segfault.
            dtype_val = _resolve_dtype_str(dtype)
            if dtype_val in ('VARCHAR', 'BLOB', 'STRING', 'BINARY', 'LARGE_STRING', 'LARGE_BINARY'):
                return StringVector.from_constant(b"", n, is_null=True)
            if dtype_val in ('DOUBLE', 'FLOAT', 'FLOAT32', 'FLOAT64'):
                return Float64Vector.from_constant(0.0, n, is_null=True)
            return Integer64Vector.from_constant(0, n, is_null=True)

        # bool must be checked before int (bool is a subclass of int)
        if isinstance(first_val, bool):
            return _int64_from_pylist(data)

        if isinstance(first_val, int):
            return _int64_from_pylist(data)

        if isinstance(first_val, float):
            return _float64_from_pylist(data)

        if isinstance(first_val, (str, bytes)):
            return _string_from_pylist(data)

        if isinstance(first_val, _decimal.Decimal):
            return _decimal_from_pylist(data)

        # Unknown element type — return raw list unchanged
        return data

    # ------------------------------------------------------------------
    # 5. Scalar / unsupported — return as-is
    # ------------------------------------------------------------------
    return data


cpdef BoolVector bool_vector_from_uint64_eq(uint64_t[::1] hashes, uint64_t target):
    """
    Compare a buffer of 64-bit hashes against a single scalar target and
    return a BoolVector of equality results (no nulls, length == hashes.shape[0]).

    This is the kernel for the multi-equals hash dispatch fast-path in
    evaluate_draken: row_hashes equals target_hash → row matches.
    """
    cdef Py_ssize_t n = hashes.shape[0]
    cdef BoolVector vec = BoolVector(<size_t> n)
    cdef uint8_t* dst
    cdef Py_ssize_t i
    cdef uint8_t bit

    if n == 0:
        return vec

    dst = <uint8_t*> vec.ptr.data
    # BoolVector(n) zero-fills the data buffer, so we only set bits where eq.
    for i in range(n):
        if hashes[i] == target:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))

    return vec


cpdef DrakenType sequence_type_to_draken(object dtype):
    """
    Convert a generic sequence-oriented dtype hint to a DrakenType enum.

    Currently a stub; returns DRAKEN_NON_NATIVE for all inputs.  Extend
    this function when callers need the Draken type tag at planning time.
    """
    if dtype is None:
        return DrakenType.DRAKEN_NON_NATIVE
    return DrakenType.DRAKEN_NON_NATIVE
