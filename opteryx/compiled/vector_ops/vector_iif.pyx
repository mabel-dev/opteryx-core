# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Vectorized IIF(condition, when_true, when_false).

Architectural contract (strict, no exceptions):
- All three parameters MUST be Draken Vectors of identical length.
- condition MUST be a BoolVector (constant-encoded BOOL Vectors are accepted).
- when_true and when_false MUST share a Draken type family:
    * matching fixed-width types, OR
    * both BOOL, OR
    * both STRING.
- Constant-encoded Vectors (e.g. via from_scalar) are accepted on any branch.
- Any contract violation raises TypeError or ValueError immediately.

Shared bitmap / type-classification / vector-construction helpers live in
_helper_select.pyx and are prefixed with `_sel_`.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DrakenFixedBuffer,
    DrakenVector,
)
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport (
    StringVector,
    StringVectorBuilder,
    _StringVectorView,
)
from draken.vectors.vector cimport Vector


# ---------------------------------------------------------------------------
# Condition probe (IIF-specific)
# ---------------------------------------------------------------------------

cdef inline bint _iif_condition_at(BoolVector condition, Py_ssize_t row) noexcept:
    """SQL semantics: NULL condition treated as FALSE."""
    if not _sel_is_valid(condition.ptr.null_bitmap, row):
        return False
    return _sel_bit_is_set(<uint8_t*>condition.ptr.data, row)


# ---------------------------------------------------------------------------
# Inner kernels
# ---------------------------------------------------------------------------

cdef object _iif_select_fixed(
    BoolVector condition,
    Vector when_true,
    Vector when_false,
    Py_ssize_t length,
    int output_type,
):
    cdef object result = _sel_new_fixed_vector(output_type, length, when_true)
    cdef DrakenFixedBuffer* out_ptr = _sel_fixed_ptr(<Vector>result)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef bint choose_true

    cdef DrakenVector* true_uv = when_true.unified()
    cdef DrakenVector* false_uv = when_false.unified()
    cdef bint true_is_const = true_uv.data_length == 1
    cdef bint false_is_const = false_uv.data_length == 1
    cdef bint true_const_null = true_is_const and (true_uv.validity != NULL)
    cdef bint false_const_null = false_is_const and (false_uv.validity != NULL)

    cdef DrakenFixedBuffer* true_ptr = NULL
    cdef DrakenFixedBuffer* false_ptr = NULL
    if not true_is_const:
        true_ptr = _sel_fixed_ptr(when_true)
    if not false_is_const:
        false_ptr = _sel_fixed_ptr(when_false)

    cdef object true_scalar = None
    cdef object false_scalar = None
    if true_is_const and not true_const_null:
        true_scalar = _sel_const_scalar(when_true)
    if false_is_const and not false_const_null:
        false_scalar = _sel_const_scalar(when_false)

    cdef char* out_data = <char*>out_ptr.data
    cdef char* source_data

    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        choose_true = _iif_condition_at(condition, row)
        if choose_true:
            if true_is_const:
                if true_const_null:
                    any_null = True
                    continue
                _sel_write_fixed_scalar(out_ptr, row, output_type, true_scalar)
            else:
                if not _sel_is_valid(true_ptr.null_bitmap, row):
                    any_null = True
                    continue
                source_data = <char*>true_ptr.data
                memcpy(
                    out_data + (row * out_ptr.itemsize),
                    source_data + (row * true_ptr.itemsize),
                    out_ptr.itemsize,
                )
        else:
            if false_is_const:
                if false_const_null:
                    any_null = True
                    continue
                _sel_write_fixed_scalar(out_ptr, row, output_type, false_scalar)
            else:
                if not _sel_is_valid(false_ptr.null_bitmap, row):
                    any_null = True
                    continue
                source_data = <char*>false_ptr.data
                memcpy(
                    out_data + (row * out_ptr.itemsize),
                    source_data + (row * false_ptr.itemsize),
                    out_ptr.itemsize,
                )
        if out_null != NULL:
            _sel_set_true_bit(out_null, row)

    if out_null == NULL or not any_null:
        if out_null != NULL:
            free(out_null)
        out_ptr.null_bitmap = NULL
    else:
        out_ptr.null_bitmap = out_null

    return result


cdef object _iif_select_bool(
    BoolVector condition,
    Vector when_true,
    Vector when_false,
    Py_ssize_t length,
):
    cdef BoolVector result = BoolVector(length)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    cdef uint8_t* out_null = NULL
    cdef Py_ssize_t row
    cdef bint choose_true
    cdef bint any_null = False
    cdef bint value

    cdef DrakenVector* true_uv = when_true.unified()
    cdef DrakenVector* false_uv = when_false.unified()
    cdef bint true_is_const = true_uv.data_length == 1
    cdef bint false_is_const = false_uv.data_length == 1
    cdef bint true_const_null = true_is_const and (true_uv.validity != NULL)
    cdef bint false_const_null = false_is_const and (false_uv.validity != NULL)

    cdef bint true_const_val = False
    cdef bint false_const_val = False
    if true_is_const and not true_const_null:
        true_const_val = (<uint8_t*>true_uv.data)[0] != 0
    if false_is_const and not false_const_null:
        false_const_val = (<uint8_t*>false_uv.data)[0] != 0

    cdef BoolVector true_vec
    cdef BoolVector false_vec
    if not true_is_const:
        true_vec = <BoolVector>when_true
    if not false_is_const:
        false_vec = <BoolVector>when_false

    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        choose_true = _iif_condition_at(condition, row)
        if choose_true:
            if true_is_const:
                if true_const_null:
                    any_null = True
                    continue
                value = true_const_val
            else:
                if not _sel_is_valid(true_vec.ptr.null_bitmap, row):
                    any_null = True
                    continue
                value = _sel_bit_is_set(<uint8_t*>true_vec.ptr.data, row)
        else:
            if false_is_const:
                if false_const_null:
                    any_null = True
                    continue
                value = false_const_val
            else:
                if not _sel_is_valid(false_vec.ptr.null_bitmap, row):
                    any_null = True
                    continue
                value = _sel_bit_is_set(<uint8_t*>false_vec.ptr.data, row)

        if value:
            _sel_set_true_bit(out_bits, row)
        if out_null != NULL:
            _sel_set_true_bit(out_null, row)

    if out_null == NULL or not any_null:
        if out_null != NULL:
            free(out_null)
        result.ptr.null_bitmap = NULL
    else:
        result.ptr.null_bitmap = out_null

    return result


cdef object _iif_select_string(
    BoolVector condition,
    Vector when_true,
    Vector when_false,
    Py_ssize_t length,
):
    cdef DrakenVector* true_uv = when_true.unified()
    cdef DrakenVector* false_uv = when_false.unified()
    # ptr.offsets == NULL iff constant (alloc_var_buffer always allocates offsets for dense/dict)
    cdef bint true_is_const = (<StringVector>when_true).ptr.offsets == NULL
    cdef bint false_is_const = (<StringVector>when_false).ptr.offsets == NULL
    cdef bint true_const_null = true_is_const and (true_uv.validity != NULL)
    cdef bint false_const_null = false_is_const and (false_uv.validity != NULL)

    cdef object true_scalar = None
    cdef object false_scalar = None
    if true_is_const and not true_const_null:
        true_scalar = _sel_const_scalar(when_true)
    if false_is_const and not false_const_null:
        false_scalar = _sel_const_scalar(when_false)

    cdef StringVector true_vec
    cdef StringVector false_vec
    cdef _StringVectorView true_view
    cdef _StringVectorView false_view
    if not true_is_const:
        true_vec = <StringVector>when_true
        true_view = <_StringVectorView>true_vec.view()
    if not false_is_const:
        false_vec = <StringVector>when_false
        false_view = <_StringVectorView>false_vec.view()

    cdef Py_ssize_t row
    cdef Py_ssize_t total_bytes = 0
    cdef bint choose_true
    cdef Py_ssize_t value_len
    cdef const char* value_ptr
    cdef StringVectorBuilder builder

    # Pass 1: total byte budget for the builder.
    for row in range(length):
        choose_true = _iif_condition_at(condition, row)
        if choose_true:
            if true_is_const:
                if not true_const_null:
                    total_bytes += len(<bytes>true_scalar)
            else:
                if not true_view.is_null(row):
                    total_bytes += true_view.value_len(row)
        else:
            if false_is_const:
                if not false_const_null:
                    total_bytes += len(<bytes>false_scalar)
            else:
                if not false_view.is_null(row):
                    total_bytes += false_view.value_len(row)

    builder = StringVectorBuilder(length, total_bytes, False, True)

    # Pass 2: emit values.
    for row in range(length):
        choose_true = _iif_condition_at(condition, row)
        if choose_true:
            if true_is_const:
                if true_const_null:
                    builder.append_null()
                else:
                    builder.append(<bytes>true_scalar)
            else:
                if true_view.is_null(row):
                    builder.append_null()
                else:
                    value_len = true_view.value_len(row)
                    value_ptr = <const char*>true_view.value_ptr(row)
                    builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)
        else:
            if false_is_const:
                if false_const_null:
                    builder.append_null()
                else:
                    builder.append(<bytes>false_scalar)
            else:
                if false_view.is_null(row):
                    builder.append_null()
                else:
                    value_len = false_view.value_len(row)
                    value_ptr = <const char*>false_view.value_ptr(row)
                    builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)

    return builder.finish()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

cpdef Vector vector_iif(
    Vector condition,
    Vector when_true,
    Vector when_false,
):
    """Row-wise selection with SQL IIF semantics.

    Strict contract — see module docstring. Raises TypeError or ValueError
    immediately on any contract violation. No coercion, no fallbacks.
    """
    if condition is None or when_true is None or when_false is None:
        raise TypeError(
            "vector_iif: arguments must be Draken Vectors, got None"
        )

    cdef Py_ssize_t length = len(condition)
    cdef DrakenVector* cond_uv
    cdef BoolVector cond_vec
    cdef int true_family
    cdef int false_family
    cdef int true_fixed
    cdef int false_fixed

    if len(when_true) != length:
        raise ValueError(
            f"vector_iif: when_true length {len(when_true)} does not match "
            f"condition length {length}"
        )
    if len(when_false) != length:
        raise ValueError(
            f"vector_iif: when_false length {len(when_false)} does not match "
            f"condition length {length}"
        )

    # condition must be BoolVector or BOOL-typed constant Vector.
    if isinstance(condition, BoolVector):
        cond_vec = <BoolVector>condition
    else:
        cond_uv = condition.unified()
        if cond_uv.data_length != 1 or cond_uv.type != DRAKEN_BOOL:
            raise TypeError(
                f"vector_iif: condition must be BoolVector, got "
                f"{type(condition).__name__}"
            )
        # Const condition short-circuits — every row picks the same branch.
        # Null condition is treated as FALSE (SQL three-valued logic).
        if cond_uv.validity != NULL or (<uint8_t*>cond_uv.data)[0] == 0:
            return when_false
        return when_true

    # Non-const condition: dispatch by branch type family.
    true_family = _sel_bool_family(when_true)
    false_family = _sel_bool_family(when_false)
    if true_family == DRAKEN_BOOL and false_family == DRAKEN_BOOL:
        return <Vector>_iif_select_bool(cond_vec, when_true, when_false, length)
    if true_family == DRAKEN_BOOL or false_family == DRAKEN_BOOL:
        raise TypeError(
            f"vector_iif: branch type mismatch (BOOL vs other) — "
            f"when_true={type(when_true).__name__}, "
            f"when_false={type(when_false).__name__}"
        )

    true_family = _sel_string_family(when_true)
    false_family = _sel_string_family(when_false)
    if true_family == DRAKEN_STRING and false_family == DRAKEN_STRING:
        return <Vector>_iif_select_string(cond_vec, when_true, when_false, length)
    if true_family == DRAKEN_STRING or false_family == DRAKEN_STRING:
        raise TypeError(
            f"vector_iif: branch type mismatch (STRING vs other) — "
            f"when_true={type(when_true).__name__}, "
            f"when_false={type(when_false).__name__}"
        )

    true_fixed = _sel_fixed_family(when_true)
    false_fixed = _sel_fixed_family(when_false)
    if true_fixed != -1 and false_fixed != -1:
        if true_fixed != false_fixed:
            raise TypeError(
                f"vector_iif: fixed-width branch type mismatch — "
                f"when_true type {true_fixed}, when_false type {false_fixed}"
            )
        return <Vector>_iif_select_fixed(
            cond_vec, when_true, when_false, length, true_fixed,
        )

    raise TypeError(
        f"vector_iif: unsupported branch types — "
        f"when_true={type(when_true).__name__}, "
        f"when_false={type(when_false).__name__}"
    )
