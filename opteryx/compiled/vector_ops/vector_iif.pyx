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
- condition MUST be a BoolVector.
- when_true and when_false MUST share a Draken type family:
    * matching fixed-width types, OR
    * both BOOL, OR
    * both STRING.
- Vectors of any layout (dense, constant, dict) are accepted; access is uniform via the unified view.
- Any contract violation raises TypeError or ValueError immediately.

Shared bitmap / type-classification / vector-construction helpers live in
_helper_select.pyx and are prefixed with `_sel_`.
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DrakenFixedBuffer,
    DrakenVector,
)
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport (
    StringVector,
    StringVectorBuilder,
)
from draken.vectors.vector cimport Vector


# ---------------------------------------------------------------------------
# Condition probe (IIF-specific)
# ---------------------------------------------------------------------------

cdef inline bint _iif_condition_at(
    const uint8_t* data,
    const uint32_t* selection,
    const uint8_t* validity,
    Py_ssize_t row,
) noexcept nogil:
    """SQL semantics: NULL condition treated as FALSE.

    Reads through the unified view: bit at index ``selection[row]`` of
    the packed data buffer.
    """
    cdef uint32_t code
    if validity != NULL and not _sel_bit_is_set(<uint8_t*>validity, row):
        return False
    code = selection[row]
    return ((data[code >> 3] >> (code & 7)) & 1) != 0


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
    cdef Py_ssize_t itemsize = out_ptr.itemsize
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row

    cdef DrakenVector* cond_uv  = condition.unified()
    cdef DrakenVector* true_uv  = when_true.unified()
    cdef DrakenVector* false_uv = when_false.unified()
    cdef const uint8_t*  cond_data  = <const uint8_t*>cond_uv.data
    cdef const uint32_t* cond_sel   = cond_uv.selection
    cdef const uint8_t*  cond_valid = cond_uv.validity

    cdef char* out_data = <char*>out_ptr.data
    cdef DrakenVector* src_uv
    cdef bint choose_true

    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        choose_true = _iif_condition_at(cond_data, cond_sel, cond_valid, row)
        src_uv = true_uv if choose_true else false_uv

        if not _sel_is_valid(src_uv.validity, row):
            any_null = True
            continue

        memcpy(
            out_data + row * itemsize,
            <char*>src_uv.data + src_uv.selection[row] * itemsize,
            itemsize,
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
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef bint choose_true
    cdef bint value
    cdef uint32_t code

    cdef DrakenVector* cond_uv  = condition.unified()
    cdef DrakenVector* true_uv  = when_true.unified()
    cdef DrakenVector* false_uv = when_false.unified()
    cdef const uint8_t*  cond_data  = <const uint8_t*>cond_uv.data
    cdef const uint32_t* cond_sel   = cond_uv.selection
    cdef const uint8_t*  cond_valid = cond_uv.validity
    cdef DrakenVector* src_uv

    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        choose_true = _iif_condition_at(cond_data, cond_sel, cond_valid, row)
        src_uv = true_uv if choose_true else false_uv

        if not _sel_is_valid(src_uv.validity, row):
            any_null = True
            continue

        code = src_uv.selection[row]
        value = ((<uint8_t*>src_uv.data)[code >> 3] >> (code & 7)) & 1

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
    cdef DrakenVector*   cond_uv     = condition.unified()
    cdef const uint8_t*  cond_data   = <const uint8_t*>cond_uv.data
    cdef const uint32_t* cond_sel    = cond_uv.selection
    cdef const uint8_t*  cond_valid  = cond_uv.validity

    cdef DrakenVector*      true_uv  = (<StringVector>when_true).unified()
    cdef DrakenVector*      false_uv = (<StringVector>when_false).unified()
    cdef DrakenStringArena* true_arena  = <DrakenStringArena*>true_uv.data
    cdef DrakenStringArena* false_arena = <DrakenStringArena*>false_uv.data
    cdef const uint32_t*    true_sel  = <const uint32_t*>true_uv.selection
    cdef const uint32_t*    false_sel = <const uint32_t*>false_uv.selection
    cdef const uint8_t*     true_nulls  = true_uv.validity
    cdef const uint8_t*     false_nulls = false_uv.validity

    cdef Py_ssize_t      row
    cdef Py_ssize_t      total_bytes = 0
    cdef bint            choose_true
    cdef Py_ssize_t      value_len
    cdef const char*     value_ptr
    cdef DrakenStringSlot* slot
    cdef StringVectorBuilder builder

    # Pass 1: total byte budget for the builder.
    for row in range(length):
        choose_true = _iif_condition_at(cond_data, cond_sel, cond_valid, row)
        if choose_true:
            if true_nulls == NULL or _sel_bit_is_set(<uint8_t*>true_nulls, row):
                slot = &true_arena.slots[true_sel[row]]
                total_bytes += <Py_ssize_t>str_length(slot)
        else:
            if false_nulls == NULL or _sel_bit_is_set(<uint8_t*>false_nulls, row):
                slot = &false_arena.slots[false_sel[row]]
                total_bytes += <Py_ssize_t>str_length(slot)

    builder = StringVectorBuilder(length, total_bytes, False, True)

    # Pass 2: emit values.
    for row in range(length):
        choose_true = _iif_condition_at(cond_data, cond_sel, cond_valid, row)
        if choose_true:
            if true_nulls != NULL and not _sel_bit_is_set(<uint8_t*>true_nulls, row):
                builder.append_null()
            else:
                slot      = &true_arena.slots[true_sel[row]]
                value_len = <Py_ssize_t>str_length(slot)
                value_ptr = <const char*>str_data(slot, true_arena.arena)
                builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)
        else:
            if false_nulls != NULL and not _sel_bit_is_set(<uint8_t*>false_nulls, row):
                builder.append_null()
            else:
                slot      = &false_arena.slots[false_sel[row]]
                value_len = <Py_ssize_t>str_length(slot)
                value_ptr = <const char*>str_data(slot, false_arena.arena)
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

    if not isinstance(condition, BoolVector):
        raise TypeError(
            f"vector_iif: condition must be BoolVector, got "
            f"{type(condition).__name__}"
        )
    cond_vec = <BoolVector>condition

    # Dispatch by branch type family.
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
