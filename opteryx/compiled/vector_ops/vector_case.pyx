# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Vectorized CASE WHEN ... THEN ... [WHEN ... THEN ...] [ELSE ...] END.

Architectural contract (strict, no exceptions):
- Two parallel sequences are required: `conditions` and `values`.
- `len(conditions) == len(values)` and both have at least one element.
- All conditions MUST be Draken Vectors of identical length.
- All conditions MUST be in the BOOL family (BoolVector or BOOL-typed
  constant Vectors).
- All values MUST be Draken Vectors of identical length matching the
  conditions.
- All values MUST share the same Draken type family:
    * matching fixed-width types, OR
    * all BOOL, OR
    * all STRING.
- Constant-encoded Vectors (e.g. via from_scalar) are accepted on either
  conditions or values.
- Any contract violation raises TypeError or ValueError immediately.

Semantics:
- For each row, scans `(conditions[i], values[i])` pairs left-to-right.
- The first row index `i` where `conditions[i]` is TRUE selects
  `values[i]` for that row.
- A NULL condition is treated as FALSE (SQL three-valued logic).
- If no condition matches, the output row is NULL.
- The selected `values[i]` may itself be NULL — in that case the output
  row is NULL.

The Python entry point validates inputs and pre-extracts raw buffer pointers
into C arrays. The inner kernels are pure C: typed Vector returns, no
Python lists, no Python scalar intermediates.

Shared bitmap / type-classification / vector-construction helpers live in
_helper_select.pyx and are prefixed with `_sel_`.
"""

from libc.stdint cimport int8_t, int32_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    ConstAccessor,
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DrakenConstantStringPayload,
    DrakenFixedBuffer,
)
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport (
    StringVector,
    StringVectorBuilder,
    _StringVectorView,
)
from draken.vectors.vector cimport Vector


# ---------------------------------------------------------------------------
# Condition probe — branch is selected only when condition is non-null TRUE.
# Const conditions short-circuit at the dispatch layer; this helper handles
# the per-row case for non-const BoolVectors.
# ---------------------------------------------------------------------------

cdef inline bint _case_cond_true_at(
    DrakenFixedBuffer* cond_ptr,
    Py_ssize_t row,
) noexcept nogil:
    """SQL semantics: NULL condition treated as FALSE."""
    if not _sel_is_valid(cond_ptr.null_bitmap, row):
        return False
    return _sel_bit_is_set(<uint8_t*>cond_ptr.data, row)


# ---------------------------------------------------------------------------
# Fixed-width inner kernel — pure C
# ---------------------------------------------------------------------------

cdef Vector _case_fixed_kernel(
    int8_t* cond_const_state,
    DrakenFixedBuffer** cond_bv_ptrs,
    ConstAccessor** val_const_accs,
    DrakenFixedBuffer** val_src_ptrs,
    Py_ssize_t n_pairs,
    Py_ssize_t length,
    int output_type,
    Vector template,
):
    """Pure C inner loop for fixed-width value branches.

    cond_const_state[i] encodes the condition's compile-time state:
        0 = non-const (use cond_bv_ptrs[i] for per-row evaluation)
        1 = const TRUE  (this branch always matches)
        2 = const FALSE/NULL (this branch never matches)
    Pairs with state == 2 are pruned at the dispatch layer; this kernel
    sees only states 0 and 1.

    val_const_accs[i] is non-NULL iff value i is constant-encoded;
    val_src_ptrs[i] is non-NULL iff value i is a regular fixed-width
    buffer. Exactly one of (val_const_accs[i], val_src_ptrs[i]) is non-NULL.
    """
    cdef Vector result = _sel_new_fixed_vector(output_type, length, template)
    cdef DrakenFixedBuffer* out_ptr = _sel_fixed_ptr(result)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef Py_ssize_t pair_idx
    cdef bint matched
    cdef bint val_is_null
    cdef ConstAccessor* val_acc
    cdef DrakenFixedBuffer* val_ptr
    cdef DrakenFixedBuffer* cond_ptr
    cdef char* out_data = <char*>out_ptr.data
    cdef Py_ssize_t itemsize = out_ptr.itemsize

    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        matched = False
        val_is_null = False
        for pair_idx in range(n_pairs):
            if cond_const_state[pair_idx] == 0:
                cond_ptr = cond_bv_ptrs[pair_idx]
                if not _case_cond_true_at(cond_ptr, row):
                    continue
            # state == 1: const TRUE — always matches.

            val_acc = val_const_accs[pair_idx]
            if val_acc != NULL:
                if val_acc.is_null != 0:
                    val_is_null = True
                else:
                    memcpy(
                        out_data + row * itemsize,
                        val_acc.value_ptr,
                        itemsize,
                    )
            else:
                val_ptr = val_src_ptrs[pair_idx]
                if not _sel_is_valid(val_ptr.null_bitmap, row):
                    val_is_null = True
                else:
                    memcpy(
                        out_data + row * itemsize,
                        <char*>val_ptr.data + row * val_ptr.itemsize,
                        itemsize,
                    )
            matched = True
            break

        if not matched or val_is_null:
            any_null = True
        elif out_null != NULL:
            _sel_set_true_bit(out_null, row)

    if out_null == NULL or not any_null:
        if out_null != NULL:
            free(out_null)
        out_ptr.null_bitmap = NULL
    else:
        out_ptr.null_bitmap = out_null

    return result


# ---------------------------------------------------------------------------
# Bool inner kernel — pure C
# ---------------------------------------------------------------------------

cdef Vector _case_bool_kernel(
    int8_t* cond_const_state,
    DrakenFixedBuffer** cond_bv_ptrs,
    ConstAccessor** val_const_accs,
    DrakenFixedBuffer** val_bv_ptrs,
    Py_ssize_t n_pairs,
    Py_ssize_t length,
):
    """Pure C inner loop for BoolVector value branches."""
    cdef BoolVector result = BoolVector(length)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef Py_ssize_t pair_idx
    cdef bint matched
    cdef bint val_is_null
    cdef bint value
    cdef ConstAccessor* val_acc
    cdef DrakenFixedBuffer* val_ptr
    cdef DrakenFixedBuffer* cond_ptr

    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        matched = False
        val_is_null = False
        value = False
        for pair_idx in range(n_pairs):
            if cond_const_state[pair_idx] == 0:
                cond_ptr = cond_bv_ptrs[pair_idx]
                if not _case_cond_true_at(cond_ptr, row):
                    continue
            # state == 1: const TRUE — always matches.

            val_acc = val_const_accs[pair_idx]
            if val_acc != NULL:
                if val_acc.is_null != 0:
                    val_is_null = True
                else:
                    value = (<uint8_t*>val_acc.value_ptr)[0] != 0
            else:
                val_ptr = val_bv_ptrs[pair_idx]
                if not _sel_is_valid(val_ptr.null_bitmap, row):
                    val_is_null = True
                else:
                    value = _sel_bit_is_set(<uint8_t*>val_ptr.data, row)
            matched = True
            break

        if not matched or val_is_null:
            any_null = True
            continue

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


# ---------------------------------------------------------------------------
# String inner kernel
# ---------------------------------------------------------------------------

cdef Vector _case_string_kernel(
    int8_t* cond_const_state,
    DrakenFixedBuffer** cond_bv_ptrs,
    ConstAccessor** val_const_accs,
    DrakenConstantStringPayload** val_const_payloads,
    tuple val_views,
    Py_ssize_t n_pairs,
    Py_ssize_t length,
):
    """Pure C inner loop for StringVector value branches.

    val_views[i] holds a _StringVectorView for non-const values, None
    otherwise. `val_views` is a tuple of cdef class instances — Cython
    generates C-level dispatch for the .is_null/.value_len/.value_ptr
    method calls.
    """
    cdef Py_ssize_t row
    cdef Py_ssize_t pair_idx
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t value_len
    cdef const char* value_ptr
    cdef bint matched
    cdef bint val_is_null
    cdef ConstAccessor* val_acc
    cdef DrakenConstantStringPayload* payload
    cdef _StringVectorView view
    cdef DrakenFixedBuffer* cond_ptr
    cdef StringVectorBuilder builder

    # Pass 1: total byte budget for the builder.
    for row in range(length):
        matched = False
        val_is_null = False
        for pair_idx in range(n_pairs):
            if cond_const_state[pair_idx] == 0:
                cond_ptr = cond_bv_ptrs[pair_idx]
                if not _case_cond_true_at(cond_ptr, row):
                    continue

            val_acc = val_const_accs[pair_idx]
            if val_acc != NULL:
                if val_acc.is_null != 0:
                    val_is_null = True
                else:
                    payload = val_const_payloads[pair_idx]
                    total_bytes += payload.length
            else:
                view = <_StringVectorView>val_views[pair_idx]
                if view.is_null(row):
                    val_is_null = True
                else:
                    total_bytes += view.value_len(row)
            matched = True
            break
        # if not matched or val_is_null: row contributes 0 bytes (null in pass 2)

    builder = StringVectorBuilder(length, total_bytes, False, True)

    # Pass 2: emit values.
    for row in range(length):
        matched = False
        val_is_null = False
        for pair_idx in range(n_pairs):
            if cond_const_state[pair_idx] == 0:
                cond_ptr = cond_bv_ptrs[pair_idx]
                if not _case_cond_true_at(cond_ptr, row):
                    continue

            val_acc = val_const_accs[pair_idx]
            if val_acc != NULL:
                if val_acc.is_null != 0:
                    val_is_null = True
                else:
                    payload = val_const_payloads[pair_idx]
                    builder.append_bytes(
                        <const char*>payload.data if payload.length > 0 else NULL,
                        payload.length,
                    )
            else:
                view = <_StringVectorView>val_views[pair_idx]
                if view.is_null(row):
                    val_is_null = True
                else:
                    value_len = view.value_len(row)
                    value_ptr = <const char*>view.value_ptr(row)
                    builder.append_bytes(
                        value_ptr if value_len > 0 else NULL, value_len
                    )
            matched = True
            break

        if not matched or val_is_null:
            builder.append_null()

    return builder.finish()


# ---------------------------------------------------------------------------
# Condition pre-processing — extract const state, drop dead branches.
# ---------------------------------------------------------------------------

cdef Py_ssize_t _case_collect_conditions(
    tuple conditions,
    tuple values,
    Py_ssize_t n_pairs_in,
    int8_t* cond_const_state,
    DrakenFixedBuffer** cond_bv_ptrs,
    list live_values,
) except -1:
    """Walk the input pairs, classify each condition, and prune pairs
    whose condition is provably FALSE/NULL at compile time.

    Pairs whose condition is provably TRUE end the scan — any pairs after
    a TRUE condition can never fire and are dropped.

    Populates the parallel arrays in-place; appends the surviving values
    to `live_values`. Returns the number of live pairs.
    """
    cdef Py_ssize_t in_idx
    cdef Py_ssize_t live = 0
    cdef Vector cond_vec
    cdef ConstAccessor* cond_acc
    cdef BoolVector cond_bv

    for in_idx in range(n_pairs_in):
        cond_vec = <Vector>conditions[in_idx]
        cond_acc = cond_vec.const_accessor()
        if cond_acc != NULL:
            if cond_acc.is_null != 0 or (<uint8_t*>cond_acc.value_ptr)[0] == 0:
                # Const FALSE/NULL — branch is dead, drop it.
                continue
            # Const TRUE — branch always fires for every row.
            cond_const_state[live] = 1
            cond_bv_ptrs[live] = NULL
            live_values.append(values[in_idx])
            live += 1
            # Any later pair is unreachable.
            return live
        # Non-const BoolVector.
        cond_bv = <BoolVector>cond_vec
        cond_const_state[live] = 0
        cond_bv_ptrs[live] = cond_bv.ptr
        live_values.append(values[in_idx])
        live += 1

    return live


# ---------------------------------------------------------------------------
# Dispatch helpers — extract C-level metadata and call the kernel.
# ---------------------------------------------------------------------------

cdef Vector _case_fixed_dispatch(
    tuple conditions,
    tuple values,
    Py_ssize_t n_pairs_in,
    Py_ssize_t length,
    int output_type,
    Vector template,
):
    cdef int8_t* cond_const_state = <int8_t*>malloc(
        n_pairs_in * sizeof(int8_t)
    )
    cdef DrakenFixedBuffer** cond_bv_ptrs = <DrakenFixedBuffer**>malloc(
        n_pairs_in * sizeof(DrakenFixedBuffer*)
    )
    cdef ConstAccessor** val_const_accs = <ConstAccessor**>malloc(
        n_pairs_in * sizeof(ConstAccessor*)
    )
    cdef DrakenFixedBuffer** val_src_ptrs = <DrakenFixedBuffer**>malloc(
        n_pairs_in * sizeof(DrakenFixedBuffer*)
    )
    if (
        cond_const_state == NULL
        or cond_bv_ptrs == NULL
        or val_const_accs == NULL
        or val_src_ptrs == NULL
    ):
        if cond_const_state != NULL:
            free(cond_const_state)
        if cond_bv_ptrs != NULL:
            free(cond_bv_ptrs)
        if val_const_accs != NULL:
            free(val_const_accs)
        if val_src_ptrs != NULL:
            free(val_src_ptrs)
        raise MemoryError()

    cdef Py_ssize_t pair_idx
    cdef Py_ssize_t n_live
    cdef Vector vec
    cdef Vector result
    cdef list live_values = []

    try:
        n_live = _case_collect_conditions(
            conditions, values, n_pairs_in,
            cond_const_state, cond_bv_ptrs, live_values,
        )

        if n_live == 0:
            # No live branches — every row is NULL.
            result = _sel_new_fixed_vector(output_type, length, template)
            return _case_fill_all_null_fixed(result, length)

        for pair_idx in range(n_live):
            vec = <Vector>live_values[pair_idx]
            val_const_accs[pair_idx] = vec.const_accessor()
            if val_const_accs[pair_idx] == NULL:
                val_src_ptrs[pair_idx] = _sel_fixed_ptr(vec)
            else:
                val_src_ptrs[pair_idx] = NULL

        result = _case_fixed_kernel(
            cond_const_state, cond_bv_ptrs,
            val_const_accs, val_src_ptrs,
            n_live, length, output_type, template,
        )
    finally:
        free(cond_const_state)
        free(cond_bv_ptrs)
        free(val_const_accs)
        free(val_src_ptrs)

    return result


cdef Vector _case_bool_dispatch(
    tuple conditions,
    tuple values,
    Py_ssize_t n_pairs_in,
    Py_ssize_t length,
):
    cdef int8_t* cond_const_state = <int8_t*>malloc(
        n_pairs_in * sizeof(int8_t)
    )
    cdef DrakenFixedBuffer** cond_bv_ptrs = <DrakenFixedBuffer**>malloc(
        n_pairs_in * sizeof(DrakenFixedBuffer*)
    )
    cdef ConstAccessor** val_const_accs = <ConstAccessor**>malloc(
        n_pairs_in * sizeof(ConstAccessor*)
    )
    cdef DrakenFixedBuffer** val_bv_ptrs = <DrakenFixedBuffer**>malloc(
        n_pairs_in * sizeof(DrakenFixedBuffer*)
    )
    if (
        cond_const_state == NULL
        or cond_bv_ptrs == NULL
        or val_const_accs == NULL
        or val_bv_ptrs == NULL
    ):
        if cond_const_state != NULL:
            free(cond_const_state)
        if cond_bv_ptrs != NULL:
            free(cond_bv_ptrs)
        if val_const_accs != NULL:
            free(val_const_accs)
        if val_bv_ptrs != NULL:
            free(val_bv_ptrs)
        raise MemoryError()

    cdef Py_ssize_t pair_idx
    cdef Py_ssize_t n_live
    cdef Vector vec
    cdef BoolVector bv
    cdef Vector result
    cdef list live_values = []

    try:
        n_live = _case_collect_conditions(
            conditions, values, n_pairs_in,
            cond_const_state, cond_bv_ptrs, live_values,
        )

        if n_live == 0:
            result = BoolVector(length)
            return _case_fill_all_null_bool(<BoolVector>result, length)

        for pair_idx in range(n_live):
            vec = <Vector>live_values[pair_idx]
            val_const_accs[pair_idx] = vec.const_accessor()
            if val_const_accs[pair_idx] == NULL:
                bv = <BoolVector>vec
                val_bv_ptrs[pair_idx] = bv.ptr
            else:
                val_bv_ptrs[pair_idx] = NULL

        result = _case_bool_kernel(
            cond_const_state, cond_bv_ptrs,
            val_const_accs, val_bv_ptrs,
            n_live, length,
        )
    finally:
        free(cond_const_state)
        free(cond_bv_ptrs)
        free(val_const_accs)
        free(val_bv_ptrs)

    return result


cdef Vector _case_string_dispatch(
    tuple conditions,
    tuple values,
    Py_ssize_t n_pairs_in,
    Py_ssize_t length,
):
    cdef int8_t* cond_const_state = <int8_t*>malloc(
        n_pairs_in * sizeof(int8_t)
    )
    cdef DrakenFixedBuffer** cond_bv_ptrs = <DrakenFixedBuffer**>malloc(
        n_pairs_in * sizeof(DrakenFixedBuffer*)
    )
    cdef ConstAccessor** val_const_accs = <ConstAccessor**>malloc(
        n_pairs_in * sizeof(ConstAccessor*)
    )
    cdef DrakenConstantStringPayload** val_const_payloads = (
        <DrakenConstantStringPayload**>malloc(
            n_pairs_in * sizeof(DrakenConstantStringPayload*)
        )
    )
    if (
        cond_const_state == NULL
        or cond_bv_ptrs == NULL
        or val_const_accs == NULL
        or val_const_payloads == NULL
    ):
        if cond_const_state != NULL:
            free(cond_const_state)
        if cond_bv_ptrs != NULL:
            free(cond_bv_ptrs)
        if val_const_accs != NULL:
            free(val_const_accs)
        if val_const_payloads != NULL:
            free(val_const_payloads)
        raise MemoryError()

    cdef Py_ssize_t pair_idx
    cdef Py_ssize_t n_live
    cdef Vector vec
    cdef StringVector sv
    cdef Vector result
    cdef list live_values = []
    cdef list view_list

    try:
        n_live = _case_collect_conditions(
            conditions, values, n_pairs_in,
            cond_const_state, cond_bv_ptrs, live_values,
        )

        if n_live == 0:
            return _case_make_all_null_string(length)

        view_list = [None] * n_live
        for pair_idx in range(n_live):
            vec = <Vector>live_values[pair_idx]
            val_const_accs[pair_idx] = vec.const_accessor()
            if val_const_accs[pair_idx] != NULL:
                val_const_payloads[pair_idx] = (
                    <DrakenConstantStringPayload*>val_const_accs[pair_idx].value_ptr
                )
            else:
                val_const_payloads[pair_idx] = NULL
                sv = <StringVector>vec
                view_list[pair_idx] = sv.view()

        result = _case_string_kernel(
            cond_const_state, cond_bv_ptrs,
            val_const_accs, val_const_payloads,
            tuple(view_list), n_live, length,
        )
    finally:
        free(cond_const_state)
        free(cond_bv_ptrs)
        free(val_const_accs)
        free(val_const_payloads)

    return result


# ---------------------------------------------------------------------------
# All-null helpers for the no-live-branches case.
# ---------------------------------------------------------------------------

cdef Vector _case_fill_all_null_fixed(Vector result, Py_ssize_t length):
    cdef DrakenFixedBuffer* out_ptr = _sel_fixed_ptr(result)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)
    out_ptr.null_bitmap = out_null
    return result


cdef Vector _case_fill_all_null_bool(BoolVector result, Py_ssize_t length):
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)
    result.ptr.null_bitmap = out_null
    return <Vector>result


cdef Vector _case_make_all_null_string(Py_ssize_t length):
    cdef StringVectorBuilder builder = StringVectorBuilder(length, 0, False, True)
    cdef Py_ssize_t row
    for row in range(length):
        builder.append_null()
    return builder.finish()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def vector_case(conditions, values):
    """Row-wise CASE WHEN ... THEN ... [ELSE ...] END.

    Strict contract — see module docstring. Raises TypeError or ValueError
    immediately on any contract violation. No coercion, no fallbacks.
    """
    if conditions is None or values is None:
        raise TypeError(
            "vector_case: conditions and values must be sequences of "
            "Draken Vectors, got None"
        )

    cdef tuple cond_tuple = tuple(conditions)
    cdef tuple val_tuple = tuple(values)
    cdef Py_ssize_t n_pairs = len(cond_tuple)

    if n_pairs == 0:
        raise ValueError("vector_case: requires at least one (condition, value) pair")
    if len(val_tuple) != n_pairs:
        raise ValueError(
            f"vector_case: conditions length {n_pairs} does not match "
            f"values length {len(val_tuple)}"
        )

    cdef Py_ssize_t pair_idx
    cdef Vector first_cond
    cdef Vector cond_vec
    cdef Vector val_vec
    cdef Py_ssize_t length
    cdef ConstAccessor* cond_acc

    for pair_idx in range(n_pairs):
        if cond_tuple[pair_idx] is None or not isinstance(cond_tuple[pair_idx], Vector):
            raise TypeError(
                f"vector_case: condition {pair_idx} must be a Draken Vector, "
                f"got {type(cond_tuple[pair_idx]).__name__}"
            )
        if val_tuple[pair_idx] is None or not isinstance(val_tuple[pair_idx], Vector):
            raise TypeError(
                f"vector_case: value {pair_idx} must be a Draken Vector, "
                f"got {type(val_tuple[pair_idx]).__name__}"
            )

    first_cond = <Vector>cond_tuple[0]
    length = len(first_cond)

    for pair_idx in range(n_pairs):
        cond_vec = <Vector>cond_tuple[pair_idx]
        if len(cond_vec) != length:
            raise ValueError(
                f"vector_case: condition {pair_idx} length {len(cond_vec)} "
                f"does not match condition 0 length {length}"
            )
        val_vec = <Vector>val_tuple[pair_idx]
        if len(val_vec) != length:
            raise ValueError(
                f"vector_case: value {pair_idx} length {len(val_vec)} "
                f"does not match condition 0 length {length}"
            )
        if _sel_bool_family(cond_vec) != DRAKEN_BOOL:
            raise TypeError(
                f"vector_case: condition {pair_idx} must be in the BOOL "
                f"family, got {type(cond_vec).__name__}"
            )

    cdef Vector first_val = <Vector>val_tuple[0]
    cdef int family
    cdef int candidate
    cdef int output_fixed

    family = _sel_bool_family(first_val)
    if family == DRAKEN_BOOL:
        for pair_idx in range(1, n_pairs):
            if _sel_bool_family(<Vector>val_tuple[pair_idx]) != DRAKEN_BOOL:
                raise TypeError(
                    f"vector_case: value {pair_idx} not BOOL family "
                    f"(got {type(val_tuple[pair_idx]).__name__})"
                )
        return _case_bool_dispatch(cond_tuple, val_tuple, n_pairs, length)

    family = _sel_string_family(first_val)
    if family == DRAKEN_STRING:
        for pair_idx in range(1, n_pairs):
            if _sel_string_family(<Vector>val_tuple[pair_idx]) != DRAKEN_STRING:
                raise TypeError(
                    f"vector_case: value {pair_idx} not STRING family "
                    f"(got {type(val_tuple[pair_idx]).__name__})"
                )
        return _case_string_dispatch(cond_tuple, val_tuple, n_pairs, length)

    output_fixed = _sel_fixed_family(first_val)
    if output_fixed != -1:
        for pair_idx in range(1, n_pairs):
            candidate = _sel_fixed_family(<Vector>val_tuple[pair_idx])
            if candidate != output_fixed:
                raise TypeError(
                    f"vector_case: value {pair_idx} type {candidate} "
                    f"does not match value 0 type {output_fixed}"
                )
        return _case_fixed_dispatch(
            cond_tuple, val_tuple, n_pairs, length, output_fixed, first_val,
        )

    raise TypeError(
        f"vector_case: unsupported value type "
        f"{type(val_tuple[0]).__name__}"
    )
