# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Vectorized COALESCE(arg1, arg2, ..., argN).

Architectural contract (strict, no exceptions):
- At least one argument is required.
- All arguments MUST be Draken Vectors of identical length.
- All arguments MUST share the same Draken type family:
    * matching fixed-width types, OR
    * all BOOL, OR
    * all STRING.
- Constant-encoded Vectors (e.g. via from_scalar) are accepted.
- Any contract violation raises TypeError or ValueError immediately.

Semantics:
- For each row, returns the first non-null value across arguments
  (left-to-right scan).
- Returns null at rows where every argument is null.
- SQL NULL is the only null value — NaN passes through as a regular float.

The Python entry point validates inputs and pre-extracts raw buffer pointers
into C arrays. The inner kernels are pure C: typed Vector returns, no
Python lists, no Python scalar intermediates. Constant values are read
directly from each argument's `DrakenVector.data` (via `unified()`) and
copied into the output via `memcpy` — no boxing through Python
`int`/`float`/`bytes`.

Shared bitmap / type-classification / vector-construction helpers live in
_helper_select.pyx and are prefixed with `_sel_`.
"""

from libc.stdint cimport int32_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DrakenConstantStringPayload,
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
# Fixed-width inner kernel — pure C
# ---------------------------------------------------------------------------

cdef Vector _coalesce_fixed_kernel(
    DrakenVector** unified_vecs,
    DrakenFixedBuffer** src_ptrs,
    Py_ssize_t n_args,
    Py_ssize_t length,
    int output_type,
    Vector template,
):
    """Pure C inner loop. unified_vecs[i].data_length == 1 iff arg i is constant-encoded;
    src_ptrs[i] is non-NULL iff arg i is a regular fixed-width buffer.
    Exactly one of (is_const, src_ptrs[i] != NULL) must hold per arg.
    """
    cdef Vector result = _sel_new_fixed_vector(output_type, length, template)
    cdef DrakenFixedBuffer* out_ptr = _sel_fixed_ptr(result)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef Py_ssize_t arg_idx
    cdef bint found
    cdef DrakenVector* uv
    cdef DrakenFixedBuffer* src_ptr
    cdef char* out_data = <char*>out_ptr.data
    cdef Py_ssize_t itemsize = out_ptr.itemsize

    if length != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for row in range(length):
        found = False
        for arg_idx in range(n_args):
            uv = unified_vecs[arg_idx]
            if not _sel_is_valid(uv.validity, row):
                continue
            memcpy(
                out_data + row * itemsize,
                <char*>uv.data + uv.selection[row] * itemsize,
                itemsize,
            )
            found = True
            break

        if not found:
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

cdef Vector _coalesce_bool_kernel(
    DrakenVector** unified_vecs,
    DrakenFixedBuffer** bv_ptrs,
    Py_ssize_t n_args,
    Py_ssize_t length,
):
    """Pure C inner loop for BoolVector args.

    Accesses bool values via unified DrakenVector: data[selection[row]] bit index.
    """
    cdef BoolVector result = BoolVector(length)
    cdef Py_ssize_t nbytes = (length + 7) >> 3
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    cdef uint8_t* out_null = NULL
    cdef bint any_null = False
    cdef Py_ssize_t row
    cdef Py_ssize_t arg_idx
    cdef bint found
    cdef bint value
    cdef DrakenVector* uv

    if nbytes != 0:
        memset(out_bits, 0, nbytes)
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    cdef uint32_t sel_idx
    for row in range(length):
        found = False
        value = False
        for arg_idx in range(n_args):
            uv = unified_vecs[arg_idx]
            if not _sel_is_valid(uv.validity, row):
                continue
            sel_idx = uv.selection[row]
            value = ((<uint8_t*>uv.data)[sel_idx >> 3] >> (sel_idx & 7)) & 1
            found = True
            break

        if not found:
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
#
# StringVector access goes through _StringVectorView (cdef class, C-level
# method dispatch). Const-string payloads are read directly from
# DrakenConstantStringPayload pointers.
# ---------------------------------------------------------------------------

cdef Vector _coalesce_string_kernel(
    DrakenVector** unified_vecs,
    DrakenConstantStringPayload** const_payloads,
    tuple views,
    Py_ssize_t n_args,
    Py_ssize_t length,
):
    """Pure C inner loop for StringVector args.

    unified_vecs[i].data_length == 1 iff arg i is constant-encoded;
    const_payloads[i] is non-NULL for constant-encoded args;
    views[i] holds a _StringVectorView for non-const args, None otherwise.
    `views` is a tuple of cdef class instances — Cython generates C-level
    dispatch for the .is_null/.value_len/.value_ptr method calls.
    """
    cdef Py_ssize_t row
    cdef Py_ssize_t arg_idx
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t value_len
    cdef const char* value_ptr
    cdef bint found
    cdef DrakenVector* uv
    cdef DrakenConstantStringPayload* payload
    cdef _StringVectorView view
    cdef StringVectorBuilder builder

    # Pass 1: total byte budget for the builder.
    for row in range(length):
        found = False
        for arg_idx in range(n_args):
            uv = unified_vecs[arg_idx]
            if const_payloads[arg_idx] != NULL:
                if uv.validity != NULL:
                    continue
                payload = const_payloads[arg_idx]
                total_bytes += payload.length
                found = True
                break

            view = <_StringVectorView>views[arg_idx]
            if view.is_null(row):
                continue
            total_bytes += view.value_len(row)
            found = True
            break
        # if not found: row contributes 0 bytes (will be null in pass 2)

    builder = StringVectorBuilder(length, total_bytes, False, True)

    # Pass 2: emit values.
    for row in range(length):
        found = False
        for arg_idx in range(n_args):
            uv = unified_vecs[arg_idx]
            if const_payloads[arg_idx] != NULL:
                if uv.validity != NULL:
                    continue
                payload = const_payloads[arg_idx]
                builder.append_bytes(
                    <const char*>payload.data if payload.length > 0 else NULL,
                    payload.length,
                )
                found = True
                break

            view = <_StringVectorView>views[arg_idx]
            if view.is_null(row):
                continue
            value_len = view.value_len(row)
            value_ptr = <const char*>view.value_ptr(row)
            builder.append_bytes(value_ptr if value_len > 0 else NULL, value_len)
            found = True
            break

        if not found:
            builder.append_null()

    return builder.finish()


# ---------------------------------------------------------------------------
# Dispatch helpers — extract C-level metadata from a tuple of Vectors and
# call the appropriate kernel. Manage the C-array allocations.
# ---------------------------------------------------------------------------

cdef Vector _coalesce_fixed_dispatch(
    tuple arrays,
    Py_ssize_t n_args,
    Py_ssize_t length,
    int output_type,
    Vector template,
):
    cdef DrakenVector** unified_vecs = <DrakenVector**>malloc(
        n_args * sizeof(DrakenVector*)
    )
    cdef DrakenFixedBuffer** src_ptrs = <DrakenFixedBuffer**>malloc(
        n_args * sizeof(DrakenFixedBuffer*)
    )
    if unified_vecs == NULL or src_ptrs == NULL:
        if unified_vecs != NULL:
            free(unified_vecs)
        if src_ptrs != NULL:
            free(src_ptrs)
        raise MemoryError()

    cdef Py_ssize_t arg_idx
    cdef Vector vec
    cdef Vector result

    try:
        for arg_idx in range(n_args):
            vec = <Vector>arrays[arg_idx]
            unified_vecs[arg_idx] = vec.unified()
            src_ptrs[arg_idx] = _sel_fixed_ptr(vec)

        result = _coalesce_fixed_kernel(
            unified_vecs, src_ptrs, n_args, length, output_type, template
        )
    finally:
        free(unified_vecs)
        free(src_ptrs)

    return result


cdef Vector _coalesce_bool_dispatch(
    tuple arrays,
    Py_ssize_t n_args,
    Py_ssize_t length,
):
    cdef DrakenVector** unified_vecs = <DrakenVector**>malloc(
        n_args * sizeof(DrakenVector*)
    )
    if unified_vecs == NULL:
        raise MemoryError()

    cdef Py_ssize_t arg_idx
    cdef Vector vec
    cdef Vector result

    try:
        for arg_idx in range(n_args):
            vec = <Vector>arrays[arg_idx]
            unified_vecs[arg_idx] = vec.unified()

        result = _coalesce_bool_kernel(unified_vecs, NULL, n_args, length)
    finally:
        free(unified_vecs)

    return result


cdef Vector _coalesce_string_dispatch(
    tuple arrays,
    Py_ssize_t n_args,
    Py_ssize_t length,
):
    cdef DrakenVector** unified_vecs = <DrakenVector**>malloc(
        n_args * sizeof(DrakenVector*)
    )
    cdef DrakenConstantStringPayload** const_payloads = (
        <DrakenConstantStringPayload**>malloc(
            n_args * sizeof(DrakenConstantStringPayload*)
        )
    )
    if unified_vecs == NULL or const_payloads == NULL:
        if unified_vecs != NULL:
            free(unified_vecs)
        if const_payloads != NULL:
            free(const_payloads)
        raise MemoryError()

    cdef Py_ssize_t arg_idx
    cdef Vector vec
    cdef StringVector sv
    cdef list view_list = [None] * n_args
    cdef Vector result

    try:
        for arg_idx in range(n_args):
            sv = <StringVector>arrays[arg_idx]
            unified_vecs[arg_idx] = sv.unified()
            if sv.ptr.offsets == NULL and sv._german_dict_values == NULL:
                const_payloads[arg_idx] = (
                    <DrakenConstantStringPayload*>unified_vecs[arg_idx].data
                )
            else:
                const_payloads[arg_idx] = NULL
                view_list[arg_idx] = sv.view()

        result = _coalesce_string_kernel(
            unified_vecs, const_payloads, tuple(view_list), n_args, length
        )
    finally:
        free(unified_vecs)
        free(const_payloads)

    return result


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def vector_coalesce(*arrays):
    """Row-wise first-non-null across N Draken Vectors (SQL COALESCE).

    Strict contract — see module docstring. Raises TypeError or ValueError
    immediately on any contract violation. No coercion, no fallbacks.
    """
    cdef Py_ssize_t n_args = len(arrays)
    if n_args == 0:
        raise ValueError("vector_coalesce: requires at least one argument")

    cdef Py_ssize_t arg_idx
    cdef Vector first_vec
    cdef Vector vec
    cdef Py_ssize_t length

    for arg_idx in range(n_args):
        if arrays[arg_idx] is None or not isinstance(arrays[arg_idx], Vector):
            raise TypeError(
                f"vector_coalesce: argument {arg_idx} must be a Draken Vector, "
                f"got {type(arrays[arg_idx]).__name__}"
            )

    first_vec = <Vector>arrays[0]
    length = len(first_vec)

    for arg_idx in range(1, n_args):
        vec = <Vector>arrays[arg_idx]
        if len(vec) != length:
            raise ValueError(
                f"vector_coalesce: argument {arg_idx} length {len(vec)} "
                f"does not match argument 0 length {length}"
            )

    cdef int family
    cdef int candidate
    cdef int output_fixed

    family = _sel_bool_family(first_vec)
    if family == DRAKEN_BOOL:
        for arg_idx in range(1, n_args):
            if _sel_bool_family(<Vector>arrays[arg_idx]) != DRAKEN_BOOL:
                raise TypeError(
                    f"vector_coalesce: argument {arg_idx} not BOOL family "
                    f"(got {type(arrays[arg_idx]).__name__})"
                )
        return _coalesce_bool_dispatch(arrays, n_args, length)

    family = _sel_string_family(first_vec)
    if family == DRAKEN_STRING:
        for arg_idx in range(1, n_args):
            if _sel_string_family(<Vector>arrays[arg_idx]) != DRAKEN_STRING:
                raise TypeError(
                    f"vector_coalesce: argument {arg_idx} not STRING family "
                    f"(got {type(arrays[arg_idx]).__name__})"
                )
        return _coalesce_string_dispatch(arrays, n_args, length)

    output_fixed = _sel_fixed_family(first_vec)
    if output_fixed != -1:
        for arg_idx in range(1, n_args):
            candidate = _sel_fixed_family(<Vector>arrays[arg_idx])
            if candidate != output_fixed:
                raise TypeError(
                    f"vector_coalesce: argument {arg_idx} type {candidate} "
                    f"does not match argument 0 type {output_fixed}"
                )
        return _coalesce_fixed_dispatch(arrays, n_args, length, output_fixed, first_vec)

    raise TypeError(
        f"vector_coalesce: unsupported argument type "
        f"{type(arrays[0]).__name__}"
    )
