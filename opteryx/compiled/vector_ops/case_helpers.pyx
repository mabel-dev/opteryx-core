# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Hot-loop Cython helpers for the lazy CASE WHEN evaluator in case_eval.py.

Public functions use typed signatures.  Python callers pass array('i'/'h')
objects; Cython callers use typed memoryviews directly.

Design contract (matches case_eval.py):
  branch_id[r] in {0 .. n_branches-1}  — row r matched that branch
  branch_id[r] == -1                     — row r unmatched (goes to ELSE or NULL)

E.25 migration: DrakenFixedBuffer/StringVector removed. assemble_fixed and
assemble_bool rewritten against DrakenVector* via Vector.unified().
assemble_flat_string and assemble_dict_string stubbed pending E.29
StringVectorBuilder port.
"""

from cpython.array cimport array as _cparr, clone as _clone
from libc.stdint cimport int8_t, int16_t, int32_t, uint8_t, uint16_t, uint32_t
from libc.stddef cimport size_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DrakenVector, DrakenType,
    DRAKEN_INT64, DRAKEN_INT32, DRAKEN_INT16, DRAKEN_INT8,
    DRAKEN_FLOAT64, DRAKEN_FLOAT32,
    DRAKEN_BOOL, DRAKEN_VARCHAR, DRAKEN_NVARCHAR,
    DRAKEN_NULL, DRAKEN_DECIMAL,
)
from draken.vectors.bool_vector cimport BoolVector, from_decoded as _bool_from_decoded
from draken.vectors.vector cimport Vector, from_decoded as _vec_from_decoded
import draken.draken_native as _draken_native_ch

from array import array as _pyarr

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void draken_free(void* p) nogil

_TEMPLATE_INT32 = _pyarr("i", [])
_TEMPLATE_INT16 = _pyarr("h", [])


# ---------------------------------------------------------------------------
# Inline helpers
# ---------------------------------------------------------------------------

cdef inline bint _sel_is_valid(const uint8_t* bitmap, Py_ssize_t i) noexcept nogil:
    """Arrow-convention: bit set = valid. NULL bitmap = all valid."""
    if bitmap == NULL:
        return True
    return (bitmap[i >> 3] >> (i & 7)) & 1


cdef inline void _sel_set_true_bit(uint8_t* bitmap, int32_t row_r) noexcept nogil:
    """Set bit row_r in a packed bitmap (Arrow convention: set = valid/true)."""
    bitmap[row_r >> 3] |= <uint8_t>(1 << (row_r & 7))


cdef inline Py_ssize_t _draken_itemsize(DrakenType t) noexcept nogil:
    """Fixed byte-width per element for numeric DrakenTypes."""
    if t == DRAKEN_INT64 or t == DRAKEN_FLOAT64:
        return 8
    if t == DRAKEN_INT32 or t == DRAKEN_FLOAT32:
        return 4
    if t == DRAKEN_INT16:
        return 2
    return 1  # INT8 and others


# ---------------------------------------------------------------------------
# Array initialisation helpers (also imported by case_eval.pyx via vector_ops.vector_ops)
# ---------------------------------------------------------------------------

def _make_range_int32(Py_ssize_t n):
    """Return array('i') containing [0, 1, ..., n-1]."""
    cdef _cparr arr = _clone(_TEMPLATE_INT32, n, False)
    cdef int32_t* ptr = <int32_t*>arr.data.as_ints
    cdef Py_ssize_t i
    for i in range(n):
        ptr[i] = <int32_t>i
    return arr


def _make_const_int16(Py_ssize_t n, int16_t value):
    """Return array('h') filled with `value` repeated n times."""
    cdef _cparr arr = _clone(_TEMPLATE_INT16, n, False)
    cdef short* ptr = arr.data.as_shorts
    cdef Py_ssize_t i
    if value == -1:
        memset(ptr, 0xFF, n * 2)
    elif value == 0:
        memset(ptr, 0, n * 2)
    else:
        for i in range(n):
            ptr[i] = <short>value
    return arr


# ---------------------------------------------------------------------------
# Phase 1 helper — decide_one_branch
# ---------------------------------------------------------------------------

def decide_one_branch(
    BoolVector bv,
    int32_t[::1] live,
    int16_t[::1] branch_id,
    int16_t branch_idx,
):
    """Process one WHEN condition against currently live rows.

    Sets branch_id[live[i]] = branch_idx for rows where bv[i] is True.
    Returns a new array of live row indices where bv[i] was False or NULL.
    """
    cdef Py_ssize_t n = live.shape[0]
    cdef Py_ssize_t i, ii
    cdef Py_ssize_t not_won_count = 0
    cdef DrakenVector* bv_uv
    cdef uint32_t slot

    bv_uv = bv.unified()
    cdef uint8_t* bv_data = <uint8_t*>bv_uv.data
    cdef uint8_t* bv_nulls = bv_uv.validity

    # Pass 1: count not-won rows
    for i in range(n):
        if not _sel_is_valid(bv_nulls, i):
            not_won_count += 1
        else:
            slot = bv_uv.selection[i]
            if not ((bv_data[slot >> 3] >> (slot & 7)) & 1):
                not_won_count += 1

    cdef _cparr not_won_arr = _clone(_TEMPLATE_INT32, not_won_count, False)
    cdef int32_t* nw = <int32_t*>not_won_arr.data.as_ints
    cdef Py_ssize_t nw_idx = 0
    cdef int32_t row_r

    # Pass 2: scatter won into branch_id, collect not-won
    for i in range(n):
        row_r = live[i]
        slot = bv_uv.selection[i]
        if not _sel_is_valid(bv_nulls, i):
            nw[nw_idx] = row_r
            nw_idx += 1
        elif (bv_data[slot >> 3] >> (slot & 7)) & 1:
            branch_id[row_r] = branch_idx
        else:
            nw[nw_idx] = row_r
            nw_idx += 1

    return not_won_arr


# ---------------------------------------------------------------------------
# Phase 1 → 3 bridge — group_indices_and_perm
# ---------------------------------------------------------------------------

def group_indices_and_perm(
    int16_t[::1] branch_id,
    Py_ssize_t n_branches,
):
    """Group row indices by branch and build position lookup.

    Returns:
        rows_per_branch  list of n_branches array('i')
        unmatched        array('i') of row indices with branch_id == -1
        pos_in_branch    array('i') of length N
    """
    cdef Py_ssize_t n = branch_id.shape[0]
    cdef Py_ssize_t i
    cdef int16_t bid

    cdef int32_t* counts = <int32_t*>malloc((n_branches + 1) * sizeof(int32_t))
    if counts == NULL:
        raise MemoryError()
    memset(counts, 0, (n_branches + 1) * sizeof(int32_t))

    for i in range(n):
        bid = branch_id[i]
        if bid >= 0:
            counts[bid] += 1
        else:
            counts[n_branches] += 1

    cdef list rows_per_branch = []
    cdef int32_t** bptrs = <int32_t**>malloc(n_branches * sizeof(int32_t*))
    if bptrs == NULL:
        free(counts)
        raise MemoryError()

    cdef _cparr arr
    for i in range(n_branches):
        arr = _clone(_TEMPLATE_INT32, counts[i], False)
        rows_per_branch.append(arr)
        bptrs[i] = <int32_t*>(<_cparr>arr).data.as_ints

    cdef _cparr unmatched_arr = _clone(_TEMPLATE_INT32, counts[n_branches], False)
    cdef int32_t* um_ptr = <int32_t*>unmatched_arr.data.as_ints

    cdef _cparr pib_arr = _clone(_TEMPLATE_INT32, n, False)
    cdef int32_t* pib = <int32_t*>pib_arr.data.as_ints

    memset(counts, 0, (n_branches + 1) * sizeof(int32_t))

    cdef int32_t j
    for i in range(n):
        bid = branch_id[i]
        if bid >= 0:
            j = counts[bid]
            bptrs[bid][j] = <int32_t>i
            pib[i] = j
            counts[bid] += 1
        else:
            j = counts[n_branches]
            um_ptr[j] = <int32_t>i
            pib[i] = j
            counts[n_branches] += 1

    free(bptrs)
    free(counts)

    return rows_per_branch, unmatched_arr, pib_arr


# ---------------------------------------------------------------------------
# Phase 3 — assemble_fixed
# ---------------------------------------------------------------------------

def assemble_fixed(
    list parts,
    object else_part,
    int16_t[::1] branch_id,
    list rows_per_branch,
    int32_t[::1] unmatched,
):
    """Scatter fixed-width branch parts into a new output Vector.

    Output DrakenType is derived from the first non-None, non-DRAKEN_NULL part
    (or else_part if no valid parts exist).
    Rows not covered by any branch and with no else_part become NULL.
    """
    cdef Py_ssize_t bid_py
    cdef Py_ssize_t num_parts = len(parts)
    cdef Vector template_vec = None
    cdef DrakenVector* candidate_uv
    cdef dict part_vecs_dict = {}  # Keep all part vectors alive, keyed by index

    # Keep part Vector objects alive by storing them explicitly
    for bid_py in range(num_parts):
        if parts[bid_py] is None:
            continue
        part_vecs_dict[bid_py] = <Vector>parts[bid_py]

    # Find first non-None, non-DRAKEN_NULL part to derive output type
    for bid_py in range(num_parts):
        if parts[bid_py] is not None:
            template_vec = <Vector>parts[bid_py]
            candidate_uv = template_vec.unified()
            # Skip DRAKEN_NULL vectors (all-null, self-describing, no data buffer)
            if candidate_uv.type != DRAKEN_NULL:
                break
            template_vec = None

    # If no valid template in parts, try else_part
    if template_vec is None and else_part is not None:
        template_vec = <Vector>else_part
        candidate_uv = template_vec.unified()
        if candidate_uv.type == DRAKEN_NULL:
            template_vec = None

    if template_vec is None:
        raise TypeError("assemble_fixed: no non-None, non-DRAKEN_NULL parts to derive output type")

    cdef DrakenVector* tmpl_uv = template_vec.unified()
    cdef DrakenType out_dtype = tmpl_uv.type
    cdef Py_ssize_t itemsize = _draken_itemsize(out_dtype)
    cdef Py_ssize_t n = branch_id.shape[0]
    cdef Py_ssize_t nbytes = n * itemsize
    cdef Py_ssize_t vbytes = (n + 7) >> 3

    cdef void* out_data = draken_malloc(<size_t>nbytes) if nbytes > 0 else NULL
    cdef uint8_t* out_validity = <uint8_t*>draken_malloc(<size_t>vbytes) if vbytes > 0 else NULL
    if (out_data == NULL and nbytes > 0) or (out_validity == NULL and vbytes > 0):
        if out_data != NULL:
            draken_free(out_data)
        if out_validity != NULL:
            draken_free(out_validity)
        raise MemoryError("assemble_fixed: draken_malloc failed")
    if nbytes > 0:
        memset(out_data, 0, <size_t>nbytes)
    if vbytes > 0:
        memset(out_validity, 0, <size_t>vbytes)  # all invalid initially

    cdef bint any_null = False
    cdef DrakenVector* src_uv
    cdef uint32_t dict_idx
    cdef int32_t row_r
    cdef Py_ssize_t j
    cdef int32_t[::1] rows_i
    cdef Vector vec
    cdef Vector else_vec = None

    # Keep else_part Vector alive by storing it explicitly
    if else_part is not None:
        else_vec = <Vector>else_part

    for bid_py in range(num_parts):
        if bid_py not in part_vecs_dict:
            continue
        # Access vector from part_vecs_dict (which we populated at the start)
        # to ensure it stays alive
        vec = part_vecs_dict[bid_py]
        src_uv = vec.unified()

        # Cause A fix: Skip DRAKEN_NULL vectors (all-null, data==NULL).
        # Unmatched rows (those not processed here) stay null in output.
        if src_uv.data == NULL:
            # DRAKEN_NULL or all-null constant: all rows in this part are NULL
            any_null = True
            continue

        rows_i = rows_per_branch[bid_py]
        for j in range(rows_i.shape[0]):
            row_r = rows_i[j]
            if not _sel_is_valid(src_uv.validity, j):
                any_null = True
            else:
                dict_idx = src_uv.selection[j]
                memcpy(
                    <char*>out_data + row_r * itemsize,
                    <char*>src_uv.data + dict_idx * itemsize,
                    <size_t>itemsize,
                )
                _sel_set_true_bit(out_validity, row_r)

    if else_vec is not None:
        src_uv = else_vec.unified()

        # Cause A fix: Handle DRAKEN_NULL in else_part (all-null, data==NULL).
        # If else_part is null, unmatched rows stay null in output.
        if src_uv.data == NULL:
            # DRAKEN_NULL else_part: all unmatched rows are NULL
            any_null = True
        else:
            # Non-null else_part: scatter values for unmatched rows
            for j in range(unmatched.shape[0]):
                row_r = unmatched[j]
                if not _sel_is_valid(src_uv.validity, j):
                    any_null = True
                else:
                    dict_idx = src_uv.selection[j]
                    memcpy(
                        <char*>out_data + row_r * itemsize,
                        <char*>src_uv.data + dict_idx * itemsize,
                        <size_t>itemsize,
                    )
                    _sel_set_true_bit(out_validity, row_r)
    elif unmatched.shape[0] > 0:
        # Cause B fix: No else_part and there are unmatched rows.
        # These rows become NULL (validity bits stay 0).
        any_null = True

    cdef Vector out_vec = _vec_from_decoded(out_data, out_validity, <uint32_t>n, out_dtype)
    # DECIMAL is int64-backed; the scatter above copies raw storage but not the
    # scale/precision descriptor. Carry it from the template so downstream ops
    # (sum, to_float64, grouped collectors) can read scale.
    if out_dtype == DRAKEN_DECIMAL:
        out_vec._nb.set_decimal_descriptor(
            template_vec._nb.logical_type_precision,
            template_vec._nb.logical_type_scale,
        )
    return out_vec


# ---------------------------------------------------------------------------
# Phase 3 — assemble_bool
# ---------------------------------------------------------------------------

def assemble_bool(
    list parts,
    object else_part,
    int16_t[::1] branch_id,
    list rows_per_branch,
    int32_t[::1] unmatched,
):
    """Scatter bool branch parts into a new BoolVector."""
    cdef Py_ssize_t n = branch_id.shape[0]
    cdef Py_ssize_t nbytes = (n + 7) >> 3

    cdef void* out_data = draken_malloc(<size_t>nbytes)
    cdef uint8_t* out_validity = <uint8_t*>draken_malloc(<size_t>nbytes)
    if out_data == NULL or out_validity == NULL:
        if out_data != NULL:
            draken_free(out_data)
        if out_validity != NULL:
            draken_free(out_validity)
        raise MemoryError("assemble_bool: draken_malloc failed")
    memset(out_data, 0, <size_t>nbytes)
    memset(out_validity, 0, <size_t>nbytes)  # all invalid initially

    cdef uint8_t* out_bits = <uint8_t*>out_data
    cdef bint any_null = False
    cdef DrakenVector* bv_uv
    cdef uint32_t slot
    cdef int32_t row_r
    cdef Py_ssize_t bid_py, j
    cdef int32_t[::1] rows_i
    cdef BoolVector bv

    for bid_py in range(len(parts)):
        if parts[bid_py] is None:
            continue
        bv = <BoolVector>parts[bid_py]
        rows_i = rows_per_branch[bid_py]
        bv_uv = bv.unified()
        for j in range(rows_i.shape[0]):
            row_r = rows_i[j]
            if not _sel_is_valid(bv_uv.validity, j):
                any_null = True
            else:
                slot = bv_uv.selection[j]
                if (<uint8_t*>bv_uv.data)[slot >> 3] >> (slot & 7) & 1:
                    _sel_set_true_bit(out_bits, row_r)
                _sel_set_true_bit(out_validity, row_r)

    if else_part is not None:
        bv = <BoolVector>else_part
        bv_uv = bv.unified()
        for j in range(unmatched.shape[0]):
            row_r = unmatched[j]
            if not _sel_is_valid(bv_uv.validity, j):
                any_null = True
            else:
                slot = bv_uv.selection[j]
                if (<uint8_t*>bv_uv.data)[slot >> 3] >> (slot & 7) & 1:
                    _sel_set_true_bit(out_bits, row_r)
                _sel_set_true_bit(out_validity, row_r)
    elif unmatched.shape[0] > 0:
        any_null = True

    return _bool_from_decoded(out_data, out_validity, <size_t>n)


# ---------------------------------------------------------------------------
# Phase 3 — assemble_flat_string
# ---------------------------------------------------------------------------

def assemble_flat_string(
    list parts,
    object else_part,
    int16_t[::1] branch_id,
    int32_t[::1] pos_in_branch,
    Py_ssize_t n,
):
    """Build a flat string Vector in row order.

    For each output row r: pick from parts[branch_id[r]] at index
    pos_in_branch[r], or from else_part at pos_in_branch[r] when bid == -1.

    Uses the draken producer surface: build a Python list and hand off to
    `vector_from_string_sequence`. The per-row Python attribute access /
    list build is acceptable here because this is invoked once per CASE WHEN
    expression evaluation per morsel, not per row from a hot loop — and the
    string-builder path that would let us go fully nogil isn't ported yet.
    """
    cdef list result_list = [None] * n
    cdef Py_ssize_t r
    cdef int16_t bid
    cdef int32_t pos
    cdef object source

    for r in range(n):
        bid = branch_id[r]
        pos = pos_in_branch[r]
        if bid >= 0:
            source = parts[bid]
            if source is None:
                continue
            result_list[r] = source[pos]
        elif else_part is not None:
            result_list[r] = else_part[pos]
        # else: None (already in result_list)

    return Vector(_draken_native_ch.vector_from_string_sequence(result_list))


# ---------------------------------------------------------------------------
# Phase 3 — assemble_dict_string  [STUB — E.29 gap: StringVector old API]
# ---------------------------------------------------------------------------

def assemble_dict_string(
    list parts,
    object else_part,
    int16_t[::1] branch_id,
    int32_t[::1] pos_in_branch,
    Py_ssize_t n,
):
    """Build a dict-encoded string Vector.

    Stubbed pending E.29 StringVector old-API removal.
    """
    raise NotImplementedError(
        "assemble_dict_string: StringVector old API not yet ported (E.29 gap). "
        "CASE WHEN with dict-encoded VARCHAR result is not yet supported."
    )
