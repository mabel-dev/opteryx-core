# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Hot-loop Cython helpers for the lazy CASE WHEN evaluator in case_eval.py.

Public functions use typed signatures.  Python callers pass array('i'/'h')
objects; Cython callers use typed memoryviews directly.

Design contract (matches case_eval.py):
  branch_id[r] in {0 .. n_branches-1}  — row r matched that branch
  branch_id[r] == -1                     — row r unmatched (goes to ELSE or NULL)
"""

from cpython.array cimport array as _cparr, clone as _clone
from libc.stdint cimport int16_t, int32_t, uint8_t, uint16_t, uint32_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DRAKEN_ENCODING_DICTIONARY,
    DrakenFixedBuffer,
)
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from draken.vectors.vector cimport Vector

from array import array as _pyarr

_TEMPLATE_INT32 = _pyarr("i", [])
_TEMPLATE_INT16 = _pyarr("h", [])


# ---------------------------------------------------------------------------
# Array initialisation helpers
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
    cdef uint8_t* bits
    cdef uint8_t* null_bm
    cdef Py_ssize_t i, ii
    cdef Py_ssize_t not_won_count = 0

    # Constant-encoded BoolVector (e.g. from BoolVector.from_constant or a LITERAL node)
    if bv._has_const:
        if bv._const_is_null or not bv._const_value:
            # False or NULL condition — no rows won, all remain live
            return live
        # True condition — every live row wins this branch
        for ii in range(n):
            branch_id[live[ii]] = branch_idx
        return _clone(_TEMPLATE_INT32, 0, False)

    bits = <uint8_t*>bv.ptr.data
    null_bm = bv.ptr.null_bitmap

    # Pass 1: count not-won rows
    for i in range(n):
        if null_bm != NULL and not _sel_is_valid(null_bm, i):
            not_won_count += 1
        elif not _sel_bit_is_set(bits, i):
            not_won_count += 1

    cdef _cparr not_won_arr = _clone(_TEMPLATE_INT32, not_won_count, False)
    cdef int32_t* nw = <int32_t*>not_won_arr.data.as_ints
    cdef Py_ssize_t nw_idx = 0
    cdef int32_t row_r

    # Pass 2: scatter won into branch_id, collect not-won
    for i in range(n):
        row_r = live[i]
        if null_bm != NULL and not _sel_is_valid(null_bm, i):
            nw[nw_idx] = row_r
            nw_idx += 1
        elif _sel_bit_is_set(bits, i):
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
        rows_per_branch  list of n_branches array('i'); rows_per_branch[i]
                         contains the original row indices matched by branch i,
                         in the order they were encountered.
        unmatched        array('i') of row indices with branch_id == -1.
        pos_in_branch    array('i') of length N.  pos_in_branch[r] is the
                         position of row r within rows_per_branch[branch_id[r]]
                         for matched rows, or within unmatched for unmatched rows.
                         The dual-purpose layout means assembly functions can
                         use a single lookup for both groups.
    """
    cdef Py_ssize_t n = branch_id.shape[0]
    cdef Py_ssize_t i
    cdef int16_t bid

    # Pass 1: count per branch + unmatched (slot n_branches)
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

    # Allocate output arrays
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

    # Reuse counts buffer as running counters (reset first)
    memset(counts, 0, (n_branches + 1) * sizeof(int32_t))

    # Pass 2: fill groups and pos_in_branch
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
    """Scatter fixed-width branch parts into a new output vector.

    Output type is derived from the first non-None part (or else_part).
    Rows not covered by any branch and with no else_part become NULL.
    """
    cdef Vector template_vec = None
    cdef int output_type = -1
    cdef Py_ssize_t bid_py

    for bid_py in range(len(parts)):
        if parts[bid_py] is not None:
            template_vec = <Vector>parts[bid_py]
            output_type = _sel_fixed_family(template_vec)
            break
    if template_vec is None and else_part is not None:
        template_vec = <Vector>else_part
        output_type = _sel_fixed_family(template_vec)
    if output_type == -1:
        raise TypeError("assemble_fixed: no non-None parts to derive output type")

    cdef Py_ssize_t n = branch_id.shape[0]
    cdef Vector result = _sel_new_fixed_vector(output_type, n, template_vec)
    cdef DrakenFixedBuffer* out_ptr = _sel_fixed_ptr(result)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* out_null = <uint8_t*>malloc(nbytes)
    if out_null == NULL:
        raise MemoryError()
    memset(out_null, 0, nbytes)
    out_ptr.null_bitmap = out_null

    cdef char* out_data = <char*>out_ptr.data
    cdef Py_ssize_t itemsize = out_ptr.itemsize
    cdef bint any_null = False
    cdef Py_ssize_t j
    cdef int32_t row_r
    cdef DrakenFixedBuffer* src_ptr
    cdef Vector vec
    cdef int32_t[::1] rows_i

    cdef object scalar_val

    for bid_py in range(len(parts)):
        if parts[bid_py] is None:
            continue
        vec = <Vector>parts[bid_py]
        src_ptr = _sel_fixed_ptr(vec)
        rows_i = rows_per_branch[bid_py]
        if src_ptr == NULL or src_ptr.data == NULL:
            # Constant-encoded source: read scalar once, scatter to all matched rows
            scalar_val = _sel_const_scalar(vec)
            if scalar_val is None:
                any_null = True
            else:
                for j in range(rows_i.shape[0]):
                    row_r = rows_i[j]
                    _sel_write_fixed_scalar(out_ptr, row_r, output_type, scalar_val)
                    _sel_set_true_bit(out_null, row_r)
        else:
            for j in range(rows_i.shape[0]):
                row_r = rows_i[j]
                if not _sel_is_valid(src_ptr.null_bitmap, j):
                    any_null = True
                else:
                    memcpy(
                        out_data + row_r * itemsize,
                        <char*>src_ptr.data + j * itemsize,
                        itemsize,
                    )
                    _sel_set_true_bit(out_null, row_r)

    if else_part is not None:
        vec = <Vector>else_part
        src_ptr = _sel_fixed_ptr(vec)
        if src_ptr == NULL or src_ptr.data == NULL:
            scalar_val = _sel_const_scalar(vec)
            if scalar_val is None:
                any_null = True
            else:
                for j in range(unmatched.shape[0]):
                    row_r = unmatched[j]
                    _sel_write_fixed_scalar(out_ptr, row_r, output_type, scalar_val)
                    _sel_set_true_bit(out_null, row_r)
        else:
            for j in range(unmatched.shape[0]):
                row_r = unmatched[j]
                if not _sel_is_valid(src_ptr.null_bitmap, j):
                    any_null = True
                else:
                    memcpy(
                        out_data + row_r * itemsize,
                        <char*>src_ptr.data + j * itemsize,
                        itemsize,
                    )
                    _sel_set_true_bit(out_null, row_r)
    elif unmatched.shape[0] > 0:
        any_null = True

    if not any_null:
        free(out_null)
        out_ptr.null_bitmap = NULL

    return result


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
    cdef BoolVector result = BoolVector(n)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* out_bits = <uint8_t*>result.ptr.data
    cdef uint8_t* out_null = <uint8_t*>malloc(nbytes)
    if out_null == NULL:
        raise MemoryError()
    memset(out_null, 0, nbytes)
    memset(out_bits, 0, nbytes)
    result.ptr.null_bitmap = out_null

    cdef bint any_null = False
    cdef Py_ssize_t bid_py, j
    cdef int32_t row_r
    cdef BoolVector bv
    cdef DrakenFixedBuffer* src_ptr
    cdef int32_t[::1] rows_i

    for bid_py in range(len(parts)):
        if parts[bid_py] is None:
            continue
        bv = <BoolVector>parts[bid_py]
        src_ptr = bv.ptr
        rows_i = rows_per_branch[bid_py]
        if bv._has_const:
            if not bv._const_is_null:
                for j in range(rows_i.shape[0]):
                    row_r = rows_i[j]
                    if bv._const_value:
                        _sel_set_true_bit(out_bits, row_r)
                    _sel_set_true_bit(out_null, row_r)
            else:
                any_null = True
        else:
            for j in range(rows_i.shape[0]):
                row_r = rows_i[j]
                if not _sel_is_valid(src_ptr.null_bitmap, j):
                    any_null = True
                else:
                    if _sel_bit_is_set(<uint8_t*>src_ptr.data, j):
                        _sel_set_true_bit(out_bits, row_r)
                    _sel_set_true_bit(out_null, row_r)

    if else_part is not None:
        bv = <BoolVector>else_part
        src_ptr = bv.ptr
        if bv._has_const:
            if not bv._const_is_null:
                for j in range(unmatched.shape[0]):
                    row_r = unmatched[j]
                    if bv._const_value:
                        _sel_set_true_bit(out_bits, row_r)
                    _sel_set_true_bit(out_null, row_r)
            else:
                any_null = True
        else:
            for j in range(unmatched.shape[0]):
                row_r = unmatched[j]
                if not _sel_is_valid(src_ptr.null_bitmap, j):
                    any_null = True
                else:
                    if _sel_bit_is_set(<uint8_t*>src_ptr.data, j):
                        _sel_set_true_bit(out_bits, row_r)
                    _sel_set_true_bit(out_null, row_r)
    elif unmatched.shape[0] > 0:
        any_null = True

    if not any_null:
        free(out_null)
        result.ptr.null_bitmap = NULL

    return result


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
    """Build a flat StringVector in row order using pos_in_branch for lookup.

    Handles const/dict/dense-encoded StringVector parts transparently via
    string_vec_get_at.  Dict-encoded parts are NOT preserved — this is the
    fallback flat path.  Use assemble_dict_string when every part is dict-encoded.
    """
    cdef StringVector else_sv = None
    if else_part is not None:
        else_sv = <StringVector>else_part
    cdef bint have_else = else_sv is not None

    # Pass 1: total byte budget
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t r, pos
    cdef int16_t bid_v
    cdef StringRow srow
    cdef StringVector sv_part

    for r in range(n):
        bid_v = branch_id[r]
        if bid_v >= 0 and parts[bid_v] is not None:
            sv_part = <StringVector>parts[bid_v]
            pos = pos_in_branch[r]
            srow = string_vec_get_at(sv_part, pos)
            if not srow.is_null:
                total_bytes += srow.length
        elif bid_v < 0 and have_else:
            pos = pos_in_branch[r]
            srow = string_vec_get_at(else_sv, pos)
            if not srow.is_null:
                total_bytes += srow.length

    cdef StringVectorBuilder builder = StringVectorBuilder(n, total_bytes, False, True)

    # Pass 2: emit values in row order
    for r in range(n):
        bid_v = branch_id[r]
        if bid_v >= 0 and parts[bid_v] is not None:
            sv_part = <StringVector>parts[bid_v]
            pos = pos_in_branch[r]
            srow = string_vec_get_at(sv_part, pos)
            if srow.is_null:
                builder.append_null()
            else:
                builder.append_bytes(
                    srow.data if srow.length > 0 else NULL, srow.length
                )
        elif bid_v < 0 and have_else:
            pos = pos_in_branch[r]
            srow = string_vec_get_at(else_sv, pos)
            if srow.is_null:
                builder.append_null()
            else:
                builder.append_bytes(
                    srow.data if srow.length > 0 else NULL, srow.length
                )
        else:
            builder.append_null()

    return builder.finish()


# ---------------------------------------------------------------------------
# Phase 3 — assemble_dict_string
# ---------------------------------------------------------------------------

def assemble_dict_string(
    list parts,
    object else_part,
    int16_t[::1] branch_id,
    int32_t[::1] pos_in_branch,
    Py_ssize_t n,
):
    """Build a dict-encoded StringVector, unifying the per-branch dictionaries.

    Every non-None element of `parts` and `else_part` MUST be dict-encoded.
    The output dictionary is the union of all input dictionaries; per-branch
    remap tables translate old codes to new unified codes.
    """
    # Phase A: build unified dict and per-branch remap tables in Python.
    # dict_size is typically small (O(hundreds)), so Python overhead is fine.
    cdef list unified = []          # bytes entries in insertion order
    cdef dict seen = {}             # bytes → int unified code
    cdef list remaps = []           # per-branch array('i') old_code → new_code

    cdef StringVector sv
    cdef Py_ssize_t k, dict_sz
    cdef object val

    for bid_py in range(len(parts)):
        if parts[bid_py] is None:
            remaps.append(None)
            continue
        sv = <StringVector>parts[bid_py]
        dict_sz = sv.dictionary_size
        remap_arr = _clone(_TEMPLATE_INT32, dict_sz, False)
        remap_ptr = <int32_t*>(<_cparr>remap_arr).data.as_ints
        for k in range(dict_sz):
            val = sv.dict_value_at(k)  # bytes or None
            if val is None:
                remap_ptr[k] = -1
            else:
                if val not in seen:
                    seen[val] = len(unified)
                    unified.append(val)
                remap_ptr[k] = seen[val]
        remaps.append(remap_arr)

    cdef _cparr else_remap_arr = None
    cdef int32_t* else_remap_ptr = NULL
    if else_part is not None:
        sv = <StringVector>else_part
        dict_sz = sv.dictionary_size
        else_remap_arr = _clone(_TEMPLATE_INT32, dict_sz, False)
        else_remap_ptr = <int32_t*>else_remap_arr.data.as_ints
        for k in range(dict_sz):
            val = sv.dict_value_at(k)
            if val is None:
                else_remap_ptr[k] = -1
            else:
                if val not in seen:
                    seen[val] = len(unified)
                    unified.append(val)
                else_remap_ptr[k] = seen[val]

    # Phase B: build output codes array (C-level inner loop over N rows)
    cdef _cparr codes_arr = _clone(_TEMPLATE_INT32, n, False)
    cdef int32_t* codes_ptr = <int32_t*>codes_arr.data.as_ints

    cdef bytearray rv_ba = bytearray(n)  # row validity: 1=valid, 0=null
    cdef uint8_t[::1] rv = rv_ba

    cdef Py_ssize_t r, pos
    cdef int16_t bid_v
    cdef int32_t old_code, new_code
    cdef StringVector sv_part
    cdef int32_t* remap_ptr2
    cdef StringRow srow

    for r in range(n):
        bid_v = branch_id[r]
        pos = pos_in_branch[r]
        if bid_v >= 0 and parts[bid_v] is not None:
            sv_part = <StringVector>parts[bid_v]
            srow = string_vec_get_at(sv_part, pos)
            if not srow.is_null:
                old_code = <int32_t>_read_packed_code(
                    sv_part._dict_codes, sv_part._dict_code_width, pos
                )
                remap_ptr2 = <int32_t*>(<_cparr>remaps[bid_v]).data.as_ints
                new_code = remap_ptr2[old_code]
                if new_code >= 0:
                    codes_ptr[r] = new_code
                    rv[r] = 1
        elif bid_v < 0 and else_remap_ptr != NULL:
            sv_part = <StringVector>else_part
            srow = string_vec_get_at(sv_part, pos)
            if not srow.is_null:
                old_code = <int32_t>_read_packed_code(
                    sv_part._dict_codes, sv_part._dict_code_width, pos
                )
                new_code = else_remap_ptr[old_code]
                if new_code >= 0:
                    codes_ptr[r] = new_code
                    rv[r] = 1

    # Phase C: construct output using StringVector.from_dict
    # unified contains bytes entries; from_dict accepts bytes in the list
    from draken.vectors.string_vector import StringVector as _SV
    return _SV.from_dict(codes_arr, unified, rv_ba)
