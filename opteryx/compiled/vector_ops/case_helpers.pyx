# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""CASE WHEN helper kernels.

decide_one_branch / group_indices_and_perm / assemble_* implement the three-
phase lazy CASE WHEN evaluator described in case_eval.pyx.  The assemble
variants are not yet ported to the new DrakenVector API and raise
NotImplementedError when called.
"""

from array import array as _pyarray
from cpython.array cimport array, clone

cdef array _INT32_TEMPLATE = array('i', [])
cdef array _INT16_TEMPLATE = array('h', [])


cpdef array _make_range_int32(Py_ssize_t n):
    """Return an int32 Python array [0, 1, ..., n-1]."""
    cdef array result = clone(_INT32_TEMPLATE, n, False)
    cdef int* ptr = result.data.as_ints
    cdef Py_ssize_t i
    for i in range(n):
        ptr[i] = <int>i
    return result


cpdef array _make_const_int16(Py_ssize_t n, short val):
    """Return an int16 Python array [val, val, ..., val] of length n."""
    cdef array result = clone(_INT16_TEMPLATE, n, False)
    cdef short* ptr = result.data.as_shorts
    cdef Py_ssize_t i
    for i in range(n):
        ptr[i] = val
    return result


cpdef object decide_one_branch(object bv, array live, array branch_id, int branch_idx):
    """Mark rows where bv is True as belonging to branch_idx; return remaining live rows.

    Args:
        bv:         BoolVector evaluated on the live rows only (len == len(live))
        live:       int32 array of currently-live global row indices
        branch_id:  int16 array[N] of per-row branch assignments (updated in-place)
        branch_idx: branch number to assign to matching rows

    Returns:
        int32 array of rows that did NOT match this branch (remain live)
    """
    raise NotImplementedError("decide_one_branch not yet ported to DrakenVector API")


cpdef object group_indices_and_perm(array branch_id, int num_branches):
    """Partition row indices by branch assignment.

    Args:
        branch_id:    int16 array[N] of branch assignments (-1 = unmatched)
        num_branches: number of WHEN branches

    Returns:
        (rpb, unmatched, pib):
            rpb       — list of int32 arrays, one per branch, of row indices
            unmatched — int32 array of unmatched row indices
            pib       — int32 array[N]: position of each row within its branch
    """
    raise NotImplementedError("group_indices_and_perm not yet ported to DrakenVector API")


cpdef object assemble_fixed(list parts, object else_part, array branch_id, object rpb, array unmatched):
    """Scatter Integer64Vector branch outputs into a single output vector.

    Not yet ported to the new DrakenVector (from_decoded) API.
    """
    raise NotImplementedError("assemble_fixed not yet ported to DrakenVector API")


cpdef object assemble_bool(list parts, object else_part, array branch_id, object rpb, array unmatched):
    """Scatter BoolVector branch outputs into a single output vector.

    Not yet ported to the new DrakenVector (from_decoded) API.
    """
    raise NotImplementedError("assemble_bool not yet ported to DrakenVector API")


cpdef object assemble_flat_string(list parts, object else_part, array branch_id, object rpb, array unmatched):
    """Scatter flat-StringVector branch outputs into a single output vector.

    Not yet ported to the new DrakenVector (from_decoded) API.
    """
    raise NotImplementedError("assemble_flat_string not yet ported to DrakenVector API")


cpdef object assemble_dict_string(list parts, object else_part, array branch_id, object rpb, array unmatched):
    """Scatter dict-encoded StringVector branch outputs into a single output vector.

    Not yet ported to the new DrakenVector (from_decoded) API.
    """
    raise NotImplementedError("assemble_dict_string not yet ported to DrakenVector API")
