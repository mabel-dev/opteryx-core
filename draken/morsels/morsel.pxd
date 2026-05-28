# Morsel cdef-class for Cython consumers.
#
# Morsel wraps a nanobind Morsel handle (_nb) plus parallel Python lists for
# names + columns. Cdef methods declared here give operator code (distinct,
# hash joins, group-by) C-level access without GIL acquisition per row.

from libc.stdint cimport int32_t, uint64_t

cdef class Morsel:
    cdef object _nb
    cdef public list _col_names
    cdef public list _columns
    cdef public int _zero_col_num_rows

    # Resolve a Python list of column names (str or bytes), or None for
    # "all columns", to a freshly-malloc'd int32_t array of column indices.
    # Writes the count into n_cols_out. Caller owns the returned buffer
    # (must `free()`). Returns NULL with an exception set on error.
    cdef int32_t* _resolve_columns_to_indices(self, object columns,
                                              int32_t* n_cols_out) except NULL

    # Hash specified columns into the caller-allocated hashes_ptr buffer
    # (which must be n uint64s, pre-zeroed). Mixes per-column hashes via
    # simd_mix_hash for multi-column; single-column shortcut writes the
    # column hash directly without mixing.
    #
    # Returns 0 on success, 1 if any column needs GIL fallback (e.g.
    # ArrayVector, which can't hash nogil). On 1, caller should re-zero
    # hashes_ptr and fall back to morsel.hash().
    cdef bint c_hash(self, uint64_t* hashes_ptr, int32_t* col_indices,
                     int32_t n_cols, Py_ssize_t n) nogil

    # _empty_inplace: zero all columns to 0 rows, in place.
    cdef void _empty_inplace(self)

    # _take_inplace: filter all columns to the given row indices, in place.
    cdef void _take_inplace(self, int32_t[::1] indices)


cpdef Morsel align_tables(Morsel left_morsel, Morsel right_morsel,
                           int32_t[::1] left_view, int32_t[::1] right_view)
