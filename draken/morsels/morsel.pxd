# Morsel cdef-class for Cython consumers.
#
# Morsel wraps a nanobind Morsel handle (_nb), a Python list of column names,
# and a C++ vector of column Vector objects (strong references). Cdef methods
# declared here give operator code (distinct, hash joins, group-by) C-level
# access without GIL acquisition per row.

from libc.stdint cimport int32_t, uint64_t

from libcpp.vector cimport vector
from cpython.object cimport PyObject

from draken.core.buffers cimport DrakenVector
from draken.vectors.vector cimport Vector

cdef class Morsel:
    cdef object _nb
    cdef public list _col_names
    # Column store: C++ vector of borrowed-then-INCREF'd Vector objects. NOT a
    # Python list — indexing one would force a GIL acquisition per access in
    # otherwise-nogil hot paths. Access via the cdef accessors below; the
    # vector holds a strong reference to each Vector (released in __dealloc__).
    cdef vector[PyObject*] _columns
    cdef public int _zero_col_num_rows

    # ---- Column store accessors (replace direct _columns[...] indexing) ----
    # Number of columns currently held.
    cdef Py_ssize_t _num_columns(self) noexcept
    # Borrowed Vector at index i (no refcount change; caller must not outlive
    # the morsel). i is assumed in range.
    cdef Vector _get_column(self, Py_ssize_t i)
    # Replace the Vector at index i: DECREF the old, INCREF the new. i in range.
    cdef void _set_column(self, Py_ssize_t i, Vector v)
    # Append a Vector, taking a strong reference.
    cdef void _append_column(self, Vector v)
    # DECREF and drop all columns.
    cdef void _clear_columns(self)

    # Resolve a Python list of column names (str or bytes), or None for
    # "all columns", to a freshly-malloc'd int32_t array of column indices.
    # Writes the count into n_cols_out. Caller owns the returned buffer
    # (must `free()`). Returns NULL with an exception set on error.
    cdef int32_t* _resolve_columns_to_indices(self, object columns,
                                              int32_t* n_cols_out) except NULL

    # Resolve column indices → a draken_malloc'd array of raw DrakenVector*
    # pointers (one per index). Touches the Python column list, so it must be
    # called under the GIL — once, before any nogil hot path. The returned
    # array lets c_hash run fully nogil. Caller owns the buffer (draken_free).
    # The morsel keeps the Vector objects alive, so the pointers stay valid
    # for the morsel's lifetime. Returns NULL with an exception set on error.
    cdef const DrakenVector** _columns_to_pointers(self, int32_t* col_indices,
                                                   int32_t n_cols) except NULL

    # Hash specified columns into the caller-allocated hashes_ptr buffer
    # (which must be n uint64s, pre-zeroed). Mixes per-column hashes via
    # simd_mix_hash for multi-column; single-column shortcut writes the
    # column hash directly without mixing.
    #
    # dvs: pre-resolved DrakenVector* array (see _columns_to_pointers),
    # n_cols entries. c_hash performs no Python access — fully nogil.
    #
    # Returns 0 on success, 1 if any column needs GIL fallback (e.g.
    # ArrayVector, which can't hash nogil). On 1, caller should re-zero
    # hashes_ptr and fall back to morsel.hash().
    cdef bint c_hash(self, uint64_t* hashes_ptr, const DrakenVector** dvs,
                     int32_t n_cols, Py_ssize_t n) nogil

    # _empty_inplace: zero all columns to 0 rows, in place.
    cdef void _empty_inplace(self)

    # _take_inplace: filter all columns to the given row indices, in place.
    cdef void _take_inplace(self, int32_t[::1] indices)


cpdef Morsel align_tables(Morsel left_morsel, Morsel right_morsel,
                           int32_t[::1] left_view, int32_t[::1] right_view)
