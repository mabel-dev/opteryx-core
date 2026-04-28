from libc.stdint cimport int32_t, uint64_t
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenMorsel, DrakenType

cdef class Morsel:
    cdef DrakenMorsel* ptr
    cdef list _encoded_names
    cdef list _columns
    cdef dict _name_to_index

    cpdef Vector column(self, bytes identity, bytes column_name=?)
    cpdef void append(self, Morsel other)
    cpdef void append_vector(self, object name, Vector vector)
    cpdef uint64_t[::1] hash(self, object columns=*)
    cdef void _take_inplace(self, object indices)
    cdef void _filter_mask_inplace(self, object mask)
    cdef bint _looks_like_boolean_mask(self, object mask)
    cdef void _empty_inplace(self)
    cdef void _select_inplace(self, object columns)
    cdef Morsel _full_copy(self)
    cdef Vector _empty_vector_like(self, Py_ssize_t column_index, Vector src_vec)
    cdef bint _vector_dtype_matches(self, Vector vector, DrakenType expected)
    cdef inline void _rebuild_name_to_index(self)
    cdef inline dict _ensure_name_map(self)
    cdef inline Py_ssize_t _column_index_from_name(self, object column)
    cdef int32_t* _resolve_columns_to_indices(self, object columns, int32_t* out_n_cols) except NULL
    cdef bint c_hash(self, uint64_t* out, int32_t* col_indices, int32_t n_cols, Py_ssize_t n_rows) noexcept nogil
