# cython: language_level=3

from libc.stdint cimport int64_t, uint32_t
from opteryx.compiled.draken.core.buffers cimport DictAccessor

cdef uint32_t _dict_read_code(const DictAccessor* ptr, Py_ssize_t row_idx) noexcept nogil
cdef int64_t _read_dictionary_fixed_key(
    object key_vector,
    Py_ssize_t row_idx,
    int64_t* key_valid_flag,
) except *
cdef int64_t _extract_stringlike_key(
    object key_vector,
    Py_ssize_t row_idx,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *
cdef int64_t _dict_accessor_key_kind(const DictAccessor* dict_accessor) noexcept
cdef int _dict_accessor_value_kind(const DictAccessor* dict_accessor) noexcept
cdef double _dict_accessor_read_float_value(
    const DictAccessor* dict_accessor,
    Py_ssize_t row_idx,
) except *
cdef int64_t _dict_accessor_read_int_value(
    const DictAccessor* dict_accessor,
    Py_ssize_t row_idx,
) except *
cdef DictAccessor* _vector_dict_accessor(object vec) noexcept
cdef DictAccessor* _vector_value_dict_accessor(object vec) noexcept
