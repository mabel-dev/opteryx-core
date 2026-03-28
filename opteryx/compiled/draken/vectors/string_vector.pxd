# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport ConstAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.vector cimport Vector


# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    bint is_null


cdef class StringVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_offs_buf
    cdef object _arrow_null_buf

    cdef DrakenVarBuffer* ptr
    cdef bint owns_data
    cdef DictAccessor _dict_accessor
    cdef DrakenVarBuffer* _dict_values
    cdef uint8_t* _dict_codes
    cdef uint8_t _dict_code_width
    cdef uint8_t _dict_ordered
    cdef ConstAccessor _const_accessor
    cdef DrakenConstantStringPayload* _const_value
    cdef bint _has_const
    cdef bint _const_is_null

    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef BoolVector equals(self, bytes value)
    cpdef BoolVector not_equals(self, bytes value)
    cpdef BoolVector less_than(self, bytes value)
    cpdef BoolVector greater_than(self, bytes value)
    cpdef BoolVector less_than_or_equals(self, bytes value)
    cpdef BoolVector greater_than_or_equals(self, bytes value)
    cpdef BoolVector in_list(self, object value_set)
    cpdef BoolVector like(self, bytes pattern, bint ignore_case=*)
    cpdef BoolVector rlike(self, bytes pattern)
    cpdef BoolVector contains(self, bytes substr, bint ignore_case=*)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cpdef StringVector take(self, int32_t[::1] indices)

    cpdef list to_pylist(self)
    cpdef Py_ssize_t byte_length(self, Py_ssize_t i)
    cpdef object buffers(self)
    cpdef object null_bitmap(self)
    cpdef int32_t[::1] lengths(self)
    cpdef object view(self)


cdef class _StringVectorCIterator:
    """C-level iterator for high-performance kernel operations."""
    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef char* _base
    cdef int32_t* _offsets
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr)
    cdef bint next(self, StringElement* elem) nogil
    cpdef void reset(self)
    cpdef StringElement get_at(self, Py_ssize_t index)


cdef class _StringVectorView:
    cdef DrakenVarBuffer* _ptr
    cdef char* _data
    cdef int32_t* _offsets
    cdef uint8_t* _nulls

    cpdef intptr_t value_ptr(self, Py_ssize_t i)
    cpdef Py_ssize_t value_len(self, Py_ssize_t i)
    cpdef bint is_null(self, Py_ssize_t i)


cdef class StringVectorBuilder:
    """Builder for constructing StringVector instances."""
    cdef StringVector _vec
    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _length
    cdef Py_ssize_t _next_index
    cdef Py_ssize_t _bytes_cap
    cdef Py_ssize_t _offset
    cdef bint _finished
    cdef bint _resizable
    cdef bint _strict_capacity
    cdef bint _mask_user_provided
    cdef char* _data
    cdef int32_t* _offsets
    cdef uint8_t* _nulls

    cpdef void append(self, bytes value)
    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length)
    cpdef void append_view(self, const uint8_t[::1] value)
    cpdef void append_null(self)
    cpdef void append_bulk(self, list values)
    cdef void append_bytes_bulk(self, const char** ptrs, Py_ssize_t* lengths, Py_ssize_t n)
    cpdef void set(self, Py_ssize_t index, bytes value)
    cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length)
    cpdef void set_view(self, Py_ssize_t index, const uint8_t[::1] value)
    cpdef void set_null(self, Py_ssize_t index)
    cpdef void set_validity_mask(self, const uint8_t[::1] mask)
    cpdef StringVector finish(self)

    # Private methods
    cdef void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *
    cdef void _set_null(self, Py_ssize_t index) except *
    cdef void _ensure_capacity(self, Py_ssize_t to_add) except *
    cdef void _initialize_null_bitmap(self) except *
    cdef void _require_index(self, Py_ssize_t index) except *


cdef StringVector from_arrow(object array)
cdef StringVector from_dict(const int32_t[::1] codes, list dictionary)
cdef StringVector from_dict_nullable(
    const int32_t[::1] codes,
    list dictionary,
    const uint8_t[::1] row_validity,
)
cdef StringVector from_dict_buffers(
    const int32_t[::1] codes,
    const int32_t[::1] dict_offsets,
    const int32_t[::1] dict_lengths,
    const uint8_t[::1] arena_bytes,
    object row_validity=*,
)
cdef StringVector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=*,
    bint ordered=*,
    const uint8_t* dict_entry_null_bitmap=*,
)
cdef StringVector from_arrow_struct(object array)

cpdef StringVector uppercase(StringVector input)
cpdef StringVector lowercase(StringVector input)
