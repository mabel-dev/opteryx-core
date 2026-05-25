# Stub .pxd for draken.vectors.string_vector.
# Declares StringVector, StringVectorBuilder, _StringVectorCIterator,
# StringElement, and the factory cdef functions consumed by opteryx/ and rugo/.
# Re-exports DrakenVarBuffer (vector_rlike.pyx cimports it via this module).

from libc.stdint cimport int32_t, uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.core.buffers cimport DrakenStringArena, DrakenVarBuffer
from draken.vectors.vector cimport Vector


cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    int is_null


cdef class StringVector(Vector):
    cdef _StringVectorCIterator c_iter(self)


cdef class StringVectorBuilder:
    cdef list _strs
    cdef void append_bytes(self, const char* data, Py_ssize_t length)


cdef class _StringVectorCIterator:
    cdef bint next(self, StringElement* elem) noexcept


# Factory functions (cdef, exported via __pyx_capi__).

cdef DrakenStringArena* _varbuffer_to_string_arena(
    const uint8_t* src_data,
    const int32_t* src_offsets,
    const uint8_t* src_nulls,
    Py_ssize_t row_count,
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

cdef StringVector make_string_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const uint32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    Py_ssize_t arena_size,
    const uint8_t* valid_bits,
)
