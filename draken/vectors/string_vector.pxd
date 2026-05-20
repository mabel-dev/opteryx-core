# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, int64_t, intptr_t, uint32_t, uint64_t, uint8_t
from draken.core.buffers cimport DrakenConstantStringPayload
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DrakenStringArena
from draken.core.buffers cimport DrakenStringSlot
from draken.core.buffers cimport STR_INLINE_MAX
from draken.core.buffers cimport str_data
from draken.core.buffers cimport str_length
from draken.core.buffers cimport str_equals
from draken.core.buffers cimport str_compare
from draken.core.string_arena cimport alloc_string_arena
from draken.core.string_arena cimport free_string_arena
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.vector cimport Vector


# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    bint is_null


# Phase 5: constant string view. A constant StringVector stores its one value
# as the single slot of a 1-slot DrakenStringArena under _unified_view.data.
# Readers used to cast _unified_view.data as `DrakenConstantStringPayload*`
# and read .data / .length directly; they now get the same two fields via this
# stack-local view. Defined here (in the .pxd) so it can be cimported across
# the vector_ops/ and operators/ modules.
ctypedef struct _ConstView:
    uint8_t* data
    int32_t length


cdef inline _ConstView _const_view(DrakenStringArena* arena) noexcept nogil:
    cdef _ConstView v
    cdef DrakenStringSlot* slot
    if arena == NULL or arena.length == 0:
        v.data = NULL
        v.length = 0
        return v
    slot = &arena.slots[0]
    v.data = <uint8_t*>str_data(slot, arena.arena)
    v.length = <int32_t>str_length(slot)
    return v


cdef class StringVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_offs_buf
    cdef object _arrow_null_buf

    cdef DrakenVarBuffer* ptr
    cdef bint owns_data
    # Independent ownership of the two buffers that back a dict-encoded value
    # payload. They can diverge: a view may allocate fresh codes while
    # referencing a parent vector's arena, or take over an arena while
    # borrowing codes. _release_dict_storage frees each independently.
    cdef bint _owns_codes        # owns _unified_view.selection

    cdef int64_t* _dict_code_counts
    cdef bint _dict_code_counts_valid

    # min/max metadata. When _min_max_valid is True:
    #   - if _min_max_all_null is True, the vector is empty/all-null (min/max → None)
    #   - else _cached_min_ptr/_cached_min_len and _cached_max_ptr/_cached_max_len
    #     point into a buffer the vector owns (vec.ptr.data, the German arena under
    #     _unified_view.data, or vec's constant payload). Pointers are stable for the
    #     vector's lifetime; PyBytes are materialized only when min()/max() is called
    #     from Python.
    # When False, min()/max() recompute on demand. Default: invalid (safe fallback).
    cdef const uint8_t* _cached_min_ptr
    cdef Py_ssize_t _cached_min_len
    cdef const uint8_t* _cached_max_ptr
    cdef Py_ssize_t _cached_max_len
    cdef bint _min_max_all_null
    cdef bint _min_max_valid

    cdef DrakenVector* unified(self) noexcept

    # Encoded-form accessors for dict/RLE-aware operators.
    # Each method assumes the caller has already verified the encoding.
    cdef Py_ssize_t c_length(self) noexcept nogil
    cdef Py_ssize_t c_dict_size(self) noexcept nogil
    cdef uint8_t c_dict_code_width(self) noexcept nogil
    cdef const uint8_t* c_dict_codes_ptr(self) noexcept nogil
    cdef const uint8_t* c_dict_value_ptr(self, Py_ssize_t i, Py_ssize_t* out_len) noexcept nogil
    cdef bint c_dict_value_is_null(self, Py_ssize_t i) noexcept nogil
    cdef const uint8_t* c_row_null_bitmap(self) noexcept nogil
    cdef const int64_t* c_dict_code_counts_ptr(self) except NULL

    # Returns the final mixed hash for the i-th dict entry, matching the
    # value c_hash_into writes for a row pointing to that dict entry when
    # the destination buffer is zeroed.
    cdef uint64_t c_dict_value_hash(self, Py_ssize_t i) noexcept nogil

    cpdef BoolVector _compare_scalar(self, bytes value, int op)
    cpdef BoolVector equals(self, bytes value)
    cpdef BoolVector not_equals(self, bytes value)
    cpdef BoolVector less_than(self, bytes value)
    cpdef BoolVector greater_than(self, bytes value)
    cpdef BoolVector less_than_or_equals(self, bytes value)
    cpdef BoolVector greater_than_or_equals(self, bytes value)
    cpdef BoolVector equals_vector(self, StringVector other)
    cpdef BoolVector not_equals_vector(self, StringVector other)
    cpdef BoolVector less_than_vector(self, StringVector other)
    cpdef BoolVector less_than_or_equals_vector(self, StringVector other)
    cpdef BoolVector greater_than_vector(self, StringVector other)
    cpdef BoolVector greater_than_or_equals_vector(self, StringVector other)
    cpdef BoolVector in_list(self, object value_set)
    cpdef BoolVector like(self, bytes pattern, bint ignore_case=*)
    cpdef BoolVector rlike(self, bytes pattern)
    cpdef BoolVector contains(self, bytes substr, bint ignore_case=*)
    cdef inline int _string_compare_pair(
        self,
        const uint8_t* d1, int32_t s1, int32_t l1,
        const uint8_t* d2, int32_t s2, int32_t l2,
        int op,
    ) nogil
    cdef BoolVector _compare_vector_op(self, StringVector other, int op)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cpdef StringVector take(self, int32_t[::1] indices)
    cpdef object min(self)
    cpdef object max(self)
    cpdef sum(self)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False

    cpdef list to_pylist(self)
    cpdef Py_ssize_t byte_length(self, Py_ssize_t i)
    cpdef object buffers(self)
    cpdef object null_bitmap(self)
    cpdef int8_t[::1] is_null(self)
    cpdef int32_t[::1] lengths(self)
    cpdef object view(self)


cdef class _StringVectorCIterator:
    """C-level iterator for high-performance kernel operations."""
    cdef DrakenStringArena* _arena
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr)
    @staticmethod
    cdef _StringVectorCIterator _from_arena(DrakenStringArena* arena, Py_ssize_t length, uint8_t* nulls)
    cdef bint next(self, StringElement* elem) nogil
    cpdef void reset(self)
    cpdef StringElement get_at(self, Py_ssize_t index)


cdef class _StringVectorView:
    cdef DrakenStringArena* _arena
    cdef const uint32_t* _selection
    cdef uint8_t* _nulls
    cdef Py_ssize_t _length

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


cdef DrakenStringArena* _varbuffer_to_string_arena(
    const uint8_t* data,
    const int32_t* offsets,
    const uint8_t* null_bitmap,
    Py_ssize_t n_rows,
)

cdef StringVector from_arrow(object array)

cdef StringVector _materialize_dict_string(StringVector vec)
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
cdef StringVector from_dict_buffers_dict_only(
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

