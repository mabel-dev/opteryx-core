# draken/vectors/bool_vector.pxd

# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, int64_t, uint8_t, uint64_t
from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.vectors.vector cimport Vector

cdef class BoolVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef DrakenVector* unified(self) noexcept
    cdef void _set_null_bitmap(self, uint8_t* bm) noexcept

    # Ops
    cpdef BoolVector take(self, int32_t[::1] indices)
    cpdef BoolVector _compare_scalar(self, bint value, int op)
    cpdef BoolVector equals(self, bint value)
    cpdef BoolVector not_equals(self, bint value)
    cpdef int8_t any(self)
    cpdef int8_t all(self)
    cpdef int8_t[::1] is_null(self)
    cpdef list to_pylist(self)
    cpdef bytes to_byte_array(self)
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
    cpdef int64_t min(self)
    cpdef int64_t max(self)
    cpdef int64_t sum(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cpdef BoolVector and_vector(self, BoolVector other)
    cpdef BoolVector or_vector(self, BoolVector other)
    cpdef BoolVector xor_vector(self, BoolVector other)
    cpdef BoolVector not_vector(self)
    cpdef BoolVector equals_vector(self, BoolVector other)
    cpdef BoolVector not_equals_vector(self, BoolVector other)

cdef BoolVector from_decoded(
    void* data,
    uint8_t* null_bitmap,
    size_t length,
)
cdef BoolVector from_arrow(object array)
cdef BoolVector from_sequence(uint8_t[::1] data)
cdef BoolVector bool_vector_from_bits(uint8_t* value_bits, uint8_t* valid_bits, Py_ssize_t n)

# nogil raw bitmap wrappers — write into caller-pre-allocated buffers.
# Return 0 = all valid (dest_null irrelevant), 1 = nulls present.
cdef bint c_get_bitmap_ptrs(
    BoolVector vec,
    uint8_t** data_out,
    uint8_t** null_out,
) noexcept nogil
cdef bint c_and_bitmap(
    uint8_t* dest, const uint8_t* a, uint8_t* a_null,
    const uint8_t* b, uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes, Py_ssize_t n,
) noexcept nogil
cdef bint c_or_bitmap(
    uint8_t* dest, const uint8_t* a, uint8_t* a_null,
    const uint8_t* b, uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes, Py_ssize_t n,
) noexcept nogil
cdef bint c_xor_bitmap(
    uint8_t* dest, const uint8_t* a, uint8_t* a_null,
    const uint8_t* b, uint8_t* b_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes, Py_ssize_t n,
) noexcept nogil
cdef bint c_not_bitmap(
    uint8_t* dest, const uint8_t* src, uint8_t* src_null,
    uint8_t* dest_null,
    Py_ssize_t nbytes, Py_ssize_t n,
) noexcept nogil
