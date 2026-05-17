from libc.stdint cimport int8_t, int32_t, int64_t, uint64_t, uint8_t

from draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer, DrakenRLEBuffer, DrakenType
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector

cdef class IntegerVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef ConstAccessor _const_accessor
    cdef int64_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null
    cdef DrakenRLEBuffer* _rle_buffer

    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef list to_pylist(self)
    cpdef IntegerVector take(self, int32_t[::1] indices)
    cpdef int64_t min(self)
    cpdef int64_t max(self)
    cpdef int64_t sum(self)
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n)
    cdef BoolVector _compare_scalar(self, int64_t value, int op)
    cdef BoolVector _compare_vector(self, IntegerVector other, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector equals_vector(self, IntegerVector other)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector not_equals_vector(self, IntegerVector other)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_vector(self, IntegerVector other)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector greater_than_or_equals_vector(self, IntegerVector other)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_vector(self, IntegerVector other)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than_or_equals_vector(self, IntegerVector other)
    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=*, bint upper_inclusive=*)

cdef IntegerVector from_arrow(object array)
cdef IntegerVector from_dict(const int32_t[::1] codes, const int64_t[::1] dictionary)
cdef IntegerVector from_dict_nullable(
    const int32_t[::1] codes,
    const int64_t[::1] dictionary,
    const uint8_t[::1] row_validity,
)
cdef IntegerVector from_rle_builder(
    int64_t* run_values,
    int32_t* run_lengths,
    size_t num_runs,
    DrakenType dtype,
    uint8_t* null_bitmap=*,
)
