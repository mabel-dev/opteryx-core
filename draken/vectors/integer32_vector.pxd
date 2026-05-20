from libc.stdint cimport int32_t, int64_t, uint64_t, uint8_t

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector

cdef class Integer32Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef DrakenVector* unified(self) noexcept

    cpdef list to_pylist(self)
    cpdef Integer32Vector take(self, int32_t[::1] indices)
    cpdef int64_t min(self)
    cpdef int64_t max(self)
    cpdef int64_t sum(self)
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n)
    cdef BoolVector _compare_scalar(self, int64_t value, int op)
    cdef BoolVector _compare_vector(self, Integer32Vector other, int op)

    cpdef BoolVector equals(self, int64_t value)
    cpdef BoolVector equals_vector(self, Integer32Vector other)
    cpdef BoolVector not_equals(self, int64_t value)
    cpdef BoolVector not_equals_vector(self, Integer32Vector other)
    cpdef BoolVector greater_than(self, int64_t value)
    cpdef BoolVector greater_than_vector(self, Integer32Vector other)
    cpdef BoolVector greater_than_or_equals(self, int64_t value)
    cpdef BoolVector greater_than_or_equals_vector(self, Integer32Vector other)
    cpdef BoolVector less_than(self, int64_t value)
    cpdef BoolVector less_than_vector(self, Integer32Vector other)
    cpdef BoolVector less_than_or_equals(self, int64_t value)
    cpdef BoolVector less_than_or_equals_vector(self, Integer32Vector other)
    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=*, bint upper_inclusive=*)

cdef Integer32Vector integer32_from_arrow(object array)
