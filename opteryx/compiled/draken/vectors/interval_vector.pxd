from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.vector cimport Vector

cdef class IntervalVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef IntervalVector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef object to_arrow_interval(self)
    cpdef object to_arrow_binary(self)
    cpdef IntervalVector add_vector(self, IntervalVector other)
    cpdef IntervalVector subtract_vector(self, IntervalVector other)
    cpdef BoolVector compare_vector(self, IntervalVector other, int8_t operation, bint reject_month_components=*)
    cdef BoolVector _compare_scalar(self, int64_t sc_months, int64_t sc_microseconds, int8_t operation, bint reject_month_components)
    cpdef BoolVector equals(self, object literal)
    cpdef BoolVector not_equals(self, object literal)
    cpdef BoolVector less_than(self, object literal)
    cpdef BoolVector greater_than(self, object literal)
    cpdef BoolVector less_than_or_equals(self, object literal)
    cpdef BoolVector greater_than_or_equals(self, object literal)
    cpdef object apply_to_temporal(self, object values, int8_t signum=*)

    cpdef uint64_t[::1] hash(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef IntervalVector from_arrow_interval(object array)
cdef IntervalVector from_arrow_binary(object array)
