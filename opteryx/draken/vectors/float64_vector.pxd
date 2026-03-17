from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t, uint8_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.bool_vector cimport BoolVector

cdef class Float64Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    cpdef Float64Vector take(self, int32_t[::1] indices)
    cdef bint _compare_float_values(self, double left, double right, int op) nogil
    cdef BoolVector _compare_scalar(self, double value, int op)
    cdef BoolVector _compare_vector(self, Float64Vector other, int op)

    cpdef BoolVector equals(self, double value)
    cpdef BoolVector equals_vector(self, Float64Vector other)
    cpdef BoolVector not_equals(self, double value)
    cpdef BoolVector not_equals_vector(self, Float64Vector other)
    cpdef BoolVector greater_than(self, double value)
    cpdef BoolVector greater_than_vector(self, Float64Vector other)
    cpdef BoolVector greater_than_or_equals(self, double value)
    cpdef BoolVector greater_than_or_equals_vector(self, Float64Vector other)
    cpdef BoolVector less_than(self, double value)
    cpdef BoolVector less_than_vector(self, Float64Vector other)
    cpdef BoolVector less_than_or_equals(self, double value)
    cpdef BoolVector less_than_or_equals_vector(self, Float64Vector other)
    cpdef BoolVector in_list(self, object value_set)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)
    #cpdef double __getitem__(self, Py_ssize_t i)

    cpdef double sum(self)
    cpdef double min(self)
    cpdef double max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Float64Vector from_arrow(object array)
cdef Float64Vector from_sequence(double[::1] data)
