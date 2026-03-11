# cython: language_level=3

from libc.stdint cimport int32_t
from libc.stdint cimport uint64_t
from libc.stddef cimport size_t
from opteryx.draken.core.buffers cimport DrakenConstantBuffer
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.vector cimport Vector


cdef class ConstantVector(Vector):
    cdef DrakenConstantBuffer* ptr
    cdef bint owns_data
    cdef void _set_scalar(self, object value) except *
    cdef void _set_null_bitmap(self, object null_bitmap) except *
    cdef bint _compare_values(self, object left, object right, int op) except *
    cdef BoolVector _compare_scalar(self, object literal, int op)

    cpdef ConstantVector take(self, int32_t[::1] indices)
    cpdef list to_pylist(self)
    cpdef object null_bitmap(self)
    cpdef object scalar_value(self)
    cpdef object sum(self)
    cpdef object min(self)
    cpdef object max(self)
    cpdef BoolVector equals(self, object literal)
    cpdef BoolVector not_equals(self, object literal)
    cpdef BoolVector less_than(self, object literal)
    cpdef BoolVector greater_than(self, object literal)
    cpdef BoolVector less_than_or_equals(self, object literal)
    cpdef BoolVector greater_than_or_equals(self, object literal)
    cpdef BoolVector in_list(self, object literals)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *


cdef object from_sequence(object data, object dtype=*)
cpdef object from_scalar(object value, size_t length, object dtype=*)
