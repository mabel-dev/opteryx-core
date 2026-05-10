# cython: language_level=3

from libc.stdint cimport int32_t, int64_t, uint64_t
from draken.vectors.vector cimport Vector

cdef class NullVector(Vector):
    cdef Py_ssize_t _length

    cpdef NullVector take(self, int32_t[::1] indices)
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
