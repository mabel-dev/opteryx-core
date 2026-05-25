# Stub .pxd for draken.vectors.vector_vector.

from libc.stdint cimport uint8_t, uint16_t
from libc.stddef cimport size_t

from draken.vectors.vector cimport Vector


cdef class VectorVector(Vector):
    cdef uint16_t* _data
    cdef uint8_t* _null_bitmap
    cdef bint _owns_data
    cdef bint _owns_null_bitmap
    cdef Py_ssize_t _length
    cdef Py_ssize_t _dimensions
    cdef object _arrow_parent
