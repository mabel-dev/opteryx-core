# cython: language_level=3

from libc.stddef cimport size_t


cdef object from_sequence(object data, object dtype=*)
cpdef object from_scalar(object value, size_t length, object dtype=*)
