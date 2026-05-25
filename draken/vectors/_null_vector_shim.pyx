# cython: language_level=3
# Cython shim for draken.vectors.null_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class NullVector(Vector):
    pass
