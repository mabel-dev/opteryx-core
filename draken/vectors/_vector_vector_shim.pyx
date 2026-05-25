# cython: language_level=3
# Cython shim for draken.vectors.vector_vector — E.24 vtable bridge.
# Fields declared in vector_vector.pxd; do NOT redeclare here.

from draken.vectors.vector cimport Vector


cdef class VectorVector(Vector):
    pass
