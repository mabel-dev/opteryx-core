# cython: language_level=3
# Cython shim for draken.vectors.interval_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class IntervalVector(Vector):
    pass
