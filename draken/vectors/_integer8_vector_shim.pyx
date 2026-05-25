# cython: language_level=3
# Cython shim for draken.vectors.integer8_vector — E.24 vtable bridge.

from libc.stdint cimport uint8_t

from draken.vectors.vector cimport Vector


cdef class Integer8Vector(Vector):
    cdef const uint8_t* null_bitmap_ptr(self) noexcept:
        return self._dv.validity
