# Stub .pxd for draken.vectors.integer32_vector.

from libc.stdint cimport uint8_t

from draken.vectors.vector cimport Vector


cdef class Integer32Vector(Vector):
    cdef const uint8_t* null_bitmap_ptr(self) noexcept
