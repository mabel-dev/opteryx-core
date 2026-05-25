# cython: language_level=3
# Cython shim for draken.vectors.integer32_vector — E.24 vtable bridge.

from libc.stdint cimport uint8_t

from draken.vectors.vector cimport Vector


cdef class Integer32Vector(Vector):
    @classmethod
    def from_constant(cls, value, num_rows, is_null=False):
        from draken.draken_native import vector_int32_from_constant
        return cls(vector_int32_from_constant(None if is_null else int(value), num_rows))

    cdef const uint8_t* null_bitmap_ptr(self) noexcept:
        return self._dv.validity
