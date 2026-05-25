# cython: language_level=3
# Cython shim for draken.vectors.time_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class TimeVector(Vector):
    @classmethod
    def from_constant(cls, value, length, is_null=False, is_time64=True):
        from draken.draken_native import vector_time64_from_constant, vector_time32_from_constant
        if is_time64:
            return cls(vector_time64_from_constant(None if is_null else int(value), length, "us"))
        return cls(vector_time32_from_constant(None if is_null else int(value), length, "s"))
