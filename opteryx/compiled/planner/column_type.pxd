# cython: language_level=3
# distutils: language = c++

from libc.stdint cimport uint32_t


cdef class ColumnType:
    # The interned type id (src/cpp/planner/column_type.hpp): equal ids are equal
    # types, and a native column row stores exactly this.
    cdef readonly uint32_t type_id


cdef ColumnType column_type_of(uint32_t type_id)
