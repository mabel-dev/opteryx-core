# cython: language_level=3
# distutils: language = c++

# The query's column rows (src/cpp/planner/column_table.hpp) and their façades -
# declared here so native consumers read the rows in C.

from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.planner.column_type cimport ColumnType


cdef extern from "planner/column_table.hpp":
    const uint32_t kNoSlot "opteryx::planner::kNoSlot"
    const uint32_t kNoColumnType "opteryx::planner::kNoColumnType"
    const uint8_t COLUMN_PLAIN "opteryx::planner::COLUMN_PLAIN"
    const uint8_t COLUMN_CONSTANT "opteryx::planner::COLUMN_CONSTANT"
    const uint8_t COLUMN_FUNCTION "opteryx::planner::COLUMN_FUNCTION"
    const uint8_t COLUMN_EXPRESSION "opteryx::planner::COLUMN_EXPRESSION"

    cdef cppclass ColumnRow "opteryx::planner::ColumnRow":
        string name
        string identity
        vector[string] aliases
        vector[string] origin
        cbool has_aliases
        cbool has_origin
        cbool nullable
        cbool has_field_id
        int64_t field_id
        uint32_t type_id
        uint8_t kind
        uint32_t alias_of

    cdef cppclass ColumnRows "opteryx::planner::ColumnRows":
        uint32_t append(ColumnRow row)
        const ColumnRow& row(uint32_t slot)
        size_t size()
        uint32_t root(uint32_t slot)


cdef class SchemaColumn:
    cdef readonly str name
    cdef readonly bytes identity
    cdef readonly tuple aliases
    cdef readonly tuple origin
    cdef readonly bint nullable
    cdef readonly ColumnType column_type
    cdef readonly uint32_t slot
    cdef bint _has_field_id
    cdef int64_t _field_id


cdef class ColumnTable:
    cdef ColumnRows _rows
    cdef list _columns   # the façade of each slot
    cdef dict _slot_of   # identity -> ROOT slot (the slot that minted it)

    cdef SchemaColumn _mint(self, type cls, str name, bytes identity, dict fields, uint32_t alias_of)
