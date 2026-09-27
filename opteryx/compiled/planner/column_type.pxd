# cython: language_level=3
# distutils: language = c++

from libc.stdint cimport int16_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libcpp cimport bool as cbool

from draken.core.buffers cimport DrakenType as CDrakenType


cdef extern from "logical_type.h":
    # Scoped enums (uint8_t underlying) - read and written through explicit casts.
    ctypedef uint8_t CLogicalKind "LogicalKind"
    ctypedef uint8_t CTimestampUnit "TimestampUnit"

    ctypedef struct CLogicalType "LogicalType":
        CLogicalKind kind
        CTimestampUnit unit
        int16_t offset_minutes
        uint8_t precision
        uint8_t scale
        uint32_t dimension


cdef extern from "planner/column_type.hpp":
    const uint32_t kNoColumnType "opteryx::planner::kNoColumnType"

    ctypedef struct ColumnTypeEntry "opteryx::planner::ColumnTypeEntry":
        CDrakenType physical
        cbool has_logical
        CLogicalType logical
        uint32_t element

    uint8_t column_type_check_code "opteryx::planner::column_type_check_code"(const ColumnTypeEntry& e)
    cbool column_type_is_parameterized "opteryx::planner::column_type_is_parameterized"(CDrakenType physical)

    cdef cppclass ColumnTypeTable "opteryx::planner::ColumnTypeTable":
        uint32_t intern(const ColumnTypeEntry& e, cbool* inserted)
        const ColumnTypeEntry& entry(uint32_t type_id)
        size_t size()


# The process's type table (a type id's descriptor), for native readers.
cdef const ColumnTypeTable* column_type_table()


cdef class ColumnType:
    # The interned type id (src/cpp/planner/column_type.hpp): equal ids are equal
    # types, and a native column row stores exactly this.
    cdef readonly uint32_t type_id

