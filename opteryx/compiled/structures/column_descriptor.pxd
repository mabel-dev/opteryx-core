# Cython declaration file for ColumnDescriptor

from libc.stdint cimport int64_t

cdef class ColumnDescriptor:
    cdef public str column_name
    cdef public str column_type
    cdef public int64_t num_rows
    cdef public int64_t null_count
    cdef public int64_t ref_id
    cdef public int64_t data_offset
    cdef public int64_t data_length
    cdef public dict metadata

    cpdef dict to_dict(self)

cpdef bytes serialize_descriptor(ColumnDescriptor desc)
cpdef ColumnDescriptor deserialize_descriptor(bytes data)
cpdef list serialize_row_group(dict row_group)
cpdef dict deserialize_row_group(list descriptors)
