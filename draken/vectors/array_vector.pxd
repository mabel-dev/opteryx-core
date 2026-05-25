# Stub .pxd for draken.vectors.array_vector.

from libc.stdint cimport int32_t, uint8_t

from draken.vectors.vector cimport Vector
from draken.vectors.string_vector cimport StringVector


cdef class ArrayVector(Vector):
    cdef public bint _child_decode_utf8


cdef ArrayVector from_sequence(object data)

cdef ArrayVector array_vector_from_parts(
    StringVector flat_child,
    int32_t* offsets,
    uint8_t* list_null_bitmap,
    Py_ssize_t num_rows,
)
