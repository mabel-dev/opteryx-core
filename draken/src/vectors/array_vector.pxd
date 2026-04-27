from libc.stdint cimport int32_t, uint8_t, uint64_t

from opteryx.compiled.draken.core.buffers cimport DrakenArrayBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector


cdef class ArrayVector(Vector):
    cdef DrakenArrayBuffer* ptr
    cdef object _child
    cdef bint owns_offsets
    cdef bint owns_null_bitmap
    cdef object _arrow_parent
    cdef object _arrow_offsets_buf
    cdef object _arrow_null_buf
    cdef object _arrow_child_array
    cdef object _child_arrow_type
    cdef bint _child_decode_utf8

    cdef object _materialize_row(self, Py_ssize_t idx)
    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=*
    ) except *
    cpdef object min(self)
    cpdef object max(self)
    cpdef object sum(self)


cdef ArrayVector from_arrow(object array)
cdef ArrayVector from_sequence(object data)
cdef ArrayVector array_vector_from_parts(
    StringVector flat_child,
    int32_t* offsets,
    uint8_t* list_null_bitmap,
    Py_ssize_t num_rows
)
