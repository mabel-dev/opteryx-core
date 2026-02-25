from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer, DrakenType
from opteryx.draken.vectors.vector cimport Vector

cdef class IntegerVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef list to_pylist(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef IntegerVector from_arrow(object array)
